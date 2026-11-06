import asyncio
from datetime import datetime
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import yfinance as yf

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==================== CONFIGURATION ====================
WATCHLIST = {
    "RELIANCE.NS": "Reliance",
    "TCS.NS": "T C S",
    "SBIN.NS": "State Bank of India",
    "KITEX.NS": "Kitex Garments",
    "HDFCBANK.NS": "H D F C Bank",
    "INFY.NS": "Infosys",
    "ONGC.NS": "O N G C"
}

POLL_INTERVAL = 30
MIN_CHANGE_TO_ALERT = 0.50
# =========================================================

connected_clients = set()
last_prices = {}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.add(websocket)
    print(f"[+] Client connected. Total: {len(connected_clients)}")

    # Immediately send whatever prices we already know, even if market is closed
    try:
        timestamp = datetime.now().strftime("%I:%M:%S %p")
        for symbol, price in last_prices.items():
            name = WATCHLIST.get(symbol, symbol)
            snapshot_msg = {
                "type": "snapshot",
                "name": name,
                "price": price,
                "time": timestamp,
                "fetched_at": timestamp,
                "text": f"{name} is currently at {price} rupees."
            }
            await websocket.send_json(snapshot_msg)
    except Exception as e:
        print(f"Snapshot send error: {e}")

    try:
        while True:
            await asyncio.sleep(30)
    except WebSocketDisconnect:
        connected_clients.discard(websocket)
        print(f"[-] Client disconnected. Total: {len(connected_clients)}")

async def broadcast(data: dict):
    dead = []
    for client in connected_clients:
        try:
            await client.send_json(data)
        except Exception:
            dead.append(client)
    for d in dead:
        connected_clients.discard(d)

def fetch_prices():
    """Fetch latest prices for each symbol independently, so one failure doesn't block others."""
    results = {}
    for symbol, name in WATCHLIST.items():
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.fast_info
            price = info.get("lastPrice") or info.get("last_price")
            if price:
                results[symbol] = {"name": name, "price": round(float(price), 2)}
        except Exception as e:
            print(f"[WARN] Could not fetch {symbol}: {e}")
    return results

async def price_polling_loop():
    """Background task: runs forever, checks prices, broadcasts changes, and reports sync health."""
    global last_prices
    print("Price polling loop started.")
    while True:
        fetch_started_at = datetime.now()
        try:
            current = fetch_prices()
            timestamp = fetch_started_at.strftime("%I:%M:%S %p")

            for symbol, data in current.items():
                name = data["name"]
                price = data["price"]
                prev = last_prices.get(symbol)

                if prev is None:
                    message = {
                        "type": "baseline",
                        "name": name,
                        "price": price,
                        "time": timestamp,
                        "fetched_at": timestamp,
                        "text": f"Monitoring started for {name} at {price} rupees."
                    }
                    await broadcast(message)
                else:
                    diff = round(price - prev, 2)
                    if abs(diff) >= MIN_CHANGE_TO_ALERT:
                        direction = "up" if diff > 0 else "down"
                        message = {
                            "type": "update",
                            "name": name,
                            "price": price,
                            "change": diff,
                            "direction": direction,
                            "time": timestamp,
                            "fetched_at": timestamp,
                            "text": f"{name} is {direction} by {abs(diff)} rupees, now at {price} rupees."
                        }
                        await broadcast(message)

                last_prices[symbol] = price

            # ---- Always send a lightweight SYNC heartbeat, even if no alert fired ----
            fetch_duration = (datetime.now() - fetch_started_at).total_seconds()
            sync_payload = {
                "type": "sync",
                "server_time": timestamp,
                "fetch_duration": round(fetch_duration, 2),
                "prices": {
                    WATCHLIST[symbol]: data["price"]
                    for symbol, data in current.items()
                }
            }
            await broadcast(sync_payload)

            print(f"[{timestamp}] Fetch cycle completed in {fetch_duration:.2f}s for {len(current)} symbol(s).")

        except Exception as e:
            print(f"Polling loop error: {e}")

        await asyncio.sleep(POLL_INTERVAL)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(price_polling_loop())

# Serve the frontend (HTML/JS files) at the root URL
app.mount("/", StaticFiles(directory="frontend", html=True), name="frontend")
