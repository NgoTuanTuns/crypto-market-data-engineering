import json
import os
import sys
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
import websocket
from azure.eventhub import EventData, EventHubProducerClient
from dotenv import load_dotenv

load_dotenv()

PARTITION_IDS = {'BINANCE:BTCUSDT':'0', 'BINANCE:ETHUSDT':'1', 'BINANCE:BNBUSDT':'2', "BINANCE:AVAXUSDT":'3', "BINANCE:LTCUSDT":"4"}
FINNHUB_KEY = (os.getenv("FINNHUB_API_KEY") or "").strip()
EH_CONN = (os.getenv("EVENT_HUB_CONNECTION_STR") or "").strip()
if not FINNHUB_KEY or not EH_CONN:
    sys.exit("Thiếu FINNHUB_API_KEY hoặc EVENTHUB_CONNECTION_STRING")

SYMBOLS = [
    "BINANCE:BTCUSDT",
    "BINANCE:ETHUSDT",
    "BINANCE:BNBUSDT",
    "BINANCE:AVAXUSDT",
    "BINANCE:LTCUSDT"
]
EH_CANDLES = os.getenv("EVENT_HUB_NAME")
GRACE_MS = 2000

def on_success(events, partition_id):
    pass

def on_error(events, partition_id, error):
    print(f"[EH ERROR] partition={partition_id} events={len(events)} {error}", flush=True)

def make_producer(hub: str) -> EventHubProducerClient:
    return EventHubProducerClient.from_connection_string(
        EH_CONN,
        eventhub_name=hub,
        buffered_mode=True,
        on_success=on_success,
        on_error=on_error,
        max_buffer_length=5000,
        max_wait_time=1,
    )


candle_producer = make_producer(EH_CANDLES)
send_lock = threading.Lock()

def send(producer: EventHubProducerClient, key: str, value: dict):
    body = json.dumps(value, separators=(",", ":"))
    with send_lock:
        producer.send_event(EventData(body), partition_id=PARTITION_IDS[key])

buckets = defaultdict(dict)
last_flushed = 0
lock = threading.Lock()

def add_tick(t: dict):
    ts, sym, p, v = t["t"], t["s"], t["p"], t["v"]
    minute = ts // 60_000 * 60_000
    with lock:
        if minute <= last_flushed:
            return
        c = buckets[minute].get(sym)
        if c is None:
            buckets[minute][sym] = {"open": p, "high": p, "low": p, "close": p, "volume": v, "trades": 1, "_t_first": ts, "_t_last": ts}
            return
        if ts < c["_t_first"]:
            c["open"], c["_t_first"] = p, ts
        if ts >= c["_t_last"]:
            c["close"], c["_t_last"] = p, ts
        c["high"] = max(c["high"], p)
        c["low"] = min(c["low"], p)
        c["volume"] += v
        c["trades"] += 1


def publish_candle(minute_ms: int, sym: str, c: dict):
    send(candle_producer, sym, {
        "source": "finnhub", "symbol": sym, "interval": "1m", "minute_ms": minute_ms,
        "open": c["open"], "high": c["high"], "low": c["low"], "close": c["close"],
        "volume": c["volume"], "trades": c["trades"],
    })
    t = datetime.fromtimestamp(minute_ms / 1000, tz=timezone.utc)
    print(f'[CANDLE] {t:%H:%M} UTC {sym:<16} C={c["close"]} V={c["volume"]} n={c["trades"]}', flush=True)

def flusher():
    global last_flushed
    while True:
        now_ms = int(time.time() * 1000)
        with lock:
            ready = sorted(m for m in list(buckets) if m + 60_000 + GRACE_MS <= now_ms)
            closed = {m: buckets.pop(m) for m in ready}
            if ready:
                last_flushed = ready[-1]
        for m in ready:
            for sym, c in closed[m].items():
                publish_candle(m, sym, c)
        time.sleep(0.5)

def handle_trade(t: dict):
    add_tick(t)

def on_open(ws):
    print("[WS] Connected, subscribe:", SYMBOLS, flush=True)
    for s in SYMBOLS:
        ws.send(json.dumps({"type": "subscribe", "symbol": s}))

def on_message(ws, message):
    msg = json.loads(message)
    if msg.get("type") == "trade":
        for t in msg["data"]:
            handle_trade(t)
    elif msg.get("type") != "ping":
        print("[WS] Other message:", msg, flush=True)

def on_ws_error(ws, error):
    print("[WS ERROR]", error, flush=True)

def on_close(ws, code, reason):
    print("[WS CLOSED]", code, reason, flush=True)

if __name__ == "__main__":
    threading.Thread(target=flusher, daemon=True).start()
    try:
        while True:
            websocket.WebSocketApp(
                f"wss://ws.finnhub.io?token={FINNHUB_KEY}",
                on_open=on_open, on_message=on_message,
                on_error=on_ws_error, on_close=on_close,
            ).run_forever(ping_interval=20, ping_timeout=10)
            print("[WS] Lost connection, try again in 5 minutes", flush=True)
            time.sleep(5)
    except KeyboardInterrupt:
        pass
    finally:
        print("Flushing and closing producer", flush=True)
        candle_producer.close()