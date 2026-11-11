import json
import os
import sys
import threading
import time
from collections import defaultdict
from datetime import UTC, datetime

import websocket
from azure.eventhub import EventData, EventHubProducerClient
from dotenv import load_dotenv

PARTITION_IDS = {
    "BINANCE:BTCUSDT": "0",
    "BINANCE:ETHUSDT": "1",
    "BINANCE:BNBUSDT": "2",
    "BINANCE:AVAXUSDT": "3",
    "BINANCE:LTCUSDT": "4",
}
SYMBOLS = [
    "BINANCE:BTCUSDT",
    "BINANCE:ETHUSDT",
    "BINANCE:BNBUSDT",
    "BINANCE:AVAXUSDT",
    "BINANCE:LTCUSDT",
]
GRACE_MS = 2000


class CandleAggregator:
    """Thread-safe 1-minute OHLCV candle aggregator with late-tick grace window."""

    def __init__(self, grace_ms: int = GRACE_MS):
        self.grace_ms = grace_ms
        self.buckets: dict[int, dict[str, dict]] = defaultdict(dict)
        self.last_flushed = 0
        self.lock = threading.Lock()
        self.late_ticks_dropped = 0

    def add_tick(self, tick: dict) -> bool:
        """Merge a trade tick into its 1-minute candle; return False if dropped as late."""
        ts, sym, price, vol = tick["t"], tick["s"], tick["p"], tick["v"]
        minute = ts // 60_000 * 60_000
        with self.lock:
            if minute <= self.last_flushed:
                self.late_ticks_dropped += 1
                return False
            candle = self.buckets[minute].get(sym)
            if candle is None:
                self.buckets[minute][sym] = {
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "volume": vol,
                    "trades": 1,
                    "_t_first": ts,
                    "_t_last": ts,
                }
                return True
            if ts < candle["_t_first"]:
                candle["open"], candle["_t_first"] = price, ts
            if ts >= candle["_t_last"]:
                candle["close"], candle["_t_last"] = price, ts
            candle["high"] = max(candle["high"], price)
            candle["low"] = min(candle["low"], price)
            candle["volume"] += vol
            candle["trades"] += 1
            return True

    def pop_ready(self, now_ms: int) -> list[tuple[int, dict[str, dict]]]:
        """Pop and advance last_flushed for all minutes past the 60s + grace window."""
        with self.lock:
            ready = sorted(m for m in list(self.buckets) if m + 60_000 + self.grace_ms <= now_ms)
            closed = [(m, self.buckets.pop(m)) for m in ready]
            if ready:
                self.last_flushed = ready[-1]
            return closed


def on_success(events, partition_id):
    pass


def on_error(events, partition_id, error):
    print(f"[EH ERROR] partition={partition_id} events={len(events)} {error}", flush=True)


def make_producer(eh_conn: str, hub: str | None) -> EventHubProducerClient:
    return EventHubProducerClient.from_connection_string(
        eh_conn,
        eventhub_name=hub,
        buffered_mode=True,
        on_success=on_success,
        on_error=on_error,
        max_buffer_length=5000,
        max_wait_time=1,
    )


def build_candle_payload(minute_ms: int, sym: str, candle: dict) -> dict:
    return {
        "source": "finnhub",
        "symbol": sym,
        "interval": "1m",
        "minute_ms": minute_ms,
        "open": candle["open"],
        "high": candle["high"],
        "low": candle["low"],
        "close": candle["close"],
        "volume": candle["volume"],
        "trades": candle["trades"],
    }


def send(producer: EventHubProducerClient, send_lock: threading.Lock, key: str, value: dict):
    body = json.dumps(value, separators=(",", ":"))
    with send_lock:
        producer.send_event(EventData(body), partition_id=PARTITION_IDS[key])


def publish_candle(
    producer: EventHubProducerClient,
    send_lock: threading.Lock,
    minute_ms: int,
    sym: str,
    candle: dict,
):
    send(producer, send_lock, sym, build_candle_payload(minute_ms, sym, candle))
    ts = datetime.fromtimestamp(minute_ms / 1000, tz=UTC)
    print(
        f"[CANDLE] {ts:%H:%M} UTC {sym:<16} C={candle['close']} V={candle['volume']} n={candle['trades']}",
        flush=True,
    )


def flusher(
    aggregator: CandleAggregator,
    producer: EventHubProducerClient,
    send_lock: threading.Lock,
):
    while True:
        now_ms = int(time.time() * 1000)
        for minute_ms, candles in aggregator.pop_ready(now_ms):
            for sym, candle in candles.items():
                publish_candle(producer, send_lock, minute_ms, sym, candle)
        time.sleep(0.5)


def main():
    load_dotenv()
    finnhub_key = (os.getenv("FINNHUB_API_KEY") or "").strip()
    eh_conn = (os.getenv("EVENT_HUB_CONNECTION_STR_PRODUCER") or "").strip()
    if not finnhub_key or not eh_conn:
        sys.exit("FINNHUB_API_KEY or EVENTHUB_CONNECTION_STRING were not found")

    eh_candles = os.getenv("EVENT_HUB_NAME")
    aggregator = CandleAggregator(grace_ms=GRACE_MS)
    send_lock = threading.Lock()
    candle_producer = make_producer(eh_conn, eh_candles)

    def on_open(ws):
        print("[WS] Connected, subscribe:", SYMBOLS, flush=True)
        for sym in SYMBOLS:
            ws.send(json.dumps({"type": "subscribe", "symbol": sym}))

    def on_message(ws, message):
        msg = json.loads(message)
        if msg.get("type") == "trade":
            for tick in msg["data"]:
                aggregator.add_tick(tick)
        elif msg.get("type") != "ping":
            print("[WS] Other message:", msg, flush=True)

    def on_ws_error(ws, error):
        print("[WS ERROR]", error, flush=True)

    def on_close(ws, code, reason):
        print("[WS CLOSED]", code, reason, flush=True)

    threading.Thread(
        target=flusher, args=(aggregator, candle_producer, send_lock), daemon=True
    ).start()
    try:
        while True:
            websocket.WebSocketApp(
                f"wss://ws.finnhub.io?token={finnhub_key}",
                on_open=on_open,
                on_message=on_message,
                on_error=on_ws_error,
                on_close=on_close,
            ).run_forever(ping_interval=20, ping_timeout=10)
            print("[WS] Lost connection, try again in 5 seconds", flush=True)
            time.sleep(5)
    except KeyboardInterrupt:
        pass
    finally:
        print(
            f"Flushing and closing producer (late_ticks_dropped={aggregator.late_ticks_dropped})",
            flush=True,
        )
        candle_producer.close()


if __name__ == "__main__":
    main()
