from crypto_pipeline.ingestion.finnhub_crypto_producer import (
    CandleAggregator,
    build_candle_payload,
)

M0 = 1_762_830_900_000  # 2025-11-11 03:15:00 UTC in ms
M1 = M0 + 60_000


def _tick(ts: int, sym: str, price: float, vol: float) -> dict:
    return {"t": ts, "s": sym, "p": price, "v": vol}


def test_aggregates_multiple_ticks_in_same_minute():
    agg = CandleAggregator(grace_ms=2000)

    assert agg.add_tick(_tick(M0 + 5_000, "BINANCE:BTCUSDT", 100.0, 0.5)) is True
    assert agg.add_tick(_tick(M0 + 20_000, "BINANCE:BTCUSDT", 110.0, 1.25)) is True
    assert agg.add_tick(_tick(M0 + 40_000, "BINANCE:BTCUSDT", 95.0, 0.25)) is True
    assert agg.add_tick(_tick(M0 + 55_000, "BINANCE:BTCUSDT", 105.0, 1.0)) is True

    ready = agg.pop_ready(M0 + 60_000 + 2000)
    assert len(ready) == 1
    minute_ms, candles = ready[0]
    assert minute_ms == M0

    btc = candles["BINANCE:BTCUSDT"]
    assert btc["open"] == 100.0
    assert btc["high"] == 110.0
    assert btc["low"] == 95.0
    assert btc["close"] == 105.0
    assert btc["volume"] == 3.0
    assert btc["trades"] == 4


def test_out_of_order_ticks_preserve_first_open_and_last_close():
    agg = CandleAggregator(grace_ms=2000)

    agg.add_tick(_tick(M0 + 30_000, "BINANCE:ETHUSDT", 200.0, 1.0))
    agg.add_tick(_tick(M0 + 10_000, "BINANCE:ETHUSDT", 190.0, 2.0))
    agg.add_tick(_tick(M0 + 50_000, "BINANCE:ETHUSDT", 210.0, 1.5))
    # Same latest timestamp should update close via >= _t_last
    agg.add_tick(_tick(M0 + 50_000, "BINANCE:ETHUSDT", 205.0, 0.5))

    _, candles = agg.pop_ready(M0 + 62_000)[0]
    eth = candles["BINANCE:ETHUSDT"]
    assert eth["open"] == 190.0
    assert eth["close"] == 205.0
    assert eth["high"] == 210.0
    assert eth["low"] == 190.0
    assert eth["volume"] == 5.0
    assert eth["trades"] == 4


def test_pop_ready_respects_grace_window_and_separates_minutes():
    agg = CandleAggregator(grace_ms=2000)

    agg.add_tick(_tick(M0 + 1_000, "BINANCE:BTCUSDT", 100.0, 1.0))
    agg.add_tick(_tick(M0 + 2_000, "BINANCE:ETHUSDT", 200.0, 2.0))
    agg.add_tick(_tick(M1 + 1_000, "BINANCE:BTCUSDT", 101.0, 1.0))

    # 1 ms before M0 grace ends -> nothing ready
    assert agg.pop_ready(M0 + 61_999) == []

    # At M0 + 60s + grace -> only M0 pops; M1 remains staged
    ready = agg.pop_ready(M0 + 62_000)
    assert [m for m, _ in ready] == [M0]
    assert set(ready[0][1].keys()) == {"BINANCE:BTCUSDT", "BINANCE:ETHUSDT"}
    assert list(agg.buckets.keys()) == [M1]


def test_late_ticks_after_flush_are_dropped_and_counted():
    agg = CandleAggregator(grace_ms=2000)

    agg.add_tick(_tick(M0 + 10_000, "BINANCE:BTCUSDT", 100.0, 1.0))
    agg.pop_ready(M0 + 62_000)
    assert agg.last_flushed == M0

    # Tick belonging to already-flushed minute M0 or earlier is dropped
    assert agg.add_tick(_tick(M0 + 59_000, "BINANCE:BTCUSDT", 99.0, 5.0)) is False
    assert agg.add_tick(_tick(M0 - 1_000, "BINANCE:BTCUSDT", 98.0, 5.0)) is False
    assert agg.late_ticks_dropped == 2
    assert M0 not in agg.buckets

    # Future minute M1 is still accepted
    assert agg.add_tick(_tick(M1 + 5_000, "BINANCE:BTCUSDT", 102.0, 1.0)) is True
    assert agg.late_ticks_dropped == 2


def test_build_candle_payload_matches_bronze_schema():
    candle = {
        "open": 100.0,
        "high": 105.0,
        "low": 99.0,
        "close": 102.5,
        "volume": 12.34,
        "trades": 7,
    }
    payload = build_candle_payload(M0, "BINANCE:BTCUSDT", candle)
    assert payload == {
        "source": "finnhub",
        "symbol": "BINANCE:BTCUSDT",
        "interval": "1m",
        "minute_ms": M0,
        "open": 100.0,
        "high": 105.0,
        "low": 99.0,
        "close": 102.5,
        "volume": 12.34,
        "trades": 7,
    }
