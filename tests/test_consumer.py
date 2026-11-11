import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

from crypto_pipeline.streaming.eventhub_to_lake_consumer import (
    CandleBatchBuffer,
    _write_jsonl_file,
)


def _make_event(payload: str | bytes):
    def _body_as_str(encoding: str = "UTF-8") -> str:
        if isinstance(payload, bytes):
            return payload.decode(encoding)
        return payload

    return SimpleNamespace(body_as_str=_body_as_str)


def _make_ctx(partition_id: str):
    return SimpleNamespace(
        partition_id=partition_id,
        update_checkpoint=AsyncMock(),
    )


def test_add_event_ignores_none_and_invalid_payloads():
    async def scenario():
        batcher = CandleBatchBuffer(upload_fn=AsyncMock())
        ctx = _make_ctx("0")

        assert await batcher.add_event(ctx, None) is False
        assert await batcher.add_event(ctx, _make_event("{not-json")) is False
        assert await batcher.add_event(ctx, _make_event(b"\xff\xfe")) is False
        assert batcher.buffer == []
        assert batcher.pending == {}

    asyncio.run(scenario())


def test_add_event_stages_rows_and_tracks_latest_event_per_partition():
    async def scenario():
        batcher = CandleBatchBuffer(upload_fn=AsyncMock())
        ctx0 = _make_ctx("0")
        ctx1 = _make_ctx("1")
        ev0_first = _make_event(json.dumps({"symbol": "BINANCE:BTCUSDT", "minute_ms": 1}))
        ev0_second = _make_event(json.dumps({"symbol": "BINANCE:BTCUSDT", "minute_ms": 2}))
        ev1 = _make_event(json.dumps({"symbol": "BINANCE:ETHUSDT", "minute_ms": 1}))

        assert await batcher.add_event(ctx0, ev0_first) is True
        assert await batcher.add_event(ctx0, ev0_second) is True
        assert await batcher.add_event(ctx1, ev1) is True

        assert len(batcher.buffer) == 3
        assert batcher.pending["0"] == (ctx0, ev0_second)
        assert batcher.pending["1"] == (ctx1, ev1)

    asyncio.run(scenario())


def test_flush_once_empty_skips_upload():
    async def scenario():
        upload = AsyncMock()
        batcher = CandleBatchBuffer(upload_fn=upload)

        assert await batcher.flush_once() is False
        upload.assert_not_awaited()

    asyncio.run(scenario())


def test_flush_once_checkpoints_only_after_upload_succeeds():
    async def scenario():
        call_order: list[str] = []

        async def upload(rows):
            call_order.append(f"upload:{len(rows)}")

        batcher = CandleBatchBuffer(upload_fn=upload)
        ctx0 = SimpleNamespace(
            partition_id="0",
            update_checkpoint=AsyncMock(side_effect=lambda _ev: call_order.append("ckpt:0")),
        )
        ctx1 = SimpleNamespace(
            partition_id="1",
            update_checkpoint=AsyncMock(side_effect=lambda _ev: call_order.append("ckpt:1")),
        )
        ev0 = _make_event(json.dumps({"symbol": "BINANCE:BTCUSDT", "minute_ms": 1}))
        ev1 = _make_event(json.dumps({"symbol": "BINANCE:ETHUSDT", "minute_ms": 1}))

        await batcher.add_event(ctx0, ev0)
        await batcher.add_event(ctx1, ev1)

        assert await batcher.flush_once() is True
        assert call_order[0] == "upload:2"
        assert set(call_order[1:]) == {"ckpt:0", "ckpt:1"}
        ctx0.update_checkpoint.assert_awaited_once_with(ev0)
        ctx1.update_checkpoint.assert_awaited_once_with(ev1)
        assert batcher.buffer == []
        assert batcher.pending == {}

    asyncio.run(scenario())


def test_flush_once_on_upload_error_restores_buffer_and_skips_checkpoint():
    async def scenario():
        upload = AsyncMock(side_effect=RuntimeError("ADLS unreachable"))
        batcher = CandleBatchBuffer(upload_fn=upload)
        ctx0 = _make_ctx("0")
        ev0 = _make_event(json.dumps({"symbol": "BINANCE:BTCUSDT", "minute_ms": 1}))

        await batcher.add_event(ctx0, ev0)

        assert await batcher.flush_once() is False
        ctx0.update_checkpoint.assert_not_awaited()
        assert batcher.buffer == [{"symbol": "BINANCE:BTCUSDT", "minute_ms": 1}]
        assert batcher.pending["0"] == (ctx0, ev0)

        # Retry succeeds on next flush
        batcher.upload_fn = AsyncMock()
        assert await batcher.flush_once() is True
        ctx0.update_checkpoint.assert_awaited_once_with(ev0)
        assert batcher.buffer == []

    asyncio.run(scenario())


def test_flush_once_failure_preserves_events_arriving_mid_upload():
    async def scenario():
        ctx0 = _make_ctx("0")
        ev_first = _make_event(json.dumps({"symbol": "BINANCE:BTCUSDT", "minute_ms": 1}))
        ev_new = _make_event(json.dumps({"symbol": "BINANCE:BTCUSDT", "minute_ms": 2}))

        async def failing_upload(_rows):
            # Simulate a newer event arriving while upload is in flight before failing
            await batcher.add_event(ctx0, ev_new)
            raise RuntimeError("transient network error")

        batcher = CandleBatchBuffer(upload_fn=failing_upload)
        await batcher.add_event(ctx0, ev_first)

        assert await batcher.flush_once() is False
        ctx0.update_checkpoint.assert_not_awaited()
        assert [r["minute_ms"] for r in batcher.buffer] == [1, 2]
        assert batcher.pending["0"] == (ctx0, ev_new)

    asyncio.run(scenario())


def test_write_jsonl_file_creates_parent_and_writes_lines(tmp_path):
    target = tmp_path / "2026-11-11" / "031500.jsonl"
    rows = [{"symbol": "BTC", "close": 100}, {"symbol": "ETH", "close": 200}]

    _write_jsonl_file(target, rows)

    lines = target.read_text(encoding="utf-8").splitlines()
    assert [json.loads(line) for line in lines] == rows
