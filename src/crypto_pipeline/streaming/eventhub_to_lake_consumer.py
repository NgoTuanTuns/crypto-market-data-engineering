import asyncio
import json
import os
import sys
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path

from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore
from azure.storage.filedatalake.aio import DataLakeServiceClient
from dotenv import load_dotenv

CONSUMER_GROUP = "$Default"
ADLS_FILESYSTEM = "source"
FLUSH_INTERVAL_SEC = 300
LOCAL_TMP_DIR = Path("/tmp/candles")


class CandleBatchBuffer:
    """Async batch buffer ensuring Event Hub checkpoints advance only after upload succeeds."""

    def __init__(self, upload_fn: Callable[[list[dict]], Awaitable[None]]):
        self.upload_fn = upload_fn
        self.buffer: list[dict] = []
        self.pending: dict[str, tuple] = {}
        self.lock = asyncio.Lock()

    async def add_event(self, partition_context, event) -> bool:
        """Parse an incoming Event Hub candle event and stage it for the next flush."""
        if event is None:
            return False
        try:
            candle = json.loads(event.body_as_str(encoding="UTF-8"))
        except (ValueError, UnicodeDecodeError) as e:
            print(f"[PARSE ERROR] partition={partition_context.partition_id} {e}", flush=True)
            return False

        print(
            f"[RECEIVED] partition={partition_context.partition_id} symbol={candle.get('symbol')}",
            flush=True,
        )
        async with self.lock:
            self.buffer.append(candle)
            self.pending[partition_context.partition_id] = (partition_context, event)
        return True

    async def flush_once(self) -> bool:
        """Upload staged rows and checkpoint partitions only after upload completes."""
        async with self.lock:
            if not self.buffer:
                return False
            rows = self.buffer.copy()
            to_commit = dict(self.pending)
            self.buffer.clear()
            self.pending.clear()

        try:
            await self.upload_fn(rows)
        except Exception as e:
            async with self.lock:
                self.buffer[:0] = rows
                for pid, ctx_ev in to_commit.items():
                    self.pending.setdefault(pid, ctx_ev)
            print(f"[UPLOAD ERROR] {e}", flush=True)
            return False

        for ctx, ev in to_commit.values():
            await ctx.update_checkpoint(ev)
        return True


def _write_jsonl_file(local_path: Path, rows: list[dict]) -> None:
    local_path.parent.mkdir(parents=True, exist_ok=True)
    with open(local_path, "w", encoding="utf-8") as f:
        f.writelines(json.dumps(row) + "\n" for row in rows)
    print(f"Have compressed {len(rows)} into {local_path}", flush=True)


async def upload_rows_to_adls(
    rows: list[dict],
    adls_account_name: str,
    adls_account_key: str,
    adls_filesystem: str = ADLS_FILESYSTEM,
    local_tmp_dir: Path = LOCAL_TMP_DIR,
) -> None:
    now = datetime.now(UTC)
    local_path = local_tmp_dir / f"{now:%Y-%m-%d}" / f"{now:%H%M%S}.jsonl"
    await asyncio.to_thread(_write_jsonl_file, local_path, rows)

    service_client = DataLakeServiceClient(
        account_url=f"https://{adls_account_name}.dfs.core.windows.net",
        credential=adls_account_key,
    )
    try:
        async with service_client:
            fs_client = service_client.get_file_system_client(adls_filesystem)
            try:
                await fs_client.create_file_system()
            except Exception as e:
                print(f"Filesystem might have an error: {e}", flush=True)

            remote_path = f"candles/{now:%Y-%m-%d}_{now:%H%M%S}.jsonl"
            file_client = fs_client.get_file_client(remote_path)
            data = await asyncio.to_thread(local_path.read_bytes)
            await file_client.upload_data(data, overwrite=True)
            print(f"Uploaded successfully to adls://{adls_filesystem}/{remote_path}", flush=True)
    finally:
        await asyncio.to_thread(local_path.unlink, missing_ok=True)


async def on_partition_init(partition_context):
    print(f"[EH] Starting to read a partition {partition_context.partition_id}....", flush=True)


async def on_error(partition_context, error):
    pid = partition_context.partition_id if partition_context else "unknown"
    print(f"[EH ERROR] partition={pid} {error}", flush=True)


async def main():
    load_dotenv()
    eh_conn = (os.getenv("EVENT_HUB_CONNECTION_STR_CONSUMER") or "").strip()
    eh_name = os.getenv("EVENT_HUB_NAME")
    blob_conn = (os.getenv("BLOB_STORAGE_CONNECTION_STRING") or "").strip()
    checkpoint_container = os.getenv("CHECKPOINT_CONTAINER", "eventhub-checkpoints")
    adls_account_name = (os.getenv("ADLS_ACCOUNT_NAME") or "").strip()
    adls_account_key = (os.getenv("ADLS_ACCOUNT_KEY") or "").strip()

    if not all([eh_conn, eh_name, blob_conn, adls_account_name, adls_account_key]):
        sys.exit("Missing required Event Hub, Blob checkpoint, or ADLS environment variables")

    batcher = CandleBatchBuffer(
        upload_fn=lambda rows: upload_rows_to_adls(rows, adls_account_name, adls_account_key)
    )

    async def flusher():
        while True:
            await asyncio.sleep(FLUSH_INTERVAL_SEC)
            await batcher.flush_once()

    checkpoint_store = BlobCheckpointStore.from_connection_string(blob_conn, checkpoint_container)
    client = EventHubConsumerClient.from_connection_string(
        eh_conn,
        consumer_group=CONSUMER_GROUP,
        eventhub_name=eh_name,
        checkpoint_store=checkpoint_store,
    )

    flush_task = asyncio.create_task(flusher())
    async with client:
        try:
            await client.receive(
                on_event=batcher.add_event,
                on_partition_initialize=on_partition_init,
                on_error=on_error,
                starting_position="-1",
            )
        finally:
            flush_task.cancel()


if __name__ == "__main__":
    asyncio.run(main())
