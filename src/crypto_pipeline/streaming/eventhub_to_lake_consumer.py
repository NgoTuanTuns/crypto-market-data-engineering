import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore
from azure.storage.filedatalake.aio import DataLakeServiceClient
from dotenv import load_dotenv

load_dotenv()

EVENT_HUB_CONNECTION_STR = os.getenv("EVENT_HUB_CONNECTION_STR_CONSUMER")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME")
CONSUMER_GROUP = os.getenv("EH_CONSUMER_GROUP", "$Default")

BLOB_STORAGE_CONNECTION_STRING = os.getenv("BLOB_STORAGE_CONNECTION_STRING")
CHECKPOINT_CONTAINER = os.getenv("CHECKPOINT_CONTAINER", "eventhub-checkpoints")

ADLS_ACCOUNT_NAME = os.getenv("ADLS_ACCOUNT_NAME")
ADLS_ACCOUNT_KEY = os.getenv("ADLS_ACCOUNT_KEY")
ADLS_FILESYSTEM = os.getenv("ADLS_FILESYSTEM", "source")

FLUSH_INTERVAL_SEC = 300 
LOCAL_TMP_DIR = Path("/tmp/candles")

buffer: list[dict] = []
buf_lock = asyncio.Lock()

async def on_event(partition_context, event):
    if event is None: 
        return
    try:
        candle = json.loads(event.body_as_str(encoding="UTF-8"))
    except (ValueError, UnicodeDecodeError) as e:
        print(f"[PARSE ERROR] partition={partition_context.partition_id} {e}", flush=True)
        return

    print(f"[RECEIVED] partition={partition_context.partition_id} symbol={candle.get('symbol')}", flush=True)

    async with buf_lock:
        buffer.append(candle)

    await partition_context.update_checkpoint(event)


async def on_partition_init(partition_context):
    print(f"[EH] Starting to read a partition {partition_context.partition_id}....", flush=True)


async def on_error(partition_context, error):
    pid = partition_context.partition_id if partition_context else "unknown"
    print(f"[EH ERROR] partition={pid} {error}", flush=True)

async def upload_rows_to_adls(rows: list[dict]):
    now = datetime.now(timezone.utc)
    out_dir = LOCAL_TMP_DIR / f"{now:%Y-%m-%d}"
    out_dir.mkdir(parents=True, exist_ok=True)
    local_path = out_dir / f"{now:%H%M%S}.jsonl"
    with open(local_path, "w") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")
    print(f"Have compressed {len(rows)} into {local_path}", flush=True)

    service_client = DataLakeServiceClient(
        account_url=f"https://{ADLS_ACCOUNT_NAME}.dfs.core.windows.net",
        credential=ADLS_ACCOUNT_KEY,
    )
    async with service_client:
        fs_client = service_client.get_file_system_client(ADLS_FILESYSTEM)
        try:
            await fs_client.create_file_system()
        except Exception as e:
            print(f"Filesystem might have an error: {e}", flush=True)

        remote_path = f"candles/{now:%Y-%m-%d}/{now:%H%M%S}.jsonl"
        file_client = fs_client.get_file_client(remote_path)
        with open(local_path, "rb") as f:
            data = f.read()
        await file_client.upload_data(data, overwrite=True)
        print(f"Uploaded successfully to adls://{ADLS_FILESYSTEM}/{remote_path}", flush=True)

    local_path.unlink()

async def flusher():
    while True:
        await asyncio.sleep(FLUSH_INTERVAL_SEC)
        async with buf_lock:
            if not buffer:
                continue
            rows = buffer.copy()
            buffer.clear()
        try:
            await upload_rows_to_adls(rows)
        except Exception as e:
            print(f"[UPLOAD ERROR] {e} — {len(rows)} lost", flush=True)


async def main():
    checkpoint_store = BlobCheckpointStore.from_connection_string(
        BLOB_STORAGE_CONNECTION_STRING, CHECKPOINT_CONTAINER
    )
    client = EventHubConsumerClient.from_connection_string(
        EVENT_HUB_CONNECTION_STR,
        consumer_group=CONSUMER_GROUP,
        eventhub_name=EVENT_HUB_NAME,
        checkpoint_store=checkpoint_store,
    )

    flush_task = asyncio.create_task(flusher())

    async with client:
        try:
            await client.receive(
                on_event=on_event,
                on_partition_initialize=on_partition_init,
                on_error=on_error,
                starting_position="-1",
            )
        finally:
            flush_task.cancel()


if __name__ == "__main__":
    asyncio.run(main())