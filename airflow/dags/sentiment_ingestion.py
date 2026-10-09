import json
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeStorageV2Hook
from dotenv import load_dotenv

load_dotenv()

MARKETAUX_API_TOKEN = (os.getenv("MARKETAUX_API_TOKEN") or "").strip()
SYMBOL_MAP = {
    "BTC": "CC:BTC",
    "ETH": "CC:ETH",
    "BNB": "CC:BNB",
    "AVAX": "CC:AVAX",
    "LTC": "CC:LTC",
}

LOCAL_TMP_DIR = Path("/tmp/sentiment_marketaux")
ADLS_FILESYSTEM = "source"
ADLS_PATH_TEMPLATE = "sentiment/{ds}.jsonl"
ADLS_CONN_ID = "adls2_connection"

default_args = {
    "owner": "tuns",
    "retries": 5,
    "retry_delay": timedelta(minutes=2),
}

def fetch_sentiment_for(symbol: str, marketaux_symbol: str) -> list[dict]:
    resp = requests.get(
        "https://api.marketaux.com/v1/news/all",
        params={
            "symbols": marketaux_symbol,
            "entity_types": "cryptocurrency",
            "language": "en",
            "limit": 3,
            "api_token": MARKETAUX_API_TOKEN,
        },
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    if "data" not in data:
        raise ValueError(f"Marketaux got an error with {symbol}: {data}")

    rows = []
    for article in data["data"]:
        for entity in article.get("entities", []):
            if entity.get("symbol") != marketaux_symbol:
                continue
            rows.append({
                "symbol": symbol,                      
                "marketaux_symbol": marketaux_symbol,
                "title": article.get("title"),
                "description": article.get("description"),
                "url": article.get("url"),
                "published_date": article.get("published_at"),
                "source": article.get("source"),
                "entity_match_score": entity.get("match_score"),
                "entity_sentiment_score": entity.get("sentiment_score"),
            })
    return rows


def sentiment_ingest_all(ds, **context):
    all_rows = []
    for symbol, marketaux_symbol in SYMBOL_MAP.items():
        try:
            rows = fetch_sentiment_for(symbol, marketaux_symbol)
            print(f"{symbol}: {len(rows)} dòng")
            all_rows.extend(rows)
        except Exception as e:
            print(f"Got an error {symbol}: {e}")
        time.sleep(1)

    LOCAL_TMP_DIR.mkdir(parents=True, exist_ok=True)
    local_path = LOCAL_TMP_DIR / f"{ds}.jsonl"
    with open(local_path, "w") as f:
        for row in all_rows:
            f.write(json.dumps(row) + "\n")
    print(f"Have written {len(all_rows)} records into {local_path}")


def upload_to_adls(ds, **context):
    local_path = LOCAL_TMP_DIR / f"{ds}.jsonl"
    if not local_path.exists():
        print(f"{local_path} is not found, skip")
        return

    hook = AzureDataLakeStorageV2Hook(adls_conn_id=ADLS_CONN_ID)
    service_client = hook.get_conn()
    fs_client = service_client.get_file_system_client(ADLS_FILESYSTEM)
    try:
        fs_client.create_file_system()
    except Exception as e:
        print(f"Filesystem might have some errors: {e}")

    remote_path = ADLS_PATH_TEMPLATE.format(ds=ds)
    file_client = fs_client.get_file_client(remote_path)
    with open(local_path, "rb") as f:
        data = f.read()
    file_client.upload_data(data, overwrite=True)
    print(f"Uploaded to ADLS2 successfully!://{ADLS_FILESYSTEM}/{remote_path}")
    local_path.unlink()


with DAG(
    dag_id="sentiment_ingestion_marketaux",
    default_args=default_args,
    start_date=datetime(2026, 9, 1),
    schedule="@daily",
    catchup=False,
    tags=["crypto", "sentiment", "bronze"],
) as dag:
    task1 = PythonOperator(task_id="fetch_sentiment", python_callable=sentiment_ingest_all)
    task2 = PythonOperator(task_id="upload_to_adls", python_callable=upload_to_adls)
    task1 >> task2