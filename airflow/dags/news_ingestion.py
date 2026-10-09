import requests
import os
from dotenv import load_dotenv
import json
from airflow import DAG
from pathlib import Path
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeStorageV2Hook

load_dotenv()
LOCAL_TMP_DIR = LOCAL_TMP_DIR = Path("/tmp/news")
ADLS_PATH_TEMPLATE = "news/{ds}.jsonl"
ADLS_FILESYSTEM = 'source'
FINNHUB_API_KEY = (os.getenv('FINNHUB_API_KEY') or "").strip()
CRYPTO_KEYWORDS = {
    "BTC": ["bitcoin", "btc"],
    "ETH": ["ethereum", "eth"],
    "BNB": ["bnb", "binance coin"],
    "AVAX": ["avalanche", "avax"],
    "LTC": ["litecoin", "ltc"],
}
default_args = {
    'owner':'tuns',
    'retries':5,
    'retry_delays': timedelta(minutes=2)
}

def news_ingestion(ds, logical_date, **context):
    try:
        r = requests.get(
                    "https://finnhub.io/api/v1/news",
                    params={"category": "crypto", "token": FINNHUB_API_KEY},
                    timeout=15,
                ).json()
        rows = []
        for i in r:
            text = f"{i.get('headline', '')} {i.get('summary', '')}".lower()
            matched = [k for k, vs in CRYPTO_KEYWORDS.items() if any(v in text for v in vs)]
            rows.append({**i, 'matched_symbols': matched})

        hour_str = logical_date.strftime("%H")
        out_dir = LOCAL_TMP_DIR / ds
        out_dir.mkdir(parents=True, exist_ok=True)
        local_path = out_dir / f"{hour_str}.jsonl"
        with open(local_path, "w") as f:
            for row in rows:
                f.write(json.dumps(row) + "\n")
        print(f"Have written {len(rows)} records into {local_path}")
    except Exception as e:
        print(f'We got errors!: {e}')


def upload_to_adls(ds, logical_date, **context):
    hour_str = logical_date.strftime("%H")
    local_path = LOCAL_TMP_DIR / ds / f"{hour_str}.jsonl"
    if not local_path.exists():
        print(f"{local_path} is not found, skip")
        return

    hook = AzureDataLakeStorageV2Hook(adls_conn_id='adls2_connection')
    try:
        hook.create_file_system(file_system_name=ADLS_FILESYSTEM)
    except Exception as e:
        print(f"Filesystem might have some errors: {e}")

    remote_path = f"news/{ds}_{hour_str}.jsonl"
    hook.upload_file(
        file_system_name=ADLS_FILESYSTEM,
        file_name=remote_path,
        file_path=str(local_path),
        overwrite=True,
    )
    print(f"Uploaded to ADLS2 successfully!://{ADLS_FILESYSTEM}/{remote_path}")
    local_path.unlink()


with DAG (
    dag_id = 'News_ingestion',
    default_args = default_args,
    start_date = datetime(2026, 9, 1),
    schedule = '@hourly'
) as dag:
    task1 = PythonOperator(task_id = 'news_ingestion', python_callable = news_ingestion)
    task2 = PythonOperator(task_id = 'upload_to_adls2', python_callable = upload_to_adls)
    task1 >> task2


