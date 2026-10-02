from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import os
import json
from pathlib import Path
import time
from datetime import timedelta, datetime
from dotenv import load_dotenv
from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeStorageV2Hook
load_dotenv()

ALPHA_VANTAGE_API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY')
MIN_RELEVANCE = 0.15
SYMBOLS = ["BTC", "ETH", "BNB", "AVAX", "LTC"]
LOCAL_TMP_DIR = Path("/tmp/sentiment")
ADLS_FILESYSTEM = 'source'
ADLS_PATH_TEMPLATE = "sentiment/{ds}.jsonl"
ADLS_CONN_ID = 'adls2_connection'
default_args = {
    'owner':'tuns',
    'retries':5,
    'retry_delay':timedelta(minutes=2)
}

def sentiment_ingest(symbol):
    try:
        resp = requests.get(
            "https://www.alphavantage.co/query",
            params={
                "function": "NEWS_SENTIMENT",
                "tickers": f"CRYPTO:{symbol}",
                "apikey": ALPHA_VANTAGE_API_KEY,
                'limit':1000
            },
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        rows = []
        for article in data['feed']:
            for t in article['ticker_sentiment']:
                if t["ticker"] != f"CRYPTO:{symbol}":
                    continue                       
                if float(t["relevance_score"]) < MIN_RELEVANCE:
                    continue     
                rows.append({
                            'symbol':symbol,
                            'title':article['title'],
                            'relevance_score':t['relevance_score'],
                            "symbol": symbol,
                            "title": article.get("title"),
                            "url": article.get("url"),
                            "published_date": article.get("time_published"),
                            "overall_sentiment_score": article.get("overall_sentiment_score"),
                            "relevance_score": t["relevance_score"],
                            "ticker_sentiment_score": t["ticker_sentiment_score"],
                            "ticker_sentiment_label": t["ticker_sentiment_label"],
                            "source": article.get("source"),
                        }
                    )
        return rows
    except Exception as e:
        print(f'We got errors!: {e}')
        return []

def sentiment_ingest_all(ds, **context):
    try:
        all_rows = []
        for sym in SYMBOLS:
            rows = sentiment_ingest(sym)
            print(f"{sym}: {len(rows)} articles have sentiment_score >= {MIN_RELEVANCE}")
            all_rows.extend(rows)
            time.sleep(1)
        
        LOCAL_TMP_DIR.mkdir(parents=True, exist_ok=True)
        local_path = LOCAL_TMP_DIR / f"{ds}.jsonl"
        with open(local_path, "w") as f:
            for row in all_rows:
                f.write(json.dumps(row) + "\n")
        print(f"Have written {len(all_rows)} records into {local_path}")
    except Exception as e:
        print(f'We got errors!: {e}')


def upload_to_adls(ds, **context):
    local_path = LOCAL_TMP_DIR / f"{ds}.jsonl"
    if not local_path.exists():
        print(f"{local_path} not found, skip")
        return

    hook = AzureDataLakeStorageV2Hook(adls_conn_id = 'adls2_connection')

    try:
        hook.create_file_system(file_system_name=ADLS_FILESYSTEM)
    except Exception as e:
        print(f"Filesystem might have some errors: {e}")

    remote_path = ADLS_PATH_TEMPLATE.format(ds=ds)
    hook.upload_file(
        file_system_name=ADLS_FILESYSTEM,
        file_name=remote_path,
        file_path=str(local_path),
        overwrite = True
    )
    print(f"Uploaded to ADLS2 successfully!://{ADLS_FILESYSTEM}/{remote_path}")

    local_path.unlink()

with DAG(
    dag_id = 'sentiment_ingestion',
    default_args = default_args,
    start_date = datetime(2026,9,1),
    schedule = '@daily'
) as dag:
    task1 = PythonOperator(task_id = 'sentiment_ingestion_task', python_callable = sentiment_ingest_all)
    task2 = PythonOperator(task_id = 'upload_to_adls2', python_callable = upload_to_adls)
    task1 >> task2
