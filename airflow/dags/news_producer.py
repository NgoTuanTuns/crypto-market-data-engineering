import requests
import os
from dotenv import load_dotenv
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

load_dotenv()

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
FINNHUB_API_KEY = os.getenv('FINNHUB_API_KEY')

def send_news():
    try:
        r = requests.get(
                    "https://finnhub.io/api/v1/news",
                    params={"category": "crypto", "token": FINNHUB_API_KEY},
                    timeout=15,
                ).json()
        row = []
        for i in r:
            text = f"{i.get('headline', '')} {i.get('summary', '')}".lower()
            matched = [k for k, vs in CRYPTO_KEYWORDS.items() if any(v in text for v in vs)]
            row.append({**i, 'matched_symbols':matched})
        print(row)
    except Exception as e:
        print('We got errors!:' + e)


with DAG (
    dag_id = 'Ingestion_news',
    default_args = default_args,
    start_date = datetime(2026, 9, 1),
    schedule = '@hourly'
) as dag:
    task1 = PythonOperator(task_id = 'Sending_task', python_callable = send_news)


