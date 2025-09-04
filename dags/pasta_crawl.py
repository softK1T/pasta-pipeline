import datetime
from datetime import timedelta
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import dotenv_values
from sqlalchemy_utils.types.enriched_datetime.pendulum_datetime import pendulum

sys.path.append('/opt/airflow')
from processors.db_loader import load_messages_to_db
from processors.message_scraper import run_scraper

config = dotenv_values(".env")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2025, 6, 16, 12, 0),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'pasta_crawl',
    default_args=default_args,
    description='Daily Telegram message crawling',
    schedule_interval=timedelta(days=1),
    catchup=False,
    params={
        "api_id": config.get("API_ID"),
        "api_hash": config.get("API_HASH"),
        "phone_number": config.get("PHONE_NUMBER"),
        "all_messages": False,
        "channel_name": config.get("CHANNEL_NAME", "@mrakopedia"),
        "session_string": config.get("SESSION_STRING"),
    }
) as dag:

    scrape_task = PythonOperator(
        task_id='scrape_telegram_messages',
        python_callable=run_scraper,
        dag=dag,
    )

    load_db_task = PythonOperator(
        task_id='load_messages_to_db',
        python_callable=load_messages_to_db,
        dag=dag,
    )

    scrape_task >> load_db_task