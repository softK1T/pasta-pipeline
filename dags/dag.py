import datetime
from datetime import timedelta
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from dotenv import dotenv_values
from sqlalchemy_utils.types.enriched_datetime.pendulum_datetime import pendulum

sys.path.append('/opt/airflow')
from processors.db_loader import load_messages_to_db
from processors.message_scraper import run_scraper
from processors.telegraph_processor import get_links_to_process, process_telegraph_link_sync

config = dotenv_values(".env")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2025, 6, 16, 12, 0),  # Fixed at UTC+2 14:00
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'pasta_pipeline',
        default_args=default_args,
        description='Pasta pipeline',
        schedule_interval=timedelta(days=1),
        catchup=False,
        params={
            "api_id": config.get("API_ID"),
            "api_hash": config.get("API_HASH"),
            "phone_number": config.get("PHONE_NUMBER"),
            "all_messages": False,
            "channel_name": "@mrakopedia",
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

    get_links_task = PythonOperator(
        task_id='get_telegraph_links',
        python_callable=get_links_to_process,
        dag=dag,
    )

    process_links_task = PythonOperator.partial(
        task_id='process_telegraph_link',
        python_callable=process_telegraph_link_sync,
        dag=dag,
    ).expand(op_args=get_links_task.output)

    scrape_task >> load_db_task >> get_links_task >> process_links_task
