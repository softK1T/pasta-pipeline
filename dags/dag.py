from datetime import timedelta
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from dotenv import dotenv_values

sys.path.append('/opt/airflow')
from processors.db_loader import load_messages_to_db, create_tables
from processors.message_scraper import run_scraper
from processors.telegraph_processor import get_links_to_process, process_telegraph_link_sync

# Load environment variables
config = dotenv_values(".env")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),  # Changed from 0 to 1 for better practice
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,  # Added 1 retry for resilience
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'pasta_pipeline',
        default_args=default_args,
        description='Pipeline for scraping Telegram messages and processing Telegraph links',
        schedule_interval=timedelta(days=1),
        catchup=False,
        max_active_runs=1,
        tags=['telegram', 'telegraph', 'scraping'],
        params={
            "api_id": config.get("API_ID"),
            "api_hash": config.get("API_HASH"),
            "phone_number": config.get("PHONE_NUMBER"),
            "all_messages": False,
            "channel_name": "@mrakopedia",
            "session_string": config.get("SESSION_STRING"),
        }
) as dag:
    create_tables_task = PythonOperator(
        task_id='create_tables',
        python_callable=create_tables,
    )

    scrape_task = PythonOperator(
        task_id='scrape_telegram_messages',
        python_callable=run_scraper,
    )

    load_db_task = PythonOperator(
        task_id='load_messages_to_db',
        python_callable=load_messages_to_db,
    )

    get_links_task = PythonOperator(
        task_id='get_telegraph_links',
        python_callable=get_links_to_process,
    )

    process_links_task = PythonOperator.partial(
        task_id='process_telegraph_link',
        python_callable=process_telegraph_link_sync,
    ).expand(op_args=get_links_task.output)

    create_tables_task >> scrape_task >> load_db_task >> get_links_task >> process_links_task
