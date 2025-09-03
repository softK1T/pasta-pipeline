import datetime
import logging
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
from processors.duplicate_remover import run_full_cleanup, get_cleanup_statistics
from configs.config import get_config, get_telegram_config, get_database_config

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load configuration
try:
    config = get_config()
    telegram_config = get_telegram_config()
    db_config = get_database_config()
except Exception as e:
    logger.error(f"Failed to load configuration: {str(e)}")
    # Fallback to environment variables
    config = dotenv_values(".env")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2025, 6, 16, 12, 0),  # Fixed at UTC+2 14:00
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
}

# Main pipeline DAG
with DAG(
        'pasta_pipeline',
        default_args=default_args,
        description='Pasta pipeline - Telegram & Telegraph ETL',
        schedule_interval=timedelta(days=1),
        catchup=False,
        max_active_runs=1,
        params={
            "api_id": telegram_config.api_id if hasattr(telegram_config, 'api_id') else config.get("API_ID"),
            "api_hash": telegram_config.api_hash if hasattr(telegram_config, 'api_hash') else config.get("API_HASH"),
            "phone_number": telegram_config.phone_number if hasattr(telegram_config, 'phone_number') else config.get("PHONE_NUMBER"),
            "all_messages": False,
            "channel_name": telegram_config.channel_name if hasattr(telegram_config, 'channel_name') else "@mrakopedia",
            "session_string": telegram_config.session_string if hasattr(telegram_config, 'session_string') else config.get("SESSION_STRING"),
            "processing_mode": "incremental",
            "retention_days": 90,
        }
) as dag:
    
    # Task 1: Scrape Telegram messages
    scrape_task = PythonOperator(
        task_id='scrape_telegram_messages',
        python_callable=run_scraper,
        dag=dag,
        retries=1,
        retry_delay=timedelta(minutes=2),
    )

    # Task 2: Load messages to database
    load_db_task = PythonOperator(
        task_id='load_messages_to_db',
        python_callable=load_messages_to_db,
        dag=dag,
        retries=1,
        retry_delay=timedelta(minutes=2),
    )

    # Task 3: Get Telegraph links to process
    get_links_task = PythonOperator(
        task_id='get_telegraph_links',
        python_callable=get_links_to_process,
        dag=dag,
        retries=1,
        retry_delay=timedelta(minutes=1),
    )

    # Task 4: Process Telegraph links (dynamic task mapping)
    process_links_task = PythonOperator.partial(
        task_id='process_telegraph_link',
        python_callable=process_telegraph_link_sync,
        dag=dag,
        retries=1,
        retry_delay=timedelta(minutes=1),
    ).expand(op_args=get_links_task.output)

    # Task 5: Get cleanup statistics
    stats_task = PythonOperator(
        task_id='get_cleanup_statistics',
        python_callable=get_cleanup_statistics,
        dag=dag,
    )

    # Set task dependencies
    scrape_task >> load_db_task >> get_links_task >> process_links_task >> stats_task

# Cleanup DAG (weekly)
with DAG(
        'pasta_cleanup',
        default_args=default_args,
        description='Pasta pipeline cleanup - weekly duplicate removal and data maintenance',
        schedule_interval='0 2 * * 0',  # Every Sunday at 2 AM
        catchup=False,
        max_active_runs=1,
        params={
            "retention_days": 90,
        }
) as cleanup_dag:
    
    # Weekly cleanup task
    cleanup_task = PythonOperator(
        task_id='run_full_cleanup',
        python_callable=run_full_cleanup,
        dag=cleanup_dag,
        retries=1,
        retry_delay=timedelta(minutes=5),
    )

# Full pipeline DAG (manual trigger)
with DAG(
        'pasta_pipeline_full',
        default_args=default_args,
        description='Pasta pipeline full run - complete backfill without limits',
        schedule_interval=None,  # Manual trigger only
        catchup=False,
        max_active_runs=1,
        params={
            "api_id": telegram_config.api_id if hasattr(telegram_config, 'api_id') else config.get("API_ID"),
            "api_hash": telegram_config.api_hash if hasattr(telegram_config, 'api_hash') else config.get("API_HASH"),
            "phone_number": telegram_config.phone_number if hasattr(telegram_config, 'phone_number') else config.get("PHONE_NUMBER"),
            "all_messages": True,  # Full backfill
            "channel_name": telegram_config.channel_name if hasattr(telegram_config, 'channel_name') else "@mrakopedia",
            "session_string": telegram_config.session_string if hasattr(telegram_config, 'session_string') else config.get("SESSION_STRING"),
            "processing_mode": "full",
        }
) as full_dag:
    
    # Full scrape task
    full_scrape_task = PythonOperator(
        task_id='scrape_all_telegram_messages',
        python_callable=run_scraper,
        dag=full_dag,
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    # Full load task
    full_load_task = PythonOperator(
        task_id='load_all_messages_to_db',
        python_callable=load_messages_to_db,
        dag=full_dag,
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    # Full links task
    full_links_task = PythonOperator(
        task_id='get_all_telegraph_links',
        python_callable=get_links_to_process,
        dag=full_dag,
        retries=2,
        retry_delay=timedelta(minutes=2),
    )

    # Full process links task
    full_process_links_task = PythonOperator.partial(
        task_id='process_all_telegraph_links',
        python_callable=process_telegraph_link_sync,
        dag=full_dag,
        retries=2,
        retry_delay=timedelta(minutes=2),
    ).expand(op_args=full_links_task.output)

    # Set full pipeline dependencies
    full_scrape_task >> full_load_task >> full_links_task >> full_process_links_task
