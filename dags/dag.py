# dags/dag.py
import sys
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import dotenv_values

# project-local imports
sys.path.append("/opt/airflow")
from processors.db_loader import load_messages_to_db
from processors.message_scraper import run_scraper
from processors.telegraph_processor import (
    get_links_to_process,
    process_telegraph_link_sync,
)

CFG = dotenv_values(".env")
DEFAULT_ARGS = dict(
    owner="airflow",
    depends_on_past=False,
    start_date=pendulum.datetime(2025, 6, 16, 12, 0),
    email_on_failure=True,
    email_on_retry=False,
    retries=0,
    retry_delay=timedelta(minutes=5),
)

def tg_params(**extra) -> dict:
    base = dict(
        api_id=CFG.get("API_ID"),
        api_hash=CFG.get("API_HASH"),
        phone_number=CFG.get("PHONE_NUMBER"),
        session_string=CFG.get("SESSION_STRING"),
        channel_name="@mrakopedia",
        all_messages=False,
    )
    base.update(extra)
    return base

def add_pasta_tasks(dag_obj: DAG):
    scrape = PythonOperator(
        task_id="scrape_telegram_messages",
        python_callable=run_scraper,
        dag=dag_obj,
    )
    load_db = PythonOperator(
        task_id="load_messages_to_db",
        python_callable=load_messages_to_db,
        dag=dag_obj,
    )
    get_links = PythonOperator(
        task_id="get_telegraph_links",
        python_callable=get_links_to_process,
        dag=dag_obj,
    )
    process_links = PythonOperator(
        task_id="process_telegraph_link",
        python_callable=process_telegraph_link_sync,
        dag=dag_obj,
    )
    scrape >> load_db >> get_links >> process_links
    return dag_obj

pasta_pipeline = add_pasta_tasks(
    DAG(
        dag_id="pasta_pipeline",
        default_args=DEFAULT_ARGS,
        description="Main pasta pipeline (flexible modes)",
        schedule_interval=timedelta(days=1),
        catchup=False,
        params=tg_params(
            processing_mode="incremental",
            reprocess_older_than_days=30,
            max_links_per_run=200,
            content_refresh_threshold_days=7,
            include_failed_links=False,
        ),
    )
)

pasta_pipeline_daily = add_pasta_tasks(
    DAG(
        "pasta_pipeline_daily",
        default_args=DEFAULT_ARGS,
        description="Daily incremental pasta run",
        schedule_interval=timedelta(days=1),
        catchup=False,
        params=tg_params(
            processing_mode="incremental",
            max_links_per_run=150,
        ),
    )
)

pasta_pipeline_refresh = add_pasta_tasks(
    DAG(
        "pasta_pipeline_refresh",
        default_args=DEFAULT_ARGS,
        description="Weekly refresh of stale content",
        schedule_interval=timedelta(weeks=1),
        catchup=False,
        params=tg_params(
            processing_mode="refresh_old",
            reprocess_older_than_days=14,
            max_links_per_run=500,
        ),
    )
)

pasta_pipeline_full = add_pasta_tasks(
    DAG(
        "pasta_pipeline_full",
        default_args=DEFAULT_ARGS,
        description="Manual full reprocessing - NO LIMITS",
        schedule_interval=None,
        catchup=False,
        params=tg_params(
            processing_mode="full",
            all_messages=True,
            reprocess_older_than_days=365,
            include_failed_links=True,
        ),
    )
)
