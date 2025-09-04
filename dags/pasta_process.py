import datetime
from datetime import timedelta
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import dotenv_values
from sqlalchemy_utils.types.enriched_datetime.pendulum_datetime import pendulum

sys.path.append('/opt/airflow')
from processors.telegraph_processor import get_links_to_process, process_telegraph_link_sync

config = dotenv_values(".env")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2025, 6, 16, 14, 0),  # 2 hours after crawl
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'pasta_process',
    default_args=default_args,
    description='Daily Telegraph link processing',
    schedule_interval=timedelta(days=1),
    catchup=False,
    max_active_runs=1,
    params={
        "processing_mode": "incremental",
        "max_links_per_run": 100,  # Limit for safety
    }
) as dag:

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

    get_links_task >> process_links_task