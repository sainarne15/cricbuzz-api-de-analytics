"""A liveness prober dag for monitoring composer.googleapis.com/environment/healthy."""
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'fetch_cricket_stats',
    default_args=default_args,
    description='will fetch the records from cricbbuzz api',
    schedule_interval='@daily',
    max_active_runs=2,
    catchup=False,
)

with dag:
    reun_script_task = BashOperator(
        task_id='run_script',
        bash_command='python /home/airflow/gcs/dags/scripts/extract_load_gcs.py'
    )