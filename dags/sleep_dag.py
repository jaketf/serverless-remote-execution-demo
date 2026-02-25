from airflow.operators.python import PythonOperator
import time
from datetime import datetime

try:
    from airflow.sdk import DAG
except ImportError:
    from airflow.models.dag import DAG


def sleep_function():
    """Sleep for 10 seconds."""
    time.sleep(10)
    return "Done sleeping"


with DAG(
    "sleep_dag",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
) as dag:
    sleep_task = PythonOperator(
        task_id="sleep_task",
        python_callable=sleep_function,
    )
