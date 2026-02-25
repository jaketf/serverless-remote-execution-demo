import pendulum
from airflow.providers.standard.operators.bash import BashOperator

try:
    from airflow.sdk import DAG
except ImportError:
    from airflow.models.dag import DAG

with DAG(
    "long_running_dag",
    schedule=None,
    start_date=(pendulum.datetime(2024, 12, 1, tz="UTC")),
):
    BashOperator(
        task_id="long_running_task",
        bash_command="sleep 100",
        cwd=".",
    )
