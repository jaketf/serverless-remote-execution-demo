import time

from airflow.decorators import (
    dag,
    task,
)  # DAG and task decorators for interfacing with the TaskFlow API


@task
def run_task(x: int):
    time.sleep(5)
    print(f"task {x} finished !")


@dag(
    catchup=False,
    default_args={
        "retries": 2,
    },
    tags=["scaling"],
    max_active_tasks=100,
    schedule="*/1 * * * *",
)
def scaling_workers():
    run_task.expand(x=list(range(0, 100)))


scaling_workers = scaling_workers()
