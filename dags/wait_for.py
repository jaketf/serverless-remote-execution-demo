import time
from datetime import datetime
from pathlib import Path

from airflow.sdk import dag, task

DEFAULT_STOP_FILE = "/tmp/stop"


@dag(
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
    params={"stop_file": DEFAULT_STOP_FILE},
)
def wait_for_stop_file_dag():
    stop_file = "{{ dag_run.conf.get('stop_file', params.stop_file) }}"

    @task
    def wait_for_stop_file(stop_file: str = DEFAULT_STOP_FILE):
        stop = Path(stop_file)
        while not stop.exists():
            time.sleep(1)
            print(f"Waiting for {stop_file}...")
        print(f"{stop_file} file detected, exiting.")

    wait_for_stop_file(stop_file=stop_file)


wait_for_stop_file_dag()
