import os

from airflow.decorators import dag, task
from airflow.models import DagRun
from datetime import datetime


@dag(schedule=None, start_date=datetime(2024, 1, 1), catchup=False)
def fail_dag():
    @task
    def conditional_task(**context):
        """Fail or succeed based on DAG conf argument `fail`."""
        dag_run: DagRun = context["dag_run"]
        conf = dag_run.conf or {}  # Get DAG conf, default to empty dict

        fail = conf.get("fail", True)  # Default to True if not provided

        if isinstance(fail, str):
            if fail.lower() == "exit":
                print("abruptly exiting 137 to simulate oom...")
                os._exit(137)
            if fail.lower() == "oom":
                print("allocating a massive list to force OOM...")
                big_list = [0] * (10**10)  # Allocates ~80GB RAM
                print(f"allocated {len(big_list)} elements")
            fail = fail.lower() in ["true", "1", "yes"]  # Convert string to bool

        if fail:
            print("simulating python raises Exception failure...")
            raise Exception("Intentional failure!")
        else:
            print("success, task completed without failure!")

    conditional_task()


dag_instance = fail_dag()
