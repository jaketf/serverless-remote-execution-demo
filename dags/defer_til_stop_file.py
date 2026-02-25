"""
DAG runs a task that defers until the expected file is created, but it never is.
"""

from __future__ import annotations

import pendulum

from airflow.sdk.definitions.param import ParamsDict
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.filesystem import FileSensor

try:
    from airflow.sdk import DAG
except ImportError:
    from airflow.models.dag import DAG

DEFAULT_STOP_FILE = "/tmp/stop"

with DAG(
    dag_id="defer_til_stop_file",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    params=ParamsDict(
        {
            "stop_file": DEFAULT_STOP_FILE  # Set the default here
        }
    ),
) as dag:
    # Retrieve the stop_file from the DAG conf, or use the default if not provided
    stop_file = "{{ dag_run.conf.get('stop_file', params.stop_file) }}"

    wait = FileSensor(
        task_id="wait",
        filepath=stop_file,
        deferrable=True,
        poke_interval=1,
    )

    finish = EmptyOperator(task_id="finish")

    wait >> finish
