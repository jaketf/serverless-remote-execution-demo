"""
Example DAG demonstrating email callback functionality.

This DAG is used to test that email callbacks (EmailRequest) are properly
processed by the DAG Processor. When a task with email_on_failure=True fails,
the scheduler creates an EmailRequest callback that is sent to the DAG Processor.

Note: This requires Airflow 3.1+ for EmailRequest support.
"""

import pendulum
from airflow.providers.standard.operators.bash import BashOperator

try:
    from airflow.sdk import DAG
except ImportError:
    from airflow.models.dag import DAG


with DAG(
    "email_callback_dag",
    schedule=None,
    start_date=pendulum.datetime(2024, 12, 1, tz="UTC"),
):
    # Task that will fail to trigger the email callback
    failing_task = BashOperator(
        task_id="failing_task_with_email",
        bash_command="exit 1",  # This will fail
        cwd=".",
        email=["test@example.com"],
        email_on_failure=True,
    )
