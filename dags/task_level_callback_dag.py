import os

import pendulum
from airflow.providers.standard.operators.bash import BashOperator

try:
    from airflow.sdk import DAG
except ImportError:
    from airflow.models.dag import DAG


def task_failure_callback(context):
    """Task-level callback that gets executed when a task fails.

    This callback is executed by the worker inline for normal task failures.
    For abnormal terminations (heartbeat timeout, external kill), it goes through
    the DAG Processor callback mechanism.
    """
    os.makedirs("/tmp/task_callback", exist_ok=True)
    task_instance = context.get("task_instance")
    task_id = task_instance.task_id if task_instance else "unknown"
    with open(f"/tmp/task_callback/task_{task_id}_failure.txt", "w") as f:
        f.write(f"Task {task_id} failed and callback was executed")


def task_success_callback(context):
    """Task-level callback that gets executed when a task succeeds.

    This callback is executed by the worker inline for normal task success.
    """
    os.makedirs("/tmp/task_callback", exist_ok=True)
    task_instance = context.get("task_instance")
    task_id = task_instance.task_id if task_instance else "unknown"
    with open(f"/tmp/task_callback/task_{task_id}_success.txt", "w") as f:
        f.write(f"Task {task_id} succeeded and callback was executed")


with DAG(
    "task_level_callback_dag",
    schedule=None,
    start_date=pendulum.datetime(2024, 12, 1, tz="UTC"),
):
    # Task that will fail normally - callback executed inline by worker
    failing_task = BashOperator(
        task_id="failing_task",
        bash_command="exit 1",  # This will fail
        cwd=".",
        on_failure_callback=task_failure_callback,
    )

    # Task that will be running when the worker is killed (abnormal termination).
    # It uses a long-running command so the worker is scaled to 0 while the task
    # is still executing, causing an abnormal termination whose callback is
    # executed by the DAG Processor.
    timeout_task = BashOperator(
        task_id="timeout_task",
        bash_command="sleep 3600",
        cwd=".",
        on_failure_callback=task_failure_callback,
        trigger_rule="all_done",  # Run even if failing_task fails
    )

    # Make timeout_task depend on failing_task so it only runs after failing_task completes
    failing_task >> timeout_task
