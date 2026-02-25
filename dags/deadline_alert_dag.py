"""
DAG with a deadline alert to test triggers without task_instance associations.

Deadline alerts create triggers that are associated with DAG runs, not individual
task instances. This DAG tests that the triggerer can handle these types of triggers.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk.definitions.deadline import (
    AsyncCallback,
    DeadlineAlert,
    DeadlineReference,
)


async def log_deadline_missed(context, **kwargs):
    """Callback function that logs when a deadline is missed."""
    import json
    from datetime import UTC, datetime

    # Write a timestamp file to prove the callback was executed
    timestamp_file = f"/tmp/deadline_callback_{context['dag_run']['dag_run_id']}.json"
    with open(timestamp_file, "w") as f:
        json.dump(
            {
                "callback_executed_at": datetime.now(UTC).isoformat(),
                "dag_id": context["dag_run"]["dag_id"],
                "run_id": context["dag_run"]["dag_run_id"],
            },
            f,
        )

    return "Deadline was missed"


with DAG(
    dag_id="deadline_alert_dag",
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    deadline=DeadlineAlert(
        # Set a short deadline to ensure it triggers during tests
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=timedelta(seconds=5),
        callback=AsyncCallback("dags.deadline_alert_dag.log_deadline_missed"),
    ),
):
    # This task intentionally sleeps longer than the deadline
    sleep_task = BashOperator(
        task_id="sleep_task",
        bash_command="sleep 10",
    )
