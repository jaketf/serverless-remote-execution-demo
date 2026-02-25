from __future__ import annotations

from typing import Any, AsyncIterator

import pendulum
from airflow.configuration import conf
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.bases.sensor import BaseSensorOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.context import Context

try:
    from airflow.sdk import DAG
except ImportError:
    from airflow.models.dag import DAG


class FailExceptionTrigger(BaseTrigger):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return ("dags.deferrable_failing_dag_exception.FailExceptionTrigger", {})

    async def run(self) -> AsyncIterator[TriggerEvent]:
        raise Exception("This trigger should not be run")
        yield


class RaiseExceptionSensor(BaseSensorOperator):
    def __init__(
        self,
        deferrable: bool = conf.getboolean(
            "operators", "default_deferrable", fallback=False
        ),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.deferrable = deferrable

    def execute(self, context: Context) -> None:
        self.defer(
            trigger=FailExceptionTrigger(),
            method_name="execute_complete",
        )

    def execute_complete(
        self,
        context: Context,
        event: dict[str, Any] | None = None,
    ) -> None:
        # We have no more work to do here. Mark as complete.
        return


with DAG(
    dag_id="deferrable_fail_exception",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
) as dag:
    wait = RaiseExceptionSensor(task_id="wait")
    finish = EmptyOperator(task_id="finish")
    wait >> finish
