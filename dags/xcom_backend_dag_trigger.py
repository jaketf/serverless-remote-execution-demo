from typing import Any, AsyncIterator

from airflow import DAG
from airflow.decorators import task
from airflow.models.taskinstance import TaskInstanceKey
from airflow.sensors.base import BaseSensorOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.context import Context


class XComTrigger(BaseTrigger):
    """Trigger that checks the value of an XCom variable."""

    def __init__(self, xcom_task_id: str, **kwargs):
        self.xcom_task_id = xcom_task_id
        super().__init__(**kwargs)

    @property
    def task_instance_key(self) -> TaskInstanceKey:
        return TaskInstanceKey(
            dag_id=self.task_instance.dag_id,
            task_id=self.xcom_task_id,
            run_id=self.task_instance.run_id,
        )

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "dags.xcom_backend_dag_trigger.XComTrigger",
            {
                "xcom_task_id": self.xcom_task_id,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        from airflow.sdk.execution_time.xcom import XCom
        from asgiref.sync import sync_to_async

        while True:
            self.log.info(f"attempting to retrieve xcom for {self.task_instance_key}")
            value = await sync_to_async(XCom.get_value)(
                ti_key=self.task_instance_key,
                key="return_value",
            )
            self.log.info(
                f"xcom value received: {value!r} (type={type(value).__name__})"
            )
            assert value == "my_value", (
                f"Expected 'my_value', got {value!r} (type={type(value).__name__})"
            )
            break

        yield TriggerEvent({"status": "success"})


class XComSensor(BaseSensorOperator):
    """Sensor that checks the value of an XCom."""

    def __init__(self, xcom_task_id: str, **kwargs):
        self.xcom_task_id = xcom_task_id
        super().__init__(**kwargs)

    def execute(self, context: Context) -> None:
        self.defer(
            trigger=XComTrigger(xcom_task_id=self.xcom_task_id),
            method_name="execute_complete",
        )

    def execute_complete(
        self, context: Context, event: dict[str, Any] | None = None
    ) -> None:
        pass


try:
    from airflow.sdk import DAG
except ImportError:
    from airflow.models.dag import DAG


with DAG("xcom_backend_dag_trigger"):

    @task()
    def xcom_producer():
        return "my_value"

    producer = xcom_producer()

    # check that a trigger can pull an xcom value
    sensor = XComSensor(
        task_id="xcom_sensor",
        xcom_task_id="xcom_producer",
    )

    producer >> sensor
