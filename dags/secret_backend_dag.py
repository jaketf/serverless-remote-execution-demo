from typing import Any

from airflow.configuration import conf
from airflow.decorators import task
from airflow.sdk import BaseSensorOperator, Connection, Context, Variable
from include.triggers import ConnectionTrigger, VariableTrigger

# Only run module-level tests when secret backend is configured
_SECRET_BACKEND_CONFIGURED = conf.get("workers", "secrets_backend", fallback=None)

try:
    from airflow.sdk import DAG
except ImportError:
    from airflow.models.dag import DAG


def test_get_variable_sync():
    assert Variable.get("my_variable") == "my_value"


def test_get_connection_sync():
    conn = Connection.get("my_connection")
    assert conn.get_uri() == "scheme://my_login:my_password@my_host:5432/my_schema"


class VariableSensor(BaseSensorOperator):
    def execute(self, context: Context) -> None:
        self.defer(trigger=VariableTrigger(), method_name="execute_complete")

    def execute_complete(
        self, context: Context, event: dict[str, Any] | None = None
    ) -> None:
        return


class ConnectionSensor(BaseSensorOperator):
    def execute(self, context: Context) -> None:
        self.defer(trigger=ConnectionTrigger(), method_name="execute_complete")

    def execute_complete(
        self, context: Context, event: dict[str, Any] | None = None
    ) -> None:
        return


with DAG("secret_backend_dag"):

    @task()
    def test_get_variable_worker():
        test_get_connection_sync()

    @task()
    def test_get_connection_worker():
        test_get_connection_sync()

    test_get_variable_worker()
    test_get_connection_worker()

    VariableSensor(task_id="test_get_variable_triggerer")
    ConnectionSensor(task_id="test_get_connection_triggerer")

    # Only run at parse time when secret backend is configured
    if _SECRET_BACKEND_CONFIGURED:
        test_get_variable_sync()  # test_get_variable_dag_processor
        test_get_connection_sync()  # test_get_connection_dag_processor
