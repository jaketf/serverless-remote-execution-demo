from pathlib import Path

from typing import Any

from airflow.decorators import task
from airflow.sdk import BaseSensorOperator, Context
from include.triggers import RelativePathTrigger

try:
    from airflow.sdk import DAG
except ImportError:
    from airflow.models.dag import DAG


def test_relative_path_exists():
    assert Path("empty").exists()


class RelativePathSensor(BaseSensorOperator):
    def execute(self, context: Context) -> None:
        self.defer(trigger=RelativePathTrigger(), method_name="execute_complete")

    def execute_complete(
        self, context: Context, event: dict[str, Any] | None = None
    ) -> None:
        return


with DAG("relative_path_dag"):

    @task()
    def test_relative_path_exists_worker():
        test_relative_path_exists()

    test_relative_path_exists_worker()

    RelativePathSensor(task_id="test_relative_path_exists_triggerer")

    test_relative_path_exists()  # test_relative_path_exists_dag_processor
