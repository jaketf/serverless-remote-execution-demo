from airflow.decorators import task

try:
    from airflow.sdk import DAG
except ImportError:
    from airflow.models.dag import DAG


with DAG("xcom_backend_dag"):

    @task()
    def xcom_producer():
        return "my_value"

    @task()
    def xcom_consumer(value):
        assert value == "my_value"

    xcom_consumer(xcom_producer())
