from airflow.decorators import dag, task, task_group


@dag()
def foo():
    """foo docstring"""

    @task_group
    def bar():
        """bar docstring"""

        @task
        def fizz():
            """fizz docstring"""

        fizz()

    bar()


foo()
