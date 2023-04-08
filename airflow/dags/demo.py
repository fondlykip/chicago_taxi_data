from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

# A DAG represents a workflow, a collection of tasks
with DAG(dag_id="demo",start_date=datetime(2020, 2, 2), schedule=None) as dag:

    # Tasks are represented as operators
    hello = BashOperator(task_id="hello", bash_command="echo hello")

    @task()
    def airflow():
        print("airflow")

    insert_test_data = PostgresOperator(
        task_id='conn_test',
        postgres_conn_id='postgres_staging_conn',
        sql="sql/test.sql"
    )
    # Set dependencies between tasks
    hello >> airflow() >> insert_test_data