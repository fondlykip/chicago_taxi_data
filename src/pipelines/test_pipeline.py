""" Airflow Pipeline to perform ETL Workflow for taxi trip data application """
from datetime import datetime
import logging
from airflow.models import Variable
from airflow.models.param import Param
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.postgres.operators.postgres import PostgresOperator
from etl_tasks import clear_down_processed_files

LOGGER = logging.getLogger(__name__)


@dag(
    start_date=datetime(2023, 1, 1),
    schedule='@once'
)
def test_pipeline():
    @task
    def test_list_files():
        from etl_functions import list_files
        bucket='remote-storage'
        path='wrvz-psew/raw'
        output = list_files(bucket, path)
        LOGGER.info(f'list_files function returned: {output}')
        return output
    
    file_list = test_list_files()
    clear_down_processed_files(file_list)

test_pipeline()


@dag(
    start_date=datetime(2023, 1, 1),
    schedule='@once'
)
def test_db_pipeline():
    
    drop_test_tbl_task = PostgresOperator(
        task_id='drop_test_table',
        postgres_conn_id='postgres_staging_conn',
        sql='DROP TABLE IF EXISTS test_table;'
    )
    @task
    def test_pandas_psql_load():
        from sqlalchemy import create_engine
        import pandas as pd
        engine = create_engine(Variable.get('postgres_staging_conn'))
        df = pd.DataFrame(columns=['a','b','c'],
                          data=[[1,1,1],[2,2,2],[3,3,3]])
        result = df.to_sql('test_table',
                            con=engine,
                            if_exists='append',
                            index=False)
        LOGGER.info(f'Result from psql write: {result}')
        return result
    
    clear_db_task = PostgresOperator(
        task_id='clear_test_table',
        postgres_conn_id='postgres_staging_conn',
        sql='SELECT COUNT(*) FROM test_table; DROP TABLE test_table;'
    )
    
    load_data_task = test_pandas_psql_load()
    drop_test_tbl_task >> load_data_task >> clear_db_task

test_db_pipeline()
