""" Airflow Pipeline to perform ETL Workflow for taxi trip data application """
from datetime import datetime
import logging
from airflow.models import Variable, Connection
from airflow.models.param import Param
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.bash import BashOperator 


from etl_tasks import (clear_down_processed_files_task,
                       load_taxi_trips_mongo_task,
                       drop_mongo_collection_task)


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
    clear_down_processed_files_task(file_list)

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


@dag(
    start_date=datetime(2023, 1, 1),
    schedule=None
)
def test_mongo_db_pipeline():

    @task
    def print_mongo_conn():
        # mongo_conn = Connection.get_connection_from_secrets('mongo_prd')
        # mongo_uri = mongo_conn.get_uri()
        # LOGGER.info(f'Mongo Connection URI: {mongo_uri}')
        # LOGGER.info(f'Mongo Connection ID: {mongo_conn.conn_id}')
        mongo_hook = MongoHook(conn_id='mongo_prd')
        mongo_hook.get_collection("system.users", 'local')    
    
    @task
    def test_json_task():
        import pandas as pd
        import json
        data = [{'a':1, 'b':"{'content':[0.1,0.2],'type':'Point'}", 'c':'2023-02-01T00:00:00.000'},
                {'a':2, 'b':"{'content':[0.3,0.4],'type':'Point'}", 'c':'2023-10-05T00:00:00.000'}]
        test_df = pd.DataFrame(data=data)
        test_df.set_index('a')
        LOGGER.info(f'test data: {test_df}')
        LOGGER.info(f'test data types: {test_df.dtypes}')
        records = test_df.to_dict(orient='records')
        for record in records:
            record['b'] = json.loads(str(record['b']).replace("'", '"'))
        LOGGER.info(f'dict Obj: {records}')
        
        mongo_hook = MongoHook(conn_id='mongo_prd')
        client = mongo_hook.get_conn()
        db = client.test_db
        test_collection = db.test_collection
        test_collection.insert_many(records)

    print_mongo_conn() >> test_json_task() 
test_mongo_db_pipeline()

@dag(
    start_date=datetime(2023, 1, 1),
    schedule=None
)
def test_mongo_insert_pipeline():
    drop = drop_mongo_collection_task('mongo_prd',
                               'test_trips_db',
                               'test_trips')
    load = load_taxi_trips_mongo_task(['/remote-storage/test/f29b5058b4654095b810ddd96ab5ba1b-0.parquet'],
                               'mongo_prd',
                               'test_trips',
                               'test_trip_db')
    drop >> load
test_mongo_insert_pipeline()


@dag(
    start_date=datetime(2023, 1, 1),
    schedule=None
)
def test_mongo_drop_pipeline():
    drop_mongo_collection_task('mongo_prd',
                               'test_trip_db',
                               'test_trips')

test_mongo_drop_pipeline()


@dag(
    start_date=datetime(2023, 1, 1),
    schedule=None
)
def test_writing():
    BashOperator(
        task_id='write_out',
        bash_command="mkdir /remote-storage/test-dr/"
    )

test_writing()