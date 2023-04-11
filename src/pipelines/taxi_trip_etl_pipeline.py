""" Airflow Pipeline to perform ETL Workflow for taxi trip data application """
from datetime import datetime
import logging
from airflow.models import Variable
from airflow.models.param import Param
from airflow.decorators import dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from etl_tasks import (extract_taxi_trips_task,
                       load_taxi_trips_postgres_task,
                       load_taxi_trips_mongo_task,
                       clear_down_processed_files_task)


LOGGER = logging.getLogger(__name__)

@dag(
    start_date=datetime(2023, 1, 1),
    schedule='@once',
    params={
        "year": Param(2023, type="integer"),
        "month": Param(1, type="integer"),
        "backfill": Param(False, type="boolean"),
        "dataset_code": Param('wrvz-psew', type="string"),
        "data_bucket": Param(Variable.get('data_bucket'), type="string"),
        "psql_staging_table": Param('chicago_trip_data_stg', type="string"),
        "psql_prd_table": Param('chicago_taxi_trip_data', type="string"),
        "mongo_collection": Param('chicago_taxi_trips')
    },
    render_template_as_native_obj=True
)
def taxi_trips_etl_pipeline():
    """ DAG Definition for ETL Pipeline """

    year = "{{ params.year }}"
    month = "{{params.month}}"
    backfill = "{{params.backfill}}"
    dataset_code = "{{params.dataset_code}}"
    data_bucket = "{{params.data_bucket}}"
    psql_staging_table = "{{params.psql_staging_table}}"
    psql_prd_table = "{{params.psql_prd_table}}"
    mongo_collection = "{{params.mongo_collection}}"

    create_psql_stg_tables = PostgresOperator(
        task_id='create_stg_tables',
        postgres_conn_id='postgres_staging_conn',
        sql="sql/create_stg_tbl.sql",
        parameters={"psql_staging_table": psql_staging_table,
                    "psql_prd_table": psql_staging_table}
        )

    extracted_files = extract_taxi_trips_task(year, month,
                                              backfill, dataset_code,
                                              data_bucket, Variable.get('chicago_app_token'))

    load_taxi_trips_postgres = load_taxi_trips_postgres_task(extracted_files,
                                                             psql_staging_table,
                                                             Variable.get('postgres_staging_conn'))
    
    load_taxi_trips_mongo = load_taxi_trips_mongo_task(extracted_files,
                                                      Variable.get('mongo_db'),
                                                      mongo_collection)

    insert_staged_data_to_prod = PostgresOperator(
        task_id='insert_stg_to_prd',
        postgres_conn_id='postgres_staging_conn',
        sql='./sql/INSERT_STG_PRD.sql',
        parameters={"staging_table": psql_staging_table,
                    "prd_table": psql_prd_table}
    )

    clear_down_processed_files = clear_down_processed_files_task(extracted_files)

    create_psql_stg_tables >> extracted_files
    load_taxi_trips_postgres >> insert_staged_data_to_prod
    [load_taxi_trips_postgres, load_taxi_trips_mongo] >> clear_down_processed_files

taxi_trips_etl_pipeline()
