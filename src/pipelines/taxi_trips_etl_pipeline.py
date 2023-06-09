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
                       clear_down_processed_files_task,
                       export_mongo_task, export_psql_task)


LOGGER = logging.getLogger(__name__)

@dag(
    start_date=datetime(2023, 4, 10),
    schedule='0 0 9 * *',
    params={
        "year": Param(2023, type="integer"),
        "month": Param(1, type="integer"),
        "backfill": Param(False, type="boolean"),
        "dataset_code": Param('wrvz-psew', type="string"),
        "data_bucket": Param(Variable.get('data_bucket'), type="string"),
        "psql_staging_table": Param(Variable.get('psql_trip_stg_table'), type="string"),
        "psql_prd_table": Param(Variable.get('psql_trip_fact_table'), type="string"),
        "mongo_collection": Param(Variable.get('mongo_collection'), type="string"),
        "mongo_database": Param(Variable.get('mongo_database'), type="string")
    },
    render_template_as_native_obj=True
)
def taxi_trips_etl_pipeline():
    """
    DAG Definition for ETL Pipeline.
    This pipeline performs the following steps:
        1 - create staging tables for landing data to.
        2 - Query Chicago City Data Portal for Taxi Trip Data.
        3 - Load extracted data to PSQL and Mongo Databases.
        4 - Export updated tables to csv and JSON for serving
            API requests.
        5 - Clear down unnecessary tables after completion.
    """

    year = "{{params.year}}"
    month = "{{params.month}}"
    backfill = "{{params.backfill}}"
    dataset_code = "{{params.dataset_code}}"
    data_bucket = "{{params.data_bucket}}"
    psql_staging_table = "{{params.psql_staging_table}}"
    psql_prd_table = "{{params.psql_prd_table}}"
    mongo_collection = "{{params.mongo_collection}}"
    mongo_database = "{{params.mongo_database}}"

    create_psql_stg_tables = PostgresOperator(
        task_id='create_stg_tables',
        postgres_conn_id='psql_trip_db_conn',
        sql="sql/create_stg_tbl.sql",
        parameters={"psql_staging_table": psql_staging_table,
                    "psql_prd_table": psql_staging_table},
        retries=1
        )

    extracted_files = extract_taxi_trips_task(year, month,
                                              backfill, dataset_code,
                                              data_bucket)

    load_taxi_trips_postgres = load_taxi_trips_postgres_task(extracted_files,
                                                             psql_staging_table,
                                                             Variable.get('psql_trip_db_conn_str'))

    load_taxi_trips_mongo = load_taxi_trips_mongo_task(extracted_files,
                                                       'mongo_prd',
                                                       mongo_collection,
                                                       mongo_database)

    insert_staged_data_to_prod = PostgresOperator(
        task_id='insert_stg_to_prd',
        postgres_conn_id='psql_trip_db_conn',
        sql='./sql/insert_stg_to_prd.sql',
        parameters={"staging_table": psql_staging_table,
                    "prd_table": psql_prd_table}
    )

    clear_down_processed_files = clear_down_processed_files_task(extracted_files)

    export_psql = export_psql_task(Variable.get('psql_trip_db_conn_str'),
                                   psql_prd_table,
                                   data_bucket)
    export_mongo = export_mongo_task('mongo_prd',
                                     mongo_collection,
                                     mongo_database,
                                     data_bucket)

    create_psql_stg_tables >> extracted_files
    load_taxi_trips_postgres >> insert_staged_data_to_prod
    load_taxi_trips_postgres >> load_taxi_trips_mongo >> clear_down_processed_files

    [insert_staged_data_to_prod, load_taxi_trips_mongo] >> export_psql >> export_mongo

taxi_trips_etl_pipeline()
