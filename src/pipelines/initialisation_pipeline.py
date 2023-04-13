from datetime import datetime
import logging
from airflow.decorators import dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from airflow.models.param import Param
from etl_tasks import (
    load_community_area_dim,
    drop_mongo_collection_task
)
    
LOGGER = logging.getLogger(__name__)

@dag(
    start_date = datetime(2023, 1, 1),
    schedule=None,
    render_template_as_native_obj=True,
    catchup=False,
    params={
        "psql_date_dim_table":Param(Variable.get('psql_date_dim_table'), type="string"),
        "psql_trip_fact_table":Param(Variable.get('psql_trip_fact_table'), type="string"),
        "psql_comm_area_table":Param(Variable.get('psql_comm_area_table'), type="string"),
        "psql_trip_db_conn_str":Param(Variable.get('psql_trip_db_conn_str'), type="string"),
        "mongo_database":Param(Variable.get('mongo_database'), type="string"),
        "mongo_collection":Param(Variable.get('mongo_collection'), type="string")
    }
)
def initialisation_pipeline():
    """ DAG Definition for initialisation Pipeline """
    psql_date_dim_table = "{{params.psql_date_dim_table}}"
    psql_trip_fact_table = "{{params.psql_trip_fact_table}}"
    psql_comm_area_table = "{{params.psql_comm_area_table}}"
    psql_trip_db_conn_str = "{{params.psql_trip_db_conn_str}}"
    mongo_collection = "{{params.mongo_collection}}"
    mongo_database = "{{params.mongo_database}}"

    drop_tables = PostgresOperator(
        task_id='drop_tables',
        postgres_conn_id='psql_trip_db_conn',
        sql='sql/drop_tables.sql',
        parameters={"psql_date_dim_table":psql_date_dim_table,
                    "psql_trip_fact_table":psql_trip_fact_table,
                    "psql_comm_area_table":psql_comm_area_table}
    )

    drop_mongo_collection = drop_mongo_collection_task('mongo_prd',
                                                       mongo_database,
                                                       mongo_collection)

    create_date_dim = PostgresOperator(
        task_id='create_date_dim',
        postgres_conn_id='psql_trip_db_conn',
        sql='sql/populate_date_dim.sql',
        parameters={"psql_date_dim_table": psql_date_dim_table}
    )

    create_psql_tables = PostgresOperator(
        task_id='create_psql_tables',
        postgres_conn_id='psql_trip_db_conn',
        sql='sql/create_prd_tbl.sql',
        parameters={"psql_trip_fact_table": psql_trip_fact_table,
                   "psql_comm_area_table": psql_comm_area_table,
                   "psql_date_dim_table": psql_date_dim_table},
        retries=1
    )

    populate_community_areas = load_community_area_dim(psql_comm_area_table,
                                                       psql_trip_db_conn_str)
    
    drop_tables >> create_date_dim >> create_psql_tables >> populate_community_areas

initialisation_pipeline()