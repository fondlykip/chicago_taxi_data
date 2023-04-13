from datetime import datetime
import logging
from airflow.decorators import dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from etl_tasks import (
    load_community_area_dim
)
    
LOGGER = logging.getLogger(__name__)

@dag(
    start_date = datetime(2023, 1, 1),
    schedule='@once',
    render_template_as_native_obj=True
)
def initialisation_pipeline():
    """ DAG Definition for initialisation Pipeline """

    create_date_dim = PostgresOperator(
        task_id='populate date dim',
        postgres_conn_id='psql_trip_db_conn',
        sql='sql/populate_date_dim.sql',
        parameters={'date_dim':Variable.get('psql_date_dim_table')}
    )

    create_psql_tables = PostgresOperator(
        task_id='create_psql_tables',
        postgres_conn_id='psql_trip_db_conn',
        sql='sql/create_prd_tbl.sql',
        parametes={"trip_table":Variable.get('psql_trip_fact_table'),
                   "comm_area_table":Variable.get('psql_comm_area_table'),
                   "date_dim":Variable.get('psql_date_dim_table')},
        retries=1
    )

    populate_community_areas = load_community_area_dim(Variable.get('psql_comm_area_table'),
                                                       Variable.get('psql_conn_str'))
    
    create_date_dim > create_psql_tables > populate_community_areas

initialisation_pipeline()