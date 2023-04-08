from datetime import datetime
from airflow.models import Variable
from airflow.models.param import Param
import logging
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator

log = logging.getLogger(__name__)


@dag(
    start_date=datetime(2023,1,1),
    schedule='@once',
    params={"year": Param(2023, type='integer'),
            "month": Param(2, type='integer'),
            "backfill": Param(False, type='boolean')}
)
def etl_pipeline(year: int = 2023, month: int = 1, backfill: bool = False):

    create_stg_tables = PostgresOperator(
        task_id='create_stg_tables',
        postgres_conn_id='postgres_staging_conn',
        sql="sql/create_stg_tbl.sql"
    )

    @task(task_id = "api_to_staging")
    def api_result_to_psql(year, month, backfill):
        import requests
        import pandas as pd
        import calendar
        from io import StringIO
        import time
        from sqlalchemy import create_engine
        from sodapy import Socrata
        def convert_point(input):
            if pd.isna(input):
                return None
            else:
                result_str = f"{input['coordinates'][0]},{input['coordinates'][1]}"
                return result_str
        dataset_code = 'wrvz-psew'
        connection_url='data.cityofchicago.org'
        client = Socrata(connection_url, 
                         app_token=Variable.get('chicago_app_token'),
                         timeout=30)
        date_from = f'{year}-{month}-01T00:00:00'
        if backfill:
            log.info(f'Query Data since {date_from}')
            query = f'trip_start_timestamp > "{date_from}"'
        else:
            _ , day_to = calendar.monthrange(int(year), int(month))
            date_to = f"{year}-{month}-{day_to}T23:59:59"
            log.info(f'Query Chicago Taxi Data between {date_from} and {date_to}')
            query = f'trip_start_timestamp between "{date_from}" and "{date_to}"'
        offset_factor = 10000
        offset_val = 0
        log.info(f'offset increment set to {offset_factor}')
        new_data = True
        output_df = pd.DataFrame()
        page_count = 1
        while new_data:
            log.info(f'Fetching Page {page_count}...')
            results = client.get(dataset_identifier=dataset_code,
                             limit=offset_factor,
                             where=query,
                             offset=offset_val)
            result_df = pd.DataFrame.from_records(results)
            if result_df.shape[0]>0:
                output_df = pd.concat([output_df, result_df])
                offset_val += offset_factor
                log.info(f'Received {result_df.shape[0]} rows from API - Total: {output_df.shape[0]} | Query records from id {offset_val}')
                time.sleep(5)
                page_count += 1
            else:
                log.info(f'No rows received from Query')
                new_data = False
        output_df.drop_duplicates(subset='trip_id', inplace=True)
        log.info(f'API returned {output_df.shape[0]} rows in total')
        output_df['pickup_centroid_location'] = output_df['pickup_centroid_location']\
                                                    .apply(lambda x: convert_point(x))
        output_df['dropoff_centroid_location'] = output_df['dropoff_centroid_location']\
                                                    .apply(lambda x: convert_point(x))
        log.info(f'Point data formatted for Postgres Load')
        engine = create_engine(Variable.get('postgres_staging_conn'))
        output_df.to_sql('trip_data_staging',
                         con=engine,
                         if_exists='append',
                         index=False)
        log.info(f'Data Successfully written to staging table')
    

    truncate_stg_tables = PostgresOperator(
        task_id='truncate_stg_tables',
        postgres_conn_id='postgres_staging_conn',
        sql="TRUNCATE TABLE trip_data_staging"
    )

    create_stg_tables >> api_result_to_psql(year=year, month=month, backfill=backfill) >> truncate_stg_tables

dag = etl_pipeline()




