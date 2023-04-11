""" Functions containing extra logic for the tasks of the ETL Pipeline """
import logging
import os
import shutil
import pandas as pd
from sqlalchemy import create_engine
from etl_functions import (query_chicago_data,
                           generate_query,
                           convert_point_cols,
                           list_files)
from airflow.decorators import task
from airflow.operators.python import get_current_context

LOGGER = logging.getLogger(__name__)


@task(retries=2)
def extract_taxi_trips_task(year,
                            month,
                            backfill,
                            dataset_code,
                            data_bucket,
                            app_token) -> list:
    """
    Task to query data from Taxi Trip API and load into Staging Table
    Args:
        - year: the year from/for which to query data
        - month: the month from/for which to query data
        - backfill: boolean flag determines whether to run in backfill mode
                    (Query data since <year>/<month>) or regular mode
                    (Query data for <year>/<month>)
        - data_bucket: remote storage bucket in which to store resulting parquet files
        - app_token: token for authenticating API Requests
    Returns:
        - produced_files: a list of files found in the raw folder in the remote storage
                          after extraction has completed.
    """
    filter_col = 'trip_start_timestamp'
    query = generate_query(backfill, year, month, filter_col)
    results_df = query_chicago_data(query=query,
                                    dataset_code=dataset_code,
                                    id_col='trip_id',
                                    token=app_token)
    results_df['trip_start_timestamp'] = pd.to_datetime(results_df['trip_start_timestamp'])
    results_df['trip_start_year'] = results_df['trip_start_timestamp'].dt.year
    results_df['trip_start_month'] = results_df['trip_start_timestamp'].dt.month
    storage_path = f'{dataset_code}/raw/'
    results_df.to_parquet(f'/{data_bucket}/{storage_path}',
                          engine='pyarrow',
                          partition_cols=['trip_start_year', 'trip_start_month'])
    produced_files = list_files(data_bucket, storage_path)
    return produced_files


@task
def load_taxi_trips_postgres_task(files_to_load: list,
                                  psql_conn: str):
    """
    Task to load extracted taxi trip data from remote storage to PSQL DB
    Args:
        - files_to_load: list of files to load to Postgres DB
        - psql_conn: connection string of Postgres DB
    """
    params = get_current_context()['params']
    destination_table = params['psql_staging_table']
    count = 0
    total_files = len(files_to_load)
    if total_files == 0:
        LOGGER.info('No files available for loading')
    loaded_ids = []
    for file_path in files_to_load:
        raw_df = pd.read_parquet(file_path,
                                 engine='pyarrow')
        raw_df.set_index('trip_id')
        raw_df = raw_df.loc[~raw_df['trip_id'].isin(loaded_ids)]
        loaded_ids.extend(raw_df['trip_id'].values)
        point_cols = ['pickup_centroid_location', 'dropoff_centroid_location']
        raw_df = convert_point_cols(raw_df, point_cols)
        LOGGER.info(f'Point data formatted for Postgres Load')
        engine = create_engine(psql_conn)
        raw_df.to_sql(destination_table,
                      con=engine,
                      if_exists='append',
                      index=False)
        count += 1
        LOGGER.info(f'{count} of {total_files} files written to Postgres DB...')
    LOGGER.info('Data Successfully written to staging table')

@task
def clear_down_processed_files(file_list):
    """
    Task to move processed files from raw folder to processed folder
    Args:
        - file_list: list of files to clean up
    """
    total = 0
    for file in file_list:
        file_str = str(file)
        file_name = file_str.rsplit('/', 1)[-1]
        destination_path = file_str.replace('/raw/', '/processed/')[:-len(file_name)]
        os.makedirs(destination_path, exist_ok=True)
        shutil.move(file, destination_path)
        LOGGER.info(f'moved file {file} to {destination_path}')
        total += 1
    LOGGER.info(f'{total} files moved from Raw folder to processed folder')