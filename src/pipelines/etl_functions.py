""" Extra Functions supporting the functionalit of the etl tasks defined in etl_tasks.py """
from os import walk
from os.path import join
import logging
import time
import calendar
from sodapy import Socrata
import pandas as pd

LOGGER = logging.getLogger(__name__)

def convert_point(input_data):
    """
    Function to convert a GIC POINT data point to Postgres Friendly Format
    Args:
        - input: data to tranform
    """
    if pd.isna(input_data):
        return None
    result_str = f"{input_data['coordinates'][0]},{input_data['coordinates'][1]}"
    return result_str

def convert_point_cols(input_df: pd.DataFrame,
                       point_cols: list):
    """
    Function to convert all Columns representing GIS Points to Postgres ready format
    Args:
        - input_df: df in which to update point formatting
        - point_cols: list of cols containing point data
    reutrns:
        - input_df: The dataframe with formatted point data columns
    """
    for col in point_cols:
        input_df[col] = input_df[col].apply(lambda x: convert_point(x))
    return input_df

def generate_query(backfill,
                   year, month,
                   filter_col):
    """
    Function to generate a query to run against Chicago City Datasets
    Args:
        - backfill: Booolean flag to determine if the query is in
                    backfill or batch mode
        - year: Year component of the target date
        - month: Month component of the target date
        - filter_col: Column by which to filter results
    Returns:
        - query: Query string to run in where clause against
                 Chicago City Data Portal API
    """
    date_from = f'{year}-{month}-01T00:00:00'
    if backfill:
        LOGGER.info(f'Query Data since {date_from}')
        query = f'{filter_col} > "{date_from}"'
    else:
        _, day_to = calendar.monthrange(int(year), int(month))
        date_to = f"{year}-{month}-{day_to}T23:59:59"
        LOGGER.info(f'Query Data between {date_from} and {date_to}')
        query = f'{filter_col} between "{date_from}" and "{date_to}"'
    return query

def query_chicago_data(query: str,
                       dataset_code: str,
                       id_col: str,
                       token: str,
                       offset_factor=50000):
    """
    function to run a given query against a given dataset from the City of Chicago
    Data Portal API.
    Args:
        - query: String containing the where clause to use in API Call
        - dataset_code: The ID of the Dataset against which to run the query
        - id_col: The name of the ID Column to use to ensure order of paged results
        - token: App Token string used for authenticating connection to API
        - offset_factor: Optional - the number of records to receive per page
    Returns:
        - output_df: DataFrame received from API Query
    """
    if id_col is None:
        LOGGER.warning("No ID Col supplied, order of paged results not guarenteed")
    client = Socrata('data.cityofchicago.org',
                     app_token=token,
                     timeout=120)
    offset_val = 0
    new_data = True
    output_df = pd.DataFrame()
    page_count = 1
    while new_data:
        LOGGER.info(f'Fetching Page {page_count} from API...')
        results = client.get(dataset_identifier=dataset_code,
                             limit=offset_factor,
                             where=query,
                             offset=offset_val,
                             order=id_col)
        result_df = pd.DataFrame.from_records(results)
        if result_df.shape[0] > 0:
            output_df = pd.concat([output_df, result_df])
            offset_val += offset_factor
            LOGGER.info(f'Received {result_df.shape[0]} rows from API '\
                        f'- Total: {output_df.shape[0]} | Query records from id {offset_val}')
            time.sleep(5)
            page_count += 1
        else:
            LOGGER.info(f'No rows received from Query')
            new_data = False
    LOGGER.info(f'API returned {output_df.shape[0]} rows in total')
    return output_df

def list_files(bucket, path):
    """
    Function to list all parquet files at a given path in a given remote storage bucket
    Args:
        - bucket: Name of the remote storage bucket to search in
        - path: Path in the storage bucket to search in
    Returns:
        - output_list: list of files at given location
    """
    path_to_list = join('/', bucket, path)
    LOGGER.info(f'Searching {path_to_list} for files...')
    output_list = []
    for root, d_names, file_list in walk(path_to_list):
        LOGGER.info(f'root: {root}, dirs: {d_names}, files: {file_list}')
        if len(file_list) > 0:
            for file in file_list:
                if file.endswith('.parquet'):
                    output_list.append(join(root, file))
    LOGGER.info(f'{len(output_list)} Parquet Files found at path {path_to_list}.')
    return output_list

def format_taxi_df_to_records(raw_df: pd.DataFrame()):
    """
    Format input dataframe of taxi trip records for Mongodb load
    """
    LOGGER.info('format data for Mongo Load')
    date_cols = [col for col in raw_df.columns if raw_df[col].dtype == 'datetime64[ns]']
    for col in date_cols:
        raw_df[col] = raw_df[col].dt.strftime("%Y-%m-%dT%H:%M:%S.000")
    point_cols = ['pickup_centroid_location', 'dropoff_centroid_location']
    LOGGER.info(f'Data reformatting successful')
    raw_df.rename(columns={'trip_id':'_id'}, inplace=True)
    records = raw_df.to_dict(orient='records')
    for record in records:
        for col in point_cols:
            if record[col] is not None:
                record[col]['coordinates'] = record[col]['coordinates'].tolist()
    return records

def sq_ft_to_sq_km(x: float) -> float:
    """
    Function to convert an input value from ft^2 to km^2
    Args:
        - x: Float value to convert
    Returns
        - x_sqkm: x converted to Square Kilometers

    """
    conv_factor = 0.000000092903
    x_sqkm = x * conv_factor
    return x_sqkm

def format_community_area_data(areas_df: pd.DataFrame) -> pd.DataFrame():
    """ 
    Function to retrieve and format Community Area data 
    from the city of chicago
    """
    areas_df =  areas_df[['AREA_NUMBE', 'COMMUNITY', 'SHAPE_AREA']]
    areas_df.rename(columns={'AREA_NUMBE': 'community_area_id',
                             'COMMUNITY':'community_area_name',
                             'SHAPE_AREA':'community_area_size'},
                    inplace=True)
    areas_df['community_area_size'] = areas_df['community_area_size'].apply(lambda x: sq_ft_to_sq_km(x))
    return areas_df


