import pandas as pd
import src.pipelines.etl_functions as etlf
from src.pipelines.etl_functions import (convert_point,
                                         convert_point_cols,)

def test_convert_point():
    test_point = {'type':'Point', 'coordinates': [33.44, 11.22]}
    expected_out = "33.44,11.22"
    actual_out = convert_point(test_point)
    assert expected_out == actual_out

def test_convert_point_cols():
    cols = ['id', 'point1', 'point2']
    input_data = [['x', {'type':'Point', 'coordinates': [33.44, 11.22]},
                   {'type':'Point', 'coordinates': [44.55, 11.22]}],
                 ['y', {'type':'Point', 'coordinates': [66.77, 44.55]},
                  {'type':'Point', 'coordinates': [77.88, 55.66]}]]
    expected_data = [['x', "33.44,11.22", "44.55,11.22"],
                     ['y', "66.77,44.55", "77.88,55.66"]]
    input_df = pd.DataFrame(data=input_data,
                            columns=cols)
    exp_out_df = pd.DataFrame(data=expected_data,
                              columns=cols)
    out_df = convert_point_cols(input_df, ['point1', 'point2'])
    pd.testing.assert_frame_equal(exp_out_df, out_df)


def test_generate_query():
    backfill = True
    year = 2023
    month = 1
    filter_col = 'trip_start_timestamp'
    exp_output = 'trip_start_timestamp > "2023-1-01T00:00:00"'
    output = etlf.generate_query(backfill,
                                 year, month,
                                 filter_col)
    assert output == exp_output
    backfill = False
    exp_output = 'trip_start_timestamp between "2023-1-01T00:00:00" and "2023-1-31T23:59:59"'
    output = etlf.generate_query(backfill,
                                 year, month,
                                 filter_col)
    assert output == exp_output

