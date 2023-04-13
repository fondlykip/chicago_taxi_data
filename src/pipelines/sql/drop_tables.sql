DROP TABLE IF EXISTS {{params.psql_date_dim_table}};
DROP TABLE IF EXISTS {{params.psql_trip_fact_table}};
DROP TABLE IF EXISTS {{params.psql_comm_area_table}};

COMMIT;