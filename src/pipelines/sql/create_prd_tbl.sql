CREATE TABLE IF NOT EXISTS {{ params.psql_comm_area_table }}(
    community_area_id SMALLINT PRIMARY KEY,
    community_area_name TEXT,
    community_area_size BIGINT
);

CREATE TABLE IF NOT EXISTS {{ params.psql_trip_fact_table }}(
    trip_id CHAR(40) PRIMARY KEY,
    taxi_id CHAR(128),
    trip_start_timestamp TIMESTAMP,
    trip_start_date_id INT references {{ params.psql_date_dim_table }},
    trip_start_time TIME,
    trip_end_timestamp TIMESTAMP,
    trip_end_date_id INT references {{ params.psql_date_dim_table }},
    trip_end_time TIME,
    trip_seconds INT,
    trip_miles FLOAT(2),
    pickup_census_tract BIGINT,
    dropoff_census_tract BIGINT,
    pickup_community_area SMALLINT references {{ params.psql_comm_area_table }}(community_area_id),
    dropoff_community_area SMALLINT references {{ params.psql_comm_area_table }}(community_area_id),
    fare NUMERIC,
    tips NUMERIC,
    tolls NUMERIC,
    extras NUMERIC,
    trip_total NUMERIC,
    payment_type VARCHAR(11),
    company TEXT,
    pickup_centroid_latitude FLOAT(8),
    pickup_centroid_longitude FLOAT(8),
    pickup_centroid_location POINT,
    dropoff_centroid_latitude FLOAT(8),
    dropoff_centroid_longitude FLOAT(8),
    dropoff_centroid_location POINT
);


