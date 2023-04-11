DROP TABLE IF EXISTS {{ params.psql_staging_table }};

SELECT pg_sleep(60);

CREATE TABLE IF NOT EXISTS {{ params.psql_staging_table }}(
    trip_id TEXT PRIMARY KEY,
    taxi_id TEXT,
    trip_start_timestamp TIMESTAMP,
    trip_end_timestamp TIMESTAMP,
    trip_seconds INT,
    trip_miles FLOAT(2),
    pickup_census_tract TEXT,
    dropoff_census_tract TEXT,
    pickup_community_area SMALLINT,
    dropoff_community_area SMALLINT,
    fare MONEY,
    tips MONEY,
    tolls MONEY,
    extras MONEY,
    trip_total MONEY,
    payment_type TEXT,
    company TEXT,
    pickup_centroid_latitude FLOAT(8),
    pickup_centroid_longitude FLOAT(8),
    pickup_centroid_location POINT,
    dropoff_centroid_latitude FLOAT(8),
    dropoff_centroid_longitude FLOAT(8),
    dropoff_centroid_location POINT
);

CREATE TABLE IF NOT EXISTS {{ params.psql_prd_table }}(
    trip_id TEXT PRIMARY KEY,
    taxi_id TEXT,
    trip_start_timestamp TIMESTAMP,
    trip_end_timestamp TIMESTAMP,
    trip_seconds INT,
    trip_miles FLOAT(2),
    pickup_census_tract TEXT,
    dropoff_census_tract TEXT,
    pickup_community_area SMALLINT,
    dropoff_community_area SMALLINT,
    fare MONEY,
    tips MONEY,
    tolls MONEY,
    extras MONEY,
    trip_total MONEY,
    payment_type TEXT,
    company TEXT,
    pickup_centroid_latitude FLOAT(8),
    pickup_centroid_longitude FLOAT(8),
    pickup_centroid_location POINT,
    dropoff_centroid_latitude FLOAT(8),
    dropoff_centroid_longitude FLOAT(8),
    dropoff_centroid_location POINT
);