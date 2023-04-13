CREATE TABLE IF NOT EXISTS {{ params.comm_area_table }}(
    community_area_id SMALLINT PRIMARY KEY,
    community_area_name TEXT,
    community_area_size BIGINT
);

CREATE TABLE IF NOT EXISTS {{ params.trip_table }}(
    trip_id TEXT PRIMARY KEY,
    taxi_id TEXT,
    trip_start_timestamp TIMESTAMP,
    trip_start_date_id TEXT references {{ params.date_dim }},
    trip_start_time TIME,
    trip_end_timestamp TIMESTAMP,
    trip_end_date_id TEXT references {{ params.date_dim }},
    trip_end_time TIME,
    trip_seconds INT,
    trip_miles FLOAT(2),
    pickup_census_tract TEXT,
    dropoff_census_tract TEXT,
    pickup_community_area SMALLINT references {{ params.comm_area_table }}(community_area_id),
    dropoff_community_area SMALLINT references {{ params.comm_area_table }}(community_area_id),
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


