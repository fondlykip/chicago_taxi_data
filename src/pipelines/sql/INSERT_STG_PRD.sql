INSERT INTO {{ params.psql_prd_table }} 
    SELECT * FROM {{ params.psql_staging_table }}
    ON CONFLICT (trip_id) DO NOTHING;

/* DROP TABLE {{ params.psql_staging_table }}; */