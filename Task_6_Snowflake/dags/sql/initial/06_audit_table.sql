USE DATABASE AIRLINE_DWH;
USE SCHEMA UTILS;

CREATE TABLE IF NOT EXISTS ETL_LOGS (
    log_id NUMBER IDENTITY(1,1),
    run_id VARCHAR,              -- ID from Airflow
    process_name VARCHAR,        -- procedure name
    layer VARCHAR,               -- Bronze/Silver/Gold
    rows_affected INT,           -- how many rows inserted
    status VARCHAR,              -- SUCCESS / ERROR
    error_message VARCHAR,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);