USE DATABASE AIRLINE_DWH;
USE SCHEMA RAW_DATA;

-- Raw Data
CREATE OR REPLACE TABLE AIRLINE_RAW (
    row_id VARCHAR,
    passenger_id VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    age VARCHAR,
    nationality VARCHAR,
    airport_name VARCHAR,
    airport_country_code VARCHAR,
    country_name VARCHAR,
    airport_continent VARCHAR,
    continents VARCHAR,
    departure_date VARCHAR,
    arrival_airport VARCHAR,
    pilot_name VARCHAR,
    flight_status VARCHAR,
    ticket_type VARCHAR,
    passenger_status VARCHAR,
    ingestion_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Stream, Fan-out pattern
CREATE OR REPLACE STREAM STREAM_FOR_PASSENGERS ON TABLE AIRLINE_RAW;
CREATE OR REPLACE STREAM STREAM_FOR_PILOTS     ON TABLE AIRLINE_RAW;
CREATE OR REPLACE STREAM STREAM_FOR_AIRPORTS   ON TABLE AIRLINE_RAW;
CREATE OR REPLACE STREAM STREAM_FOR_FACTS      ON TABLE AIRLINE_RAW;