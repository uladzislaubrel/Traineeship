USE DATABASE AIRLINE_DWH;
USE SCHEMA SILVER_DATA;

-- 1. DIM_PASSENGERS (SCD Type 2)
-- Store history of changing passenger data
CREATE OR REPLACE TABLE DIM_PASSENGERS (
    passenger_sk NUMBER IDENTITY(1,1), -- unique number of raw in base
    passenger_id VARCHAR,              -- business-key
    
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    age INT,
    nationality VARCHAR,
    
    row_hash VARCHAR,                  -- MD5(name + last name + nationality)
    
    -- fields SCD2
    valid_from TIMESTAMP_NTZ,
    valid_to TIMESTAMP_NTZ,
    is_current BOOLEAN,
    
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


-- 2. DIM_PILOTS (scd1)
CREATE OR REPLACE TABLE DIM_PILOTS (
    pilot_sk NUMBER IDENTITY(1,1),
    pilot_name VARCHAR,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 3. DIM_AIRPORTS (scd1)
CREATE OR REPLACE TABLE DIM_AIRPORTS (
    airport_sk NUMBER IDENTITY(1,1),
    airport_name VARCHAR,
    country_name VARCHAR,
    continent VARCHAR,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 4. FACT_AIRLINE
CREATE OR REPLACE TABLE FACT_AIRLINE (
    flight_sk NUMBER IDENTITY(1,1),
    passenger_sk NUMBER,             
    pilot_sk NUMBER,             
    airport_sk NUMBER,               
    
    departure_date DATE,
    flight_status VARCHAR,
    ticket_type VARCHAR,
    
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);