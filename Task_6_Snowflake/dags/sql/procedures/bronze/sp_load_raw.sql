USE DATABASE AIRLINE_DWH;
USE SCHEMA RAW_DATA;

CREATE OR REPLACE PROCEDURE SP_LOAD_RAW(FILE_NAME VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    rows_loaded INT;
BEGIN
    BEGIN TRANSACTION;

    -- Snowflake puts 18 columns from file into 18 colimns of table
    -- and 19th columns(ingestion_time) will be filled automatically
    EXECUTE IMMEDIATE 'COPY INTO AIRLINE_DWH.RAW_DATA.AIRLINE_RAW 
                       FROM @AIRLINE_DWH.RAW_DATA.MY_INTERNAL_STAGE/' || :FILE_NAME || ' 
                       FILE_FORMAT = (
                            FORMAT_NAME = AIRLINE_DWH.RAW_DATA.MY_CSV_FORMAT
                            ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE 
                       ) 
                       ON_ERROR = ''ABORT_STATEMENT'' 
                       FORCE = TRUE'; 
    
    rows_loaded := SQLROWCOUNT;

    INSERT INTO AIRLINE_DWH.UTILS.ETL_LOGS (process_name, layer, rows_affected, status)
    VALUES ('SP_LOAD_RAW', 'BRONZE', :rows_loaded, 'SUCCESS');

    COMMIT;

    RETURN 'Loaded Raw Rows: ' || :rows_loaded;

END;
$$;