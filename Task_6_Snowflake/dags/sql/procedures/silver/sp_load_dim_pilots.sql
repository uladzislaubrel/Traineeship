CREATE OR REPLACE PROCEDURE AIRLINE_DWH.SILVER_DATA.SP_LOAD_DIM_PILOTS()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    rows_affected INT;
BEGIN
    BEGIN TRANSACTION;
    -- read from stream
    IF (SYSTEM$STREAM_HAS_DATA('AIRLINE_DWH.RAW_DATA.STREAM_FOR_PILOTS') = FALSE) THEN
        COMMIT;
        RETURN 'No data';
    END IF;

    -- MERGE (insert or ignore)
    MERGE INTO AIRLINE_DWH.SILVER_DATA.DIM_PILOTS Target
    USING (
        SELECT DISTINCT pilot_name 
        FROM AIRLINE_DWH.RAW_DATA.STREAM_FOR_PILOTS 
        WHERE METADATA$ACTION = 'INSERT' AND pilot_name IS NOT NULL
    ) Source
    ON Target.pilot_name = Source.pilot_name
    WHEN NOT MATCHED THEN
        INSERT (pilot_name) VALUES (Source.pilot_name);

    rows_affected := SQLROWCOUNT;

    INSERT INTO AIRLINE_DWH.UTILS.ETL_LOGS (process_name, layer, rows_affected, status)
    VALUES ('SP_LOAD_DIM_PILOTS', 'SILVER', :rows_affected, 'SUCCESS');

    COMMIT;
    RETURN 'Loaded Pilots: ' || :rows_affected;

END;
$$;