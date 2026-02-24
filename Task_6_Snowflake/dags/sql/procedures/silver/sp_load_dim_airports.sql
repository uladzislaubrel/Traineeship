CREATE OR REPLACE PROCEDURE AIRLINE_DWH.SILVER_DATA.SP_LOAD_DIM_AIRPORTS()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    rows_affected INT;
BEGIN
    BEGIN TRANSACTION;

    IF (SYSTEM$STREAM_HAS_DATA('AIRLINE_DWH.RAW_DATA.STREAM_FOR_AIRPORTS') = FALSE) THEN
        COMMIT;
        RETURN 'No data';
    END IF;

    MERGE INTO AIRLINE_DWH.SILVER_DATA.DIM_AIRPORTS Target
    USING (
        SELECT DISTINCT airport_name, country_name, airport_continent 
        FROM AIRLINE_DWH.RAW_DATA.STREAM_FOR_AIRPORTS 
        WHERE METADATA$ACTION = 'INSERT' AND airport_name IS NOT NULL
    ) Source
    ON Target.airport_name = Source.airport_name
    WHEN NOT MATCHED THEN
        INSERT (airport_name, country_name, continent) 
        VALUES (Source.airport_name, Source.country_name, Source.airport_continent);

    rows_affected := SQLROWCOUNT;

    INSERT INTO AIRLINE_DWH.UTILS.ETL_LOGS (process_name, layer, rows_affected, status)
    VALUES ('SP_LOAD_DIM_AIRPORTS', 'SILVER', :rows_affected, 'SUCCESS');

    COMMIT;
    RETURN 'Loaded Airports: ' || :rows_affected;

END;
$$;