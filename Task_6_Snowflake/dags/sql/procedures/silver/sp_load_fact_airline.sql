CREATE OR REPLACE PROCEDURE AIRLINE_DWH.SILVER_DATA.SP_LOAD_FACT_AIRLINE()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    rows_inserted INT;
BEGIN
    BEGIN TRANSACTION;
    IF (SYSTEM$STREAM_HAS_DATA('AIRLINE_DWH.RAW_DATA.STREAM_FOR_FACTS') = FALSE) THEN
        COMMIT;
        RETURN 'No data';
    END IF;

    -- insert facts with keys (JOIN)
    INSERT INTO AIRLINE_DWH.SILVER_DATA.FACT_AIRLINE (
        passenger_sk, pilot_sk, airport_sk, 
        departure_date, flight_status, ticket_type
    )
    SELECT 
        P.passenger_sk,
        PL.pilot_sk,
        A.airport_sk,
        TRY_TO_DATE(R.departure_date, 'MM/DD/YYYY'), -- Parse date
        R.flight_status,
        R.ticket_type
    FROM AIRLINE_DWH.RAW_DATA.STREAM_FOR_FACTS R
    -- 1. Looking fopr passenger (actual)
    LEFT JOIN AIRLINE_DWH.SILVER_DATA.DIM_PASSENGERS P 
        ON R.passenger_id = P.passenger_id 
        AND P.is_current = TRUE -- Only current version
    -- 2. Looking for pilot
    LEFT JOIN AIRLINE_DWH.SILVER_DATA.DIM_PILOTS PL 
        ON R.pilot_name = PL.pilot_name
    -- 3. Looking for airport
    LEFT JOIN AIRLINE_DWH.SILVER_DATA.DIM_AIRPORTS A 
        ON R.airport_name = A.airport_name
    WHERE R.METADATA$ACTION = 'INSERT';

    rows_inserted := SQLROWCOUNT;

    INSERT INTO AIRLINE_DWH.UTILS.ETL_LOGS (process_name, layer, rows_affected, status)
    VALUES ('SP_LOAD_FACT_AIRLINE', 'SILVER', :rows_inserted, 'SUCCESS');

    COMMIT;
    RETURN 'Loaded Facts: ' || :rows_inserted;

END;
$$;