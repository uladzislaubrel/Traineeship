CREATE OR REPLACE PROCEDURE AIRLINE_DWH.GOLD_DATA.SP_REFRESH_GOLD_REPORT()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    rows_affected INT;
BEGIN
    BEGIN TRANSACTION;
    CREATE OR REPLACE TABLE AIRLINE_DWH.GOLD_DATA.FLIGHTS_BY_COUNTRY_REPORT AS
    SELECT 
        A.country_name,
        COUNT(F.flight_sk) as total_flights,
        COUNT(DISTINCT F.pilot_sk) as distinct_pilots,
        MAX(F.departure_date) as last_flight_date
    FROM AIRLINE_DWH.SILVER_DATA.FACT_AIRLINE F
    JOIN AIRLINE_DWH.SILVER_DATA.DIM_AIRPORTS A ON F.airport_sk = A.airport_sk
    GROUP BY A.country_name
    ORDER BY total_flights DESC;

    rows_affected := SQLROWCOUNT;

    INSERT INTO AIRLINE_DWH.UTILS.ETL_LOGS (process_name, layer, rows_affected, status)
    VALUES ('SP_REFRESH_GOLD_REPORT', 'GOLD', :rows_affected, 'SUCCESS');

    COMMIT;
    RETURN 'Gold Report Refreshed. Rows: ' || :rows_affected;

END;
$$;