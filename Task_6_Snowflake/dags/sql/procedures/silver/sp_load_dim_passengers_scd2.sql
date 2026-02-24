CREATE OR REPLACE PROCEDURE AIRLINE_DWH.SILVER_DATA.SP_LOAD_DIM_PASSENGERS_SCD2_MERGE()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    rows_affected INT := 0;
    run_timestamp TIMESTAMP := CURRENT_TIMESTAMP();
BEGIN
    BEGIN TRANSACTION;

    -- 1. If there is no data - exit
    IF (SYSTEM$STREAM_HAS_DATA('AIRLINE_DWH.RAW_DATA.STREAM_FOR_PASSENGERS') = FALSE) THEN
        COMMIT;
        RETURN 'No new data found';
    END IF;

    -- 2. MERGE
    MERGE INTO AIRLINE_DWH.SILVER_DATA.DIM_PASSENGERS T
    USING (
        WITH STREAM_DATA AS (
            SELECT DISTINCT
                passenger_id,
                first_name,
                last_name,
                gender,
                TRY_TO_NUMBER(age) AS age,
                nationality,
                HASH(first_name, last_name, nationality) AS new_row_hash
            FROM AIRLINE_DWH.RAW_DATA.STREAM_FOR_PASSENGERS
            WHERE METADATA$ACTION = 'INSERT'
        )
        
        -- Part 1 - rows for update(were changed recently)
        SELECT 
            src.passenger_id AS merge_key, -- ID for compare with Target
            src.*
        FROM STREAM_DATA src
        JOIN AIRLINE_DWH.SILVER_DATA.DIM_PASSENGERS tgt 
          ON src.passenger_id = tgt.passenger_id 
         AND tgt.is_current = TRUE 
         AND src.new_row_hash <> tgt.row_hash

        UNION ALL

        -- Part 2 - Rows for INSERT (new + new versions of updated rows)
        SELECT 
            NULL AS merge_key, -- synthetic NULL, for MERGE doesn't find coincidence
            src.*
        FROM STREAM_DATA src
    ) S
    ON T.passenger_id = S.merge_key -- match targer and source

    -- Step 1: If key matches (part 1) -> close old row
    WHEN MATCHED THEN
        UPDATE SET 
            T.is_current = FALSE,
            T.valid_to = :run_timestamp

    -- Step 2: If key doesn't match(part 2, bc of merge_key = NULL) -> insert new row
    WHEN NOT MATCHED THEN
        INSERT (
            passenger_id, first_name, last_name, gender, age, nationality, 
            row_hash, valid_from, valid_to, is_current
        )
        VALUES (
            S.passenger_id, S.first_name, S.last_name, S.gender, S.age, S.nationality,
            S.new_row_hash, 
            :run_timestamp, 
            NULL, 
            TRUE
        );

    -- 3. number of affected rows (update+ insert)
    rows_affected := SQLROWCOUNT;

    -- 4. Логируем
    INSERT INTO AIRLINE_DWH.UTILS.ETL_LOGS (process_name, layer, rows_affected, status, error_message)
    VALUES ('SP_LOAD_DIM_PASSENGERS_SCD2_MERGE', 'SILVER', :rows_affected, 'SUCCESS', 
            'Total rows affected (Merged): ' || :rows_affected);

    COMMIT;
    RETURN 'Done. Total rows affected: ' || :rows_affected;

END;
$$;