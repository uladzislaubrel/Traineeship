CREATE OR REPLACE PROCEDURE AIRLINE_DWH.SILVER_DATA.SP_LOAD_DIM_PASSENGERS_SCD2()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    rows_inserted INT := 0;
    rows_updated INT := 0;
BEGIN
    BEGIN TRANSACTION;
    -- 1. if there is not new data - doing nothing
    IF (SYSTEM$STREAM_HAS_DATA('AIRLINE_DWH.RAW_DATA.STREAM_FOR_PASSENGERS') = FALSE) THEN
        COMMIT;
        RETURN 'No new data found';
    END IF;

    -- 2. create new temp table (STAGING)
    -- immediately calculate new hash for new raws
    CREATE OR REPLACE TEMPORARY TABLE T_PASSENGERS_STG AS
    SELECT DISTINCT
        passenger_id,
        first_name,
        last_name,
        gender,
        TRY_TO_NUMBER(age) as age,
        nationality,
        -- calculate hash of important/changeable colimns
        MD5(CONCAT(
            IFNULL(first_name, ''), 
            IFNULL(last_name, ''), 
            IFNULL(nationality, '')
        )) as new_row_hash,
        ingestion_time
    FROM AIRLINE_DWH.RAW_DATA.STREAM_FOR_PASSENGERS
    WHERE METADATA$ACTION = 'INSERT';

    -- 3. step 1 (UPDATE): close old raws if data has updated
    -- If ID is similar, but hashes are different- it means person updated his data
    UPDATE AIRLINE_DWH.SILVER_DATA.DIM_PASSENGERS T
    SET 
        is_current = FALSE,
        valid_to = CURRENT_TIMESTAMP()
    FROM T_PASSENGERS_STG S
    WHERE T.passenger_id = S.passenger_id   -- same person
      AND T.is_current = TRUE               -- last current version(raw)
      AND T.row_hash <> S.new_row_hash;     -- comparasing hashes

    rows_updated := SQLROWCOUNT; -- write number of update

    -- 4. step 2 (INSERT): 
    -- insert, if:
    -- a) There wasn't this person(new)
    -- b) Person was here, but he was closed recently(updated)
    INSERT INTO AIRLINE_DWH.SILVER_DATA.DIM_PASSENGERS (
        passenger_id, first_name, last_name, gender, age, nationality, 
        row_hash, valid_from, valid_to, is_current
    )
    SELECT 
        S.passenger_id, S.first_name, S.last_name, S.gender, S.age, S.nationality,
        S.new_row_hash,      -- write hash to base
        CURRENT_TIMESTAMP(), -- actions from now
        NULL,                -- action for eternal
        TRUE                 -- actual
    FROM T_PASSENGERS_STG S
    LEFT JOIN AIRLINE_DWH.SILVER_DATA.DIM_PASSENGERS T 
        ON S.passenger_id = T.passenger_id 
        AND T.is_current = TRUE
        AND T.row_hash = S.new_row_hash -- looking for complete match
    WHERE T.passenger_id IS NULL; -- insert just these, which doesn't found(either completely new, or updated)

    rows_inserted := SQLROWCOUNT;

    -- 5. logging
    INSERT INTO AIRLINE_DWH.UTILS.ETL_LOGS (process_name, layer, rows_affected, status, error_message)
    VALUES ('SP_LOAD_DIM_PASSENGERS_SCD2', 'SILVER', :rows_inserted + :rows_updated, 'SUCCESS', 
            'Inserted: ' || :rows_inserted || ', Updated (Closed): ' || :rows_updated);
    COMMIT;
    RETURN 'Done. Inserted: ' || :rows_inserted || ', Updated: ' || :rows_updated;

END;
$$;

