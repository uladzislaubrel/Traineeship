-- Imitation of losting data: delete aall pilots with name John
DELETE FROM AIRLINE_DWH.SILVER_DATA.DIM_PILOTS WHERE pilot_name LIKE '%John%';

-- Show data 2 minutes ago(before DELETE)
SELECT * FROM AIRLINE_DWH.SILVER_DATA.DIM_PILOTS AT(OFFSET => -120)
WHERE pilot_name LIKE '%John%';

-- Restore deleted rows from the past (insert deleted)
INSERT INTO AIRLINE_DWH.SILVER_DATA.DIM_PILOTS
SELECT * FROM AIRLINE_DWH.SILVER_DATA.DIM_PILOTS AT(OFFSET => -120)
WHERE pilot_name LIKE '%John%';