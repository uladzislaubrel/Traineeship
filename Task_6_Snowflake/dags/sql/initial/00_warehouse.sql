
CREATE WAREHOUSE IF NOT EXISTS AIRLINE_WH
    WITH WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60           -- sleep after 60 seconds
    AUTO_RESUME = TRUE          -- wake up itself if there is query
    INITIALLY_SUSPENDED = TRUE; -- initially it would be sleep