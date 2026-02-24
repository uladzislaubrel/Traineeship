USE DATABASE AIRLINE_DWH;
USE SCHEMA GOLD_DATA;

-- Using Standard Edition it is impossible to create object POLICY.
-- So Row Level Security (RLS) will be in SECURE VIEW.

-- 1. Create SECURE VIEW
-- Keyword SECURE hides code of view from users
CREATE OR REPLACE SECURE VIEW REPORT_FACT_FLIGHTS_SECURE AS
SELECT 
    F.flight_sk,
    P.first_name,
    P.last_name,
    A.country_name,
    F.ticket_type,
    F.flight_status
FROM AIRLINE_DWH.SILVER_DATA.FACT_AIRLINE F
JOIN AIRLINE_DWH.SILVER_DATA.DIM_PASSENGERS P ON F.passenger_sk = P.passenger_sk
JOIN AIRLINE_DWH.SILVER_DATA.DIM_AIRPORTS A ON F.airport_sk = A.airport_sk
WHERE 
    CASE 
        WHEN CURRENT_ROLE() = 'ACCOUNTADMIN' THEN TRUE
        WHEN F.ticket_type = 'Business' THEN TRUE
        ELSE FALSE
    END;