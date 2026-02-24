from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta


LOCAL_FILENAME = 'Airline_Dataset.csv'
SNOWFLAKE_FILENAME = 'Airline_Dataset.csv.gz' 
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='02_etl_pipeline_daily',
    default_args=default_args,
    start_date=datetime(2026, 2, 18),
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'airline', 'production']
) as dag:

    wait_for_file = FileSensor(
        task_id='wait_for_input_file',
        filepath=f'/opt/airflow/data/{LOCAL_FILENAME}', 
        fs_conn_id='fs_default',
        poke_interval=10,  # check every 10 seconds
        timeout=60*10,   # Wait 10 min and then fail
        mode='poke'
    )

    # 2. STAGE: loading to internal stage
    upload_to_stage = SQLExecuteQueryOperator(
        task_id='upload_to_stage',
        conn_id='snowflake_default',
        sql=f"PUT 'file:///opt/airflow/data/{LOCAL_FILENAME}' @AIRLINE_DWH.RAW_DATA.MY_INTERNAL_STAGE AUTO_COMPRESS=TRUE OVERWRITE=TRUE;"
    )

    # 3. BRONZE: loading Raw
    load_bronze = SQLExecuteQueryOperator(
        task_id='load_bronze_raw',
        conn_id='snowflake_default',
        sql=f"CALL AIRLINE_DWH.RAW_DATA.SP_LOAD_RAW('{SNOWFLAKE_FILENAME}');"
    )

    # 4. SILVER: Dimensions/ How? parallel :)
    load_dim_passengers = SQLExecuteQueryOperator(
        task_id='load_silver_passengers',
        conn_id='snowflake_default',
        sql="CALL AIRLINE_DWH.SILVER_DATA.SP_LOAD_DIM_PASSENGERS_SCD2();"
    )

    load_dim_pilots = SQLExecuteQueryOperator(
        task_id='load_silver_pilots',
        conn_id='snowflake_default',
        sql="CALL AIRLINE_DWH.SILVER_DATA.SP_LOAD_DIM_PILOTS();"
    )

    load_dim_airports = SQLExecuteQueryOperator(
        task_id='load_silver_airports',
        conn_id='snowflake_default',
        sql="CALL AIRLINE_DWH.SILVER_DATA.SP_LOAD_DIM_AIRPORTS();"
    )

    # 5. SILVER: Facts - after dims
    load_facts = SQLExecuteQueryOperator(
        task_id='load_silver_facts',
        conn_id='snowflake_default',
        sql="CALL AIRLINE_DWH.SILVER_DATA.SP_LOAD_FACT_AIRLINE();"
    )

    # 6. GOLD: report
    load_gold = SQLExecuteQueryOperator(
        task_id='load_gold_analytics',
        conn_id='snowflake_default',
        sql="CALL AIRLINE_DWH.GOLD_DATA.SP_REFRESH_GOLD_REPORT();"
    )


    wait_for_file >> upload_to_stage >> load_bronze
    load_bronze >> [load_dim_passengers, load_dim_pilots, load_dim_airports]
    [load_dim_passengers, load_dim_pilots, load_dim_airports] >> load_facts
    load_facts >> load_gold