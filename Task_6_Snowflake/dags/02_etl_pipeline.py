import os
import shutil
import glob
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator


INPUT_DIR = '/opt/airflow/data/input'
ARCHIVE_DIR = '/opt/airflow/data/archive'

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def check_for_file(**kwargs):
    search_pattern = os.path.join(INPUT_DIR, '*.csv')
    files = glob.glob(search_pattern)
    
    if not files:
        print("There aren't new files. Pass.")
        return False # ShortCircuitOperator stop DAG here
        
    files.sort(key=os.path.getmtime)
    
    # get name of the first file
    first_file = os.path.basename(files[0])
    print(f"File to processing: {first_file}")
    
    return first_file


def archive_processed_file(ti, **kwargs):
    filename = ti.xcom_pull(task_ids='check_and_get_file')
    
    if not filename:
        raise ValueError("There is not filename in XCom!")

    source_path = os.path.join(INPUT_DIR, filename)
    
    name, ext = os.path.splitext(filename)
    current_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    new_filename = f"{name}_processed_{current_timestamp}{ext}"
    
    archive_path = os.path.join(ARCHIVE_DIR, new_filename)
    
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    
    shutil.move(source_path, archive_path)
    print(f"File was moved to: {archive_path}")


with DAG(
    dag_id='02_etl_pipeline_incremental',
    default_args=default_args,
    start_date=datetime(2026, 2, 24),
    schedule_interval='*/5 * * * *', # runs every 5 minutes
    catchup=False,
    max_active_runs=1, # Only one active run, bc of runnings can't process the same file parallel
    tags=['etl', 'airline', 'production']
) as dag:

    # 1. check folder and get file
    get_file = ShortCircuitOperator(
        task_id='check_and_get_file',
        python_callable=check_for_file
    )

    # 2. STAGE: Get file name dynamically using Jinja XCom Pull
    upload_to_stage = SQLExecuteQueryOperator(
        task_id='upload_to_stage',
        conn_id='snowflake_default',
        # ti.xcom_pull take file before start task
        sql=f"PUT 'file://{INPUT_DIR}/{{{{ ti.xcom_pull(task_ids='check_and_get_file') }}}}' @AIRLINE_DWH.RAW_DATA.MY_INTERNAL_STAGE AUTO_COMPRESS=TRUE OVERWRITE=TRUE;"
    )

    # 3. BRONZE: Send file name .gz for Snowflake
    load_bronze = SQLExecuteQueryOperator(
        task_id='load_bronze_raw',
        conn_id='snowflake_default',
        sql="CALL AIRLINE_DWH.RAW_DATA.SP_LOAD_RAW('{{ ti.xcom_pull(task_ids='check_and_get_file') }}.gz');"
    )

    # 4. SILVER: Dimensions 
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

    # 5. SILVER: Facts
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

    # 7. UTILS
    archive_file = PythonOperator(
        task_id='archive_processed_file',
        python_callable=archive_processed_file
    )


    get_file >> upload_to_stage >> load_bronze
    load_bronze >> [load_dim_passengers, load_dim_pilots, load_dim_airports]
    [load_dim_passengers, load_dim_pilots, load_dim_airports] >> load_facts
    load_facts >> load_gold
    load_gold >> archive_file 