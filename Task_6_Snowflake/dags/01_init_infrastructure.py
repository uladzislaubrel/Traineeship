from airflow import DAG
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
import os


# path to SQL folder
SQL_PATH = '/opt/airflow/dags/sql/initial'

def read_sql(filename):
    path = os.path.join(SQL_PATH, filename)
    with open(path, 'r') as file:
        return file.read()

with DAG(
    dag_id='01_init_infrastructure',
    start_date=datetime(2026, 2, 10),
    schedule_interval=None,
    catchup=False,
    tags=['setup', 'airline']
) as dag:
    
    # 0 Create warehouse
    create_wh = SQLExecuteQueryOperator(
        task_id='00_create_warehouse',
        conn_id='snowflake_default',
        sql=read_sql('00_warehouse.sql')
    )

    # 1 Create base
    create_db = SQLExecuteQueryOperator(
        task_id='01_create_database',
        conn_id='snowflake_default',
        sql=read_sql('01_database.sql')
    )

    # 2 Create schemas
    create_schemas = SQLExecuteQueryOperator(
        task_id='02_create_schemas',
        conn_id='snowflake_default',
        sql=read_sql('02_schemas.sql')
    )

    # 3. Formatsand Stage
    create_formats = SQLExecuteQueryOperator(
        task_id='03_create_formats_stages',
        conn_id='snowflake_default',
        sql=read_sql('03_formats_stages.sql')
    )

    # 4 Bronze layer
    create_bronze = SQLExecuteQueryOperator(
        task_id='04_create_bronze_layer',
        conn_id='snowflake_default',
        sql=read_sql('04_bronze_tables.sql')
    )

    # 5 Silver layer
    create_silver = SQLExecuteQueryOperator(
        task_id='05_create_silver_layer',
        conn_id='snowflake_default',
        sql=read_sql('05_silver_tables.sql')
    )

    # 6 Audit(gold) layer
    create_audit = SQLExecuteQueryOperator(
        task_id='06_create_audit_table',
        conn_id='snowflake_default',
        sql=read_sql('06_audit_table.sql')
    )

    # 7. create Bronze procedure
    create_sp_raw = SQLExecuteQueryOperator(
        task_id='07_sp_raw',
        conn_id='snowflake_default',
        sql=read_sql('../procedures/bronze/sp_load_raw.sql')
    )
    
    # 8. Procedurees (Silver SCD2)
    create_sp_passengers = SQLExecuteQueryOperator(
        task_id='08_create_sp_passengers',
        conn_id='snowflake_default',
        sql=read_sql('../procedures/silver/sp_load_dim_passengers_scd2.sql') 
    )

    create_sp_pilots = SQLExecuteQueryOperator(
        task_id='09_sp_pilots',
        conn_id='snowflake_default',
        sql=read_sql('../procedures/silver/sp_load_dim_pilots.sql')
    )

    create_sp_airports = SQLExecuteQueryOperator(
        task_id='10_sp_airports',
        conn_id='snowflake_default',
        sql=read_sql('../procedures/silver/sp_load_dim_airports.sql')
    )
    
    create_sp_facts = SQLExecuteQueryOperator(
        task_id='11_sp_facts',
        conn_id='snowflake_default',
        sql=read_sql('../procedures/silver/sp_load_fact_airline.sql')
    )

    # 9. create gold procedure
    create_sp_gold = SQLExecuteQueryOperator(
        task_id='12_sp_gold',
        conn_id='snowflake_default',
        sql=read_sql('../procedures/gold/sp_refresh_gold_report.sql')
    )

    # 10. Security (RLS & Views)
    create_security = SQLExecuteQueryOperator(
        task_id='13_create_security',
        conn_id='snowflake_default',
        sql=read_sql('07_security_and_views.sql')
    )


    create_db >> create_schemas >> create_formats
    create_formats >> create_bronze >> create_silver >> create_audit
    create_audit >> create_sp_raw >> create_sp_passengers
    create_sp_passengers >> create_sp_pilots >> create_sp_airports >> create_sp_facts
    create_sp_facts >> create_sp_gold >> create_security