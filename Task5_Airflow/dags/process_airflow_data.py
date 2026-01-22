from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow import Dataset 
import pandas as pd
import re
from datetime import datetime

# Определяем датасет для связи DAG-ов
MY_DATASET = Dataset("/opt/airflow/dags/processed_data.csv")

def check_file_size():
    import os
    path = '/opt/airflow/dags/data.csv'
    if os.path.exists(path) and os.path.getsize(path) > 0:
        return 'processing_tasks.replace_nulls' # Переход в TaskGroup
    return 'log_empty'

def replace_nulls():
    df = pd.read_csv('/opt/airflow/dags/data.csv')
    df = df.fillna("-")
    df.to_csv('/opt/airflow/dags/temp_data.csv', index=False)

def sort_data():
    df = pd.read_csv('/opt/airflow/dags/temp_data.csv')
    df['created_date'] = pd.to_datetime(df['created_date'])
    df = df.sort_values(by='created_date')
    df.to_csv('/opt/airflow/dags/temp_data.csv', index=False)

def clean_content():
    df = pd.read_csv('/opt/airflow/dags/temp_data.csv')
    # Regex: оставляем только буквы, цифры и пунктуацию
    df['content'] = df['content'].apply(lambda x: re.sub(r'[^\w\s.,!?;:-]', '', str(x)))
    # Сохраняем финальный файл, который триггернет 2-й DAG
    df.to_csv('/opt/airflow/dags/processed_data.csv', index=False)

with DAG(
    'process_airflow_data',
    start_date=datetime(2023, 1, 1),
    schedule=None, 
    catchup=False
) as dag:

    wait_for_file = FileSensor(
        task_id='wait_for_file',
        filepath='data.csv',
        fs_conn_id='fs_default',
        poke_interval=10
    )

    check_file = BranchPythonOperator(
        task_id='check_if_empty',
        python_callable=check_file_size
    )

    log_empty = BashOperator(
        task_id='log_empty',
        bash_command='echo "File is empty, skipping..."'
    )

    with TaskGroup('processing_tasks') as processing_tasks:
        t1 = PythonOperator(task_id='replace_nulls', python_callable=replace_nulls)
        t2 = PythonOperator(task_id='sort_data', python_callable=sort_data)
        t3 = PythonOperator(
            task_id='clean_content', 
            python_callable=clean_content,
            outlets=[MY_DATASET] # Говорим, что обновили датасет
        )
        t1 >> t2 >> t3

    wait_for_file >> check_file >> [log_empty, processing_tasks]


