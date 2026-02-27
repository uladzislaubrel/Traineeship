from airflow.decorators import dag, task, task_group
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.datasets import Dataset
import pandas as pd
import re
from datetime import datetime
import os


FILE_PATH = '/opt/airflow/data/tiktok_google_play_reviews.csv'
TEMP_PATH = '/opt/airflow/data/temp_data.csv'
FINAL_PATH = '/opt/airflow/data/processed_data.csv'
MY_DATASET = Dataset(FINAL_PATH)

@dag(dag_id='file_working',
    start_date=datetime(2026, 1, 20),
    schedule=None, 
    catchup=False)
def file_working():

    wait_for_file = FileSensor(task_id='wait_for_file',
                               filepath='tiktok_google_play_reviews.csv',
                               fs_conn_id='fs_default',
                               poke_interval=10)
    
    @task.branch(task_id='check_file_size')
    def check_file_size():
        if os.path.exists(FILE_PATH) and os.path.getsize(FILE_PATH) > 0:
            return 'processing_tasks.replace_nulls'
        return 'log_empty'
    
    log_empty = BashOperator(task_id='log_empty',
                             bash_command='echo "File is empty, that is all"')
    
    @task_group(group_id='processing_tasks')
    def processing_tasks():

        @task()
        def replace_nulls():
            df = pd.read_csv(FILE_PATH)
            df = df.fillna('-')
            df.to_csv(TEMP_PATH, index=False)

        @task()
        def sort_date():
            df = pd.read_csv(TEMP_PATH)
            df['at']= pd.to_datetime(df['at'])
            df = df.sort_values(by='at')
            df.to_csv(TEMP_PATH, index=False)

        @task(outlets=[MY_DATASET])
        def clean_data():
            df = pd.read_csv(TEMP_PATH)
            df['content'] = df['content'].apply(lambda x: re.sub(r'[^\w\s.,!?;:-]', '', str(x)))
            df.to_csv(FINAL_PATH, index=False)
            if os.path.exists(TEMP_PATH):
                os.remove(TEMP_PATH)
        
        t1 = replace_nulls()
        t2 = sort_date()
        t3 = clean_data()
        t1 >> t2 >> t3

    check = check_file_size()
    processing = processing_tasks()
    wait_for_file >> check >> [log_empty, processing]

file_working()
    