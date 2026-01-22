import pandas as pd
import pymongo
from datetime import datetime
from airflow.decorators import dag, task
from airflow.datasets import Dataset


FINAL_PATH = '/opt/airflow/dags/processed_data.csv'
MY_DATASET = Dataset(FINAL_PATH)

@dag(dag_id='load_to_mongo',
     start_date=datetime(2026,1,21),
     schedule=[MY_DATASET],
     catchup=False,
     tags=['mongodb'])
def load_data_to_mongo():
    
    @task()
    def upload_data():
        df = pd.read_csv(FINAL_PATH)

        client = pymongo.MongoClient('mongodb://mongodb:27017/')
        db = client['airflow_db']
        collection = db['cleaned_records']

        records = df.to_dict('records')

        if records:
            result = collection.insert_many(records)
            print(f'Loading is ended. Added {len(result.inserted_ids)}')
        else:
            print('File is empty')
     
        client.close()

    upload_data()

load_data_to_mongo()


       
          




