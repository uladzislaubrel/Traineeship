import pandas as pd
from datetime import datetime
from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.providers.mongo.hooks.mongo import MongoHook
import os



FINAL_PATH = '/opt/airflow/data/processed_data.csv'
MY_DATASET = Dataset(FINAL_PATH)
print(os.path.exists(FINAL_PATH))
print(os.path.getsize(FINAL_PATH))
@dag(dag_id='load_to_mongo',
     start_date=datetime(2026,1,26),
     schedule=[MY_DATASET],
     catchup=False,
     tags=['mongodb'])
def load_data_to_mongo():
    
    @task()
    def upload_data():
        df = pd.read_csv(FINAL_PATH)
        records = df.to_dict('records')
  
        if not records:
            print('File is empty')
            return
        
        hook = MongoHook(conn_id="mongo_default")
        
        client = hook.get_conn()
        try:
            db = client['airflow_db']
            collection = db['cleaned_records']

            collection.delete_many({})
            collection.insert_many(records)
            print(f'Loading ended. Added {len(records)} records')
        finally:
            client.close()

    upload_data()

load_data_to_mongo()


       
          




