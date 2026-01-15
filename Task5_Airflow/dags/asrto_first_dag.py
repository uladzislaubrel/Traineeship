from airflow.sdk import dag, task
from datetime import datetime


@dag(schedule =None,
     catchup=False,
     start_date=datetime(2026, 1, 9),
     description='DAG for check data',
     tags='Data engineering')
def check_dag():

    @task.bash
    def create_file():
        return 'echo "Hi there uipak" >/tmp/dummy'
    
    @task.bash
    def check_file_exists():
        return 'test -f /tmp/dummy'

    @task
    def read_file():
        print(open('/tmp/dummy', 'rb').read())

    create_file() >> check_file_exists() >> read_file()

check_dag()

