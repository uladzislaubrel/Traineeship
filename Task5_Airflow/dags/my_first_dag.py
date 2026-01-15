from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Описываем логику (что именно делать)
def hello_function():
    print("Привет! Мой DAG работает!")

# Описываем сам DAG (инструкция для Airflow)
with DAG(
    dag_id='my_python_dag',      # Имя, которое будет в интерфейсе
    start_date=datetime(2025, 1, 1),
    schedule=None,      # Запуск только кнопкой
    catchup=False
) as dag:

    # Создаем задачу
    task1 = PythonOperator(
        task_id='say_hello_task',
        python_callable=hello_function
    )