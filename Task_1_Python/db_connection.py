import psycopg2
from psycopg2 import OperationalError

def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )
        print("Подключение к базе данных PostgreSQL успешно выполнено")
    except OperationalError as e:
        print(f"Произошла ошибка '{e}' при подключении к БД")
    return connection

DB_PARAMS = {
    "db_name": "postgres",
    "db_user": "postgres",
    "db_password": "123123POIU123",
    "db_host": "localhost",
    "db_port": "5432"
}

if __name__ == '__main__':
    conn = create_connection(**DB_PARAMS)


