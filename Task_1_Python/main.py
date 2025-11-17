# Импортируем функцию подключения из нашего модуля database.connection
from db_connection import create_connection

# Импортируем функции загрузки данных из нашего модуля dataloader
from data_loader import load_json_data, insert_rooms, insert_students

# !!! ЗАМЕНИТЕ ЭТИ ПАРАМЕТРЫ НА ВАШИ !!!
DB_PARAMS = {
    "db_name": "postgres",
    "db_user": "postgres",
    "db_password": "123123POIU123",
    "db_host": "localhost",
    "db_port": "5432"
}
ROOMS_FILE = input('Введите путь для комнат: ')  #'C:\Tasks\Traineeship\Task_1_Python\rooms.json'
STUDENTS_FILE = input('Введите путь для студентов: ') #'C:\Tasks\Traineeship\Task_1_Python\students.json'


if __name__ == '__main__':
    # 1. Подключение к БД
    conn = create_connection(**DB_PARAMS)

    if conn:
        try:
            # 2. Чтение данных из JSON
            rooms_data = load_json_data(ROOMS_FILE)
            students_data = load_json_data(STUDENTS_FILE)

            if rooms_data is not None and students_data is not None:
                # 3. Загрузка данных в таблицы
                print(f"\n--- Запуск загрузки данных ---")
                insert_rooms(conn, rooms_data)
                insert_students(conn, students_data)
                print(f"\n--- Процесс загрузки данных завершен. ---")

        except Exception as e:
            print(f"Произошла непредвиденная ошибка во время выполнения: {e}")
        finally:
            # 4. Закрытие соединения
            conn.close()
            print("Соединение с БД закрыто.")
    else:
        print("Не удалось подключиться к базе данных. Загрузка данных отменена.")