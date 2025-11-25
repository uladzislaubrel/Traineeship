from db_connection import create_connection
from data_loader import load_json_data, insert_rooms, insert_students
from indexes import apply_indexes
from queries import get_count_students_by_room, get_less_populated_rooms, get_rooms_with_large_diff, get_rooms_with_diff_sex

from export import export_results

DB_PARAMS = {
    "db_name": "postgres",
    "db_user": "postgres",
    "db_password": "123123POIU123",
    "db_host": "localhost",
    "db_port": "5432"
}
ROOMS_FILE = input('Введите путь для комнат: ')  #'C:\Tasks\Traineeship\Task_1_Python\rooms.json'      rooms.json
STUDENTS_FILE = input('Введите путь для студентов: ') #'C:\Tasks\Traineeshirooms.jsonp\Task_1_Python\students.json'     students.json
FILE_FORMAT = input('Введите 1 для выгрузки итогов в формате JSON; 2 для выгрузки в формате XML: ')

if __name__ == '__main__':
    conn = create_connection(**DB_PARAMS)

    if conn:
        try:
            rooms_data = load_json_data(ROOMS_FILE)
            students_data = load_json_data(STUDENTS_FILE)

            if rooms_data is not None and students_data is not None:
                print(f"\nЗапуск загрузки данных")
                insert_rooms(conn, rooms_data)
                insert_students(conn, students_data)
                print(f"\nПроцесс загрузки данных завершен")
                print(f"\nЗапуск оптимизации БД")
                apply_indexes(conn)
                print(f"\nПроцесс завершен")
                print(f"\nВыполнение SELECT запросов")
                analysis_data = {}
                analysis_data['1_students_count_by_room'] = get_count_students_by_room(conn)
                analysis_data['2_less_populated_rooms'] = get_less_populated_rooms(conn)
                analysis_data['3_largest_age_diff_rooms'] = get_rooms_with_large_diff(conn)
                analysis_data['4_different_sex_rooms'] = get_rooms_with_diff_sex(conn)
                print(f"--- Все запросы выполнены.")
                if analysis_data:
                    export_results(analysis_data, FILE_FORMAT)
                else:
                    print("Не удалось получить данные для экспорта.")

        except Exception as e:
            print(f"Произошла непредвиденная ошибка во время выполнения: {e}")
        finally:
            conn.close()
            print("Соединение с БД закрыто.")
    else:
        print("Не удалось подключиться к базе данных.")