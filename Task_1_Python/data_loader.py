import json
import os
import psycopg2

def load_json_data(filepath):
    """Читает данные из JSON файла."""
    if not os.path.exists(filepath):
        print(f"Ошибка: файл не найден по пути {filepath}")
        return None
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        print(f"Ошибка декодирования JSON в файле {filepath}: {e}")
        return None

def insert_rooms(connection, rooms_data):
    """Загружает данные комнат в таблицу rooms."""
    cursor = connection.cursor()
    insert_query = """
    INSERT INTO rooms (id, name) VALUES (%s, %s)
    ON CONFLICT (id) DO NOTHING;
    """
    for room in rooms_data:
        try:
            cursor.execute(insert_query, (room['id'], room['name']))
        except psycopg2.Error as e:
            print(f"Ошибка при вставке комнаты {room['name']}: {e}")
            connection.rollback()
    connection.commit()
    print(f"Загружено {len(rooms_data)} комнат.")
    cursor.close()

def insert_students(connection, students_data):
    """Загружает данные студентов в таблицу students."""
    cursor = connection.cursor()
    insert_query = """
    INSERT INTO students (id, name, room, birthday, sex) 
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (id) DO NOTHING;
    """
    for student in students_data:
        try:
            cursor.execute(insert_query, (
                student['id'],
                student['name'],
                student['room'],
                student['birthday'],
                student['sex']
            ))
        except psycopg2.Error as e:
            print(f"Ошибка при вставке студента {student['name']}: {e}")
            connection.rollback()
    connection.commit()
    print(f"Загружено {len(students_data)} студентов.")
    cursor.close()
