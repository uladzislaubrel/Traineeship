import psycopg2

def apply_indexes(connection):
    cursor = connection.cursor()

    index_student_room = """
                             CREATE INDEX IF NOT EXISTS ind_students_room ON students (room);
                             """

    index_students_sex = """
                          CREATE INDEX IF NOT EXISTS ind_students_sex ON students (sex);
                          """

    index_student_birthday = """
                                 CREATE INDEX IF NOT EXISTS idx_students_birthday ON students (birthday);
                                 """

    try:
        print("Создание индексов")
        cursor.execute(index_student_room)
        cursor.execute(index_students_sex)
        cursor.execute(index_student_birthday)
        connection.commit()
        print("Индексы успешно созданы/обновлены.")
    except psycopg2.Error as e:
        print(f"Ошибка при создании индексов: {e}")
        connection.rollback()
    finally:
        cursor.close()
