import psycopg2

def execute_select_query(connection, query):

    cursor = connection.cursor()
    results = None

    try:
        cursor.execute(query)

        records = cursor.fetchall()

        column_names = [desc[0] for desc in cursor.description]

        results = [dict(zip(column_names, record)) for record in records]

        print(f"Запрос успешно выполнен, получено {len(records)} записей.")

    except psycopg2.Error as e:
        print(f"Ошибка при выполнении SELECT запроса: {e}")
        results = None

    finally:
        cursor.close()
        return results

def get_count_students_by_room(connection):
    #query 1: List of rooms and the number of students in each of them
    query = """
            WITH cou AS (
                SELECT room,
                    COUNT(id) AS cnt
                FROM students
                GROUP BY room
)
            SELECT
                r.name,
                cnt
            FROM cou
            INNER JOIN rooms AS r 
                ON cou.room = r.id; 
            """
    return execute_select_query(connection, query)

def get_less_populated_rooms(connection):
    #query 2: 5 rooms with the smallest average age of students
    query = """
            WITH av_age AS
                (SELECT room, 
                        AVG(extract(year from AGE(birthday))) as av_age
                from students
                GROUP BY room
                ORDER BY av_age),

            ranking AS(
            SELECT room, 
                   av_age,
                    RANK() OVER (PARTITION BY 1 ORDER BY av_age) AS rang
FROM av_age)

SELECT room, av_age
FROM ranking
WHERE rang < 6
            """
    # Передаем параметры в кортеже
    return execute_select_query(connection, query)

def get_rooms_with_large_diff(connection):
    #query 3: 5 rooms with the largest difference in the age of students
    query = """
            WITH age_diff AS(
                SELECT room,
                    MIN(extract(year from AGE(birthday))) AS minage, 
                    MAX(extract(year from AGE(birthday))) AS maxage,
                    (MAX(extract(year from AGE(birthday))) - MIN(extract(year from AGE(birthday)))) AS diff 
                FROM STUDENTS
                GROUP BY room
                ORDER BY diff DESC),
            ranking AS(
                SELECT room, diff,
                    ROW_NUMBER ()OVER(PARTITION BY 1 ORDER BY diff DESC) AS rang
                FROM age_diff)
                SELECT room, rang, diff
                FROM ranking
                WHERE rang < 6
            """
    return execute_select_query(connection, query)

def get_rooms_with_diff_sex(connection):
    #query 4: List of rooms where different-sex students live
    query = """
            SELECT DISTINCT room
            FROM students 
            WHERE sex = 'M'
            INTERSECT 
            SELECT DISTINCT room
            FROM students 
            WHERE sex = 'F'
            """
    return execute_select_query(connection, query)