import psycopg2
from psycopg2 import OperationalError, DatabaseError

from db_connection import DBConnection


class QueryExecutor:
    """
    Class responsible for executing queries against a PostgreSQL database
    """

    def __init__(self, db_connection: DBConnection):
        self.db_connection_manager = db_connection

    def _execute_query(self, connection, query: str) -> list or None:
        results = None

        try:
            with connection.cursor() as cursor:
                cursor.execute(query)
                records = cursor.fetchall()

                column_names = [desc[0] for desc in cursor.description]

                results = [dict(zip(column_names, record)) for record in records]

                print(f"Query executed with: {len(records)} records")
            return results

        except Exception as e:
            print(f"Query doesn't executed with: {e}")
            return None

    def get_count_students_by_room(self) -> list or None:
        """
        Query 1: list of rooms with count of students in each room
        """
        query = """
                WITH cou AS(
                SELECT room,
                    COUNT(id) AS cnt
                FROM students
                GROUP BY room)
                
                SELECT r.name, cnt
                FROM cou
                INNER JOIN rooms AS r 
                    ON r.id = cou.room;
                """
        connection = self.db_connection_manager.conn
        if connection is None:
            return None
        return self._execute_query(connection, query)

    def get_less_age_rooms(self) -> list or None:
        """
        Query 2: 5 rooms with the smallest average age of students
        """
        query = """
                WITH av_age AS (
                    SELECT 
                        room, 
                        AVG(extract(year from AGE(birthday))) AS avg_age
                    FROM students
                    GROUP BY room
                ),
                ranking AS (
                    SELECT 
                        room, 
                        avg_age,
                        RANK() OVER (ORDER BY avg_age ASC) AS rank_num
                    FROM av_age
                )
                SELECT 
                    r.name, 
                    ROUND(avg_age::numeric, 2) AS avg_age
                FROM ranking
                INNER JOIN rooms AS r
                    ON ranking.room = r.id
                WHERE rank_num <= 5
                ORDER BY avg_age;
                """
        connection = self.db_connection_manager.conn
        if connection is None:
            return None
        return self._execute_query(connection, query)

    def get_rooms_with_large_diff(self) -> list or None:
        """
        Query 3: 5 rooms with the largest difference in the age of students.
        """
        query = """
                WITH age_diff AS (
                    SELECT 
                        room,
                        (MAX(extract(year from AGE(birthday))) - MIN(extract(year from AGE(birthday)))) AS age_diff
                    FROM STUDENTS
                    GROUP BY room
                    HAVING COUNT(id) > 1 -- Ensure at least two students for a difference
                ),
                ranking AS (
                    SELECT 
                        room, 
                        age_diff,
                        ROW_NUMBER ()OVER(ORDER BY age_diff DESC) AS rank_num
                    FROM age_diff
                )
                SELECT 
                    r.name, 
                    age_diff
                FROM ranking
                INNER JOIN rooms AS r
                    ON ranking.room = r.id
                WHERE rank_num <= 5
                ORDER BY age_diff DESC;
                """
        connection = self.db_connection_manager.conn
        if connection is None:
            return None
        return self._execute_query(connection, query)

    def get_rooms_with_diff_sex(self) -> list or None:
        """
        Query 4: List of rooms where different-sex students live (containing both 'M' and 'F').
        """
        query = """
                SELECT DISTINCT room
                FROM students 
                WHERE sex = 'M'
                INTERSECT 
                SELECT DISTINCT room
                FROM students 
                WHERE sex = 'F';
                """
        connection = self.db_connection_manager.conn
        if connection is None:
            return None
        return self._execute_query(connection, query)
