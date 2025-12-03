import psycopg2
from psycopg2 import OperationalError, DatabaseError

from db_connection import DBConnection


class IndexManager:
    """
    Class responsible for applying indexes on database
    """

    def __init__(self, db_connection_manager: DBConnection):
        self.db_connection_manager = db_connection_manager

    def apply_indexes(self):
        print("Applying indexes")

        index_queries = [
            """CREATE INDEX IF NOT EXISTS ind_students_room ON students (room);""",
            """CREATE INDEX IF NOT EXISTS ind_students_sex ON students (sex);""",
            """CREATE INDEX IF NOT EXISTS idx_students_birthday ON students (birthday);""",
        ]

        connection = self.db_connection_manager.conn

        if connection is None:
            print("No connection")
            return

        try:
            with connection.cursor() as cursor:
                for query in index_queries:
                    cursor.execute(query)

            connection.commit()
            print("Indexes applied")

        except Exception as e:
            connection.rollback()
            print(f"During applying indexes error: {e}")
