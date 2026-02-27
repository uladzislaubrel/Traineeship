import json
import os
from psycopg2 import extras, OperationalError, DatabaseError

from db_connection import DBConnection


class DataLoader:
    """
    Class for import data from json files and insert
    it into PostgreSQL tables
    """

    def __init__(self, db_connection_manager: DBConnection):
        self.db_connection_manager = db_connection_manager

    def import_json_file(self, json_file_path) -> list:
        if not os.path.exists(json_file_path):
            print(f"File not found: {json_file_path}")
            return []

        try:
            with open(json_file_path, "r", encoding="utf-8") as json_file:
                data = json.load(json_file)
            print(f"File loaded: {json_file_path}")
            return data
        except (json.JSONDecodeError, Exception) as e:
            print(f"Error reading or decoding json file: {e}")
            return []

    def insert_data(self, connection, table_name: str, data: list, columns: list):
        """Inserts a list o dicts into table using
        psycopg2.extras.execute_values"""
        if not data or not columns:
            print(f"No data or columns for table {table_name}. Skip insertion")
            return

        columns_str = ",".join(columns)

        insert_query = f"""
            INSERT INTO {table_name} ({columns_str}) 
            VALUES %s
            ON CONFLICT (id) DO NOTHING
        """

        values = [tuple(row.get(col) for col in columns) for row in data]

        try:
            with connection.cursor() as cursor:
                extras.execute_values(cursor, insert_query, values)
            connection.commit()
            print(f"Successfully inserted into '{table_name}'")
        except (OperationalError, Exception, DatabaseError) as e:
            connection.rollback()
            print(f"Error while inserting  '{table_name}': {e}")

    def load_and_insert(self, filename: str, table_name: str, columns: list):
        print(f"Starting process fot {filename}")
        connection = self.db_connection_manager.conn

        if connection is None:
            print(f"Dataloading aborted because of inactive connection")
            return

        data_to_insert = self.import_json_file(filename)

        if data_to_insert:
            self.insert_data(connection, table_name, data_to_insert, columns)
