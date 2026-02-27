import os
from dotenv import load_dotenv

from db_connection import DBConnection
from data_loader import DataLoader
from indexes import IndexManager
from queries import QueryExecutor
from export import Exporter


def main():
    print("Starting application")

    ROOMS_FILE = input(
        "Введите путь для комнат: "
    )  # 'C:\Tasks\Traineeship\Task_1_Python\rooms.json'    rooms.json
    STUDENTS_FILE = input(
        "Введите путь для студентов: "
    )  # 'C:\Tasks\Traineeshirooms.jsonp\Task_1_Python\students.json'   students.json
    FILE_FORMAT = input(
        "Введите 1 для выгрузки итогов в формате JSON; 2 для выгрузки в формате XML: "
    )

    load_dotenv()
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    conn = DBConnection(db_name, db_user, db_password, db_host, db_port)
    print()
    if conn:
        print("Connection is active")
        try:
            rooms_loader = DataLoader(conn)
            students_loader = DataLoader(conn)
            print(f"\nStart data loading ")
            rooms_loader.load_and_insert(ROOMS_FILE, "rooms", ["id", "name"])
            students_loader.load_and_insert(
                STUDENTS_FILE, "students", ["id", "name", "room", "birthday", "sex"]
            )
            print(f"\nData loading was successful!!!")

            print(f"\nStart indexing ")
            index_manager = IndexManager(conn)
            index_manager.apply_indexes()
            print(f"\nIndexing was successful!!!")

            data = {}
            print(f"\nStart querying ")
            query_executor = QueryExecutor(conn)
            data["1"] = query_executor.get_count_students_by_room()
            data["2"] = query_executor.get_less_age_rooms()
            data["3"] = query_executor.get_rooms_with_large_diff()
            data["4"] = query_executor.get_rooms_with_diff_sex()
            print(f"\nQueries was successful!!!")

            if data:
                print(f"\nStart exporting ")
                exporter = Exporter()
                exporter.export_results(data, FILE_FORMAT)
                print(f"\nExport was successful!!!")
            else:
                print(f"There are no data to export")

        except Exception as e:
            print("ERROR!!!!!")
            print(f"Error {e} \n")

        finally:
            conn.close()
            print("Connection closed")


if __name__ == "__main__":
    main()
