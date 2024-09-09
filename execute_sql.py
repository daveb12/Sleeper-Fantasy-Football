import os
from data.db_connection import DatabaseConnection
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

def execute_sql(script_path):
    db_conn = DatabaseConnection()
    conn = db_conn.connect()
    if conn is None:
        print("Failed to connect to the database.")
        return

    try:
        with conn.cursor() as cursor:
            with open(script_path, 'r') as file:
                sql_script = file.read()
            cursor.execute(sql_script)
            conn.commit()
            print(f"Successfully executed {script_path}")
    except Exception as e:
        print(f"Error executing {script_path}: {e}")
    finally:
        db_conn.close(conn)  # Close the connection using the DatabaseConnection class

if __name__ == '__main__':
    # Example usage
    script_file = 'scripts/random_sql.sql'
    execute_sql(script_file)
