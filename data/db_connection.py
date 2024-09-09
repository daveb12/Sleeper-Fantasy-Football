import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class DatabaseConnection:
    def __init__(self):
        self.connection_string = self.get_connection_string()

    def get_connection_string(self):
        return os.getenv('DATABASE_URL')

    def connect(self):
        try:
            conn = psycopg2.connect(self.connection_string)
            return conn
        except Exception as e:
            print(f"Error connecting to the database: {e}")
            return None

    def close(self, conn):
        try:
            if conn:
                conn.close()
        except Exception as e:
            print(f"Error closing the database connection: {e}")

