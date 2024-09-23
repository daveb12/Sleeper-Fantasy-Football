from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class DatabaseConnection:
    def __init__(self):
        self.engine = create_engine(self.get_connection_string())

    def get_connection_string(self):
        return os.getenv('DATABASE_URL')

    def connect(self):
        return self.engine.connect()

    def close(self, conn):
        conn.close()
    
    def __enter__(self):
        self.conn = self.connect()
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

