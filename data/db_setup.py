from .db_connection import DatabaseConnection

class DBSetup:
    def __init__(self):
        self.db_connection = DatabaseConnection()

    def create_tables(self):
        commands = [
            """
            CREATE TABLE IF NOT EXISTS matchups (
                id SERIAL PRIMARY KEY,
                matchup_id INT NOT NULL,
                week INT NOT NULL,
                year INT NOT NULL,
                team1_name VARCHAR(255),
                team1_starters_points FLOAT,
                team2_name VARCHAR(255),
                team2_starters_points FLOAT
            )
            """,

            """
            CREATE TABLE IF NOT EXISTS
            """
            # Add more table creation commands here
        ]
        
        conn = self.db_connection.connect()
        if conn is None:
            return

        try:
            cursor = conn.cursor()
            for command in commands:
                cursor.execute(command)
            conn.commit()
            cursor.close()
            print("Tables created successfully!")
        except Exception as e:
            print(f"Error creating tables: {e}")
        finally:
            self.db_connection.close()

if __name__ == '__main__':
    db_setup = DBSetup()
    db_setup.create_tables()
