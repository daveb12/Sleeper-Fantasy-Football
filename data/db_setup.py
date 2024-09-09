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
                team1_starters_points NUMERIC(38, 2),
                team2_name VARCHAR(255),
                team2_starters_points NUMERIC(38, 2),
                UNIQUE (matchup_id, week, year)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS teams (
                id SERIAL PRIMARY KEY,
                team_name VARCHAR(255) NOT NULL,
                owner_name_1 VARCHAR(255) NOT NULL,
                owner_name_2 VARCHAR(255),
                is_active BOOLEAN,
                division VARCHAR(255)
            )
            """,
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
