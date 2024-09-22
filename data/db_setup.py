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
            """
            CREATE TABLE IF NOT EXISTS players (
                sleeper_id VARCHAR PRIMARY KEY,
                yahoo_id INT,
                birth_country VARCHAR,
                college VARCHAR,
                birth_date DATE,
                search_first_name VARCHAR,
                search_rank INT,
                sportradar_id VARCHAR,
                team_changed_at TIMESTAMP,
                injury_body_part VARCHAR,
                hashtag VARCHAR,
                swish_id INT,
                espn_id INT,
                search_last_name VARCHAR,
                injury_status VARCHAR,
                opta_id VARCHAR,
                active BOOLEAN,
                channel_id VARCHAR,
                rookie_year VARCHAR,
                first_name VARCHAR,
                practice_participation VARCHAR,
                team VARCHAR,
                status VARCHAR,
                rotowire_id INT,
                last_name VARCHAR,
                gsis_id VARCHAR,
                news_updated BIGINT,
                number INT,
                depth_chart_order INT,
                fantasy_positions VARCHAR[],
                high_school VARCHAR,
                full_name VARCHAR,
                rotoworld_id INT,
                fantasy_data_id INT,
                birth_city VARCHAR,
                depth_chart_position VARCHAR,
                search_full_name VARCHAR,
                practice_description VARCHAR,
                stats_id INT,
                pandascore_id INT,
                sport VARCHAR,
                age INT,
                position VARCHAR,
                birth_state VARCHAR,
                injury_start_date DATE,
                height VARCHAR,
                years_exp INT,
                team_abbr VARCHAR,
                weight VARCHAR,
                oddsjam_id VARCHAR,
                injury_notes VARCHAR
            );

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
            self.db_connection.close(conn)

if __name__ == '__main__':
    db_setup = DBSetup()
    db_setup.create_tables()
