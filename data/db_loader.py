import pandas as pd
from .db_connection import DatabaseConnection
from sqlalchemy import create_engine

class DBLoader:
    def __init__(self):
        self.db_connection = DatabaseConnection()

    def upsert_dataframe(self, table_name: str, df: pd.DataFrame, conflict_columns: list):
        """
        Inserts or updates the data from a DataFrame into the specified table.
        
        :param table_name: Name of the table to upsert data into.
        :param df: The pandas DataFrame containing the data to be upserted.
        :param conflict_columns: List of columns to check for conflicts (used in ON CONFLICT clause).
        """
        conn = self.db_connection.connect()
        if conn is None:
            print("Failed to connect to the database.")
            return

        # Convert DataFrame columns to match SQL column names (in case of mismatches)
        df.columns = df.columns.str.lower()

        # Use the connection to create an engine for SQLAlchemy
        engine = create_engine(self.db_connection.get_connection_string())

        try:
            # Insert the DataFrame into a temporary table
            df.to_sql('temp_' + table_name, engine, index=False, if_exists='replace')

            # Build the insert and on conflict query
            insert_query = f"""
            INSERT INTO {table_name} ({', '.join(df.columns)})
            SELECT {', '.join(df.columns)} FROM temp_{table_name}
            ON CONFLICT ({', '.join(conflict_columns)})
            DO UPDATE SET {', '.join([f"{col} = EXCLUDED.{col}" for col in df.columns if col not in conflict_columns])};
            """

            # Execute the query
            with conn.cursor() as cursor:
                cursor.execute(insert_query)
                conn.commit()

            print(f"Upsert into {table_name} completed successfully!")

            # Optionally, drop the temp table after the upsert
            drop_temp_table_query = f"DROP TABLE IF EXISTS temp_{table_name};"
            with conn.cursor() as cursor:
                cursor.execute(drop_temp_table_query)
                conn.commit()

        except Exception as e:
            print(f"Error upserting DataFrame into {table_name}: {e}")
        finally:
            self.db_connection.close(conn)
