from data.db_setup import DBSetup

def main():
    db_setup = DBSetup()
    db_setup.create_tables()  # Call the method that creates tables

if __name__ == "__main__":
    main()
