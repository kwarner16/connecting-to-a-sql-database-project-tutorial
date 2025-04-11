import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# 1) Connect to the database with SQLAlchemy
def connect():
    global engine
    try:
        connection_string = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
        print("Starting the connection...")
        engine = create_engine(connection_string, isolation_level="AUTOCOMMIT")
        engine.connect()
        print("Connected successfully!")
        return engine
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

engine = connect()

def execute_sql_file(file_path):
    with open(file_path, "r") as file:
        sql_commands = file.read()
        with engine.connect() as connection:
            connection.execute(text(sql_commands))
        print(f"Executed SQL from {file_path}")
        
# 2) Create the tables
execute_sql_file("./sql/create.sql")

# 3) Insert data
execute_sql_file("./sql/insert.sql")

# 4) Use Pandas to read and display a table
def print_table(table_name):
    try:
        df = pd.read_sql(f"SELECT * FROM {table_name}", con=engine)
        print(df)
    except Exception as e:
        print(f"Error: {e}")

print(f"Publishers Data Table: {print_table("publishers")}")
print(f"Authors Data Table: {print_table("authors")}")
print(f"Books Data Table: {print_table("books")}")
print(f"Book-Authors Data Table: {print_table("book_authors")}")
