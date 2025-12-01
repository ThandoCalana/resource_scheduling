import pandas as pd
import pyodbc
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
load_dotenv()

# CONFIGURATION
DB_SERVER = os.getenv("SQL_SERVER")
DB_NAME = os.getenv("SQL_DATABASE")
TABLE_NAME = "meetings"
CSV_FILE = "data_transformed.csv"
CSV_SEPARATOR = "`"

# Creating database using pyodbc
print(f"Connecting to SQL Server {DB_SERVER} (master DB)...")

conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={DB_SERVER};DATABASE=master;Trusted_Connection=yes"
with pyodbc.connect(conn_str, autocommit=True) as conn:
    cursor = conn.cursor()
    print(f"Ensuring database '{DB_NAME}' exists...")
    cursor.execute(f"IF DB_ID('{DB_NAME}') IS NULL CREATE DATABASE [{DB_NAME}]")
    print(f"Database '{DB_NAME}' is ready!")

# Connecting to the target database via SQLAlchemy
engine_db = create_engine(
    f"mssql+pyodbc://@{DB_SERVER}/{DB_NAME}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
)

#Loading CSV
print(f"Loading CSV: {CSV_FILE} (backtick-separated)")
df = pd.read_csv(CSV_FILE, sep=CSV_SEPARATOR)
print(f"Loaded {len(df)} rows. Columns: {list(df.columns)}")


# Importing CSV into SQL Server
print(f"Importing data into table '{TABLE_NAME}'...")
df.to_sql(TABLE_NAME, con=engine_db, if_exists="replace", index=False)
print("CSV imported successfully!")
print(f"All done! Data is now in {DB_NAME}.{TABLE_NAME}")
