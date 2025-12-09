import pandas as pd
import pyodbc
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
load_dotenv()

# CONFIGURATION
SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DATABASE = os.getenv("SQL_DATABASE")
DB_DRIVER = os.getenv("SQL_DRIVER")
TABLE_NAME = "meetings"
CSV_FILE = "data_transformed.csv"
CSV_SEPARATOR = "`"

# Creating database using pyodbc
print(f"Connecting to SQL Server {SQL_SERVER} (master DB)...")

conn_str = f"DRIVER={DB_DRIVER};SERVER={SQL_SERVER};DATABASE=master;Trusted_Connection=yes;TrustServerCertificate=yes;"
with pyodbc.connect(conn_str, autocommit=True) as conn:
    cursor = conn.cursor()
    print(f"Ensuring database '{SQL_DATABASE}' exists...")
    cursor.execute(f"IF DB_ID('{SQL_DATABASE}') IS NULL CREATE DATABASE [{SQL_DATABASE}]")
    print(f"Database '{SQL_DATABASE}' is ready!")

# Connecting to the target database via SQLAlchemy
engine_db = create_engine(
    f"mssql+pyodbc://@{SQL_SERVER}/{SQL_DATABASE}?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes;TrustServerCertificate=yes;"
)

#Loading CSV
print(f"Loading CSV: {CSV_FILE} (backtick-separated)")
df = pd.read_csv(CSV_FILE, sep=CSV_SEPARATOR)
print(f"Loaded {len(df)} rows. Columns: {list(df.columns)}")


# Importing CSV into SQL Server
print(f"Importing data into table '{TABLE_NAME}'...")
df.to_sql(TABLE_NAME, con=engine_db, if_exists="replace", index=False)
print("CSV imported successfully!")
print(f"All done! Data is now in {SQL_DATABASE}.{TABLE_NAME}")
