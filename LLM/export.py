import pyodbc
from sqlalchemy import create_engine
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DB = os.getenv("SQL_DB")

engine = create_engine(
    f"mssql+pyodbc://{SQL_SERVER}/{SQL_DB}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=no&TrustServerCertificate=yes&Trusted_Connection=yes"
)
df = pd.read_sql("SELECT * FROM dbo.OutlookCalendarTest", engine)
df.to_csv(r"C:\Users\Tcala\OneDrive\Documents\Slipstream\Resrouce_scheduling\resource_scheduling\LLM\output.csv", index=False)
