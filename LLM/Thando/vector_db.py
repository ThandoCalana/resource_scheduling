from langchain_ollama import OllamaEmbeddings
from langchain_core.documents import Document
from langchain_chroma import Chroma
from langchain_core.vectorstores import VectorStoreRetriever
import pyodbc
import os
from dotenv import load_dotenv

load_dotenv()
SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DB = os.getenv("SQL_DB")

# --- SQL Server connection ---
conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={SQL_SERVER};DATABASE={SQL_DB};"
        f"Trusted_Connection=yes;TrustServerCertificate=yes;"
    )
cursor = conn.cursor()
cursor.execute("""
    SELECT *
    FROM OutlookCalendarSummary
""")
rows = cursor.fetchall()

# --- Prepare documents from SQL Server data ---
documents = []
ids = []
for row in rows:
    id, first_name, date_val, time_slot, meeting_subject, start_time, end_time, content = row
    duration = None
    if start_time and end_time:
        duration = (end_time - start_time).total_seconds() / 60

    metadata = {
        "meeting_subject": meeting_subject,
        "first_name": first_name,
        "date": str(date_val) if date_val else None,
        "duration_minutes": duration
    }

    documents.append(Document(page_content=content, metadata=metadata, id=str(id)))
    ids.append(str(id))
# --- Embeddings ---
embeddings = OllamaEmbeddings(model="mxbai-embed-large")

# --- Chroma vector store ---
db_location = "./chroma_langchain_db"
add_documents = not os.path.exists(db_location)

vector_store = Chroma(
    collection_name="team_schedule",
    persist_directory=db_location,
    embedding_function=embeddings
)

# --- Add documents only if DB folder is empty ---
if add_documents:
    vector_store.add_documents(documents=documents, ids=ids)

# --- Retriever ---
retriever = vector_store.as_retriever(search_kwargs={"k": 20})
