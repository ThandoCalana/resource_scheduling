# extract_to_rag.py
import os
import pyodbc
from dotenv import load_dotenv
from langchain_core.documents import Document
from langchain_ollama import OllamaEmbeddings
from langchain_chroma import Chroma

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
    SELECT user_email, first_name, [date], summary_text, load_percentage
    FROM dbo.OutlookCalendarSummary
""")
rows = cursor.fetchall()
cursor.close()
conn.close()

# --- Convert rows to LangChain Documents ---
documents = []
for row in rows:
    user_email, first_name, date_val, summary_text, load = row
    metadata = {
        "user_email": user_email,
        "first_name": first_name,
        "date": str(date_val),
        "load_percentage": load
    }
    documents.append(Document(page_content=summary_text, metadata=metadata))

# --- Embeddings ---
embeddings = OllamaEmbeddings(model="mxbai-embed-large")

# --- Chroma vector store ---
db_location = "./chroma_langchain_db"
collection_name = "team_schedule"

# Always load the vector store, create if missing
vector_store = Chroma(
    collection_name=collection_name,
    persist_directory=db_location,
    embedding_function=embeddings
)

# --- Add documents if the collection is empty ---
if len(vector_store._collection.get()) == 0:
    vector_store.add_documents(documents)
    print(f"Added {len(documents)} documents to vector store.")
else:
    print(f"Vector store loaded with {len(vector_store._collection.get())} documents.")

# --- Retriever ---
retriever = vector_store.as_retriever(search_kwargs={"k": 20})

print(f"RAG-ready vector store is ready. Total documents: {len(documents)}")
