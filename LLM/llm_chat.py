import os
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

from langchain_community.vectorstores import Chroma
from langchain_community.document_loaders import DataFrameLoader
from langchain_community.vectorstores.utils import filter_complex_metadata
from langchain_text_splitters import RecursiveCharacterTextSplitter

import ollama

# --- Load environment variables ---
load_dotenv()
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")

# --- Connect to Snowflake and load data ---
conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse="COMPUTE_WH",
    database="RESOURCING",
    schema="PUBLIC"
)
df = pd.read_sql_query("SELECT * FROM OUTLOOKCALENDARTEST", conn)
conn.close()

# --- Convert complex metadata to strings ---
df["DATE"] = df["DATE"].astype(str)
df["TIME_SLOT"] = df["TIME_SLOT"].astype(str)
df["START_TIME"] = df["START_TIME"].astype(str)
df["END_TIME"] = df["END_TIME"].astype(str)

# --- Prepare text for retrieval ---
df["text"] = (
    "User: " + df["FIRST_NAME"].fillna("") +
    ", Email: " + df["USER_EMAIL"].fillna("") +
    ", Meeting: " + df["MEETING_SUBJECT"].fillna("") +
    ", Date: " + df["DATE"] +
    ", Load%: " + df["LOAD_PERCENTAGE"].astype(str)
)

# --- Convert DataFrame to LangChain documents ---
loader = DataFrameLoader(df, page_content_column="text")
documents = loader.load()

# --- Filter unsupported metadata ---
documents = filter_complex_metadata(documents)

# --- Split documents into chunks ---
splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=100)
docs = splitter.split_documents(documents)

# --- Build Chroma vector store ---
vectordb = Chroma.from_documents(
    documents=docs,
    embedding=None,
    persist_directory="chroma_store"
)

# --- Initialize Ollama LLM ---
model_name = "llama3.2"  # choose the Ollama model installed locally

# --- Interactive query loop ---
print("Connected to Snowflake and vector store initialized.")
print("Type your question below. Type 'q' to quit.\n")

while True:
    user_query = input(">> ").strip()
    if user_query.lower() == "q":
        print("Session ended.")
        break
    if not user_query:
        continue
    try:
        # Retrieve top 3 most relevant chunks
        retrieved_docs = vectordb.similarity_search(user_query, k=20)
        context_text = "\n".join([doc.page_content for doc in retrieved_docs])

        # Build prompt for Ollama
        prompt = f"Context:\n{context_text}\n\nQuestion:\n{user_query}"
        messages = [{"role": "user", "content": prompt}]
        
        # Query Ollama model correctly
        response = ollama.chat(model="llama3.2", messages=[{"role": "user", "content": prompt}])
        answer = response.message.content

        print("\nAnswer:", answer, "\n")

    except Exception as e:
        print(f"Error: {e}\n")
