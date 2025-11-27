from langchain_ollama import OllamaEmbeddings
from langchain_core.documents import Document
from langchain_chroma import Chroma
import os
import pandas as pd

df = pd.read_csv("output.csv")
embeddings = OllamaEmbeddings(model="mxbai-embed-large")

db_location = "./chrome_langchain_db"
add_documents = not os.path.exists(db_location)

if add_documents:
    documents = []
    ids = []

    for i, row in df.iterrows():
        document = Document(
            page_content =row["summary_text"],
            metadata = {"first_name": row["first_name"], "date": row["date"], "load_percentage": row["load_percentage"]}
        )
        ids.append(str(i))
        documents.append(document)


vector_store = Chroma(
    collection_name = "team_schedule",
    persist_directory = db_location,
    embedding_function = embeddings
)

if add_documents:
    vector_store.add_documents(documents = documents, ids = ids)

retriever = vector_store.as_retriever(
    search_kwargs = {"k": 10 }
)

