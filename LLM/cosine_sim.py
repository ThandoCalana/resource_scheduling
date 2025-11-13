import numpy as np
import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")


# connect to Snowflake
conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA,
    role=SNOWFLAKE_ROLE
)
cur = conn.cursor()

# get stored embeddings
cur.execute("SELECT ID, CONTENT, CONTENT_EMBEDDING FROM OUTLOOKCALENDARTEST WHERE CONTENT_EMBEDDING IS NOT NULL")
rows = cur.fetchall()

# your query embedding
query = "Find meetings with Thando"
cur.execute(f"""
SELECT SNOWFLAKE.CORTEX.EMBED_TEXT_1024('snowflake-arctic-embed-l-v2.0', %s)
""", (query,))
query_emb = np.array(cur.fetchone()[0], dtype=float)

def cosine_sim(a, b):
    a, b = np.array(a), np.array(b)
    return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))

ranked = []
for row in rows:
    id_, content, emb = row
    score = cosine_sim(query_emb, emb)
    ranked.append((score, id_, content))

ranked.sort(reverse=True)
for s, i, c in ranked[:5]:
    print(f"{s:.3f} | {c[:100]}")
