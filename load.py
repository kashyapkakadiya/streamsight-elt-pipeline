import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

def get_engine():
    user     = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host     = os.getenv("POSTGRES_HOST")
    port     = os.getenv("POSTGRES_PORT")
    db       = os.getenv("POSTGRES_DB")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(url)
    return engine


def load(df: pd.DataFrame, table_name: str = "spotify_songs") -> None:
    print(f"[LOAD] Connecting to PostgreSQL...")
    engine = get_engine()

    # --- 1. Test connection ---
    with engine.connect() as conn:
        result = conn.execute(text("SELECT version();"))
        print(f"[LOAD] Connected. PostgreSQL version: {result.fetchone()[0]}")

    # --- 2. Load dataframe into PostgreSQL ---
    print(f"[LOAD] Loading {len(df)} rows into table '{table_name}'...")

    df.to_sql(
        name=table_name,
        con=engine,
        if_exists='replace',   # drop and recreate table on every run
        index=False,
        chunksize=500,         # insert 500 rows at a time
        method='multi'         # faster bulk insert
    )

    print(f"[LOAD] Successfully loaded {len(df)} rows into '{table_name}'")

    # --- 3. Verify row count in DB ---
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name};"))
        count = result.fetchone()[0]
        print(f"[LOAD] Verified: {count} rows exist in '{table_name}' in PostgreSQL")

    engine.dispose()
    print(f"[LOAD] Connection closed.")