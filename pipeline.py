import time
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl.extract import extract
from etl.transform import transform
from etl.load import load

def run_pipeline():
    print("=" * 50)
    print("   STREAMSIGHT ETL PIPELINE")
    print("=" * 50)

    start_time = time.time()

    # --- 1. Extract ---
    print("\n[PIPELINE] Step 1: Extract")
    filepath = "/app/data/Most Streamed Spotify Songs 2024.csv"
    df_raw = extract(filepath)

    # --- 2. Transform ---
    print("\n[PIPELINE] Step 2: Transform")
    df_clean = transform(df_raw)

    # --- 3. Load ---
    print("\n[PIPELINE] Step 3: Load")
    load(df_clean, table_name="spotify_songs")

    # --- 4. Summary ---
    elapsed = round(time.time() - start_time, 2)
    print("\n" + "=" * 50)
    print(f"   PIPELINE COMPLETE in {elapsed}s")
    print(f"   Rows processed : {len(df_clean)}")
    print(f"   Table          : spotify_songs")
    print(f"   Database       : spotify_etl")
    print("=" * 50)


if __name__ == "__main__":
    # Retry logic — wait for PostgreSQL to be ready
    max_retries = 5
    for attempt in range(1, max_retries + 1):
        try:
            run_pipeline()
            break
        except Exception as e:
            print(f"\n[PIPELINE] Attempt {attempt}/{max_retries} failed: {e}")
            if attempt < max_retries:
                print(f"[PIPELINE] Retrying in 5 seconds...")
                time.sleep(5)
            else:
                print(f"[PIPELINE] All {max_retries} attempts failed. Exiting.")
                sys.exit(1)