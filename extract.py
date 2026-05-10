import pandas as pd
import os

def extract(filepath: str) -> pd.DataFrame:
    print(f"[EXTRACT] Reading file: {filepath}")
    
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"CSV file not found at: {filepath}")
    
    df = pd.read_csv(filepath, encoding='latin-1')
    
    print(f"[EXTRACT] Loaded {len(df)} rows and {len(df.columns)} columns")
    print(f"[EXTRACT] Columns: {list(df.columns)}")
    
    return df