import pandas as pd

def transform(df: pd.DataFrame) -> pd.DataFrame:
    print(f"[TRANSFORM] Starting transformation. Input rows: {len(df)}")

    # --- 1. Normalize column names ---
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(' ', '_')
        .str.replace(r'[^\w]', '', regex=True)
    )
    print(f"[TRANSFORM] Columns normalized: {list(df.columns)}")

    # --- 2. Drop fully duplicate rows ---
    before = len(df)
    df = df.drop_duplicates()
    print(f"[TRANSFORM] Dropped {before - len(df)} duplicate rows")

    # --- 3. Numeric columns: strip commas and convert ---
    numeric_cols = [
        'spotify_streams', 'spotify_playlist_count', 'spotify_playlist_reach',
        'youtube_views', 'youtube_likes', 'tiktok_posts', 'tiktok_likes',
        'tiktok_views', 'pandora_streams', 'soundcloud_streams',
        'apple_music_playlist_count', 'deezer_playlist_count',
        'amazon_playlist_count', 'shazam_counts'
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(',', '', regex=False)
                .str.strip()
            )
            df[col] = pd.to_numeric(df[col], errors='coerce')

    print(f"[TRANSFORM] Numeric columns converted")

    # --- 4. Release date: parse to proper date ---
    if 'release_date' in df.columns:
        df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
        print(f"[TRANSFORM] release_date parsed. Nulls: {df['release_date'].isna().sum()}")

    # --- 5. Extract release year as separate column ---
    if 'release_date' in df.columns:
        df['release_year'] = df['release_date'].dt.year.astype('Int64')

    # --- 6. Fill missing numeric values with 0 ---
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].fillna(0)

    # --- 7. Drop rows missing critical fields ---
    critical_cols = ['track', 'artist']
    before = len(df)
    existing_critical = [c for c in critical_cols if c in df.columns]
    df = df.dropna(subset=existing_critical)
    print(f"[TRANSFORM] Dropped {before - len(df)} rows missing critical fields")

    # --- 8. Strip whitespace from string columns ---
    str_cols = df.select_dtypes(include='object').columns
    for col in str_cols:
        df[col] = df[col].str.strip()

    print(f"[TRANSFORM] Done. Output rows: {len(df)}")
    return df