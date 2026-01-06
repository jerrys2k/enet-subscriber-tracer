import os
import sqlite3
import pandas as pd
from glob import glob

DB_PATH = os.path.join("data", "tacdb.sqlite3")
CSV_GLOB = os.path.join("data", "device_info_*.csv")
BATCH_SIZE = 1000

def normalize_and_combine():
    all_files = glob(CSV_GLOB)
    frames = []
    for file in all_files:
        df = pd.read_csv(file)
        df = df.rename(columns={
            "TAC": "tac",
            "MARKETING_VENDOR": "brand_name",
            "PHONE_NAME": "model_name"
        })
        df = df[["tac", "brand_name", "model_name"]].dropna()
        df["tac"] = df["tac"].astype(str).str.strip().str[:8]
        frames.append(df)
    return pd.concat(frames, ignore_index=True).drop_duplicates(subset=["tac"])

def merge_into_database(df):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    inserted, skipped = 0, 0

    try:
        for i, row in df.iterrows():
            tac = row["tac"]
            brand = row["brand_name"]
            model = row["model_name"]

            cursor.execute("SELECT 1 FROM tac WHERE tac = ?", (tac,))
            if cursor.fetchone():
                skipped += 1
                continue

            cursor.execute("SELECT id FROM brand WHERE name = ?", (brand,))
            brand_row = cursor.fetchone()
            brand_id = brand_row[0] if brand_row else None

            if not brand_id:
                cursor.execute("INSERT INTO brand (name) VALUES (?)", (brand,))
                brand_id = cursor.lastrowid

            cursor.execute("INSERT INTO model (brand, name) VALUES (?, ?)", (brand_id, model))
            model_id = cursor.lastrowid

            cursor.execute("""
                INSERT INTO tac (tac, model, date, contributor, comment)
                VALUES (?, ?, DATE('now'), 'bulk_csv', '')
            """, (tac, model_id))

            inserted += 1

            if (inserted + skipped) % BATCH_SIZE == 0:
                conn.commit()
                print(f"🔄 Progress: {inserted + skipped:,} processed | {inserted:,} inserted | {skipped:,} skipped")

        conn.commit()
    finally:
        conn.close()

    return inserted, skipped

if __name__ == "__main__":
    print("🔍 Reading and combining CSVs...")
    df = normalize_and_combine()
    print(f"📦 Total rows to consider: {len(df):,}")

    print("🚀 Merging into tacdb.sqlite3...")
    inserted, skipped = merge_into_database(df)

    print(f"\n✅ Merge Complete. Inserted: {inserted:,} | Skipped: {skipped:,}")
