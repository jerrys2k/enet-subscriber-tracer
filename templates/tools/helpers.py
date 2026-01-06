import pandas as pd
from app import normalize_provider  # or define it inline here

def get_original_provider(number):
    try:
        df = pd.read_csv("data/ranges.csv")
        number = int(number)
        for _, row in df.iterrows():
            if row["start"] <= number <= row["end"]:
                return normalize_provider(row["provider"])
    except Exception as e:
        print(f"[ERROR] Provider match failed: {e}")
    return "Unknown"
