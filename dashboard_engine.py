# ✅ FILE: dashboard_engine.py
# 📊 ENet Metrics Engine - Summarizes nmp_master.xlsx

import pandas as pd
from collections import Counter
from datetime import datetime

# ✅ Load NMP master file
def load_nmp_master():
    try:
        df = pd.read_excel("data/nmp_master.xlsx")
        df["number"] = df["number"].astype(str).str.strip()
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        return df
    except Exception as e:
        print("[ERROR] Could not load nmp_master.xlsx:", e)
        return pd.DataFrame()

# ✅ Metric 1: Ports per day
def summarize_by_day(df):
    if "date" not in df:
        return []
    grouped = df.groupby(df["date"].dt.date).size()
    return grouped.reset_index(name="total").to_dict(orient="records")

# ✅ Metric 2: From → To route summary
def summarize_provider_routes(df):
    if not {"from", "to"}.issubset(df.columns):
        return []
    grouped = df.groupby(["from", "to"]).size().reset_index(name="total")
    return grouped.to_dict(orient="records")

# ✅ Metric 3: Prefix block summary
def summarize_prefix_blocks(df):
    prefix_counts = Counter()
    for number in df["number"]:
        if len(number) >= 3:
            prefix = number[:3]
            prefix_counts[prefix] += 1
    return [{"prefix": k, "count": v} for k, v in sorted(prefix_counts.items())]

# ✅ Run all metrics
def generate_dashboard():
    df = load_nmp_master()
    return {
        "by_day": summarize_by_day(df),
        "routes": summarize_provider_routes(df),
        "prefixes": summarize_prefix_blocks(df),
    }

if __name__ == "__main__":
    dashboard = generate_dashboard()
    print("📊 Daily Stats:", dashboard["by_day"][:3])
    print("🔁 Top Routes:", dashboard["routes"][:3])
    print("📟 Prefix Summary:", dashboard["prefixes"][:3])
