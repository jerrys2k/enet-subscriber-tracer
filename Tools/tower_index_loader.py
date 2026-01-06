import pandas as pd
import os

def load_tower_index():
    # Ensure absolute path regardless of caller's working directory
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    excel_path = os.path.join(base_dir, "data", "E_Networks_EPT_2025APR25.xlsx")

    df = pd.read_excel(excel_path, sheet_name="LTE", usecols=[
        "eNodeB ID", "Cell ID", "eNodeB Name", "Latitude", "Longitude", "Sector ID", "Cluster"
    ])
    df.dropna(subset=["eNodeB ID", "Cell ID"], inplace=True)
    df["eNodeB ID"] = df["eNodeB ID"].astype(int)
    df["Cell ID"] = df["Cell ID"].astype(int)

    index = {}
    for _, row in df.iterrows():
        key = (int(row["eNodeB ID"]), int(row["Cell ID"]))
        index[key] = {
            "tower_name": row["eNodeB Name"],
            "lat": float(row["Latitude"]),
            "lon": float(row["Longitude"]),
            "sector": row.get("Sector ID", ""),
            "cluster": row.get("Cluster", "")
        }
    return index
