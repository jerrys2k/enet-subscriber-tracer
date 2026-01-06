import pandas as pd

# --- Load input/output files ---
input_df = pd.read_excel("data/input.xlsx")
output_df = pd.read_excel("data/output.xlsx")
ranges_df = pd.read_csv("data/ranges.csv")

# --- Normalize phone numbers ---
for df in [input_df, output_df]:
    df["Phone number"] = df["Phone number"].astype(str).str.strip()

# --- Normalize structure ---
def format_nmp(df):
    return pd.DataFrame({
        "number": df["Phone number"],
        "from": df["Donor Network Operator"],
        "to": df["Recipient Network Operator"],
        "date": pd.to_datetime(df["Date actual"]).dt.strftime("%Y-%m-%d %H:%M:%S"),
        "source": "input_output"
    })

input_clean = format_nmp(input_df)
output_clean = format_nmp(output_df)

# --- Combine and deduplicate ---
combined_df = pd.concat([input_clean, output_clean])
combined_df.drop_duplicates(subset=["number", "to", "date"], inplace=True)

# --- Build prefix map from Number Range.csv ---
prefix_map = []
for _, row in ranges_df.iterrows():
    try:
        start = int(str(row["start"]).strip())
        end = int(str(row["end"]).strip())
        provider = str(row["provider"]).strip().upper()
        prefix_map.append((start, end, provider))
    except Exception as e:
        print("Error reading row:", row, e)

# --- Add original_provider by checking number prefix against ranges ---
def resolve_original_provider(number):
    try:
        n = int(number)
        for start, end, provider in prefix_map:
            if start <= n <= end:
                return provider
    except:
        return "Unknown"
    return "Unknown"

combined_df["original_provider"] = combined_df["number"].apply(resolve_original_provider)

# --- Save final result to master file ---
combined_df.to_excel("data/nmp_master.xlsx", index=False)

print(f"✅ nmp_master.xlsx created with {len(combined_df)} records.")
