import pandas as pd

# Load your LTE mapping file
df = pd.read_excel("data/E_Networks_EPT_2025APR25.xlsx", sheet_name="LTE")

# Filter for known failing eNodeBs
failing_ids = [4001, 4048, 4070, 4076, 4091, 4092, 4107, 4552, 4592, 4631, 4648]
matches = df[df['eNodeB ID'].isin(failing_ids)]

# Print results
print(matches)
