import pandas as pd

# Load CSV
df = pd.read_csv("sales_file.csv")

print("Original shape:", df.shape)

# --- Cleaning ---

# Remove duplicates
df = df.drop_duplicates()

# Drop missing values
df = df.dropna()

# Clean column names
df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

# Trim string values
for col in df.select_dtypes(include='object'):
    df[col] = df[col].str.strip()

# Ensure 'amount' is numeric (important!)
df['amount'] = pd.to_numeric(df['amount'], errors='coerce')

# Drop rows where amount became NaN after conversion
df = df.dropna(subset=['amount'])

# 👉 Filter condition
df = df[df['amount'] > 1000]

print("Filtered shape:", df.shape)

# Save cleaned file
df.to_csv("cleaned_file.csv", index=False)

print("✅ Done. Filtered data saved.")
