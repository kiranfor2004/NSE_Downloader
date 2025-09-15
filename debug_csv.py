import pandas as pd
import os

# Debug CSV column mapping
csv_file = r"NSE_February_2025_Data\cm03022025bhav.csv"

print("🔍 DEBUGGING CSV COLUMN MAPPING")
print("=" * 50)

# Load CSV and inspect
df = pd.read_csv(csv_file)
print(f"📁 File: {csv_file}")
print(f"📊 Shape: {df.shape}")
print(f"📋 Columns: {list(df.columns)}")

# Check first row
print(f"\n📖 First row data:")
first_row = df.iloc[0]
for col in df.columns:
    value = first_row[col]
    print(f"  '{col}': '{value}' (type: {type(value)})")

# Test the mapping we're using
print(f"\n🧪 TESTING COLUMN ACCESS:")
print(f"  SYMBOL: '{first_row.get('SYMBOL')}'")
print(f"  ' SERIES': '{first_row.get(' SERIES')}'")
print(f"  ' OPEN_PRICE': '{first_row.get(' OPEN_PRICE')}'")
print(f"  ' HIGH_PRICE': '{first_row.get(' HIGH_PRICE')}'")
print(f"  ' CLOSE_PRICE': '{first_row.get(' CLOSE_PRICE')}'")
print(f"  ' AVG_PRICE': '{first_row.get(' AVG_PRICE')}'")
print(f"  ' TTL_TRD_QNTY': '{first_row.get(' TTL_TRD_QNTY')}'")

# Test our safe conversion functions
def safe_float(value, default=None):
    if pd.isna(value) or str(value).strip() in ['-', '--', '']:
        return default
    try:
        return float(str(value).strip())
    except:
        return default

def safe_int(value, default=None):
    if pd.isna(value) or str(value).strip() in ['-', '--', '']:
        return default
    try:
        return int(float(str(value).strip()))
    except:
        return default

print(f"\n🔧 TESTING SAFE CONVERSION:")
open_price = safe_float(first_row.get(' OPEN_PRICE'))
high_price = safe_float(first_row.get(' HIGH_PRICE'))
close_price = safe_float(first_row.get(' CLOSE_PRICE'))
contracts = safe_int(first_row.get(' TTL_TRD_QNTY'))

print(f"  Open Price: {open_price}")
print(f"  High Price: {high_price}")
print(f"  Close Price: {close_price}")
print(f"  Contracts: {contracts}")
