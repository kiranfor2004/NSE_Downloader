import pyodbc
import pandas as pd
import json

# Load database configuration
with open('database_config.json', 'r') as f:
    config = json.load(f)

# Connect to database
conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={config['server']};"
    f"DATABASE={config['database']};"
    f"Trusted_Connection=yes;"
)
conn = pyodbc.connect(conn_str)

# Check available dates
df = pd.read_sql("SELECT DISTINCT Trade_date FROM step04_fo_udiff_daily WHERE Trade_date IS NOT NULL ORDER BY Trade_date", conn)
print("Available dates:")
print(df.head(20))
print(f"\nTotal dates: {len(df)}")

# Check specifically for February 2025
feb_df = pd.read_sql("SELECT DISTINCT Trade_date FROM step04_fo_udiff_daily WHERE Trade_date >= '2025-02-01' AND Trade_date < '2025-03-01' ORDER BY Trade_date", conn)
print(f"\nFebruary 2025 dates: {len(feb_df)}")
if len(feb_df) > 0:
    print(feb_df)

# Check date range
min_max_df = pd.read_sql("SELECT MIN(Trade_date) as min_date, MAX(Trade_date) as max_date FROM step04_fo_udiff_daily WHERE Trade_date IS NOT NULL", conn)
print(f"\nDate range: {min_max_df.iloc[0]['min_date']} to {min_max_df.iloc[0]['max_date']}")

conn.close()