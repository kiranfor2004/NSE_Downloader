#!/usr/bin/env python3
"""Clear existing data and reload with ALL series"""

import pyodbc
import json

# Load database configuration
with open('database_config.json', 'r') as f:
    config = json.load(f)

# Connect to database
conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={config['server']};DATABASE={config['database']};Trusted_Connection=yes;"
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# Clear existing EQ-only data
cursor.execute("DELETE FROM step01_equity_daily")
deleted_count = cursor.rowcount
conn.commit()

print(f"🗑️ Deleted {deleted_count:,} EQ-only records")

# Check remaining count
cursor.execute("SELECT COUNT(*) FROM step01_equity_daily")
remaining = cursor.fetchone()[0]
print(f"📊 Records remaining: {remaining}")

conn.close()
print("✅ Database cleared and ready for complete data reload")
