import pyodbc
import json

# Load database configuration
with open('database_config.json', 'r') as f:
    config = json.load(f)

# Connect to database
conn = pyodbc.connect(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={config['server']};"
    f"DATABASE={config['database']};"
    f"Trusted_Connection=yes"
)

cursor = conn.cursor()

# Get all tables
cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
tables = cursor.fetchall()

print("Available tables:")
for table in tables:
    print(f"- {table[0]}")

conn.close()