import pyodbc
import json

# Load database config
with open('database_config.json', 'r') as f:
    config = json.load(f)

connection_string = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=SRIKIRANREDDY\\SQLEXPRESS;"
    "DATABASE=master;"
    "Trusted_Connection=yes;"
)

try:
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    
    # Get column names
    cursor.execute("SELECT TOP 1 * FROM step03_compare_monthvspreviousmonth")
    columns = [desc[0] for desc in cursor.description]
    
    print("Available columns:")
    for i, col in enumerate(columns, 1):
        print(f"{i:2d}. {col}")
    
    conn.close()
    
except Exception as e:
    print(f"Error: {e}")