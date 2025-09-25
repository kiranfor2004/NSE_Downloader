import pyodbc
import pandas as pd

# Database connection
connection_string = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=SRIKIRANREDDY\\SQLEXPRESS;"
    "Database=master;"
    "Trusted_Connection=yes;"
)

conn = pyodbc.connect(connection_string)

# Check column names
cursor = conn.cursor()
cursor.execute("SELECT TOP 1 * FROM step03_compare_monthvspreviousmonth")
columns = [desc[0] for desc in cursor.description]
print("Columns in step03_compare_monthvspreviousmonth:")
for i, col in enumerate(columns):
    print(f"{i+1}. {col}")

# Get sample data
cursor.execute("SELECT TOP 3 * FROM step03_compare_monthvspreviousmonth WHERE symbol = 'ABB'")
rows = cursor.fetchall()
print("\nSample data for ABB:")
for row in rows:
    print(row)

conn.close()