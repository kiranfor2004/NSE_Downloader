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

# Get column information for step03_compare_monthvspreviousmonth
cursor.execute("""
    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'step03_compare_monthvspreviousmonth'
    ORDER BY ORDINAL_POSITION
""")

columns = cursor.fetchall()

print("Columns in step03_compare_monthvspreviousmonth:")
print("Column Name".ljust(40), "Data Type".ljust(15), "Length".ljust(10), "Nullable")
print("-" * 80)

for col in columns:
    column_name = col[0]
    data_type = col[1]
    max_length = col[2] if col[2] else "N/A"
    is_nullable = col[3]
    
    print(column_name.ljust(40), data_type.ljust(15), str(max_length).ljust(10), is_nullable)

print(f"\nTotal columns: {len(columns)}")

# Show some sample data
cursor.execute("SELECT TOP 3 * FROM step03_compare_monthvspreviousmonth")
rows = cursor.fetchall()

if rows:
    print("\nSample data:")
    print("First few columns of first row:")
    for i, col in enumerate(cursor.description[:10]):  # First 10 columns
        print(f"{col[0]}: {rows[0][i]}")

conn.close()