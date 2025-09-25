"""
Check Current F&O Database Status
"""
import pyodbc
import json

# Load database configuration
with open('database_config.json', 'r') as f:
    db_config = json.load(f)

conn = pyodbc.connect(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={db_config['server']};"
    f"DATABASE={db_config['database']};"
    f"Trusted_Connection=yes;"
)

cursor = conn.cursor()

# Check current F&O data
print('📊 Current F&O Data in step04_fo_udiff_daily:')
cursor.execute('''
    SELECT 
        LEFT(trade_date, 6) as year_month,
        MIN(trade_date) as first_date,
        MAX(trade_date) as last_date,
        COUNT(*) as total_records
    FROM step04_fo_udiff_daily 
    GROUP BY LEFT(trade_date, 6)
    ORDER BY year_month
''')

data_found = False
for row in cursor.fetchall():
    data_found = True
    print(f'   {row[0]}: {row[1]} to {row[2]} ({row[3]:,} records)')

if not data_found:
    print('   No data found!')

# Check total records
print('\n📈 Total F&O Records:')
cursor.execute('SELECT COUNT(*) FROM step04_fo_udiff_daily')
total = cursor.fetchone()[0]
print(f'   Total: {total:,} records')

# Check recent dates
print('\n📅 Recent trading dates (last 10):')
cursor.execute('''
    SELECT TOP 10 trade_date, COUNT(*) as records
    FROM step04_fo_udiff_daily 
    GROUP BY trade_date
    ORDER BY trade_date DESC
''')

for date, records in cursor.fetchall():
    print(f'   {date}: {records:,} records')

conn.close()