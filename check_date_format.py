import pyodbc
import pandas as pd

conn = pyodbc.connect('Driver={SQL Server};Server=SRIKIRANREDDY\\SQLEXPRESS;Database=master;Trusted_Connection=yes;')

print('Checking IDEA data in step01_equity_daily:')
print('=' * 50)

# Check if IDEA exists at all
cursor = conn.cursor()
cursor.execute("SELECT COUNT(*) FROM step01_equity_daily WHERE symbol = 'IDEA'")
idea_count = cursor.fetchone()[0]
print(f'Total IDEA records: {idea_count}')

# Check date range
cursor.execute("SELECT MIN(trade_date), MAX(trade_date) FROM step01_equity_daily WHERE symbol = 'IDEA'")
date_range = cursor.fetchone()
print(f'Date range: {date_range[0]} to {date_range[1]}')

# Check sample data
cursor.execute("SELECT TOP 5 trade_date, symbol, deliv_qty, close_price FROM step01_equity_daily WHERE symbol = 'IDEA' ORDER BY trade_date DESC")
sample_data = cursor.fetchall()

print('\nLatest IDEA data:')
print('trade_date | symbol | deliv_qty | close_price')
print('-' * 50)
for row in sample_data:
    print(f'{row[0]} | {row[1]} | {row[2]} | {row[3]}')

# Check February data more broadly
cursor.execute("SELECT COUNT(*) FROM step01_equity_daily WHERE trade_date LIKE '2025-02%'")
feb_count = cursor.fetchone()[0]
print(f'\nTotal February 2025 records (all symbols): {feb_count}')

# Check if any IDEA records exist in 2025
cursor.execute("SELECT COUNT(*) FROM step01_equity_daily WHERE symbol = 'IDEA' AND trade_date LIKE '2025%'")
idea_2025_count = cursor.fetchone()[0]
print(f'IDEA records in 2025: {idea_2025_count}')

conn.close()