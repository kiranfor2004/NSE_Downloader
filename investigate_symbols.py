import pyodbc
import pandas as pd

conn = pyodbc.connect('Driver={SQL Server};Server=SRIKIRANREDDY\\SQLEXPRESS;Database=master;Trusted_Connection=yes;')

print('INVESTIGATING DATE FORMATS AND SYMBOL PATTERNS:')
print('=' * 60)

# Check step03 date format and symbols
print('STEP03 Sample:')
cursor = conn.cursor()
cursor.execute('SELECT TOP 5 current_trade_date, symbol FROM step03_compare_monthvspreviousmonth')
step03_sample = cursor.fetchall()
for row in step03_sample:
    print(f'  Date: {row[0]} | Symbol: {row[1]}')

print('\nSTEP04 Sample:')
cursor.execute('SELECT TOP 5 trade_date, symbol FROM step04_fo_udiff_daily')
step04_sample = cursor.fetchall()
for row in step04_sample:
    print(f'  Date: {row[0]} | Symbol: {row[1]}')

print('\nCHECKING IF ANY F&O SYMBOLS MATCH EQUITY SYMBOLS:')
cursor.execute('SELECT DISTINCT TOP 10 symbol FROM step04_fo_udiff_daily ORDER BY symbol')
fo_symbols = cursor.fetchall()
fo_symbol_list = [row[0] for row in fo_symbols]
print(f'Sample F&O symbols: {fo_symbol_list}')

cursor.execute('SELECT DISTINCT TOP 10 symbol FROM step03_compare_monthvspreviousmonth ORDER BY symbol')
equity_symbols = cursor.fetchall()
equity_symbol_list = [row[0] for row in equity_symbols]
print(f'Sample Equity symbols: {equity_symbol_list}')

# Check if any equity symbols have F&O versions
print('\nCHECKING POPULAR EQUITY SYMBOLS FOR F&O EQUIVALENTS:')
popular_equity = ['RELIANCE', 'TCS', 'INFOSYS', 'HDFCBANK', 'ICICIBANK', 'SBIN', 'WIPRO', 'ITC']
for equity_sym in popular_equity:
    cursor.execute(f'SELECT COUNT(*) FROM step03_compare_monthvspreviousmonth WHERE symbol = "{equity_sym}"')
    equity_count = cursor.fetchone()[0]
    cursor.execute(f'SELECT COUNT(*) FROM step04_fo_udiff_daily WHERE symbol = "{equity_sym}"')
    fo_count = cursor.fetchone()[0]
    if equity_count > 0 and fo_count > 0:
        print(f'  ✅ {equity_sym}: Equity({equity_count}) + F&O({fo_count})')
    elif equity_count > 0:
        print(f'  📈 {equity_sym}: Only Equity({equity_count})')
    elif fo_count > 0:
        print(f'  📊 {equity_sym}: Only F&O({fo_count})')

conn.close()