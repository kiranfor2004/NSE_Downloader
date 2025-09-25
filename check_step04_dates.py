import pyodbc

conn = pyodbc.connect('Driver={SQL Server};Server=SRIKIRANREDDY\\SQLEXPRESS;Database=master;Trusted_Connection=yes;')

print('CHECKING AVAILABLE DATES IN STEP04:')
print('=' * 40)

cursor = conn.cursor()
cursor.execute('SELECT DISTINCT TOP 10 trade_date FROM step04_fo_udiff_daily ORDER BY trade_date DESC')
dates = cursor.fetchall()
print('Latest dates in step04_fo_udiff_daily:')
for date in dates:
    print(f'  {date[0]}')

print('\nCHECKING ABB DATA IN STEP04:')
cursor.execute("SELECT DISTINCT TOP 5 trade_date FROM step04_fo_udiff_daily WHERE symbol = 'ABB' ORDER BY trade_date DESC")
abb_dates = cursor.fetchall()
print('ABB dates in step04:')
for date in abb_dates:
    print(f'  {date[0]}')

print('\nCHECKING SAMPLE ABB F&O DATA:')
cursor.execute("SELECT TOP 3 trade_date, symbol, strike_price, option_type, close_price FROM step04_fo_udiff_daily WHERE symbol = 'ABB' ORDER BY trade_date DESC")
abb_sample = cursor.fetchall()
print('ABB F&O sample:')
for row in abb_sample:
    print(f'  {row[0]} | {row[1]} | Strike: {row[2]} | {row[3]} | Price: {row[4]}')

conn.close()