#!/usr/bin/env python3
import pyodbc
import pandas as pd
import numpy as np

conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=SRIKIRANREDDY\\SQLEXPRESS;DATABASE=master;Trusted_Connection=yes;')

query = """
SELECT TOP 1000
    s5.analysis_id,
    s5.Symbol,
    s5.Strike_price,
    s5.option_type,
    s5.Current_trade_date as base_date,
    s5.close_price as base_close_price,
    s4.trade_date,
    s4.close_price as trading_close_price
FROM Step05_strikepriceAnalysisderived s5
LEFT JOIN step04_fo_udiff_daily s4 
    ON s5.Symbol = s4.symbol 
    AND s5.Strike_price = s4.strike_price
    AND s5.option_type = s4.option_type
    AND s4.trade_date > s5.Current_trade_date
    AND s4.trade_date BETWEEN '20250204' AND '20250228'
    AND s4.close_price IS NOT NULL
ORDER BY s5.analysis_id, s4.trade_date
"""

df = pd.read_sql(query, conn)
df['reduction_percentage'] = ((df['base_close_price'] - df['trading_close_price']) / df['base_close_price']) * 100

print('Data Range Analysis:')
print(f'Base close price range: {df["base_close_price"].min():.4f} to {df["base_close_price"].max():.4f}')
print(f'Trading close price range: {df["trading_close_price"].min():.4f} to {df["trading_close_price"].max():.4f}')
print(f'Reduction percentage range: {df["reduction_percentage"].min():.4f} to {df["reduction_percentage"].max():.4f}')

# Check for inf or very large values
inf_count = np.isinf(df['reduction_percentage']).sum()
very_large = (np.abs(df['reduction_percentage']) > 10000).sum()
print(f'Infinite values: {inf_count}')
print(f'Very large values (>10000%): {very_large}')

# Check for zero base prices
zero_base = (df['base_close_price'] == 0).sum()
print(f'Zero base prices: {zero_base}')

conn.close()