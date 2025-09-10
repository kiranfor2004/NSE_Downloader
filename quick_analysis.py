#!/usr/bin/env python3
"""
Quick NSE Data Analysis Overview
"""

import sqlite3
import pandas as pd

def analyze_nse_data():
    # Connect to database
    conn = sqlite3.connect('nse_data.db')
    
    print('📊 NSE AUGUST 2025 DATA ANALYSIS')
    print('=' * 45)
    print()
    
    # Check tables
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    table_name = tables[0][0] if tables else None
    
    if not table_name:
        print("❌ No tables found in database")
        return
    
    print(f"📋 Using table: {table_name}")
    print()
    
    # Basic statistics
    basic_stats = conn.execute(f'''
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT symbol) as unique_stocks,
            COUNT(DISTINCT date) as trading_days,
            MIN(date) as start_date,
            MAX(date) as end_date
        FROM {table_name}
    ''').fetchone()
    
    print('📈 DATASET OVERVIEW:')
    print(f'   • Total Records: {basic_stats[0]:,}')
    print(f'   • Unique Stocks: {basic_stats[1]:,}')
    print(f'   • Trading Days: {basic_stats[2]:,}')
    print(f'   • Date Range: {basic_stats[3]} to {basic_stats[4]}')
    print()
    
    # Latest day analysis
    latest_date = basic_stats[4]
    
    # Market summary for latest day
    market_summary = conn.execute(f'''
        SELECT 
            COUNT(*) as stocks_traded,
            AVG(close_price) as avg_price,
            SUM(total_traded_qty) as total_volume,
            SUM(turnover_lacs) as total_turnover,
            AVG(delivery_percentage) as avg_delivery
        FROM {table_name}
        WHERE date = '{latest_date}'
    ''').fetchone()
    
    print(f'📊 LATEST DAY SUMMARY ({latest_date}):')
    print(f'   • Stocks Traded: {market_summary[0]:,}')
    print(f'   • Average Price: ₹{market_summary[1]:.2f}')
    print(f'   • Total Volume: {market_summary[2]:,}')
    print(f'   • Total Turnover: ₹{market_summary[3]:.1f} Lacs')
    print(f'   • Avg Delivery %: {market_summary[4]:.2f}%')
    print()
    
    # Top 5 highest priced stocks
    print('💰 TOP 5 HIGHEST PRICED STOCKS:')
    top_priced = conn.execute(f'''
        SELECT symbol, close_price, total_traded_qty, turnover_lacs
        FROM {table_name}
        WHERE date = '{latest_date}'
        ORDER BY close_price DESC
        LIMIT 5
    ''').fetchall()
    
    for i, (symbol, price, volume, turnover) in enumerate(top_priced, 1):
        print(f'   {i}. {symbol:<15} ₹{price:>8.2f}  Vol: {volume:>10,}')
    print()
    
    # Top 5 by volume
    print('📊 TOP 5 BY TRADING VOLUME:')
    top_volume = conn.execute(f'''
        SELECT symbol, close_price, total_traded_qty, turnover_lacs
        FROM {table_name}
        WHERE date = '{latest_date}'
        ORDER BY total_traded_qty DESC
        LIMIT 5
    ''').fetchall()
    
    for i, (symbol, price, volume, turnover) in enumerate(top_volume, 1):
        print(f'   {i}. {symbol:<15} ₹{price:>8.2f}  Vol: {volume:>10,}')
    print()
    
    # High delivery stocks
    print('📦 TOP 5 HIGH DELIVERY STOCKS:')
    high_delivery = conn.execute(f'''
        SELECT symbol, close_price, delivery_percentage, total_traded_qty
        FROM {table_name}
        WHERE date = '{latest_date}' AND delivery_percentage > 0
        ORDER BY delivery_percentage DESC
        LIMIT 5
    ''').fetchall()
    
    for i, (symbol, price, delivery_pct, volume) in enumerate(high_delivery, 1):
        print(f'   {i}. {symbol:<15} ₹{price:>8.2f}  Delivery: {delivery_pct:>6.2f}%')
    print()
    
    # Price ranges
    print('📈 PRICE RANGE ANALYSIS:')
    price_ranges = conn.execute(f'''
        SELECT 
            COUNT(CASE WHEN close_price < 100 THEN 1 END) as below_100,
            COUNT(CASE WHEN close_price BETWEEN 100 AND 500 THEN 1 END) as between_100_500,
            COUNT(CASE WHEN close_price BETWEEN 500 AND 1000 THEN 1 END) as between_500_1000,
            COUNT(CASE WHEN close_price > 1000 THEN 1 END) as above_1000
        FROM {table_name}
        WHERE date = '{latest_date}'
    ''').fetchone()
    
    total_stocks = sum(price_ranges)
    print(f'   • Below ₹100:     {price_ranges[0]:>4,} ({price_ranges[0]/total_stocks*100:>5.1f}%)')
    print(f'   • ₹100 - ₹500:    {price_ranges[1]:>4,} ({price_ranges[1]/total_stocks*100:>5.1f}%)')
    print(f'   • ₹500 - ₹1000:   {price_ranges[2]:>4,} ({price_ranges[2]/total_stocks*100:>5.1f}%)')
    print(f'   • Above ₹1000:    {price_ranges[3]:>4,} ({price_ranges[3]/total_stocks*100:>5.1f}%)')
    print()
    
    print('✅ Analysis Complete!')
    print('🔍 Use nse_query_tool.py for detailed interactive analysis')
    
    conn.close()

if __name__ == "__main__":
    analyze_nse_data()
