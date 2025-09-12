#!/usr/bin/env python3
"""
🔍 NSE Data Explorer
Quick queries to explore the imported NSE stock data
"""

import json
import psycopg2
import pandas as pd
from datetime import datetime

def load_config():
    """Load database configuration"""
    with open('database_config.json', 'r') as f:
        return json.load(f)

def connect_to_database():
    """Connect to PostgreSQL database"""
    config = load_config()
    conn = psycopg2.connect(
        host=config['host'],
        port=config['port'],
        database=config['database'],
        user=config['username'],
        password=config['password']
    )
    return conn

def run_query(query, description=""):
    """Run a query and display results"""
    conn = connect_to_database()
    try:
        print(f"\n📊 {description}")
        print("=" * 60)
        
        df = pd.read_sql_query(query, conn)
        print(df.to_string(index=False))
        print(f"\nTotal rows: {len(df)}")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
    finally:
        conn.close()

def main():
    """Main exploration function"""
    print("🔍 NSE Data Explorer")
    print("=" * 50)
    
    # Basic statistics
    run_query("""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT symbol) as unique_symbols,
            MIN(trade_date) as earliest_date,
            MAX(trade_date) as latest_date
        FROM nse_stock_data
    """, "📈 Database Overview")
    
    # Monthly breakdown
    run_query("""
        SELECT 
            EXTRACT(YEAR FROM trade_date) as year,
            EXTRACT(MONTH FROM trade_date) as month,
            TO_CHAR(trade_date, 'Month YYYY') as month_name,
            COUNT(*) as records,
            COUNT(DISTINCT symbol) as unique_symbols
        FROM nse_stock_data
        GROUP BY EXTRACT(YEAR FROM trade_date), EXTRACT(MONTH FROM trade_date), TO_CHAR(trade_date, 'Month YYYY')
        ORDER BY year, month
    """, "📅 Monthly Data Breakdown")
    
    # Top symbols by trading volume
    run_query("""
        SELECT 
            symbol,
            COUNT(*) as trading_days,
            SUM(total_traded_qty) as total_volume,
            AVG(close_price) as avg_price,
            MAX(high_price) as highest_price,
            MIN(low_price) as lowest_price
        FROM nse_stock_data 
        WHERE total_traded_qty IS NOT NULL
        GROUP BY symbol
        ORDER BY total_volume DESC
        LIMIT 10
    """, "🏆 Top 10 Symbols by Total Trading Volume")
    
    # Recent data sample
    run_query("""
        SELECT 
            symbol, series, trade_date, close_price, 
            total_traded_qty, delivery_percent
        FROM nse_stock_data
        WHERE trade_date = (SELECT MAX(trade_date) FROM nse_stock_data)
        ORDER BY total_traded_qty DESC
        LIMIT 10
    """, "📊 Latest Trading Day - Top 10 by Volume")
    
    # Series breakdown
    run_query("""
        SELECT 
            series,
            COUNT(*) as records,
            COUNT(DISTINCT symbol) as unique_symbols
        FROM nse_stock_data
        GROUP BY series
        ORDER BY records DESC
    """, "📋 Data by Series Type")

if __name__ == "__main__":
    main()
