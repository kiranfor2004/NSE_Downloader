#!/usr/bin/env python3
"""
Step 01 Equity Data Summary
Show month-wise breakdown of loaded equity data
"""

import pyodbc
import json
from datetime import datetime

def get_equity_summary():
    """Get month-wise summary of loaded equity data"""
    try:
        # Load database configuration
        with open('database_config.json', 'r') as f:
            config = json.load(f)
        
        # Connect to database
        conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={config['server']};DATABASE={config['database']};Trusted_Connection=yes;"
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        # Get month-wise breakdown
        query = """
        SELECT 
            YEAR(Trade_Date) as year,
            MONTH(Trade_Date) as month_num,
            DATENAME(month, Trade_Date) as month_name,
            MIN(Trade_Date) as first_date,
            MAX(Trade_Date) as last_date,
            COUNT(DISTINCT Trade_Date) as trading_days,
            COUNT(*) as total_records,
            COUNT(DISTINCT Symbol) as unique_symbols
        FROM step01_equity_daily 
        GROUP BY YEAR(Trade_Date), MONTH(Trade_Date), DATENAME(month, Trade_Date)
        ORDER BY year, month_num
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        
        print("📊 Step 01: Equity Daily Data Summary")
        print("=" * 70)
        print(f"{'Month':<12} {'Trading Days':<13} {'Records':<10} {'Symbols':<8} {'Date Range'}")
        print("-" * 70)
        
        total_records = 0
        total_days = 0
        
        for row in rows:
            month_name = row.month_name
            trading_days = row.trading_days
            records = row.total_records
            symbols = row.unique_symbols
            first_date = row.first_date.strftime('%d-%m')
            last_date = row.last_date.strftime('%d-%m')
            
            total_records += records
            total_days += trading_days
            
            print(f"{month_name:<12} {trading_days:<13} {records:<10,} {symbols:<8} {first_date} to {last_date}")
        
        print("-" * 70)
        print(f"{'TOTAL':<12} {total_days:<13} {total_records:<10,}")
        print()
        
        # Get overall stats
        cursor.execute("SELECT COUNT(DISTINCT Symbol) as total_symbols FROM step01_equity_daily")
        total_symbols = cursor.fetchone().total_symbols
        
        cursor.execute("SELECT COUNT(DISTINCT Trade_Date) as total_dates FROM step01_equity_daily")
        total_dates = cursor.fetchone().total_dates
        
        print(f"📈 Overall Statistics:")
        print(f"   • Total unique symbols: {total_symbols:,}")
        print(f"   • Total trading dates: {total_dates}")
        print(f"   • Average records per day: {total_records//total_dates:,}")
        print(f"   • Data coverage: {total_days} trading days across {len(rows)} months")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    get_equity_summary()
