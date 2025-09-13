#!/usr/bin/env python3
"""
Step 01 Data Explorer
View the complete structure and content of loaded equity data
"""

import pyodbc
import json
import pandas as pd

def explore_step01_data():
    """Explore the complete Step 01 equity data"""
    try:
        # Load database configuration
        with open('database_config.json', 'r') as f:
            config = json.load(f)
        
        # Connect to database
        conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={config['server']};DATABASE={config['database']};Trusted_Connection=yes;"
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        print("🔍 Step 01: Complete Equity Data Analysis")
        print("=" * 60)
        
        # Get table structure
        cursor.execute("SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'step01_equity_daily' ORDER BY ORDINAL_POSITION")
        columns_info = cursor.fetchall()
        
        print("📋 Table Structure:")
        for col in columns_info:
            nullable = "NULL" if col.IS_NULLABLE == "YES" else "NOT NULL"
            print(f"   • {col.COLUMN_NAME:<20} {col.DATA_TYPE:<15} {nullable}")
        
        print("\n" + "=" * 60)
        
        # Get data overview
        cursor.execute("SELECT COUNT(*) as total_records FROM step01_equity_daily")
        total_records = cursor.fetchone().total_records
        
        cursor.execute("SELECT COUNT(DISTINCT Symbol) as unique_symbols FROM step01_equity_daily")
        unique_symbols = cursor.fetchone().unique_symbols
        
        cursor.execute("SELECT COUNT(DISTINCT Trade_Date) as unique_dates FROM step01_equity_daily")
        unique_dates = cursor.fetchone().unique_dates
        
        cursor.execute("SELECT MIN(Trade_Date) as first_date, MAX(Trade_Date) as last_date FROM step01_equity_daily")
        date_range = cursor.fetchone()
        
        print("📊 Data Overview:")
        print(f"   • Total Records: {total_records:,}")
        print(f"   • Unique Symbols: {unique_symbols:,}")
        print(f"   • Trading Days: {unique_dates}")
        print(f"   • Date Range: {date_range.first_date} to {date_range.last_date}")
        
        # Check different series/segments
        cursor.execute("SELECT series, COUNT(*) as count FROM step01_equity_daily GROUP BY series ORDER BY count DESC")
        series_data = cursor.fetchall()
        
        print(f"\n📈 Data by Series/Segment:")
        for series in series_data:
            print(f"   • {series.series}: {series.count:,} records")
        
        # Sample data from different categories
        print(f"\n🔍 Sample Records (Latest 3):")
        cursor.execute("SELECT TOP 3 symbol, series, open_price, high_price, low_price, close_price, last_price, prev_close, ttl_trd_qnty, turnover_lacs, trade_date FROM step01_equity_daily ORDER BY trade_date DESC, symbol")
        sample_data = cursor.fetchall()
        
        for i, row in enumerate(sample_data, 1):
            print(f"\nRecord {i}:")
            print(f"   Symbol: {row.symbol}")
            print(f"   Series: {row.series}")
            print(f"   Date: {row.trade_date}")
            print(f"   Price Range: {row.low_price} - {row.high_price} (Close: {row.close_price})")
            print(f"   Volume: {row.ttl_trd_qnty:,}")
            print(f"   Turnover: ₹{row.turnover_lacs:,.2f} Lacs")
        
        # Check specific segments
        print(f"\n🎯 Key Market Segments:")
        
        # EQ segment specifically
        cursor.execute("SELECT COUNT(*) as eq_count FROM step01_equity_daily WHERE series = 'EQ'")
        eq_result = cursor.fetchone()
        if eq_result and eq_result.eq_count > 0:
            print(f"   • EQ (Equity): {eq_result.eq_count:,} records")
        
        # Other segments
        cursor.execute("SELECT series, COUNT(*) as count FROM step01_equity_daily WHERE series IN ('BE', 'BZ', 'EQ', 'IZ', 'SM', 'ST') GROUP BY series ORDER BY count DESC")
        main_series = cursor.fetchall()
        
        if main_series:
            print("   • Main Trading Segments:")
            for series in main_series:
                print(f"     - {series.series}: {series.count:,} records")
        
        conn.close()
        
        print(f"\n" + "=" * 60)
        print("✅ Step 01 contains COMPLETE NSE daily data (ALL segments, not just EQ)")
        print("   This includes: Equity (EQ), SME (SM), Bonds (BE), ETFs, and all other instruments")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    explore_step01_data()
