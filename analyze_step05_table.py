#!/usr/bin/env python3
"""
Analyze Step05_strikepriceAnalysisderived table structure and content
"""

import pyodbc
import pandas as pd

def analyze_step05_table():
    try:
        conn = pyodbc.connect(
            "Driver={ODBC Driver 17 for SQL Server};"
            "Server=SRIKIRANREDDY\\SQLEXPRESS;"
            "Database=master;"
            "Trusted_Connection=yes;"
        )
        
        print("=== TABLE STRUCTURE ===")
        cursor = conn.cursor()
        cursor.execute("""
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'Step05_strikepriceAnalysisderived'
        ORDER BY ORDINAL_POSITION
        """)
        
        print(f"{'Column Name':<25} {'Data Type':<15} {'Nullable':<10} {'Length':<10}")
        print("-" * 70)
        for row in cursor.fetchall():
            length = str(row[3]) if row[3] else ""
            print(f"{row[0]:<25} {row[1]:<15} {row[2]:<10} {length:<10}")
        
        print("\n=== DATA SUMMARY ===")
        summary_df = pd.read_sql("""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT Current_trade_date) as trading_days,
            COUNT(DISTINCT Symbol) as unique_symbols,
            MIN(Current_trade_date) as first_date,
            MAX(Current_trade_date) as last_date,
            AVG(CAST(Current_close_price AS FLOAT)) as avg_underlying_price
        FROM Step05_strikepriceAnalysisderived
        """, conn)
        
        print(summary_df.to_string(index=False))
        
        print("\n=== RECORDS PER DAY ===")
        daily_df = pd.read_sql("""
        SELECT 
            Current_trade_date,
            COUNT(DISTINCT Symbol) as symbols,
            COUNT(*) as records,
            AVG(CAST(records_per_symbol AS FLOAT)) as avg_records_per_symbol
        FROM (
            SELECT 
                Current_trade_date,
                Symbol,
                COUNT(*) as records_per_symbol
            FROM Step05_strikepriceAnalysisderived
            GROUP BY Current_trade_date, Symbol
        ) t
        GROUP BY Current_trade_date
        ORDER BY Current_trade_date
        """, conn)
        
        print(daily_df.head(10).to_string(index=False))
        
        print("\n=== STRIKE DISTRIBUTION ===")
        strike_df = pd.read_sql("""
        SELECT 
            strike_position,
            option_type,
            COUNT(*) as record_count,
            AVG(CAST(Strike_price AS FLOAT)) as avg_strike,
            AVG(CAST(close_price AS FLOAT)) as avg_close_price
        FROM Step05_strikepriceAnalysisderived
        GROUP BY strike_position, option_type
        ORDER BY strike_position, option_type
        """, conn)
        
        print(strike_df.to_string(index=False))
        
        print("\n=== SAMPLE DATA (ABB on 2025-02-03) ===")
        sample_df = pd.read_sql("""
        SELECT 
            Current_trade_date, Symbol, Current_close_price, Strike_price, 
            option_type, close_price, strike_position, expiry_date
        FROM Step05_strikepriceAnalysisderived 
        WHERE Symbol = 'ABB' AND Current_trade_date = '20250203'
        ORDER BY option_type, Strike_price
        """, conn)
        
        print(sample_df.to_string(index=False))
        
        print("\n=== SYMBOL COVERAGE ===")
        symbol_df = pd.read_sql("""
        SELECT TOP 10
            Symbol,
            COUNT(DISTINCT Current_trade_date) as trading_days,
            COUNT(*) as total_records,
            AVG(CAST(records_per_day AS FLOAT)) as avg_records_per_day
        FROM (
            SELECT 
                Symbol,
                Current_trade_date,
                COUNT(*) as records_per_day
            FROM Step05_strikepriceAnalysisderived
            GROUP BY Symbol, Current_trade_date
        ) t
        GROUP BY Symbol
        ORDER BY total_records DESC
        """, conn)
        
        print(symbol_df.to_string(index=False))
        
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    analyze_step05_table()