#!/usr/bin/env python3
"""
Find Symbols with Both Step03 and F&O Data for Testing
======================================================
This script finds symbols that have data in both:
1. step03_compare_monthvspreviousmonth 
2. step04_fo_udiff_daily

This ensures we can test our strike finder logic properly.
"""

import pyodbc
import pandas as pd

def find_common_symbols_with_data():
    """Find symbols that exist in both step03 and step04 tables"""
    conn = pyodbc.connect('Driver={SQL Server};Server=SRIKIRANREDDY\\SQLEXPRESS;Database=master;Trusted_Connection=yes;')
    
    # Get symbols from step03 with their dates and prices
    step03_query = """
    SELECT DISTINCT 
        current_trade_date,
        symbol,
        current_close_price
    FROM step03_compare_monthvspreviousmonth 
    WHERE current_close_price IS NOT NULL
    AND symbol IS NOT NULL
    """
    
    step03_df = pd.read_sql(step03_query, conn)
    print(f"📊 Step03 symbols: {len(step03_df)} records, {step03_df['symbol'].nunique()} unique symbols")
    
    # Get symbols from step04 (F&O data)
    step04_query = """
    SELECT DISTINCT 
        trade_date,
        symbol
    FROM step04_fo_udiff_daily 
    WHERE symbol IS NOT NULL
    AND strike_price IS NOT NULL
    """
    
    step04_df = pd.read_sql(step04_query, conn)
    print(f"📊 Step04 symbols: {len(step04_df)} records, {step04_df['symbol'].nunique()} unique symbols")
    
    # Find common symbols with matching dates
    common_data = []
    
    for _, step03_row in step03_df.iterrows():
        symbol = step03_row['symbol']
        trade_date = step03_row['current_trade_date']
        close_price = step03_row['current_close_price']
        
        # Check if this symbol and date exists in step04
        matching_fo = step04_df[
            (step04_df['symbol'] == symbol) & 
            (step04_df['trade_date'] == trade_date)
        ]
        
        if not matching_fo.empty:
            common_data.append({
                'symbol': symbol,
                'trade_date': trade_date,
                'current_close_price': close_price
            })
    
    conn.close()
    
    common_df = pd.DataFrame(common_data)
    if not common_df.empty:
        print(f"✅ Found {len(common_df)} records with both step03 and step04 data")
        print(f"✅ Unique symbols with F&O data: {common_df['symbol'].nunique()}")
        
        # Show sample common symbols
        print(f"\nSample symbols with both datasets:")
        sample_symbols = common_df['symbol'].value_counts().head(5)
        for symbol, count in sample_symbols.items():
            sample_row = common_df[common_df['symbol'] == symbol].iloc[0]
            print(f"  {symbol}: {count} records | Sample: {sample_row['trade_date']} at ₹{sample_row['current_close_price']:.2f}")
    else:
        print("❌ No common symbols found between step03 and step04")
    
    return common_df

if __name__ == "__main__":
    print("🔍 FINDING SYMBOLS WITH BOTH STEP03 AND F&O DATA")
    print("=" * 60)
    
    common_df = find_common_symbols_with_data()
    
    if not common_df.empty:
        print(f"\n📋 TOP 10 TESTABLE SCENARIOS:")
        print("-" * 50)
        for i, (_, row) in enumerate(common_df.head(10).iterrows(), 1):
            print(f"{i:2d}. {row['symbol']:12} | {row['trade_date']} | ₹{row['current_close_price']:8.2f}")
    
    print(f"\n🎯 These symbols can be used for testing the strike finder logic!")