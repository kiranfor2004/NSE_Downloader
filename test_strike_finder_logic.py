#!/usr/bin/env python3
"""
Day-wise Strike Price Finder Based on Current Close Price
=========================================================
This script:
1. Takes current_close_price from step03_compare_monthvspreviousmonth 
2. Finds nearest 3 strike prices for both PE and CE options in step04_fo_udiff_daily
3. Returns 6 records total (3 PE + 3 CE) for each day/symbol combination

Test Logic Flow:
- Get symbol and date from step03_compare_monthvspreviousmonth
- Use current_close_price as reference
- Find 3 nearest strikes for PE and 3 nearest strikes for CE
- Total: 6 records per day/symbol
"""

import pyodbc
import pandas as pd
import numpy as np
from datetime import datetime

def get_sample_data_from_step03():
    """Get sample data from step03_compare_monthvspreviousmonth for testing"""
    conn = pyodbc.connect('Driver={SQL Server};Server=SRIKIRANREDDY\\SQLEXPRESS;Database=master;Trusted_Connection=yes;')
    
    # Get symbols that are likely to have F&O data
    fo_symbols = ['ABB', 'AARTIIND', 'ABBOTINDIA', 'ABCAPITAL', 'ABFRL', 'ACC']
    fo_symbols_str = "', '".join(fo_symbols)
    
    query = f"""
    SELECT TOP 10
        current_trade_date,
        symbol,
        current_close_price
    FROM step03_compare_monthvspreviousmonth 
    WHERE current_close_price IS NOT NULL
    AND symbol IN ('{fo_symbols_str}')
    ORDER BY current_trade_date DESC
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def find_nearest_strikes_for_date_symbol(trade_date, symbol, target_price):
    """Find nearest 3 strikes for both PE and CE for given date and symbol"""
    conn = pyodbc.connect('Driver={SQL Server};Server=SRIKIRANREDDY\\SQLEXPRESS;Database=master;Trusted_Connection=yes;')
    
    # Convert date format from YYYY-MM-DD to YYYYMMDD for step04 table
    if isinstance(trade_date, str) and '-' in trade_date:
        trade_date_fo_format = trade_date.replace('-', '')
    else:
        trade_date_fo_format = trade_date
    
    print(f"\n🔍 Finding strikes for {symbol} on {trade_date} (F&O format: {trade_date_fo_format}) with target price ₹{target_price:.2f}")
    
    # Get available strikes for this symbol and date
    strikes_query = f"""
    SELECT DISTINCT 
        strike_price,
        option_type
    FROM step04_fo_udiff_daily 
    WHERE trade_date = '{trade_date_fo_format}'
    AND symbol = '{symbol}'
    AND strike_price IS NOT NULL
    ORDER BY strike_price
    """
    
    strikes_df = pd.read_sql(strikes_query, conn)
    
    if strikes_df.empty:
        print(f"❌ No F&O data found for {symbol} on {trade_date_fo_format}")
        conn.close()
        return pd.DataFrame()
    
    # Get unique strikes
    available_strikes = sorted(strikes_df['strike_price'].unique())
    print(f"📊 Available strikes: {available_strikes}")
    
    # Find 3 nearest strikes
    strike_distances = [(strike, abs(strike - target_price)) for strike in available_strikes]
    strike_distances.sort(key=lambda x: x[1])  # Sort by distance
    nearest_3_strikes = [strike[0] for strike in strike_distances[:3]]
    
    print(f"🎯 3 Nearest strikes: {nearest_3_strikes}")
    
    # Get F&O data for these 3 strikes (both PE and CE)
    strikes_placeholder = ','.join(['?' for _ in nearest_3_strikes])
    
    fo_query = f"""
    SELECT 
        trade_date,
        symbol,
        strike_price,
        option_type,
        close_price,
        open_interest,
        contracts_traded,
        expiry_date,
        underlying
    FROM step04_fo_udiff_daily 
    WHERE trade_date = ?
    AND symbol = ?
    AND strike_price IN ({strikes_placeholder})
    ORDER BY option_type, strike_price
    """
    
    params = [trade_date_fo_format, symbol] + nearest_3_strikes
    fo_df = pd.read_sql(fo_query, conn, params=params)
    
    conn.close()
    
    # Verify we have both PE and CE for each strike
    pe_records = fo_df[fo_df['option_type'] == 'PE']
    ce_records = fo_df[fo_df['option_type'] == 'CE']
    
    print(f"📈 PE records found: {len(pe_records)}")
    print(f"📉 CE records found: {len(ce_records)}")
    print(f"📊 Total records: {len(fo_df)} (Expected: 6)")
    
    return fo_df

def display_results(fo_df, target_price):
    """Display the results in a formatted way"""
    if fo_df.empty:
        print("❌ No results to display")
        return
    
    print(f"\n📋 DETAILED RESULTS:")
    print("=" * 80)
    
    # Group by option type
    for option_type in ['CE', 'PE']:
        subset = fo_df[fo_df['option_type'] == option_type]
        if not subset.empty:
            print(f"\n💰 {option_type} OPTIONS:")
            print("-" * 50)
            for _, row in subset.iterrows():
                distance = abs(row['strike_price'] - target_price)
                print(f"Strike {row['strike_price']:6.1f} | Price: ₹{row['close_price']:7.2f} | "
                      f"OI: {row['open_interest']:>10,} | Volume: {row['contracts_traded']:>8,} | "
                      f"Distance: {distance:.2f}")
    
    print(f"\n📊 SUMMARY:")
    print(f"Target Price: ₹{target_price:.2f}")
    print(f"Records Found: {len(fo_df)}/6")
    print(f"Unique Strikes: {sorted(fo_df['strike_price'].unique())}")

def test_single_scenario():
    """Test the logic with a single scenario"""
    print("🧪 TESTING STRIKE FINDER LOGIC")
    print("=" * 50)
    
    # Get sample data
    print("1️⃣ Getting sample data from step03_compare_monthvspreviousmonth...")
    sample_df = get_sample_data_from_step03()
    
    if sample_df.empty:
        print("❌ No data found in step03_compare_monthvspreviousmonth")
        return
    
    print(f"✅ Found {len(sample_df)} records")
    print("\nSample data:")
    for _, row in sample_df.iterrows():
        print(f"  {row['current_trade_date']} | {row['symbol']} | ₹{row['current_close_price']:.2f}")
    
    # Test with first record
    test_record = sample_df.iloc[0]
    trade_date = test_record['current_trade_date']
    symbol = test_record['symbol']
    target_price = test_record['current_close_price']
    
    print(f"\n2️⃣ Testing with: {symbol} on {trade_date} at ₹{target_price:.2f}")
    
    # Find nearest strikes
    fo_results = find_nearest_strikes_for_date_symbol(trade_date, symbol, target_price)
    
    # Display results
    display_results(fo_results, target_price)
    
    return fo_results

def test_multiple_scenarios():
    """Test with multiple scenarios"""
    print("\n" + "="*60)
    print("🔄 TESTING MULTIPLE SCENARIOS")
    print("="*60)
    
    # Get sample data
    sample_df = get_sample_data_from_step03()
    
    if len(sample_df) < 3:
        print("❌ Need at least 3 records for multiple scenario testing")
        return
    
    success_count = 0
    total_tests = min(3, len(sample_df))
    
    for i in range(total_tests):
        test_record = sample_df.iloc[i]
        trade_date = test_record['current_trade_date']
        symbol = test_record['symbol']
        target_price = test_record['current_close_price']
        
        print(f"\n🧪 Test {i+1}/{total_tests}: {symbol} on {trade_date} at ₹{target_price:.2f}")
        print("-" * 40)
        
        fo_results = find_nearest_strikes_for_date_symbol(trade_date, symbol, target_price)
        
        if len(fo_results) == 6:
            success_count += 1
            print("✅ SUCCESS: Found exactly 6 records (3 PE + 3 CE)")
        elif len(fo_results) > 0:
            print(f"⚠️  PARTIAL: Found {len(fo_results)} records (expected 6)")
        else:
            print("❌ FAILED: No records found")
    
    print(f"\n📊 MULTIPLE SCENARIO RESULTS:")
    print(f"Successful tests: {success_count}/{total_tests}")
    print(f"Success rate: {success_count/total_tests*100:.1f}%")

def main():
    """Main testing function"""
    print("🚀 STRIKE PRICE FINDER - TESTING PHASE")
    print("="*60)
    print("Logic: Get current_close_price from step03 → Find 3 nearest PE + 3 nearest CE strikes")
    print()
    
    # Test single scenario first
    fo_results = test_single_scenario()
    
    # If single test works, test multiple scenarios
    if not fo_results.empty:
        test_multiple_scenarios()
    
    print(f"\n🎯 CONCLUSION:")
    print("If tests are successful, this logic can be implemented for all symbols!")

if __name__ == "__main__":
    main()