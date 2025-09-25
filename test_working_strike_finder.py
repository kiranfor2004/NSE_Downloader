#!/usr/bin/env python3
"""
Working Strike Price Finder Test
================================
Test with dates and symbols that actually exist in both tables.
"""

import pyodbc
import pandas as pd
import numpy as np

def test_with_known_working_data():
    """Test with known working date and symbol"""
    # Use February 28, 2025 and ABB (we know these exist)
    test_symbol = 'ABB'
    test_date = '2025-02-28'  # step03 format
    test_date_fo = '20250228'  # step04 format
    
    print(f"🧪 TESTING WITH KNOWN WORKING DATA")
    print("=" * 50)
    print(f"Symbol: {test_symbol}")
    print(f"Date: {test_date} (step03) / {test_date_fo} (step04)")
    
    # Get current_close_price from step03
    conn = pyodbc.connect('Driver={SQL Server};Server=SRIKIRANREDDY\\SQLEXPRESS;Database=master;Trusted_Connection=yes;')
    
    step03_query = f"""
    SELECT current_close_price 
    FROM step03_compare_monthvspreviousmonth 
    WHERE symbol = '{test_symbol}' 
    AND current_trade_date = '{test_date}'
    """
    
    step03_df = pd.read_sql(step03_query, conn)
    
    if step03_df.empty:
        print(f"❌ No step03 data found for {test_symbol} on {test_date}")
        # Let's find any available date for ABB
        cursor = conn.cursor()
        cursor.execute(f"SELECT TOP 1 current_trade_date, current_close_price FROM step03_compare_monthvspreviousmonth WHERE symbol = '{test_symbol}' ORDER BY current_trade_date DESC")
        alt_data = cursor.fetchone()
        if alt_data:
            test_date = alt_data[0]
            target_price = float(alt_data[1])
            test_date_fo = test_date.strftime('%Y%m%d') if hasattr(test_date, 'strftime') else test_date.replace('-', '')
            print(f"📅 Using alternative date: {test_date} with price ₹{target_price:.2f}")
        else:
            print(f"❌ No ABB data found in step03 at all")
            conn.close()
            return
    else:
        target_price = float(step03_df.iloc[0]['current_close_price'])
        print(f"✅ Found step03 data: ₹{target_price:.2f}")
    
    # Now get F&O strikes for this symbol and date
    print(f"\n🔍 Getting F&O strikes for {test_symbol} on {test_date_fo}...")
    
    strikes_query = f"""
    SELECT DISTINCT strike_price
    FROM step04_fo_udiff_daily 
    WHERE trade_date = '{test_date_fo}'
    AND symbol = '{test_symbol}'
    AND strike_price IS NOT NULL
    ORDER BY strike_price
    """
    
    strikes_df = pd.read_sql(strikes_query, conn)
    
    if strikes_df.empty:
        print(f"❌ No F&O strikes found for {test_symbol} on {test_date_fo}")
        conn.close()
        return
    
    available_strikes = strikes_df['strike_price'].tolist()
    print(f"📊 Available strikes: {available_strikes}")
    
    # Find 3 nearest strikes
    strike_distances = [(strike, abs(strike - target_price)) for strike in available_strikes]
    strike_distances.sort(key=lambda x: x[1])
    nearest_3_strikes = [strike[0] for strike in strike_distances[:3]]
    
    print(f"🎯 Target price: ₹{target_price:.2f}")
    print(f"🎯 3 Nearest strikes: {nearest_3_strikes}")
    
    for strike in nearest_3_strikes:
        distance = abs(strike - target_price)
        print(f"   Strike {strike}: {distance:.2f} away from target")
    
    # Get F&O data for these strikes
    strikes_str = "', '".join([str(s) for s in nearest_3_strikes])
    
    fo_query = f"""
    SELECT 
        trade_date,
        symbol,
        strike_price,
        option_type,
        close_price,
        open_interest,
        contracts_traded,
        expiry_date
    FROM step04_fo_udiff_daily 
    WHERE trade_date = '{test_date_fo}'
    AND symbol = '{test_symbol}'
    AND strike_price IN ('{strikes_str}')
    ORDER BY option_type, strike_price
    """
    
    fo_df = pd.read_sql(fo_query, conn)
    conn.close()
    
    print(f"\n📋 F&O RECORDS FOUND:")
    print("=" * 60)
    
    if fo_df.empty:
        print("❌ No F&O records found")
        return
    
    # Group by option type
    pe_records = fo_df[fo_df['option_type'] == 'PE']
    ce_records = fo_df[fo_df['option_type'] == 'CE']
    
    print(f"📊 Total records: {len(fo_df)}")
    print(f"📈 PE records: {len(pe_records)}")
    print(f"📉 CE records: {len(ce_records)}")
    
    # Display PE options
    if not pe_records.empty:
        print(f"\n💰 PUT OPTIONS (PE):")
        print("-" * 40)
        for _, row in pe_records.iterrows():
            distance = abs(row['strike_price'] - target_price)
            print(f"Strike {row['strike_price']:6.0f} | Price: ₹{row['close_price']:8.2f} | OI: {row['open_interest']:>10,} | Volume: {row['contracts_traded']:>8,}")
    
    # Display CE options
    if not ce_records.empty:
        print(f"\n📈 CALL OPTIONS (CE):")
        print("-" * 40)
        for _, row in ce_records.iterrows():
            distance = abs(row['strike_price'] - target_price)
            print(f"Strike {row['strike_price']:6.0f} | Price: ₹{row['close_price']:8.2f} | OI: {row['open_interest']:>10,} | Volume: {row['contracts_traded']:>8,}")
    
    # Final validation
    expected_records = 6  # 3 strikes × 2 option types
    actual_records = len(fo_df)
    
    print(f"\n🎯 RESULT VALIDATION:")
    print(f"Expected records: {expected_records} (3 strikes × 2 option types)")
    print(f"Actual records: {actual_records}")
    
    if actual_records == expected_records:
        print("✅ SUCCESS: Perfect match! Logic is working correctly.")
    elif actual_records > 0:
        print(f"⚠️  PARTIAL SUCCESS: Found {actual_records} records (some option types may be missing)")
    else:
        print("❌ FAILED: No records found")
    
    return fo_df

if __name__ == "__main__":
    test_with_known_working_data()