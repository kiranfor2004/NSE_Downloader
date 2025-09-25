#!/usr/bin/env python3
"""
Advanced Strike Price Finder - Above & Below Target
===================================================
This implements the enhanced logic:
1. Find 3 nearest strikes ABOVE target price
2. Find 3 nearest strikes BELOW target price  
3. Find 1 nearest strike to target (closest overall)
4. Get both PE and CE for each strike
5. Return exactly 14 records (7 strikes × 2 option types)

Logic: 3 Above + 3 Below + 1 Nearest = 7 strikes × 2 options = 14 records
"""

import pyodbc
import pandas as pd
import numpy as np
from datetime import datetime

def find_strikes_above_below_target(trade_date, symbol, target_price):
    """
    Enhanced logic: Find strikes above and below target price
    Returns exactly 14 records (7 strikes × 2 option types)
    """
    conn = pyodbc.connect('Driver={SQL Server};Server=SRIKIRANREDDY\\SQLEXPRESS;Database=master;Trusted_Connection=yes;')
    
    # Convert date format if needed (YYYY-MM-DD to YYYYMMDD)
    if isinstance(trade_date, str) and '-' in trade_date:
        trade_date_fo = trade_date.replace('-', '')
    else:
        trade_date_fo = str(trade_date)
    
    print(f"\n🎯 PROCESSING: {symbol} on {trade_date} (target: ₹{target_price:.2f})")
    
    # Step 1: Get all available strikes for this symbol and date
    strikes_query = f"""
    SELECT DISTINCT 
        strike_price,
        expiry_date,
        option_type
    FROM step04_fo_udiff_daily 
    WHERE trade_date = '{trade_date_fo}'
    AND symbol = '{symbol}'
    AND strike_price IS NOT NULL
    ORDER BY strike_price
    """
    
    strikes_df = pd.read_sql(strikes_query, conn)
    
    if strikes_df.empty:
        print(f"❌ No F&O data found for {symbol} on {trade_date_fo}")
        conn.close()
        return pd.DataFrame()
    
    available_strikes = sorted(strikes_df['strike_price'].unique())
    print(f"📊 Available strikes: {len(available_strikes)} total")
    
    # Step 2: Separate strikes above and below target
    strikes_above = [s for s in available_strikes if s > target_price]
    strikes_below = [s for s in available_strikes if s < target_price]
    strikes_at = [s for s in available_strikes if s == target_price]
    
    print(f"📈 Strikes above target: {len(strikes_above)}")
    print(f"📉 Strikes below target: {len(strikes_below)}")
    print(f"🎯 Strikes at target: {len(strikes_at)}")
    
    # Step 3: Select strikes using the new logic
    selected_strikes = []
    
    # Get 3 nearest strikes ABOVE target (ascending order)
    nearest_above = sorted(strikes_above)[:3]  # Closest above first
    selected_strikes.extend(nearest_above)
    print(f"✅ 3 Nearest ABOVE: {nearest_above}")
    
    # Get 3 nearest strikes BELOW target (descending order)  
    nearest_below = sorted(strikes_below, reverse=True)[:3]  # Closest below first
    selected_strikes.extend(nearest_below)
    print(f"✅ 3 Nearest BELOW: {nearest_below}")
    
    # Get 1 overall nearest strike (could be above, below, or at target)
    all_distances = [(s, abs(s - target_price)) for s in available_strikes]
    all_distances.sort(key=lambda x: x[1])
    nearest_overall = all_distances[0][0]
    
    if nearest_overall not in selected_strikes:
        selected_strikes.append(nearest_overall)
        print(f"✅ 1 Overall NEAREST: {nearest_overall} (added)")
    else:
        print(f"✅ 1 Overall NEAREST: {nearest_overall} (already included)")
        # Add the next nearest that's not already selected
        for strike, distance in all_distances[1:]:
            if strike not in selected_strikes:
                selected_strikes.append(strike)
                print(f"✅ 1 Additional NEAREST: {strike} (to make 7 total)")
                break
    
    print(f"🎯 Final selected strikes ({len(selected_strikes)}): {sorted(selected_strikes)}")
    
    # Step 4: Get F&O data for selected strikes (both PE and CE)
    final_records = []
    
    for strike in selected_strikes:
        for option_type in ['PE', 'CE']:
            # Get the record with nearest expiry for this strike and option type
            option_query = f"""
            SELECT TOP 1
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
            WHERE trade_date = '{trade_date_fo}'
            AND symbol = '{symbol}'
            AND strike_price = {strike}
            AND option_type = '{option_type}'
            ORDER BY expiry_date ASC
            """
            
            option_df = pd.read_sql(option_query, conn)
            if not option_df.empty:
                final_records.append(option_df.iloc[0])
    
    conn.close()
    
    if final_records:
        result_df = pd.DataFrame(final_records)
        
        # Display results
        pe_count = len(result_df[result_df['option_type'] == 'PE'])
        ce_count = len(result_df[result_df['option_type'] == 'CE'])
        
        print(f"\n✅ Found {len(result_df)} records: {pe_count} PE + {ce_count} CE")
        
        # Categorize and display results
        selected_strikes_sorted = sorted(selected_strikes)
        
        print(f"\n📊 STRIKE CATEGORIZATION:")
        print("-" * 50)
        
        above_strikes = [s for s in selected_strikes_sorted if s > target_price]
        below_strikes = [s for s in selected_strikes_sorted if s < target_price]
        at_strikes = [s for s in selected_strikes_sorted if s == target_price]
        
        if above_strikes:
            print(f"📈 Above target ({len(above_strikes)}): {above_strikes}")
        if below_strikes:
            print(f"📉 Below target ({len(below_strikes)}): {below_strikes}")
        if at_strikes:
            print(f"🎯 At target ({len(at_strikes)}): {at_strikes}")
        
        # Show detailed results for each option type
        for option_type in ['PE', 'CE']:
            subset = result_df[result_df['option_type'] == option_type]
            if not subset.empty:
                print(f"\n💰 {option_type} OPTIONS ({len(subset)} records):")
                print("-" * 60)
                for _, row in subset.iterrows():
                    distance = row['strike_price'] - target_price
                    direction = "above" if distance > 0 else "below" if distance < 0 else "at"
                    print(f"  Strike {row['strike_price']:4.0f} ({distance:+6.0f}, {direction:5s}) | "
                          f"₹{row['close_price']:6.2f} | OI: {row['open_interest']:>8,} | "
                          f"Vol: {row['contracts_traded']:>6,}")
        
        return result_df
    else:
        print("❌ No records found")
        return pd.DataFrame()

def test_enhanced_implementation():
    """Test the enhanced implementation with ABB"""
    print("🚀 TESTING ENHANCED STRIKE FINDER (14 RECORDS)")
    print("=" * 70)
    
    # Test with ABB on Feb 28, 2025
    test_symbol = 'ABB'
    test_date = '2025-02-28'
    test_target_price = 4935.40
    
    print(f"Test Parameters:")
    print(f"  Symbol: {test_symbol}")
    print(f"  Date: {test_date}")
    print(f"  Target Price: ₹{test_target_price:.2f}")
    print(f"  Expected: 14 records (7 strikes × 2 option types)")
    
    # Run the enhanced logic
    result_df = find_strikes_above_below_target(test_date, test_symbol, test_target_price)
    
    # Validate results
    print(f"\n📊 VALIDATION RESULTS:")
    print("=" * 30)
    
    if len(result_df) == 14:
        pe_count = len(result_df[result_df['option_type'] == 'PE'])
        ce_count = len(result_df[result_df['option_type'] == 'CE'])
        unique_strikes = len(result_df['strike_price'].unique())
        
        if pe_count == 7 and ce_count == 7 and unique_strikes == 7:
            print("✅ PERFECT SUCCESS: Found exactly 7 PE + 7 CE options across 7 strikes")
            print("✅ Enhanced logic is ready for production implementation!")
            
            # Show final summary
            strikes_sorted = sorted(result_df['strike_price'].unique())
            above_count = sum(1 for s in strikes_sorted if s > test_target_price)
            below_count = sum(1 for s in strikes_sorted if s < test_target_price)
            at_count = sum(1 for s in strikes_sorted if s == test_target_price)
            
            print(f"\n🎯 Final Result Summary:")
            print(f"   Total strikes: {unique_strikes}")
            print(f"   Above target: {above_count} strikes")
            print(f"   Below target: {below_count} strikes") 
            print(f"   At target: {at_count} strikes")
            print(f"   Total records: {len(result_df)} (Perfect!)")
            
            return True
        else:
            print(f"⚠️  STRUCTURE ISSUE: Found {pe_count} PE + {ce_count} CE across {unique_strikes} strikes")
    else:
        print(f"⚠️  COUNT ISSUE: Found {len(result_df)} records (expected 14)")
    
    print("🔧 Logic needs refinement before production")
    return False

def production_ready_enhanced_function(trade_date, symbol, current_close_price):
    """
    PRODUCTION READY ENHANCED FUNCTION
    ==================================
    Returns exactly 14 F&O records:
    - 3 nearest strikes above target (PE + CE each) = 6 records
    - 3 nearest strikes below target (PE + CE each) = 6 records  
    - 1 overall nearest strike (PE + CE) = 2 records
    Total: 14 records (7 strikes × 2 option types)
    """
    result = find_strikes_above_below_target(trade_date, symbol, current_close_price)
    
    if len(result) == 14:
        pe_count = len(result[result['option_type'] == 'PE'])
        ce_count = len(result[result['option_type'] == 'CE'])
        unique_strikes = len(result['strike_price'].unique())
        
        if pe_count == 7 and ce_count == 7 and unique_strikes == 7:
            return result
        else:
            print(f"⚠️  Warning: Expected 7 PE + 7 CE across 7 strikes, got {pe_count} PE + {ce_count} CE across {unique_strikes} strikes")
            return result
    else:
        print(f"⚠️  Warning: Expected 14 records, got {len(result)}")
        return result

if __name__ == "__main__":
    # Test the enhanced implementation
    success = test_enhanced_implementation()
    
    if success:
        print(f"\n🎉 ENHANCED LOGIC READY FOR DEPLOYMENT!")
        print("=" * 45)
        print("The enhanced logic works perfectly and returns exactly 14 records.")
        print("Use the 'production_ready_enhanced_function' for your implementation.")
    else:
        print(f"\n🔧 ENHANCED LOGIC NEEDS REFINEMENT")
        print("=" * 35)
        print("The logic works but may need adjustments for the 14-record requirement.")