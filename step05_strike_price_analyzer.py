#!/usr/bin/env python3
"""
Step 5: Strike Price Analyzer for F&O Options
==============================================

This script finds the nearest strike prices (7 above and 7 below) 
for PE and CE options for symbols from February 2025 data.

Features:
- Gets closing price from step03_compare_monthvspreviousmonth table
- Finds nearest 7 strikes above and below closing price
- Returns 14 records total (7 PE + 7 CE) for each symbol
- Testing with 3 symbols initially

Author: NSE Data Analysis Team
Date: September 2025
"""

import pyodbc
import pandas as pd
import logging
from datetime import datetime
import numpy as np

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def get_database_connection():
    """Create database connection."""
    connection_string = (
        'Driver={ODBC Driver 17 for SQL Server};'
        'Server=SRIKIRANREDDY\\SQLEXPRESS;'
        'Database=master;'
        'Trusted_Connection=yes;'
    )
    return pyodbc.connect(connection_string)

def get_february_symbol_data(conn, symbol):
    """Get symbol data from step03 table for February 2025."""
    query = """
    SELECT 
        symbol,
        current_close_price as closing_price,
        current_trade_date as trading_date,
        current_ttl_trd_qnty as trading_volume
    FROM step03_compare_monthvspreviousmonth
    WHERE symbol = ?
    AND current_trade_date LIKE '2025-02-%'
    ORDER BY current_trade_date DESC
    """
    
    df = pd.read_sql(query, conn, params=[symbol])
    if not df.empty:
        return df.iloc[0]  # Get most recent record
    return None

def get_available_strikes(conn, symbol, month='202502'):
    """Get all available strike prices for a symbol in February 2025."""
    query = """
    SELECT DISTINCT 
        strike_price,
        option_type,
        COUNT(*) as trade_count,
        AVG(close_price) as avg_close_price,
        SUM(contracts_traded) as total_volume
    FROM step04_fo_udiff_daily
    WHERE symbol = ?
    AND trade_date LIKE ?
    AND instrument = 'STO'  -- Stock Options
    AND strike_price IS NOT NULL
    AND option_type IN ('CE', 'PE')
    GROUP BY strike_price, option_type
    ORDER BY strike_price, option_type
    """
    
    df = pd.read_sql(query, conn, params=[symbol, f'{month}%'])
    return df

def find_nearest_strikes_combined(available_strikes, closing_price):
    """Find 7 nearest strikes (total) around closing price for both PE and CE options."""
    # Get unique strike prices from both CE and PE
    unique_strikes = available_strikes['strike_price'].unique()
    
    if len(unique_strikes) == 0:
        return pd.DataFrame()
    
    # Calculate distance from closing price for each unique strike
    strike_distances = []
    for strike in unique_strikes:
        distance = abs(strike - closing_price)
        strike_distances.append({'strike_price': strike, 'distance': distance})
    
    # Sort by distance and take the 7 nearest strikes
    strike_distances = sorted(strike_distances, key=lambda x: x['distance'])[:7]
    selected_strikes = [item['strike_price'] for item in strike_distances]
    
    # Get all records (both PE and CE) for these 7 selected strikes
    result = available_strikes[available_strikes['strike_price'].isin(selected_strikes)].copy()
    
    # Calculate additional fields
    result['distance_from_close'] = abs(result['strike_price'] - closing_price)
    result['price_diff'] = result['strike_price'] - closing_price
    
    # Sort by strike price, then by option type
    result = result.sort_values(['strike_price', 'option_type'])
    
    return result

def analyze_symbol_strikes(conn, symbol):
    """Analyze strike prices for a single symbol."""
    logging.info(f"Analyzing symbol: {symbol}")
    
    # Get symbol data from step03
    symbol_data = get_february_symbol_data(conn, symbol)
    if symbol_data is None:
        logging.warning(f"No February 2025 data found for {symbol} in step03 table")
        return None
    
    closing_price = float(symbol_data['closing_price'])
    trading_date = symbol_data['trading_date']
    
    logging.info(f"{symbol} - Closing Price: ₹{closing_price:.2f}, Date: {trading_date}")
    
    # Get available strikes from F&O data
    available_strikes = get_available_strikes(conn, symbol)
    if available_strikes.empty:
        logging.warning(f"No F&O data found for {symbol} in February 2025")
        return None
    
    logging.info(f"{symbol} - Found {len(available_strikes)} strike-option combinations")
    
    # Find 7 nearest strikes with both PE and CE for each
    all_strikes = find_nearest_strikes_combined(available_strikes, closing_price)
    
    if all_strikes.empty:
        logging.warning(f"No suitable strikes found for {symbol}")
        return None
    
    # Add symbol and other details
    all_strikes['symbol'] = symbol
    all_strikes['closing_price'] = closing_price
    all_strikes['trading_date'] = trading_date
    
    # Add moneyness calculation
    all_strikes['moneyness'] = all_strikes.apply(
        lambda row: calculate_moneyness(row['strike_price'], closing_price, row['option_type']), 
        axis=1
    )
    
    return all_strikes

def calculate_moneyness(strike_price, spot_price, option_type):
    """Calculate option moneyness."""
    if option_type == 'CE':  # Call option
        if strike_price < spot_price * 0.95:
            return 'Deep ITM'
        elif strike_price < spot_price:
            return 'ITM'
        elif strike_price <= spot_price * 1.05:
            return 'Near ATM'
        elif strike_price <= spot_price * 1.15:
            return 'OTM'
        else:
            return 'Deep OTM'
    else:  # Put option
        if strike_price > spot_price * 1.05:
            return 'Deep ITM'
        elif strike_price > spot_price:
            return 'ITM'
        elif strike_price >= spot_price * 0.95:
            return 'Near ATM'
        elif strike_price >= spot_price * 0.85:
            return 'OTM'
        else:
            return 'Deep OTM'

def display_results(symbol, results):
    """Display results in a formatted manner."""
    if results is None or results.empty:
        print(f"\n❌ No results for {symbol}")
        return
    
    closing_price = results.iloc[0]['closing_price']
    trading_date = results.iloc[0]['trading_date']
    
    print(f"\n📊 STRIKE ANALYSIS: {symbol}")
    print("=" * 70)
    print(f"Closing Price: ₹{closing_price:.2f} | Date: {trading_date}")
    print(f"Total Records: {len(results)}")
    
    # Separate CE and PE results
    ce_results = results[results['option_type'] == 'CE'].sort_values('strike_price')
    pe_results = results[results['option_type'] == 'PE'].sort_values('strike_price')
    
    print(f"\n📈 CALL OPTIONS (CE) - {len(ce_results)} records:")
    print("-" * 70)
    if not ce_results.empty:
        for _, row in ce_results.iterrows():
            strike = row['strike_price']
            avg_price = row['avg_close_price']
            volume = row['total_volume']
            moneyness = row['moneyness']
            price_diff = ((strike - closing_price) / closing_price) * 100
            print(f"Strike: {strike:>7.1f} | Avg Price: {avg_price:>6.2f} | Volume: {volume:>8.0f} | {price_diff:>+6.1f}% | {moneyness}")
    
    print(f"\n📉 PUT OPTIONS (PE) - {len(pe_results)} records:")
    print("-" * 70)
    if not pe_results.empty:
        for _, row in pe_results.iterrows():
            strike = row['strike_price']
            avg_price = row['avg_close_price']
            volume = row['total_volume']
            moneyness = row['moneyness']
            price_diff = ((strike - closing_price) / closing_price) * 100
            print(f"Strike: {strike:>7.1f} | Avg Price: {avg_price:>6.2f} | Volume: {volume:>8.0f} | {price_diff:>+6.1f}% | {moneyness}")

def main():
    """Main function to run the strike price analysis."""
    print("🎯 STEP 5: STRIKE PRICE ANALYZER")
    print("=" * 50)
    print("Testing with 3 symbols from February 2025")
    print("Finding 7 nearest strikes around closing price")
    print("For both CE and PE options (14 records total per symbol)")
    
    # Test symbols - symbols with good F&O activity and step03 data
    test_symbols = ['SBIN', 'ICICIBANK', 'HDFCBANK']
    
    try:
        conn = get_database_connection()
        logging.info("Database connection established")
        
        all_results = []
        
        for symbol in test_symbols:
            try:
                result = analyze_symbol_strikes(conn, symbol)
                if result is not None:
                    all_results.append(result)
                    display_results(symbol, result)
                    
                    # Validate 14 records requirement
                    ce_count = len(result[result['option_type'] == 'CE'])
                    pe_count = len(result[result['option_type'] == 'PE'])
                    total_count = len(result)
                    unique_strikes = len(result['strike_price'].unique())
                    
                    print(f"✅ Record count validation: {total_count} total ({ce_count} CE + {pe_count} PE) across {unique_strikes} strikes")
                    if total_count == 14 and ce_count == 7 and pe_count == 7:
                        print("✅ Perfect! Exactly 14 records as required (7 CE + 7 PE)")
                    else:
                        print(f"📊 Got {total_count} records from {unique_strikes} strikes ({ce_count} CE + {pe_count} PE)")
                        
                else:
                    print(f"\n❌ No data available for {symbol}")
                    
            except Exception as e:
                logging.error(f"Error analyzing {symbol}: {e}")
                print(f"\n❌ Error analyzing {symbol}: {e}")
        
        # Summary
        print(f"\n🎯 SUMMARY")
        print("=" * 50)
        total_symbols = len([r for r in all_results if r is not None])
        total_records = sum([len(r) for r in all_results if r is not None])
        
        print(f"Symbols analyzed: {total_symbols}/{len(test_symbols)}")
        print(f"Total strike records: {total_records}")
        print(f"Average records per symbol: {total_records/total_symbols:.1f}" if total_symbols > 0 else "N/A")
        
        if all_results:
            # Check if all results have exactly 14 records
            all_have_14 = all(len(r) == 14 for r in all_results)
            print(f"All symbols have exactly 14 records: {'✅ YES' if all_have_14 else '❌ NO'}")
            
            # Combine all results for further analysis
            combined_results = pd.concat(all_results, ignore_index=True)
            print(f"\n✅ Analysis completed successfully!")
            print(f"Ready to scale to all symbols with F&O data.")
        
    except Exception as e:
        logging.error(f"Database connection error: {e}")
        print(f"❌ Error: {e}")
    
    finally:
        if 'conn' in locals():
            conn.close()
            logging.info("Database connection closed")

if __name__ == "__main__":
    main()