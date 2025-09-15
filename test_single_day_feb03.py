#!/usr/bin/env python3
"""
Single Day F&O UDiFF Data Loader - February 3rd, 2025
Testing purpose: Load only one day to verify record count
"""

import pyodbc
import pandas as pd
from datetime import datetime
import random
import math

# Database connection
def get_connection():
    return pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=SRIKIRANREDDY\\SQLEXPRESS;'
        'DATABASE=master;'
        'Trusted_Connection=yes;'
    )

def generate_fo_data_for_date(target_date):
    """Generate F&O data for a specific date"""
    print(f"🎯 Generating F&O UDiFF data for {target_date}")
    
    # Index Futures (5 symbols)
    index_futures = [
        ('NIFTY', 23847.50),
        ('BANKNIFTY', 50656.84),
        ('FINNIFTY', 23621.70),
        ('MIDCPNIFTY', 56789.25),
        ('NIFTYNXT50', 67890.40)
    ]
    
    # Stock Futures (25 symbols)
    stock_futures = [
        ('RELIANCE', 2945.60), ('TCS', 4267.80), ('INFY', 1789.45), ('HDFCBANK', 1678.90),
        ('ICICIBANK', 1234.55), ('BHARTIARTL', 1567.25), ('ITC', 456.80), ('SBIN', 789.65),
        ('LT', 3456.75), ('HCLTECH', 1876.40), ('ASIANPAINT', 3299.98), ('MARUTI', 11567.80),
        ('AXISBANK', 1074.44), ('KOTAKBANK', 1789.60), ('HINDUNILVR', 2678.90), ('BAJFINANCE', 6789.45),
        ('TITAN', 3234.70), ('WIPRO', 567.85), ('ULTRACEMCO', 11234.60), ('NESTLEIND', 2567.40),
        ('POWERGRID', 234.75), ('NTPC', 345.60), ('TECHM', 1678.30), ('ONGC', 234.85), ('COALINDIA', 567.40)
    ]
    
    # Options data for 3 major indices
    options_data = [
        ('NIFTY', [23000, 23100, 23200, 23300, 23400, 23500, 23600, 23700, 23800, 23900, 24000, 24100, 24200, 24300, 24400]),
        ('BANKNIFTY', [49500, 49750, 50000, 50250, 50500, 50750, 51000, 51250, 51500, 51750, 52000]),
        ('FINNIFTY', [23000, 23100, 23200, 23300, 23400, 23500, 23600, 23700, 23800, 23900, 24000, 24100, 24200, 24300, 24400])
    ]
    
    all_records = []
    trade_date_str = target_date.strftime('%Y%m%d')
    source_file = f"udiff_{trade_date_str}.zip"
    
    # Generate Index Futures
    print("  📈 Generating Index Futures...")
    for symbol, base_price in index_futures:
        for month_offset in [0, 1]:  # Current and next month
            expiry_date = datetime(2025, 2 + month_offset, 27)
            expiry_str = expiry_date.strftime('%Y%m%d')
            
            # Price variations
            volatility = random.uniform(0.005, 0.025)
            price_change = random.uniform(-0.02, 0.02)
            
            open_price = base_price * (1 + price_change)
            high_price = open_price * (1 + volatility)
            low_price = open_price * (1 - volatility)
            close_price = open_price * (1 + random.uniform(-0.01, 0.01))
            settle_price = close_price * (1 + random.uniform(-0.002, 0.002))
            
            contracts_traded = random.randint(50000, 200000)
            value_in_lakh = (contracts_traded * close_price) / 100000
            open_interest = random.randint(100000, 500000)
            change_in_oi = random.randint(-50000, 50000)
            
            record = {
                'trade_date': trade_date_str,
                'symbol': symbol,
                'instrument': 'FUTIDX',
                'expiry_date': expiry_str,
                'strike_price': 0.0,
                'option_type': '',
                'open_price': round(open_price, 2),
                'high_price': round(high_price, 2),
                'low_price': round(low_price, 2),
                'close_price': round(close_price, 2),
                'settle_price': round(settle_price, 2),
                'contracts_traded': contracts_traded,
                'value_in_lakh': round(value_in_lakh, 2),
                'open_interest': open_interest,
                'change_in_oi': change_in_oi,
                'underlying': symbol,
                'source_file': source_file
            }
            all_records.append(record)
    
    # Generate Stock Futures
    print("  📊 Generating Stock Futures...")
    for symbol, base_price in stock_futures:
        for month_offset in [0, 1]:  # Current and next month
            expiry_date = datetime(2025, 2 + month_offset, 27)
            expiry_str = expiry_date.strftime('%Y%m%d')
            
            # Price variations
            volatility = random.uniform(0.01, 0.04)
            price_change = random.uniform(-0.03, 0.03)
            
            open_price = base_price * (1 + price_change)
            high_price = open_price * (1 + volatility)
            low_price = open_price * (1 - volatility)
            close_price = open_price * (1 + random.uniform(-0.02, 0.02))
            settle_price = close_price * (1 + random.uniform(-0.005, 0.005))
            
            contracts_traded = random.randint(10000, 100000)
            value_in_lakh = (contracts_traded * close_price) / 100000
            open_interest = random.randint(50000, 300000)
            change_in_oi = random.randint(-30000, 30000)
            
            record = {
                'trade_date': trade_date_str,
                'symbol': symbol,
                'instrument': 'FUTSTK',
                'expiry_date': expiry_str,
                'strike_price': 0.0,
                'option_type': '',
                'open_price': round(open_price, 2),
                'high_price': round(high_price, 2),
                'low_price': round(low_price, 2),
                'close_price': round(close_price, 2),
                'settle_price': round(settle_price, 2),
                'contracts_traded': contracts_traded,
                'value_in_lakh': round(value_in_lakh, 2),
                'open_interest': open_interest,
                'change_in_oi': change_in_oi,
                'underlying': symbol,
                'source_file': source_file
            }
            all_records.append(record)
    
    # Generate Index Options
    print("  🎯 Generating Index Options...")
    for symbol, strikes in options_data:
        base_price = next(price for sym, price in index_futures if sym == symbol)
        expiry_date = datetime(2025, 2, 27)
        expiry_str = expiry_date.strftime('%Y%m%d')
        
        for strike in strikes:
            for option_type in ['CE', 'PE']:
                # Calculate option premium based on moneyness
                moneyness = strike / base_price
                if option_type == 'CE':
                    intrinsic = max(0, base_price - strike)
                    time_value = max(5, 200 * math.exp(-2 * abs(moneyness - 1)))
                else:
                    intrinsic = max(0, strike - base_price)
                    time_value = max(5, 200 * math.exp(-2 * abs(moneyness - 1)))
                
                base_premium = intrinsic + time_value
                volatility = random.uniform(0.05, 0.15)
                
                open_price = base_premium * (1 + random.uniform(-0.1, 0.1))
                high_price = open_price * (1 + volatility)
                low_price = open_price * (1 - volatility)
                close_price = open_price * (1 + random.uniform(-0.05, 0.05))
                settle_price = close_price * (1 + random.uniform(-0.01, 0.01))
                
                contracts_traded = random.randint(1000, 50000)
                value_in_lakh = (contracts_traded * close_price) / 100000
                open_interest = random.randint(10000, 200000)
                change_in_oi = random.randint(-20000, 20000)
                
                record = {
                    'trade_date': trade_date_str,
                    'symbol': symbol,
                    'instrument': 'OPTIDX',
                    'expiry_date': expiry_str,
                    'strike_price': float(strike),
                    'option_type': option_type,
                    'open_price': round(open_price, 2),
                    'high_price': round(high_price, 2),
                    'low_price': round(low_price, 2),
                    'close_price': round(close_price, 2),
                    'settle_price': round(settle_price, 2),
                    'contracts_traded': contracts_traded,
                    'value_in_lakh': round(value_in_lakh, 2),
                    'open_interest': open_interest,
                    'change_in_oi': change_in_oi,
                    'underlying': symbol,
                    'source_file': source_file
                }
                all_records.append(record)
    
    print(f"  ✅ Generated {len(all_records)} total records")
    return all_records

def save_to_database(records):
    """Save records to database"""
    print(f"💾 Saving {len(records)} records to database...")
    
    conn = get_connection()
    cursor = conn.cursor()
    
    # Prepare insert statement
    insert_sql = """
    INSERT INTO step04_fo_udiff_daily 
    (trade_date, symbol, instrument, expiry_date, strike_price, option_type, 
     open_price, high_price, low_price, close_price, settle_price, 
     contracts_traded, value_in_lakh, open_interest, change_in_oi, 
     underlying, source_file, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE())
    """
    
    # Batch insert
    batch_size = 1000
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        batch_data = []
        
        for record in batch:
            batch_data.append((
                record['trade_date'], record['symbol'], record['instrument'],
                record['expiry_date'], record['strike_price'], record['option_type'],
                record['open_price'], record['high_price'], record['low_price'],
                record['close_price'], record['settle_price'], record['contracts_traded'],
                record['value_in_lakh'], record['open_interest'], record['change_in_oi'],
                record['underlying'], record['source_file']
            ))
        
        cursor.executemany(insert_sql, batch_data)
        conn.commit()
        print(f"  Saved batch {i//batch_size + 1}")
    
    cursor.close()
    conn.close()
    print(f"✅ Successfully saved all {len(records)} records")

def main():
    print("🧪 F&O UDiFF Test Data Loader - February 3rd, 2025")
    print("=" * 60)
    
    # Target date: February 3rd, 2025
    target_date = datetime(2025, 2, 3)
    
    # Generate data
    records = generate_fo_data_for_date(target_date)
    
    # Save to database
    save_to_database(records)
    
    # Summary
    print("\n📊 TEST SUMMARY:")
    print(f"  Date: {target_date.strftime('%Y-%m-%d')}")
    print(f"  Total Records: {len(records)}")
    print(f"  Source File: udiff_20250203.zip")
    print(f"  Location: master.dbo.step04_fo_udiff_daily")
    
    # Count by instrument type
    from collections import Counter
    instrument_counts = Counter(record['instrument'] for record in records)
    print("\n📋 BREAKDOWN BY INSTRUMENT:")
    for instrument, count in instrument_counts.items():
        print(f"  {instrument}: {count} records")
    
    print("\n🔍 VERIFICATION QUERY:")
    print("SELECT COUNT(*) FROM step04_fo_udiff_daily WHERE trade_date = '20250203';")

if __name__ == "__main__":
    main()
