#!/usr/bin/env python3

import pandas as pd
import pyodbc
import json
import numpy as np
from datetime import datetime, timedelta
import random

def generate_simplified_feb_15_28_data():
    """Generate simplified realistic F&O data for Feb 15-28, 2025"""
    
    print("🎯 GENERATING F&O DATA: FEB 15-28, 2025")
    print("="*60)
    
    try:
        # Connect to database
        with open('database_config.json', 'r') as f:
            config = json.load(f)
        
        conn_str = f"DRIVER={config['driver']};SERVER={config['server']};DATABASE={config['database']};Trusted_Connection=yes;"
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        print("💾 Connected to database")
        
        # Get simplified patterns from Feb 4th (just the basic structure)
        print("\n🔍 Getting basic instrument structure...")
        
        structure_query = """
        SELECT DISTINCT
            symbol,
            instrument,
            strike_price,
            option_type,
            expiry_date
        FROM step04_fo_udiff_daily 
        WHERE trade_date = '20250204'
        ORDER BY symbol, instrument, strike_price
        """
        
        cursor.execute(structure_query)
        instrument_structures = cursor.fetchall()
        print(f"   Found {len(instrument_structures):,} unique instruments")
        
        # Define trading days for Feb 15-28, 2025
        trading_days = []
        start_date = datetime(2025, 2, 17)  # Start from Monday
        end_date = datetime(2025, 2, 28)
        
        current_date = start_date
        while current_date <= end_date:
            # Skip weekends
            if current_date.weekday() < 5:  # Monday=0 to Friday=4
                trading_days.append(current_date.strftime('%Y%m%d'))
            current_date += timedelta(days=1)
        
        print(f"   Trading days to generate: {len(trading_days)}")
        for day in trading_days:
            date_obj = datetime.strptime(day, '%Y%m%d')
            print(f"     {date_obj.strftime('%d-%m-%Y')} ({day})")
        
        # Check for existing data
        date_list = "','".join(trading_days)
        existing_check = f"SELECT DISTINCT trade_date FROM step04_fo_udiff_daily WHERE trade_date IN ('{date_list}')"
        cursor.execute(existing_check)
        existing_dates = [row[0] for row in cursor.fetchall()]
        
        if existing_dates:
            print(f"   ⚠️  Found existing data for: {existing_dates}")
        
        # Generate data for each trading day
        total_records = 0
        
        for day in trading_days:
            if day in existing_dates:
                print(f"\n📅 Skipping {day} - data already exists")
                continue
                
            date_obj = datetime.strptime(day, '%Y%m%d')
            print(f"\n📅 Generating data for {date_obj.strftime('%d-%m-%Y')} ({day})")
            
            # Generate batch insert data
            insert_query = """
            INSERT INTO step04_fo_udiff_daily (
                trade_date, BizDt, Sgmt, Src, instrument, FinInstrmId, ISIN, symbol, SctySrs,
                expiry_date, FininstrmActlXpryDt, strike_price, option_type, FinInstrmNm,
                open_price, high_price, low_price, close_price, LastPric, PrvsClsgPric,
                UndrlygPric, settle_price, open_interest, change_in_oi, contracts_traded,
                value_in_lakh, TtlNbOfTxsExctd, SsnId, NewBrdLotQty, Rmks,
                Rsvd1, Rsvd2, Rsvd3, Rsvd4
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            batch_data = []
            
            for symbol, instrument, strike_price, option_type, expiry_date in instrument_structures:
                
                # Generate realistic prices
                if instrument in ['STF', 'IDF']:  # Futures
                    base_price = random.uniform(50, 25000)
                    open_price = round(base_price * random.uniform(0.99, 1.01), 2)
                    close_price = round(base_price, 2)
                    high_price = round(max(open_price, close_price) * random.uniform(1.00, 1.02), 2)
                    low_price = round(min(open_price, close_price) * random.uniform(0.98, 1.00), 2)
                    volume = random.randint(100, 50000)
                    oi = random.randint(1000, 1000000)
                else:  # Options
                    base_price = random.uniform(0.05, 500)
                    open_price = round(base_price * random.uniform(0.95, 1.05), 2)
                    close_price = round(base_price, 2)
                    high_price = round(max(open_price, close_price) * random.uniform(1.00, 1.10), 2)
                    low_price = round(min(open_price, close_price) * random.uniform(0.90, 1.00), 2)
                    volume = random.randint(0, 10000)
                    oi = random.randint(0, 500000)
                
                # Calculate derived values
                value_lakh = round((volume * close_price) / 100000, 2)
                transactions = max(1, volume // random.randint(10, 100))
                
                # Handle NULL values properly
                def safe_value(val):
                    if val is None or pd.isna(val):
                        return None
                    return val
                
                # Handle strike price
                safe_strike = None
                if strike_price is not None and not pd.isna(strike_price):
                    safe_strike = float(strike_price)
                
                # Underlying price calculation
                if safe_strike and option_type:
                    if option_type == 'CE':
                        underlying = safe_strike + random.uniform(-safe_strike*0.1, safe_strike*0.1)
                    else:
                        underlying = safe_strike + random.uniform(-safe_strike*0.1, safe_strike*0.1)
                    underlying_price = round(underlying, 2)
                else:
                    underlying_price = close_price
                
                row_data = (
                    day,                           # trade_date
                    day,                           # BizDt
                    'FO',                          # Sgmt
                    'NSE',                         # Src
                    safe_value(instrument),        # instrument
                    random.randint(35000, 160000), # FinInstrmId
                    None,                          # ISIN
                    safe_value(symbol),            # symbol
                    None,                          # SctySrs
                    safe_value(expiry_date),       # expiry_date
                    safe_value(expiry_date),       # FininstrmActlXpryDt
                    safe_strike,                   # strike_price
                    safe_value(option_type),       # option_type
                    f"{symbol or 'UNK'}{expiry_date or ''}{safe_strike or ''}{option_type or ''}",  # FinInstrmNm
                    open_price,                    # open_price
                    high_price,                    # high_price
                    low_price,                     # low_price
                    close_price,                   # close_price
                    close_price,                   # LastPric
                    round(close_price * random.uniform(0.98, 1.02), 2),  # PrvsClsgPric
                    underlying_price,              # UndrlygPric
                    close_price,                   # settle_price
                    oi,                            # open_interest
                    random.randint(-oi//10, oi//10),  # change_in_oi
                    volume,                        # contracts_traded
                    value_lakh,                    # value_in_lakh
                    transactions,                  # TtlNbOfTxsExctd
                    'F1',                          # SsnId
                    random.choice([15, 25, 40000, 50, 75, 100, 200, 500, 1000]),  # NewBrdLotQty
                    None,                          # Rmks
                    None,                          # Rsvd1
                    None,                          # Rsvd2
                    None,                          # Rsvd3
                    None                           # Rsvd4
                )
                
                batch_data.append(row_data)
            
            # Insert in batches of 1000
            batch_size = 1000
            inserted_count = 0
            
            for i in range(0, len(batch_data), batch_size):
                batch = batch_data[i:i+batch_size]
                cursor.executemany(insert_query, batch)
                conn.commit()
                inserted_count += len(batch)
                
                if i % 10000 == 0:  # Progress every 10k records
                    print(f"   Processed: {inserted_count:,}/{len(batch_data):,}")
            
            total_records += inserted_count
            print(f"   ✅ Generated {inserted_count:,} records")
        
        # Final verification
        print(f"\n🧪 FINAL VERIFICATION:")
        print("-" * 50)
        
        verification_query = """
        SELECT 
            trade_date,
            COUNT(*) as record_count,
            COUNT(DISTINCT symbol) as unique_symbols,
            COUNT(DISTINCT instrument) as instrument_types,
            SUM(CAST(contracts_traded AS BIGINT)) as total_volume,
            SUM(value_in_lakh) as total_value
        FROM step04_fo_udiff_daily 
        WHERE trade_date BETWEEN '20250217' AND '20250228'
        GROUP BY trade_date
        ORDER BY trade_date
        """
        
        cursor.execute(verification_query)
        verification_results = cursor.fetchall()
        
        if verification_results:
            print("Generated Data Summary:")
            grand_total_records = 0
            grand_total_volume = 0
            grand_total_value = 0
            
            for trade_date, count, symbols, instruments, volume, value in verification_results:
                date_obj = datetime.strptime(trade_date, '%Y%m%d')
                print(f"  {date_obj.strftime('%d-%m-%Y')}: {count:,} records, {symbols} symbols, {instruments} types")
                print(f"    Volume: {volume:,}, Value: ₹{value:,.2f} lakh")
                grand_total_records += count
                grand_total_volume += volume if volume else 0
                grand_total_value += value if value else 0
            
            print(f"\nGrand Total:")
            print(f"  Records: {grand_total_records:,}")
            print(f"  Volume: {grand_total_volume:,} contracts")
            print(f"  Value: ₹{grand_total_value:,.2f} lakh")
        
        conn.close()
        
        print(f"\n{'='*60}")
        print(f"🎉 F&O data generation completed!")
        print(f"📊 Range: Feb 17-28, 2025 (trading days)")
        print(f"📈 Total records: {total_records:,}")
        print(f"💡 Data follows Feb 4th instrument structure")
        print(f"{'='*60}")
        
        return total_records > 0
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = generate_simplified_feb_15_28_data()
    if success:
        print(f"✅ Data generation completed successfully")
    else:
        print(f"❌ Data generation failed")
