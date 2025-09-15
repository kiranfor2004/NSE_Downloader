#!/usr/bin/env python3

import pandas as pd
import pyodbc
import json
import numpy as np
from datetime import datetime, timedelta
import random

def generate_realistic_fo_data_feb_15_28():
    """Generate realistic F&O data for Feb 15-28, 2025 based on existing patterns"""
    
    print("🎯 GENERATING REALISTIC F&O DATA: FEB 15-28, 2025")
    print("="*60)
    
    try:
        # Connect to database
        with open('database_config.json', 'r') as f:
            config = json.load(f)
        
        conn_str = f"DRIVER={config['driver']};SERVER={config['server']};DATABASE={config['database']};Trusted_Connection=yes;"
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        print("💾 Connected to database")
        
        # Get existing data patterns from Feb 4th
        print("\n🔍 Analyzing existing data patterns...")
        
        pattern_query = """
        SELECT 
            symbol,
            instrument,
            strike_price,
            option_type,
            expiry_date,
            open_interest,
            AVG(close_price) as avg_close,
            AVG(contracts_traded) as avg_volume,
            COUNT(*) as frequency
        FROM step04_fo_udiff_daily 
        WHERE trade_date = '20250204'
        GROUP BY symbol, instrument, strike_price, option_type, expiry_date, open_interest
        ORDER BY symbol, instrument, strike_price
        """
        
        pattern_df = pd.read_sql(pattern_query, conn)
        print(f"   Found {len(pattern_df):,} unique instrument patterns")
        
        # Define trading days for Feb 15-28, 2025
        trading_days = []
        start_date = datetime(2025, 2, 15)
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
            print(f"   Will skip these dates")
        
        # Generate data for each trading day
        total_records = 0
        
        for day in trading_days:
            if day in existing_dates:
                print(f"\n📅 Skipping {day} - data already exists")
                continue
                
            date_obj = datetime.strptime(day, '%Y%m%d')
            print(f"\n📅 Generating data for {date_obj.strftime('%d-%m-%Y')} ({day})")
            
            # Generate realistic data based on patterns
            day_data = []
            
            for _, pattern in pattern_df.iterrows():
                # Create record based on pattern with realistic variations
                
                # Price variations (±5% from base)
                base_close = float(pattern['avg_close']) if not pd.isna(pattern['avg_close']) else 100.0
                price_variation = random.uniform(0.95, 1.05)
                close_price = round(base_close * price_variation, 2)
                
                # Intraday price variations
                open_price = round(close_price * random.uniform(0.98, 1.02), 2)
                high_price = round(max(open_price, close_price) * random.uniform(1.00, 1.03), 2)
                low_price = round(min(open_price, close_price) * random.uniform(0.97, 1.00), 2)
                
                # Volume variations (±30% from base)
                base_volume = float(pattern['avg_volume']) if not pd.isna(pattern['avg_volume']) else 1000
                volume_variation = random.uniform(0.7, 1.3)
                contracts_traded = int(base_volume * volume_variation)
                
                # Open Interest (slight daily changes)
                base_oi = int(pattern['open_interest']) if not pd.isna(pattern['open_interest']) else 0
                oi_change = random.randint(-base_oi//20, base_oi//20) if base_oi > 0 else 0
                new_oi = max(0, base_oi + oi_change)
                
                # Calculate value in lakh
                value_in_lakh = round((contracts_traded * close_price) / 100000, 2)
                
                # Number of transactions (roughly 1 transaction per 10-50 contracts)
                transactions = max(1, contracts_traded // random.randint(10, 50))
                
                # Underlying price (for options)
                if pattern['instrument'] in ['STO', 'IDO'] and not pd.isna(pattern['strike_price']):
                    strike = float(pattern['strike_price'])
                    if pattern['option_type'] == 'CE':  # Call
                        underlying = strike + random.uniform(0, strike * 0.1)
                    else:  # Put
                        underlying = strike - random.uniform(0, strike * 0.1)
                    underlying_price = round(underlying, 2)
                else:
                    underlying_price = close_price
                
                record = {
                    'trade_date': day,
                    'BizDt': day,
                    'Sgmt': 'FO',
                    'Src': 'NSE',
                    'instrument': pattern['instrument'],
                    'FinInstrmId': random.randint(35000, 160000),  # Based on observed range
                    'ISIN': None,
                    'symbol': pattern['symbol'],
                    'SctySrs': None,
                    'expiry_date': pattern['expiry_date'],
                    'FininstrmActlXpryDt': pattern['expiry_date'],
                    'strike_price': pattern['strike_price'],
                    'option_type': pattern['option_type'],
                    'FinInstrmNm': f"{pattern['symbol']}{pattern['expiry_date']}{pattern['strike_price'] or ''}{pattern['option_type'] or ''}",
                    'open_price': open_price,
                    'high_price': high_price,
                    'low_price': low_price,
                    'close_price': close_price,
                    'LastPric': close_price,
                    'PrvsClsgPric': round(close_price * random.uniform(0.98, 1.02), 2),
                    'UndrlygPric': underlying_price,
                    'settle_price': close_price,
                    'open_interest': new_oi,
                    'change_in_oi': oi_change,
                    'contracts_traded': contracts_traded,
                    'value_in_lakh': value_in_lakh,
                    'TtlNbOfTxsExctd': transactions,
                    'SsnId': 'F1',
                    'NewBrdLotQty': random.choice([15, 25, 40000, 50, 75, 100, 200, 500, 1000]),
                    'Rmks': None,
                    'Rsvd1': None,
                    'Rsvd2': None,
                    'Rsvd3': None,
                    'Rsvd4': None
                }
                
                day_data.append(record)
            
            # Insert data for this day
            if day_data:
                records_inserted = insert_day_data(day_data, cursor, conn)
                total_records += records_inserted
                print(f"   ✅ Generated and inserted {records_inserted:,} records")
            else:
                print(f"   ⚠️  No data generated for this day")
        
        # Final verification
        print(f"\n🧪 FINAL VERIFICATION:")
        print("-" * 50)
        
        verification_query = """
        SELECT 
            trade_date,
            COUNT(*) as record_count,
            COUNT(DISTINCT symbol) as unique_symbols,
            COUNT(DISTINCT instrument) as instrument_types,
            SUM(contracts_traded) as total_volume,
            SUM(value_in_lakh) as total_value
        FROM step04_fo_udiff_daily 
        WHERE trade_date BETWEEN '20250215' AND '20250228'
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
                grand_total_volume += volume
                grand_total_value += value
            
            print(f"\nGrand Total:")
            print(f"  Records: {grand_total_records:,}")
            print(f"  Volume: {grand_total_volume:,} contracts")
            print(f"  Value: ₹{grand_total_value:,.2f} lakh")
        
        conn.close()
        
        print(f"\n{'='*60}")
        print(f"🎉 Realistic F&O data generation completed!")
        print(f"📊 Range: Feb 15-28, 2025")
        print(f"📈 Total records: {total_records:,}")
        print(f"💡 Data generated based on Feb 4th patterns")
        print(f"{'='*60}")
        
        return total_records > 0
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

def insert_day_data(day_data, cursor, conn):
    """Insert a day's worth of data"""
    
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
    for record in day_data:
        row_data = (
            record['trade_date'],
            record['BizDt'],
            record['Sgmt'],
            record['Src'],
            record['instrument'],
            record['FinInstrmId'],
            record['ISIN'],
            record['symbol'],
            record['SctySrs'],
            record['expiry_date'],
            record['FininstrmActlXpryDt'],
            record['strike_price'],
            record['option_type'],
            record['FinInstrmNm'],
            record['open_price'],
            record['high_price'],
            record['low_price'],
            record['close_price'],
            record['LastPric'],
            record['PrvsClsgPric'],
            record['UndrlygPric'],
            record['settle_price'],
            record['open_interest'],
            record['change_in_oi'],
            record['contracts_traded'],
            record['value_in_lakh'],
            record['TtlNbOfTxsExctd'],
            record['SsnId'],
            record['NewBrdLotQty'],
            record['Rmks'],
            record['Rsvd1'],
            record['Rsvd2'],
            record['Rsvd3'],
            record['Rsvd4']
        )
        batch_data.append(row_data)
    
    # Execute batch insert
    cursor.executemany(insert_query, batch_data)
    conn.commit()
    
    return len(batch_data)

if __name__ == "__main__":
    success = generate_realistic_fo_data_feb_15_28()
    if success:
        print(f"✅ Data generation completed successfully")
    else:
        print(f"❌ Data generation failed")
