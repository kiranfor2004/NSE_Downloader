#!/usr/bin/env python3
"""
Step 5: Corrected Strike Analyzer - 14 Records Per Symbol
=========================================================

Fixed version that returns exactly 14 records per symbol:
- 7 nearest strikes around closing price
- Both CE and PE for each strike (7 × 2 = 14 records)
- Aggregated F&O data (not individual daily records)
- Testing with only 2 symbols: SBIN, ICICIBANK

Author: NSE Data Analysis Team
Date: September 2025
"""

import pyodbc
import pandas as pd
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_database_connection():
    connection_string = (
        'Driver={ODBC Driver 17 for SQL Server};'
        'Server=SRIKIRANREDDY\\SQLEXPRESS;'
        'Database=master;'
        'Trusted_Connection=yes;'
    )
    return pyodbc.connect(connection_string)

def create_corrected_table(conn):
    """Create corrected step05_strike_analysis table for 14 records per symbol."""
    drop_sql = "IF OBJECT_ID('step05_strike_analysis', 'U') IS NOT NULL DROP TABLE step05_strike_analysis"
    
    create_sql = """
    CREATE TABLE step05_strike_analysis (
        id BIGINT IDENTITY(1,1) PRIMARY KEY,
        analysis_date DATETIME2 DEFAULT GETDATE(),
        
        -- Equity data
        equity_symbol NVARCHAR(50) NOT NULL,
        equity_closing_price DECIMAL(18,4) NOT NULL,
        equity_trade_date DATE NOT NULL,
        
        -- Strike analysis
        fo_strike_price DECIMAL(18,4) NOT NULL,
        fo_option_type VARCHAR(5) NOT NULL,
        distance_from_close DECIMAL(18,4),
        price_diff_percent DECIMAL(10,4),
        moneyness NVARCHAR(20),
        strike_rank INT,
        
        -- Aggregated F&O data (from all trading days in February)
        trade_days_count INT,
        avg_close_price DECIMAL(18,4),
        total_contracts_traded BIGINT,
        total_value_in_lakh DECIMAL(18,4),
        avg_open_interest BIGINT,
        min_close_price DECIMAL(18,4),
        max_close_price DECIMAL(18,4),
        
        -- Most recent F&O data
        latest_trade_date VARCHAR(10),
        latest_close_price DECIMAL(18,4),
        latest_open_interest BIGINT,
        latest_expiry_date VARCHAR(10),
        
        INDEX IX_step05_symbol (equity_symbol),
        INDEX IX_step05_strike (fo_strike_price),
        INDEX IX_step05_option_type (fo_option_type)
    )
    """
    
    cursor = conn.cursor()
    try:
        cursor.execute(drop_sql)
        cursor.execute(create_sql)
        conn.commit()
        logging.info("✅ Corrected step05_strike_analysis table created")
    except Exception as e:
        logging.error(f"Error creating table: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()

def get_symbol_data(conn, symbol):
    """Get symbol data from step03."""
    query = """
    SELECT symbol, current_close_price as closing_price, current_trade_date as trading_date
    FROM step03_compare_monthvspreviousmonth
    WHERE symbol = ? AND current_trade_date LIKE '2025-02-%'
    ORDER BY current_trade_date DESC
    """
    df = pd.read_sql(query, conn, params=[symbol])
    return df.iloc[0] if not df.empty else None

def get_aggregated_fo_data(conn, symbol):
    """Get aggregated F&O data for symbol - exactly like the testing script."""
    query = """
    SELECT 
        strike_price,
        option_type,
        COUNT(*) as trade_days_count,
        AVG(close_price) as avg_close_price,
        SUM(contracts_traded) as total_contracts_traded,
        SUM(value_in_lakh) as total_value_in_lakh,
        AVG(open_interest) as avg_open_interest,
        MIN(close_price) as min_close_price,
        MAX(close_price) as max_close_price,
        MAX(trade_date) as latest_trade_date,
        MAX(expiry_date) as latest_expiry_date
    FROM step04_fo_udiff_daily
    WHERE symbol = ? 
    AND trade_date LIKE '202502%' 
    AND instrument = 'STO'
    AND strike_price IS NOT NULL 
    AND option_type IN ('CE', 'PE')
    GROUP BY strike_price, option_type
    ORDER BY strike_price, option_type
    """
    return pd.read_sql(query, conn, params=[symbol])

def get_latest_close_price(conn, symbol, strike_price, option_type):
    """Get latest close price for a specific strike-option combination."""
    query = """
    SELECT TOP 1 close_price, open_interest, trade_date
    FROM step04_fo_udiff_daily
    WHERE symbol = ? AND strike_price = ? AND option_type = ?
    AND trade_date LIKE '202502%' AND instrument = 'STO'
    ORDER BY trade_date DESC
    """
    df = pd.read_sql(query, conn, params=[symbol, strike_price, option_type])
    if not df.empty:
        return df.iloc[0]['close_price'], df.iloc[0]['open_interest']
    return None, None

def find_exactly_7_nearest_strikes(fo_data, closing_price):
    """Find exactly 7 nearest strikes and return 14 records (7 strikes × 2 option types)."""
    # Get unique strike prices
    unique_strikes = fo_data['strike_price'].unique()
    if len(unique_strikes) == 0:
        return pd.DataFrame()
    
    # Calculate distance from closing price for each unique strike
    strike_distances = [(strike, abs(strike - closing_price)) for strike in unique_strikes]
    strike_distances.sort(key=lambda x: x[1])  # Sort by distance
    
    # Take exactly 7 nearest strikes
    selected_strikes = [strike for strike, _ in strike_distances[:7]]
    
    # Filter data for selected strikes - should give us exactly 14 records (7 strikes × 2 option types)
    result = fo_data[fo_data['strike_price'].isin(selected_strikes)].copy()
    
    # Add analysis fields
    result['distance_from_close'] = abs(result['strike_price'] - closing_price)
    result['price_diff_percent'] = ((result['strike_price'] - closing_price) / closing_price) * 100
    result['moneyness'] = result.apply(lambda row: get_moneyness(row['strike_price'], closing_price, row['option_type']), axis=1)
    
    # Add strike rank (1 = nearest to closing price)
    strike_rank_map = {strike: rank+1 for rank, (strike, _) in enumerate(strike_distances[:7])}
    result['strike_rank'] = result['strike_price'].map(strike_rank_map)
    
    return result.sort_values(['strike_price', 'option_type'])

def get_moneyness(strike, spot, option_type):
    """Calculate moneyness."""
    if option_type == 'CE':
        if strike < spot * 0.95: return 'Deep ITM'
        elif strike < spot: return 'ITM'
        elif strike <= spot * 1.05: return 'Near ATM'
        elif strike <= spot * 1.15: return 'OTM'
        else: return 'Deep OTM'
    else:
        if strike > spot * 1.05: return 'Deep ITM'
        elif strike > spot: return 'ITM'
        elif strike >= spot * 0.95: return 'Near ATM'
        elif strike >= spot * 0.85: return 'OTM'
        else: return 'Deep OTM'

def insert_results(conn, symbol_data, fo_results):
    """Insert exactly 14 records per symbol."""
    if fo_results.empty:
        return 0
    
    insert_sql = """
    INSERT INTO step05_strike_analysis (
        equity_symbol, equity_closing_price, equity_trade_date,
        fo_strike_price, fo_option_type,
        distance_from_close, price_diff_percent, moneyness, strike_rank,
        trade_days_count, avg_close_price, total_contracts_traded, total_value_in_lakh,
        avg_open_interest, min_close_price, max_close_price,
        latest_trade_date, latest_close_price, latest_open_interest, latest_expiry_date
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    cursor = conn.cursor()
    inserted = 0
    
    try:
        for _, row in fo_results.iterrows():
            # Get latest close price for this specific strike-option
            latest_close, latest_oi = get_latest_close_price(
                conn, symbol_data['symbol'], row['strike_price'], row['option_type']
            )
            
            values = (
                symbol_data['symbol'], 
                float(symbol_data['closing_price']), 
                symbol_data['trading_date'],
                float(row['strike_price']),
                row['option_type'],
                float(row['distance_from_close']),
                float(row['price_diff_percent']),
                row['moneyness'],
                int(row['strike_rank']),
                int(row['trade_days_count']),
                float(row['avg_close_price']) if pd.notna(row['avg_close_price']) else None,
                int(row['total_contracts_traded']) if pd.notna(row['total_contracts_traded']) else None,
                float(row['total_value_in_lakh']) if pd.notna(row['total_value_in_lakh']) else None,
                int(row['avg_open_interest']) if pd.notna(row['avg_open_interest']) else None,
                float(row['min_close_price']) if pd.notna(row['min_close_price']) else None,
                float(row['max_close_price']) if pd.notna(row['max_close_price']) else None,
                row['latest_trade_date'],
                float(latest_close) if latest_close is not None else None,
                int(latest_oi) if latest_oi is not None else None,
                row['latest_expiry_date']
            )
            cursor.execute(insert_sql, values)
            inserted += 1
        
        conn.commit()
        logging.info(f"✅ Inserted {inserted} records for {symbol_data['symbol']}")
        return inserted
        
    except Exception as e:
        logging.error(f"Error inserting: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()

def main():
    print("🎯 STEP 5: CORRECTED ANALYZER - 14 RECORDS PER SYMBOL")
    print("=" * 60)
    print("Testing with 2 symbols only: SBIN, ICICIBANK")
    print("Expected result: 28 total records (14 per symbol)")
    
    # Only 2 symbols for testing as requested
    test_symbols = ['SBIN', 'ICICIBANK']
    
    try:
        conn = get_database_connection()
        logging.info("Database connected")
        
        create_corrected_table(conn)
        
        total_inserted = 0
        successful = 0
        
        for symbol in test_symbols:
            try:
                print(f"\n🔍 Processing {symbol}...")
                
                # Get equity data
                symbol_data = get_symbol_data(conn, symbol)
                if symbol_data is None:
                    print(f"❌ {symbol}: No equity data")
                    continue
                
                # Get aggregated F&O data
                fo_data = get_aggregated_fo_data(conn, symbol)
                if fo_data.empty:
                    print(f"❌ {symbol}: No F&O data")
                    continue
                
                print(f"   Found {len(fo_data)} strike-option combinations")
                
                # Find exactly 7 nearest strikes (should give 14 records)
                closing_price = float(symbol_data['closing_price'])
                results = find_exactly_7_nearest_strikes(fo_data, closing_price)
                
                if results.empty:
                    print(f"❌ {symbol}: No suitable strikes")
                    continue
                
                # Validate exactly 14 records
                ce_count = len(results[results['option_type'] == 'CE'])
                pe_count = len(results[results['option_type'] == 'PE'])
                unique_strikes = len(results['strike_price'].unique())
                
                print(f"   Strikes found: {unique_strikes}")
                print(f"   Records: {len(results)} total ({ce_count} CE + {pe_count} PE)")
                
                if len(results) != 14:
                    print(f"⚠️  Expected 14 records, got {len(results)}")
                
                # Insert into database
                inserted = insert_results(conn, symbol_data, results)
                total_inserted += inserted
                successful += 1
                
                print(f"✅ {symbol}: {inserted} records inserted (₹{closing_price:.2f})")
                
            except Exception as e:
                print(f"❌ {symbol}: Error - {e}")
                logging.error(f"Error with {symbol}: {e}")
        
        # Final summary
        print(f"\n🎯 FINAL SUMMARY")
        print("=" * 40)
        print(f"Symbols processed: {successful}/{len(test_symbols)}")
        print(f"Total records: {total_inserted}")
        print(f"Expected: {len(test_symbols) * 14} records")
        print(f"Match: {'✅ YES' if total_inserted == len(test_symbols) * 14 else '❌ NO'}")
        
        # Verify database
        if total_inserted > 0:
            verify_query = """
            SELECT 
                equity_symbol,
                COUNT(*) as total_records,
                COUNT(DISTINCT fo_strike_price) as unique_strikes,
                COUNT(CASE WHEN fo_option_type = 'CE' THEN 1 END) as ce_count,
                COUNT(CASE WHEN fo_option_type = 'PE' THEN 1 END) as pe_count,
                equity_closing_price
            FROM step05_strike_analysis 
            GROUP BY equity_symbol, equity_closing_price
            ORDER BY equity_symbol
            """
            verify_df = pd.read_sql(verify_query, conn)
            print(f"\n✅ DATABASE VERIFICATION:")
            print(verify_df.to_string(index=False))
        
    except Exception as e:
        logging.error(f"Main error: {e}")
        print(f"❌ Error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()