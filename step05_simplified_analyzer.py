#!/usr/bin/env python3
"""
Step 5: Simplified Production Strike Analyzer 
=============================================

Simplified version with proper parameter handling and debugging.
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

def create_simplified_table(conn):
    """Create simplified step05_strike_analysis table."""
    drop_sql = "IF OBJECT_ID('step05_strike_analysis', 'U') IS NOT NULL DROP TABLE step05_strike_analysis"
    
    create_sql = """
    CREATE TABLE step05_strike_analysis (
        id BIGINT IDENTITY(1,1) PRIMARY KEY,
        analysis_date DATETIME2 DEFAULT GETDATE(),
        
        -- Equity data
        equity_symbol NVARCHAR(50) NOT NULL,
        equity_closing_price DECIMAL(18,4) NOT NULL,
        equity_trade_date DATE NOT NULL,
        
        -- Analysis data
        distance_from_close DECIMAL(18,4),
        price_diff_percent DECIMAL(10,4),
        moneyness NVARCHAR(20),
        strike_rank INT,
        
        -- Key F&O data
        fo_trade_date VARCHAR(10),
        fo_symbol VARCHAR(50),
        fo_instrument VARCHAR(10),
        fo_expiry_date VARCHAR(10),
        fo_strike_price FLOAT,
        fo_option_type VARCHAR(5),
        fo_open_price FLOAT,
        fo_high_price FLOAT,
        fo_low_price FLOAT,
        fo_close_price FLOAT,
        fo_settle_price FLOAT,
        fo_contracts_traded INT,
        fo_value_in_lakh FLOAT,
        fo_open_interest BIGINT,
        fo_change_in_oi BIGINT,
        fo_underlying VARCHAR(50),
        
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
        logging.info("✅ Simplified step05_strike_analysis table created")
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

def get_fo_data(conn, symbol):
    """Get F&O data for symbol."""
    query = """
    SELECT trade_date, symbol, instrument, expiry_date, strike_price, option_type,
           open_price, high_price, low_price, close_price, settle_price,
           contracts_traded, value_in_lakh, open_interest, change_in_oi, underlying
    FROM step04_fo_udiff_daily
    WHERE symbol = ? AND trade_date LIKE '202502%' AND instrument = 'STO'
    AND strike_price IS NOT NULL AND option_type IN ('CE', 'PE')
    """
    return pd.read_sql(query, conn, params=[symbol])

def find_nearest_strikes(fo_data, closing_price):
    """Find 7 nearest strikes."""
    unique_strikes = fo_data['strike_price'].unique()
    if len(unique_strikes) == 0:
        return pd.DataFrame()
    
    # Find 7 nearest strikes
    strike_distances = [(strike, abs(strike - closing_price)) for strike in unique_strikes]
    strike_distances.sort(key=lambda x: x[1])
    selected_strikes = [strike for strike, _ in strike_distances[:7]]
    
    # Filter data for selected strikes
    result = fo_data[fo_data['strike_price'].isin(selected_strikes)].copy()
    
    # Add analysis fields
    result['distance_from_close'] = abs(result['strike_price'] - closing_price)
    result['price_diff_percent'] = ((result['strike_price'] - closing_price) / closing_price) * 100
    result['moneyness'] = result.apply(lambda row: get_moneyness(row['strike_price'], closing_price, row['option_type']), axis=1)
    
    # Add strike rank
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
    """Insert results into database."""
    if fo_results.empty:
        return 0
    
    insert_sql = """
    INSERT INTO step05_strike_analysis (
        equity_symbol, equity_closing_price, equity_trade_date,
        distance_from_close, price_diff_percent, moneyness, strike_rank,
        fo_trade_date, fo_symbol, fo_instrument, fo_expiry_date, fo_strike_price, fo_option_type,
        fo_open_price, fo_high_price, fo_low_price, fo_close_price, fo_settle_price,
        fo_contracts_traded, fo_value_in_lakh, fo_open_interest, fo_change_in_oi, fo_underlying
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    cursor = conn.cursor()
    inserted = 0
    
    try:
        for _, row in fo_results.iterrows():
            values = (
                symbol_data['symbol'], float(symbol_data['closing_price']), symbol_data['trading_date'],
                float(row['distance_from_close']), float(row['price_diff_percent']), 
                row['moneyness'], int(row['strike_rank']),
                row['trade_date'], row['symbol'], row['instrument'], row['expiry_date'],
                float(row['strike_price']), row['option_type'],
                float(row['open_price']) if pd.notna(row['open_price']) else None,
                float(row['high_price']) if pd.notna(row['high_price']) else None,
                float(row['low_price']) if pd.notna(row['low_price']) else None,
                float(row['close_price']) if pd.notna(row['close_price']) else None,
                float(row['settle_price']) if pd.notna(row['settle_price']) else None,
                int(row['contracts_traded']) if pd.notna(row['contracts_traded']) else None,
                float(row['value_in_lakh']) if pd.notna(row['value_in_lakh']) else None,
                int(row['open_interest']) if pd.notna(row['open_interest']) else None,
                int(row['change_in_oi']) if pd.notna(row['change_in_oi']) else None,
                row['underlying']
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
    print("🎯 STEP 5: SIMPLIFIED PRODUCTION ANALYZER")
    print("=" * 50)
    
    test_symbols = ['SBIN', 'ICICIBANK', 'HDFCBANK']
    
    try:
        conn = get_database_connection()
        logging.info("Database connected")
        
        create_simplified_table(conn)
        
        total_inserted = 0
        successful = 0
        
        for symbol in test_symbols:
            try:
                # Get data
                symbol_data = get_symbol_data(conn, symbol)
                if symbol_data is None:
                    print(f"❌ {symbol}: No equity data")
                    continue
                
                fo_data = get_fo_data(conn, symbol)
                if fo_data.empty:
                    print(f"❌ {symbol}: No F&O data")
                    continue
                
                # Analyze
                closing_price = float(symbol_data['closing_price'])
                results = find_nearest_strikes(fo_data, closing_price)
                
                if results.empty:
                    print(f"❌ {symbol}: No suitable strikes")
                    continue
                
                # Insert
                inserted = insert_results(conn, symbol_data, results)
                total_inserted += inserted
                successful += 1
                
                print(f"✅ {symbol}: {len(results)} records inserted (₹{closing_price:.2f})")
                
            except Exception as e:
                print(f"❌ {symbol}: Error - {e}")
                logging.error(f"Error with {symbol}: {e}")
        
        # Summary
        print(f"\n🎯 SUMMARY")
        print(f"Successful: {successful}/{len(test_symbols)}")
        print(f"Total records: {total_inserted}")
        
        # Verify
        verify_query = """
        SELECT equity_symbol, COUNT(*) as records, 
               COUNT(DISTINCT fo_strike_price) as strikes,
               equity_closing_price
        FROM step05_strike_analysis 
        GROUP BY equity_symbol, equity_closing_price
        """
        verify_df = pd.read_sql(verify_query, conn)
        if not verify_df.empty:
            print(f"\n✅ VERIFICATION:")
            print(verify_df.to_string(index=False))
        
    except Exception as e:
        logging.error(f"Main error: {e}")
        print(f"❌ Error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()