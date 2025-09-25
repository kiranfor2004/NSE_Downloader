#!/usr/bin/env python3
"""
Step 5: Production Strike Price Analyzer with Database Storage
=============================================================

This script processes test symbols and stores complete F&O strike analysis 
results in a database table with all fields from step04_fo_udiff_daily.

Features:
- Processes only test symbols (SBIN, ICICIBANK, HDFCBANK)
- Gets closing price from step03_compare_monthvspreviousmonth table
- Finds nearest 7 strikes with both PE and CE options (14 records per symbol)
- Includes ALL fields from step04_fo_udiff_daily table
- Creates and populates step05_strike_analysis table
- Complete data validation and error handling

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

def create_step05_table(conn):
    """Create step05_strike_analysis table with all F&O fields plus analysis fields."""
    
    # Drop table if exists
    drop_sql = "IF OBJECT_ID('step05_strike_analysis', 'U') IS NOT NULL DROP TABLE step05_strike_analysis"
    
    # Create table with all fields from step04 plus additional analysis fields
    create_sql = """
    CREATE TABLE step05_strike_analysis (
        -- Primary key
        analysis_id BIGINT IDENTITY(1,1) PRIMARY KEY,
        
        -- Analysis metadata
        analysis_date DATETIME2 DEFAULT GETDATE(),
        equity_symbol NVARCHAR(50) NOT NULL,
        equity_closing_price DECIMAL(18,4) NOT NULL,
        equity_trade_date DATE NOT NULL,
        distance_from_close DECIMAL(18,4),
        price_diff_percent DECIMAL(10,4),
        moneyness NVARCHAR(20),
        strike_rank INT,
        
        -- All fields from step04_fo_udiff_daily
        fo_strike_price DECIMAL(18,4) NOT NULL,
        fo_option_type VARCHAR(5) NOT NULL,
        
        -- Additional F&O fields positioned after fo_option_type
        fo_id INT,
        fo_trade_date VARCHAR(10),
        fo_close_price FLOAT,
        fo_open_price FLOAT,
        
        -- Remaining F&O fields
        trade_date VARCHAR(10),
        symbol VARCHAR(50),
        instrument VARCHAR(10),
        expiry_date VARCHAR(10),
        strike_price FLOAT,
        option_type VARCHAR(5),
        open_price FLOAT,
        high_price FLOAT,
        low_price FLOAT,
        close_price FLOAT,
        settle_price FLOAT,
        contracts_traded INT,
        value_in_lakh FLOAT,
        open_interest BIGINT,
        change_in_oi BIGINT,
        underlying VARCHAR(50),
        source_file VARCHAR(100),
        created_at DATETIME,
        BizDt VARCHAR(10),
        Sgmt VARCHAR(10),
        Src VARCHAR(10),
        FininstrmActlXpryDt VARCHAR(10),
        FinInstrmId VARCHAR(50),
        ISIN VARCHAR(20),
        SctySrs VARCHAR(10),
        FinInstrmNm VARCHAR(100),
        LastPric FLOAT,
        PrvsClsgPric FLOAT,
        UndrlygPric FLOAT,
        TtlNbOfTxsExctd INT,
        SsnId VARCHAR(10),
        NewBrdLotQty INT,
        Rmks VARCHAR(100),
        Rsvd1 VARCHAR(50),
        Rsvd2 VARCHAR(50),
        Rsvd3 VARCHAR(50),
        Rsvd4 VARCHAR(50),
        
        -- Indexes
        INDEX IX_step05_symbol (equity_symbol),
        INDEX IX_step05_strike (strike_price),
        INDEX IX_step05_option_type (option_type),
        INDEX IX_step05_trade_date (equity_trade_date)
    )
    """
    
    cursor = conn.cursor()
    
    try:
        logging.info("Dropping existing step05_strike_analysis table if exists...")
        cursor.execute(drop_sql)
        
        logging.info("Creating step05_strike_analysis table...")
        cursor.execute(create_sql)
        
        conn.commit()
        logging.info("✅ step05_strike_analysis table created successfully")
        
    except Exception as e:
        logging.error(f"Error creating table: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()

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

def get_detailed_fo_strikes(conn, symbol, month='202502'):
    """Get all F&O data with complete fields for a symbol in February 2025."""
    query = """
    SELECT *
    FROM step04_fo_udiff_daily
    WHERE symbol = ?
    AND trade_date LIKE ?
    AND instrument = 'STO'  -- Stock Options
    AND strike_price IS NOT NULL
    AND option_type IN ('CE', 'PE')
    ORDER BY strike_price, option_type
    """
    
    df = pd.read_sql(query, conn, params=[symbol, f'{month}%'])
    return df

def find_nearest_strikes_detailed(available_strikes, closing_price):
    """Find 7 nearest strikes around closing price with all F&O details."""
    # Get unique strike prices
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
    
    # Calculate additional analysis fields
    result['distance_from_close'] = abs(result['strike_price'] - closing_price)
    result['price_diff_percent'] = ((result['strike_price'] - closing_price) / closing_price) * 100
    
    # Add moneyness calculation
    result['moneyness'] = result.apply(
        lambda row: calculate_moneyness(row['strike_price'], closing_price, row['option_type']), 
        axis=1
    )
    
    # Add strike rank (1 = closest to closing price)
    strike_rank_map = {strike: rank+1 for rank, (strike, _) in enumerate(strike_distances)}
    result['strike_rank'] = result['strike_price'].map(strike_rank_map)
    
    # Sort by strike price, then by option type
    result = result.sort_values(['strike_price', 'option_type'])
    
    return result

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

def safe_int_convert(value):
    """Safely convert value to integer, handling NaN and None."""
    if pd.isna(value) or value is None:
        return None
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None

def safe_float_convert(value):
    """Safely convert value to float, handling NaN and None."""
    if pd.isna(value) or value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def insert_analysis_results(conn, symbol_data, fo_results):
    """Insert analysis results into step05_strike_analysis table."""
    if fo_results.empty:
        return 0
    
    # Prepare data for insertion
    equity_symbol = symbol_data['symbol']
    equity_closing_price = float(symbol_data['closing_price'])
    equity_trade_date = symbol_data['trading_date']
    
    insert_sql = """
    INSERT INTO step05_strike_analysis (
        equity_symbol, equity_closing_price, equity_trade_date,
        distance_from_close, price_diff_percent, moneyness, strike_rank,
        fo_strike_price, fo_option_type, fo_id, fo_trade_date, fo_close_price, fo_open_price,
        trade_date, symbol, instrument, expiry_date, strike_price, option_type,
        open_price, high_price, low_price, close_price, settle_price,
        contracts_traded, value_in_lakh, open_interest, change_in_oi, underlying,
        source_file, created_at, BizDt, Sgmt, Src, FininstrmActlXpryDt,
        FinInstrmId, ISIN, SctySrs, FinInstrmNm, LastPric, PrvsClsgPric,
        UndrlygPric, TtlNbOfTxsExctd, SsnId, NewBrdLotQty, Rmks,
        Rsvd1, Rsvd2, Rsvd3, Rsvd4
    ) VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
    )
    """
    
    cursor = conn.cursor()
    inserted_count = 0
    
    try:
        for _, row in fo_results.iterrows():
            values = (
                equity_symbol, equity_closing_price, equity_trade_date,
                safe_float_convert(row['distance_from_close']), 
                safe_float_convert(row['price_diff_percent']), 
                row['moneyness'], safe_int_convert(row['strike_rank']),
                safe_float_convert(row['strike_price']), row['option_type'],
                safe_int_convert(row['id']), row['trade_date'], 
                safe_float_convert(row['close_price']), safe_float_convert(row['open_price']),
                row['trade_date'], row['symbol'], row['instrument'],
                row['expiry_date'], safe_float_convert(row['strike_price']),
                row['option_type'], safe_float_convert(row['open_price']),
                safe_float_convert(row['high_price']), safe_float_convert(row['low_price']),
                safe_float_convert(row['close_price']), safe_float_convert(row['settle_price']),
                safe_int_convert(row['contracts_traded']), safe_float_convert(row['value_in_lakh']),
                safe_int_convert(row['open_interest']), safe_int_convert(row['change_in_oi']),
                row['underlying'], row['source_file'], row['created_at'],
                row['BizDt'], row['Sgmt'], row['Src'], row['FininstrmActlXpryDt'],
                row['FinInstrmId'], row['ISIN'], row['SctySrs'], row['FinInstrmNm'],
                safe_float_convert(row['LastPric']), safe_float_convert(row['PrvsClsgPric']),
                safe_float_convert(row['UndrlygPric']), safe_int_convert(row['TtlNbOfTxsExctd']),
                row['SsnId'], safe_int_convert(row['NewBrdLotQty']),
                row['Rmks'], row['Rsvd1'], row['Rsvd2'], row['Rsvd3'], row['Rsvd4']
            )
            
            cursor.execute(insert_sql, values)
            inserted_count += 1
        
        conn.commit()
        logging.info(f"✅ Inserted {inserted_count} records for {equity_symbol}")
        
    except Exception as e:
        logging.error(f"Error inserting data for {equity_symbol}: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
    
    return inserted_count

def analyze_symbol_production(conn, symbol):
    """Analyze strike prices for a single symbol with full F&O data."""
    logging.info(f"Analyzing symbol: {symbol}")
    
    # Get symbol data from step03
    symbol_data = get_february_symbol_data(conn, symbol)
    if symbol_data is None:
        logging.warning(f"No February 2025 data found for {symbol} in step03 table")
        return None, None
    
    closing_price = float(symbol_data['closing_price'])
    trading_date = symbol_data['trading_date']
    
    logging.info(f"{symbol} - Closing Price: ₹{closing_price:.2f}, Date: {trading_date}")
    
    # Get detailed F&O data
    available_strikes = get_detailed_fo_strikes(conn, symbol)
    if available_strikes.empty:
        logging.warning(f"No F&O data found for {symbol} in February 2025")
        return symbol_data, None
    
    logging.info(f"{symbol} - Found {len(available_strikes)} detailed F&O records")
    
    # Find nearest strikes with all details
    result_strikes = find_nearest_strikes_detailed(available_strikes, closing_price)
    
    return symbol_data, result_strikes

def display_summary(symbol, symbol_data, results):
    """Display summary of results."""
    if symbol_data is None:
        print(f"\n❌ {symbol}: No equity data found")
        return
    
    if results is None or results.empty:
        print(f"\n⚠️  {symbol}: No F&O data found")
        return
    
    closing_price = float(symbol_data['closing_price'])
    trading_date = symbol_data['trading_date']
    
    print(f"\n📊 {symbol}")
    print(f"Closing Price: ₹{closing_price:.2f} | Date: {trading_date}")
    print(f"Strike Analysis: {len(results)} records | {len(results['strike_price'].unique())} unique strikes")
    
    # Show strikes summary
    strikes_summary = results.groupby(['strike_price', 'strike_rank']).agg({
        'option_type': 'count',
        'close_price': 'mean',
        'contracts_traded': 'sum'
    }).round(2)
    
    print(f"Strikes: {list(results['strike_price'].unique())}")

def main():
    """Main function to run the production strike price analysis."""
    print("🎯 STEP 5: PRODUCTION STRIKE PRICE ANALYZER")
    print("=" * 60)
    print("Processing test symbols with complete F&O data storage")
    print("Creating step05_strike_analysis table with all F&O fields")
    
    # Test symbols only for initial validation
    test_symbols = ['SBIN', 'ICICIBANK', 'HDFCBANK']
    
    try:
        conn = get_database_connection()
        logging.info("Database connection established")
        
        # Create the results table
        create_step05_table(conn)
        
        total_inserted = 0
        successful_symbols = 0
        
        for symbol in test_symbols:
            try:
                symbol_data, fo_results = analyze_symbol_production(conn, symbol)
                
                if symbol_data is not None and fo_results is not None and not fo_results.empty:
                    # Insert results into database
                    inserted_count = insert_analysis_results(conn, symbol_data, fo_results)
                    total_inserted += inserted_count
                    successful_symbols += 1
                    
                    display_summary(symbol, symbol_data, fo_results)
                else:
                    display_summary(symbol, symbol_data, fo_results)
                    
            except Exception as e:
                logging.error(f"Error processing {symbol}: {e}")
                print(f"\n❌ Error processing {symbol}: {e}")
        
        # Final summary
        print(f"\n🎯 PRODUCTION SUMMARY")
        print("=" * 60)
        print(f"Symbols processed: {successful_symbols}/{len(test_symbols)}")
        print(f"Total records inserted: {total_inserted}")
        print(f"Average records per symbol: {total_inserted/successful_symbols:.1f}" if successful_symbols > 0 else "N/A")
        
        # Verify data in table
        verify_query = """
        SELECT 
            equity_symbol,
            COUNT(*) as total_records,
            COUNT(DISTINCT strike_price) as unique_strikes,
            COUNT(DISTINCT option_type) as option_types,
            MIN(equity_closing_price) as closing_price
        FROM step05_strike_analysis
        GROUP BY equity_symbol, equity_closing_price
        ORDER BY equity_symbol
        """
        
        verify_df = pd.read_sql(verify_query, conn)
        if not verify_df.empty:
            print(f"\n✅ DATA VERIFICATION:")
            print(verify_df.to_string(index=False))
        
        print(f"\n✅ Production analysis completed successfully!")
        print(f"📋 Results stored in step05_strike_analysis table")
        print(f"🚀 Ready to scale to all symbols when testing is validated")
        
    except Exception as e:
        logging.error(f"Production analysis error: {e}")
        print(f"❌ Error: {e}")
    
    finally:
        if 'conn' in locals():
            conn.close()
            logging.info("Database connection closed")

if __name__ == "__main__":
    main()