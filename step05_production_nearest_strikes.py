#!/usr/bin/env python3
"""
Step 5 Production: Strike Price Analyzer with Database Storage
==============================================================

This script processes all symbols from February 2025, finds the nearest 7 strikes
for each symbol, and stores the complete results in a database table with all
fields from step04_fo_udiff_daily.

Features:
- Processes all symbols with both step03 and F&O data
- Gets 7 nearest strikes with both PE and CE (14 records per symbol)
- Includes all original F&O data fields
- Stores results in step05_nearest_strikes table
- Comprehensive logging and progress tracking

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

def create_results_table(conn):
    """Create the step05_nearest_strikes table if it doesn't exist."""
    create_table_sql = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='step05_nearest_strikes' AND xtype='U')
    CREATE TABLE step05_nearest_strikes (
        id bigint IDENTITY(1,1) PRIMARY KEY,
        
        -- Step03 equity data
        equity_symbol varchar(50) NOT NULL,
        equity_closing_price decimal(10,2) NOT NULL,
        equity_trading_date date NOT NULL,
        
        -- F&O selection metadata
        strike_rank int NOT NULL,
        distance_from_close decimal(10,2) NOT NULL,
        price_diff_percent decimal(8,4) NOT NULL,
        moneyness varchar(20) NOT NULL,
        
        -- All original F&O fields from step04_fo_udiff_daily
        fo_id int NOT NULL,
        trade_date varchar(8) NOT NULL,
        symbol varchar(50) NOT NULL,
        instrument varchar(30) NOT NULL,
        expiry_date varchar(10) NULL,
        strike_price float NULL,
        option_type varchar(10) NULL,
        open_price float NOT NULL,
        high_price float NOT NULL,
        low_price float NOT NULL,
        close_price float NOT NULL,
        settle_price float NULL,
        contracts_traded int NULL,
        value_in_lakh float NULL,
        open_interest bigint NULL,
        change_in_oi bigint NULL,
        underlying varchar(50) NULL,
        source_file varchar(100) NULL,
        fo_created_at datetime NULL,
        BizDt varchar(8) NULL,
        Sgmt varchar(10) NULL,
        Src varchar(10) NULL,
        FininstrmActlXpryDt varchar(10) NULL,
        FinInstrmId varchar(50) NULL,
        ISIN varchar(12) NULL,
        SctySrs varchar(10) NULL,
        FinInstrmNm varchar(200) NULL,
        LastPric float NULL,
        PrvsClsgPric float NULL,
        UndrlygPric float NULL,
        TtlNbOfTxsExctd int NULL,
        SsnId varchar(20) NULL,
        NewBrdLotQty int NULL,
        Rmks varchar(500) NULL,
        Rsvd1 varchar(50) NULL,
        Rsvd2 varchar(50) NULL,
        Rsvd3 varchar(50) NULL,
        Rsvd4 varchar(50) NULL,
        
        -- Processing metadata
        analysis_created_at datetime DEFAULT GETDATE(),
        
        -- Indexes for performance
        INDEX IX_step05_symbol (equity_symbol),
        INDEX IX_step05_strike (strike_price),
        INDEX IX_step05_option_type (option_type),
        INDEX IX_step05_trade_date (trade_date)
    )
    """
    
    cursor = conn.cursor()
    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()
    logging.info("Results table 'step05_nearest_strikes' created/verified")

def get_symbols_with_both_data(conn, month='2025-02'):
    """Get all symbols that have both step03 and F&O data for the specified month."""
    query = """
    SELECT DISTINCT 
        s3.symbol,
        s3.current_close_price,
        s3.current_trade_date,
        COUNT(DISTINCT fo.strike_price) as fo_strikes_count
    FROM step03_compare_monthvspreviousmonth s3
    INNER JOIN step04_fo_udiff_daily fo ON s3.symbol = fo.symbol
    WHERE s3.current_trade_date LIKE ?
    AND fo.trade_date LIKE ?
    AND fo.instrument = 'STO'
    AND fo.strike_price IS NOT NULL
    AND fo.option_type IN ('CE', 'PE')
    GROUP BY s3.symbol, s3.current_close_price, s3.current_trade_date
    HAVING COUNT(DISTINCT fo.strike_price) >= 7
    ORDER BY s3.current_close_price DESC
    """
    
    month_pattern = f'{month}%'
    month_fo = month.replace('-', '') + '%'
    
    df = pd.read_sql(query, conn, params=[month_pattern, month_fo])
    return df

def get_symbol_fo_data_with_all_fields(conn, symbol, month='202502'):
    """Get all F&O data with complete fields for a symbol."""
    query = """
    SELECT 
        id, trade_date, symbol, instrument, expiry_date, strike_price, option_type,
        open_price, high_price, low_price, close_price, settle_price,
        contracts_traded, value_in_lakh, open_interest, change_in_oi,
        underlying, source_file, created_at, BizDt, Sgmt, Src,
        FininstrmActlXpryDt, FinInstrmId, ISIN, SctySrs, FinInstrmNm,
        LastPric, PrvsClsgPric, UndrlygPric, TtlNbOfTxsExctd,
        SsnId, NewBrdLotQty, Rmks, Rsvd1, Rsvd2, Rsvd3, Rsvd4,
        
        -- Aggregated metrics for strike selection
        COUNT(*) OVER (PARTITION BY strike_price, option_type) as trade_count,
        AVG(close_price) OVER (PARTITION BY strike_price, option_type) as avg_close_price,
        SUM(contracts_traded) OVER (PARTITION BY strike_price, option_type) as total_volume
    FROM step04_fo_udiff_daily
    WHERE symbol = ?
    AND trade_date LIKE ?
    AND instrument = 'STO'
    AND strike_price IS NOT NULL
    AND option_type IN ('CE', 'PE')
    ORDER BY strike_price, option_type, trade_date
    """
    
    df = pd.read_sql(query, conn, params=[symbol, f'{month}%'])
    return df

def find_nearest_strikes_with_all_data(fo_data, closing_price):
    """Find 7 nearest strikes with complete F&O data."""
    if fo_data.empty:
        return pd.DataFrame()
    
    # Get unique strikes and their distances
    unique_strikes = fo_data['strike_price'].unique()
    strike_distances = []
    
    for strike in unique_strikes:
        distance = abs(strike - closing_price)
        strike_distances.append({'strike_price': strike, 'distance': distance})
    
    # Sort by distance and take 7 nearest
    strike_distances = sorted(strike_distances, key=lambda x: x['distance'])[:7]
    selected_strikes = [item['strike_price'] for item in strike_distances]
    
    # Filter data for selected strikes
    result = fo_data[fo_data['strike_price'].isin(selected_strikes)].copy()
    
    # Add analysis fields
    result['distance_from_close'] = abs(result['strike_price'] - closing_price)
    result['price_diff_percent'] = ((result['strike_price'] - closing_price) / closing_price) * 100
    
    # Add strike ranking
    strike_rank_map = {strike: rank+1 for rank, strike in enumerate(sorted(selected_strikes, key=lambda x: abs(x - closing_price)))}
    result['strike_rank'] = result['strike_price'].map(strike_rank_map)
    
    # Add moneyness
    result['moneyness'] = result.apply(
        lambda row: calculate_moneyness(row['strike_price'], closing_price, row['option_type']), 
        axis=1
    )
    
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

def insert_results_to_db(conn, symbol_data, fo_results):
    """Insert results to step05_nearest_strikes table."""
    if fo_results.empty:
        return 0
    
    # Prepare insert query
    insert_query = """
    INSERT INTO step05_nearest_strikes (
        equity_symbol, equity_closing_price, equity_trading_date,
        strike_rank, distance_from_close, price_diff_percent, moneyness,
        fo_id, trade_date, symbol, instrument, expiry_date, strike_price, option_type,
        open_price, high_price, low_price, close_price, settle_price,
        contracts_traded, value_in_lakh, open_interest, change_in_oi,
        underlying, source_file, fo_created_at, BizDt, Sgmt, Src,
        FininstrmActlXpryDt, FinInstrmId, ISIN, SctySrs, FinInstrmNm,
        LastPric, PrvsClsgPric, UndrlygPric, TtlNbOfTxsExctd,
        SsnId, NewBrdLotQty, Rmks, Rsvd1, Rsvd2, Rsvd3, Rsvd4
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    cursor = conn.cursor()
    records_inserted = 0
    
    for _, row in fo_results.iterrows():
        try:
            cursor.execute(insert_query, (
                symbol_data['symbol'],
                float(symbol_data['current_close_price']),
                symbol_data['current_trade_date'],
                int(row['strike_rank']),
                float(row['distance_from_close']),
                float(row['price_diff_percent']),
                row['moneyness'],
                int(row['id']),
                row['trade_date'],
                row['symbol'],
                row['instrument'],
                row['expiry_date'],
                float(row['strike_price']) if pd.notna(row['strike_price']) else None,
                row['option_type'],
                float(row['open_price']),
                float(row['high_price']),
                float(row['low_price']),
                float(row['close_price']),
                float(row['settle_price']) if pd.notna(row['settle_price']) else None,
                int(row['contracts_traded']) if pd.notna(row['contracts_traded']) else None,
                float(row['value_in_lakh']) if pd.notna(row['value_in_lakh']) else None,
                int(row['open_interest']) if pd.notna(row['open_interest']) else None,
                int(row['change_in_oi']) if pd.notna(row['change_in_oi']) else None,
                row['underlying'],
                row['source_file'],
                row['created_at'],
                row['BizDt'],
                row['Sgmt'],
                row['Src'],
                row['FininstrmActlXpryDt'],
                row['FinInstrmId'],
                row['ISIN'],
                row['SctySrs'],
                row['FinInstrmNm'],
                float(row['LastPric']) if pd.notna(row['LastPric']) else None,
                float(row['PrvsClsgPric']) if pd.notna(row['PrvsClsgPric']) else None,
                float(row['UndrlygPric']) if pd.notna(row['UndrlygPric']) else None,
                int(row['TtlNbOfTxsExctd']) if pd.notna(row['TtlNbOfTxsExctd']) else None,
                row['SsnId'],
                int(row['NewBrdLotQty']) if pd.notna(row['NewBrdLotQty']) else None,
                row['Rmks'],
                row['Rsvd1'],
                row['Rsvd2'],
                row['Rsvd3'],
                row['Rsvd4']
            ))
            records_inserted += 1
        except Exception as e:
            logging.error(f"Error inserting record for {symbol_data['symbol']}: {e}")
    
    conn.commit()
    cursor.close()
    return records_inserted

def clear_existing_results(conn):
    """Clear existing results from the table."""
    cursor = conn.cursor()
    cursor.execute("DELETE FROM step05_nearest_strikes")
    conn.commit()
    cursor.close()
    logging.info("Cleared existing results from step05_nearest_strikes table")

def main():
    """Main function to process all symbols and store results."""
    print("🎯 STEP 5 PRODUCTION: NEAREST STRIKES ANALYZER WITH DATABASE STORAGE")
    print("=" * 80)
    print("Processing all symbols from February 2025")
    print("Finding 7 nearest strikes with both PE and CE options")
    print("Storing complete results with all F&O fields to database")
    
    try:
        conn = get_database_connection()
        logging.info("Database connection established")
        
        # Create results table
        create_results_table(conn)
        
        # Clear existing results
        clear_existing_results(conn)
        
        # Get all symbols with both step03 and F&O data
        logging.info("Getting symbols with both equity and F&O data...")
        symbols_df = get_symbols_with_both_data(conn)
        
        if symbols_df.empty:
            print("❌ No symbols found with both equity and F&O data")
            return
        
        total_symbols = len(symbols_df)
        print(f"\n📊 Found {total_symbols} symbols to process")
        print(f"Average F&O strikes per symbol: {symbols_df['fo_strikes_count'].mean():.1f}")
        
        # Process each symbol
        total_records_inserted = 0
        successful_symbols = 0
        
        for idx, symbol_row in symbols_df.iterrows():
            symbol = symbol_row['symbol']
            closing_price = float(symbol_row['current_close_price'])
            
            try:
                print(f"\n[{idx+1}/{total_symbols}] Processing {symbol} (₹{closing_price:.2f})...")
                
                # Get complete F&O data
                fo_data = get_symbol_fo_data_with_all_fields(conn, symbol)
                
                if fo_data.empty:
                    print(f"  ⚠️  No F&O data found for {symbol}")
                    continue
                
                # Find nearest strikes
                nearest_strikes = find_nearest_strikes_with_all_data(fo_data, closing_price)
                
                if nearest_strikes.empty:
                    print(f"  ⚠️  No suitable strikes found for {symbol}")
                    continue
                
                # Insert to database
                records_inserted = insert_results_to_db(conn, symbol_row, nearest_strikes)
                total_records_inserted += records_inserted
                successful_symbols += 1
                
                print(f"  ✅ {records_inserted} records inserted for {symbol}")
                
            except Exception as e:
                logging.error(f"Error processing {symbol}: {e}")
                print(f"  ❌ Error processing {symbol}: {e}")
        
        # Final summary
        print(f"\n🎯 PROCESSING COMPLETE")
        print("=" * 50)
        print(f"Symbols processed successfully: {successful_symbols}/{total_symbols}")
        print(f"Total records inserted: {total_records_inserted:,}")
        print(f"Average records per symbol: {total_records_inserted/successful_symbols:.1f}" if successful_symbols > 0 else "N/A")
        
        # Verify results
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) as total_records FROM step05_nearest_strikes")
        db_count = cursor.fetchone()[0]
        cursor.close()
        
        print(f"Records verified in database: {db_count:,}")
        
        if db_count == total_records_inserted:
            print("✅ All records successfully stored in database!")
        else:
            print("⚠️  Record count mismatch - please check database")
        
        print(f"\n📊 Results stored in table: step05_nearest_strikes")
        print("Ready for further analysis and reporting!")
        
    except Exception as e:
        logging.error(f"Critical error: {e}")
        print(f"❌ Critical error: {e}")
    
    finally:
        if 'conn' in locals():
            conn.close()
            logging.info("Database connection closed")

if __name__ == "__main__":
    main()