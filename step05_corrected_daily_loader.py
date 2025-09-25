#!/usr/bin/env python3
"""
Step 5: CORRECTED Day-wise F&O Strike Price Analysis
Fixed to get exactly 14 records per symbol (7 strikes × 2 option types)
Only nearest expiry date per strike-option combination
"""

import pyodbc
import pandas as pd
import numpy as np
import logging
from datetime import datetime
import sys

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_database_connection():
    """Get database connection"""
    try:
        connection_string = (
            "Driver={ODBC Driver 17 for SQL Server};"
            "Server=SRIKIRANREDDY\\SQLEXPRESS;"
            "Database=master;"
            "Trusted_Connection=yes;"
        )
        conn = pyodbc.connect(connection_string)
        logger.info("Database connection established successfully")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        return None

def create_table(conn):
    """Create the corrected analysis table"""
    try:
        cursor = conn.cursor()
        
        # Drop existing table
        cursor.execute("DROP TABLE IF EXISTS Step05_strikepriceAnalysisderived")
        conn.commit()
        
        # Create new table
        create_table_query = """
        CREATE TABLE Step05_strikepriceAnalysisderived (
            ID INT IDENTITY(1,1) PRIMARY KEY,
            Current_trade_date VARCHAR(10) NOT NULL,
            Symbol NVARCHAR(20) NOT NULL,
            Current_close_price DECIMAL(15,4) NOT NULL,
            FO_Record_ID INT NOT NULL,
            Trade_date VARCHAR(10) NOT NULL,
            Expiry_date VARCHAR(10) NOT NULL,
            Strike_price DECIMAL(10,2) NOT NULL,
            Option_type NVARCHAR(5) NOT NULL,
            Close_price DECIMAL(15,4) NOT NULL,
            Strike_rank INT NOT NULL,
            Analysis_timestamp DATETIME2 DEFAULT GETDATE()
        )
        """
        cursor.execute(create_table_query)
        conn.commit()
        logger.info("Created corrected Step05_strikepriceAnalysisderived table")
        return True
    except Exception as e:
        logger.error(f"Error creating table: {e}")
        return False

def get_trading_days(conn):
    """Get all trading days in February 2025"""
    query = """
    SELECT DISTINCT Trade_date 
    FROM step04_fo_udiff_daily 
    WHERE Trade_date LIKE '202502%'
    ORDER BY Trade_date
    """
    
    try:
        df = pd.read_sql(query, conn)
        trading_days = df['Trade_date'].tolist()
        logger.info(f"Found {len(trading_days)} trading days in February 2025")
        return trading_days
    except Exception as e:
        logger.error(f"Error getting trading days: {e}")
        return []

def process_single_day(conn, trade_date):
    """Process all symbols for a single trading day with corrected logic"""
    logger.info(f"Processing trading day: {trade_date}")
    
    # Get all symbols for this date with valid underlying prices
    symbols_query = """
    SELECT DISTINCT Symbol, AVG(UndrlygPric) as avg_underlying
    FROM step04_fo_udiff_daily 
    WHERE Trade_date = ? 
      AND UndrlygPric IS NOT NULL 
      AND Strike_price IS NOT NULL
      AND Close_price IS NOT NULL
      AND Strike_price > 0
    GROUP BY Symbol
    HAVING COUNT(*) >= 10  -- Only symbols with sufficient F&O data
    ORDER BY Symbol
    """
    
    try:
        symbols_df = pd.read_sql(symbols_query, conn, params=[trade_date])
        logger.info(f"Found {len(symbols_df)} symbols for date {trade_date}")
        
        if symbols_df.empty:
            return 0
        
        total_records = 0
        processed_symbols = 0
        
        for _, symbol_row in symbols_df.iterrows():
            symbol = symbol_row['Symbol']
            underlying_price = symbol_row['avg_underlying']
            
            # Process this symbol and get exactly 14 records
            records_inserted = process_single_symbol(conn, trade_date, symbol, underlying_price)
            total_records += records_inserted
            processed_symbols += 1
            
            if processed_symbols % 50 == 0:
                logger.info(f"  Processed {processed_symbols}/{len(symbols_df)} symbols, {total_records} records inserted")
        
        logger.info(f"Day {trade_date} completed: {processed_symbols} symbols, {total_records} total records")
        return total_records
        
    except Exception as e:
        logger.error(f"Error processing day {trade_date}: {e}")
        return 0

def process_single_symbol(conn, trade_date, symbol, underlying_price):
    """Process a single symbol and return exactly 14 records (7 strikes × 2 option types)"""
    
    # Step 1: Get all F&O data for this symbol with nearest expiry selection
    fo_query = """
    WITH RankedOptions AS (
        SELECT 
            ID,
            Trade_date,
            Expiry_date,
            Strike_price,
            Option_type,
            Close_price,
            ROW_NUMBER() OVER (
                PARTITION BY Strike_price, Option_type 
                ORDER BY ABS(DATEDIFF(day, Trade_date, Expiry_date))
            ) as expiry_rank
        FROM step04_fo_udiff_daily 
        WHERE Symbol = ? AND Trade_date = ?
          AND Strike_price IS NOT NULL 
          AND Close_price IS NOT NULL
          AND Strike_price > 0
          AND Expiry_date > Trade_date  -- Only future expiries
    )
    SELECT 
        ID,
        Trade_date,
        Expiry_date,
        Strike_price,
        Option_type,
        Close_price
    FROM RankedOptions
    WHERE expiry_rank = 1  -- Only nearest expiry for each strike-option combination
    ORDER BY Option_type, Strike_price
    """
    
    try:
        fo_df = pd.read_sql(fo_query, conn, params=[symbol, trade_date])
        
        if fo_df.empty:
            return 0
        
        # Step 2: Get unique valid strikes (no NaN)
        strikes = sorted([s for s in fo_df['Strike_price'].unique() if not pd.isna(s) and s > 0])
        
        if len(strikes) < 7:
            return 0  # Need at least 7 strikes
        
        # Step 3: Find 7 nearest strikes to underlying price
        nearest_strike = min(strikes, key=lambda x: abs(x - underlying_price))
        nearest_index = strikes.index(nearest_strike)
        
        # Get 7 strikes (3 up + 3 down + 1 nearest)
        start_index = max(0, nearest_index - 3)
        end_index = min(len(strikes), nearest_index + 4)
        
        # Ensure we have exactly 7 strikes
        while end_index - start_index < 7 and start_index > 0:
            start_index -= 1
        while end_index - start_index < 7 and end_index < len(strikes):
            end_index += 1
        
        selected_strikes = strikes[start_index:end_index]
        
        # Step 4: Filter data for selected strikes only
        selected_data = fo_df[fo_df['Strike_price'].isin(selected_strikes)].copy()
        
        # Step 5: Ensure we have both CE and PE for each strike (should be exactly 14 records)
        ce_records = selected_data[selected_data['Option_type'] == 'CE']
        pe_records = selected_data[selected_data['Option_type'] == 'PE']
        
        expected_records = len(selected_strikes) * 2  # 7 strikes × 2 option types = 14
        actual_records = len(selected_data)
        
        if actual_records != expected_records:
            logger.warning(f"  {symbol}: Expected {expected_records} records, got {actual_records}")
        
        # Step 6: Insert records
        records_inserted = insert_records(conn, trade_date, symbol, underlying_price, 
                                        selected_data, selected_strikes, nearest_strike)
        
        return records_inserted
        
    except Exception as e:
        logger.error(f"Error processing symbol {symbol}: {e}")
        return 0

def insert_records(conn, trade_date, symbol, underlying_price, fo_data, selected_strikes, nearest_strike):
    """Insert exactly 14 records for a symbol"""
    insert_query = """
    INSERT INTO Step05_strikepriceAnalysisderived 
    (Current_trade_date, Symbol, Current_close_price, FO_Record_ID, Trade_date, 
     Expiry_date, Strike_price, Option_type, Close_price, Strike_rank)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    try:
        cursor = conn.cursor()
        inserted_count = 0
        
        for _, row in fo_data.iterrows():
            strike_rank = selected_strikes.index(row['Strike_price']) - selected_strikes.index(nearest_strike)
            
            cursor.execute(insert_query, (
                trade_date,
                symbol,
                underlying_price,
                row['ID'],
                row['Trade_date'],
                row['Expiry_date'],
                row['Strike_price'],
                row['Option_type'],
                row['Close_price'],
                strike_rank
            ))
            inserted_count += 1
        
        conn.commit()
        return inserted_count
        
    except Exception as e:
        logger.error(f"Error inserting records for {symbol}: {e}")
        conn.rollback()
        return 0

def main():
    """Main execution function"""
    logger.info("Starting CORRECTED Step 5 Day-wise F&O Analysis")
    logger.info("Target: Exactly 14 records per symbol (7 strikes × 2 option types)")
    
    conn = get_database_connection()
    if not conn:
        return
    
    try:
        # Create corrected table
        if not create_table(conn):
            return
        
        # Get trading days
        trading_days = get_trading_days(conn)
        if not trading_days:
            logger.error("No trading days found")
            return
        
        # Process each day
        total_records = 0
        for i, trade_date in enumerate(trading_days, 1):
            logger.info(f"=== Processing Day {i}/{len(trading_days)}: {trade_date} ===")
            
            day_records = process_single_day(conn, trade_date)
            total_records += day_records
            
            logger.info(f"Progress: {i}/{len(trading_days)} days, {total_records} total records")
            print(f"Progress: {i}/{len(trading_days)} days completed, {total_records} total records")
        
        # Final summary with validation
        summary_query = """
        SELECT 
            Current_trade_date,
            Symbol,
            COUNT(*) as record_count,
            COUNT(CASE WHEN Option_type = 'CE' THEN 1 END) as ce_count,
            COUNT(CASE WHEN Option_type = 'PE' THEN 1 END) as pe_count,
            COUNT(DISTINCT Strike_price) as unique_strikes
        FROM Step05_strikepriceAnalysisderived
        GROUP BY Current_trade_date, Symbol
        HAVING COUNT(*) != 14  -- Show any symbols that don't have exactly 14 records
        ORDER BY Current_trade_date, Symbol
        """
        
        validation_df = pd.read_sql(summary_query, conn)
        
        if not validation_df.empty:
            logger.warning(f"Found {len(validation_df)} symbol-date combinations with incorrect record count:")
            for _, row in validation_df.head(10).iterrows():
                logger.warning(f"  {row['Current_trade_date']} {row['Symbol']}: {row['record_count']} records")
        
        # Overall summary
        overall_query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT Current_trade_date) as trading_days,
            COUNT(DISTINCT Symbol) as symbols,
            COUNT(*) / COUNT(DISTINCT CONCAT(Current_trade_date, Symbol)) as avg_records_per_symbol
        FROM Step05_strikepriceAnalysisderived
        """
        
        overall_df = pd.read_sql(overall_query, conn)
        summary = overall_df.iloc[0]
        
        logger.info(f"=== ANALYSIS COMPLETED ===")
        logger.info(f"Total records: {summary['total_records']}")
        logger.info(f"Trading days: {summary['trading_days']}")
        logger.info(f"Symbols: {summary['symbols']}")
        logger.info(f"Average records per symbol: {summary['avg_records_per_symbol']:.1f} (target: 14.0)")
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        
    finally:
        conn.close()
        logger.info("Database connection closed")

if __name__ == "__main__":
    main()