#!/usr/bin/env python3
"""
Step 5: Day-wise F&O Strike Price Analysis - ROBUST VERSION
Process one day at a time with proper error handling
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
    """Create the analysis table"""
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
        logger.info("Created Step05_strikepriceAnalysisderived table")
        return True
    except Exception as e:
        logger.error(f"Error creating table: {e}")
        return False

def get_trading_days(conn):
    """Get first 3 trading days of February 2025 for testing"""
    query = """
    SELECT DISTINCT Trade_date 
    FROM step04_fo_udiff_daily 
    WHERE Trade_date LIKE '202502%'
    ORDER BY Trade_date
    """
    
    try:
        df = pd.read_sql(query, conn)
        trading_days = df['Trade_date'].tolist()[:3]  # Just first 3 days for testing
        logger.info(f"Processing {len(trading_days)} trading days: {trading_days}")
        return trading_days
    except Exception as e:
        logger.error(f"Error getting trading days: {e}")
        return []

def process_single_day(conn, trade_date):
    """Process all symbols for a single trading day"""
    logger.info(f"Processing trading day: {trade_date}")
    
    # Get top 5 symbols for testing (symbols with most F&O records)
    symbols_query = """
    SELECT TOP 5 Symbol, COUNT(*) as record_count, AVG(UndrlygPric) as avg_underlying
    FROM step04_fo_udiff_daily 
    WHERE Trade_date = ? 
      AND UndrlygPric IS NOT NULL 
      AND Strike_price IS NOT NULL
      AND Close_price IS NOT NULL
    GROUP BY Symbol
    ORDER BY COUNT(*) DESC
    """
    
    try:
        symbols_df = pd.read_sql(symbols_query, conn, params=[trade_date])
        logger.info(f"Found {len(symbols_df)} symbols for date {trade_date}")
        
        if symbols_df.empty:
            return 0
        
        total_records = 0
        
        for _, symbol_row in symbols_df.iterrows():
            symbol = symbol_row['Symbol']
            underlying_price = symbol_row['avg_underlying']
            
            logger.info(f"Processing {symbol} (underlying: {underlying_price:.2f})")
            
            # Get F&O data for this symbol
            fo_query = """
            SELECT 
                ID,
                Trade_date,
                Expiry_date,
                Strike_price,
                Option_type,
                Close_price
            FROM step04_fo_udiff_daily 
            WHERE Symbol = ? AND Trade_date = ?
              AND Strike_price IS NOT NULL 
              AND Close_price IS NOT NULL
              AND Strike_price > 0
            ORDER BY Option_type, Strike_price
            """
            
            fo_df = pd.read_sql(fo_query, conn, params=[symbol, trade_date])
            
            if fo_df.empty:
                logger.warning(f"No valid F&O data for {symbol}")
                continue
            
            # Get unique valid strikes (no NaN)
            strikes = sorted([s for s in fo_df['Strike_price'].unique() if not pd.isna(s) and s > 0])
            
            if len(strikes) < 3:
                logger.warning(f"Not enough strikes for {symbol}: {len(strikes)}")
                continue
            
            # Find nearest strike
            nearest_strike = min(strikes, key=lambda x: abs(x - underlying_price))
            nearest_index = strikes.index(nearest_strike)
            
            # Get 7 strikes (3 up + 3 down + 1 nearest) or as many as available
            start_index = max(0, nearest_index - 3)
            end_index = min(len(strikes), nearest_index + 4)
            
            # Ensure we have up to 7 strikes
            while end_index - start_index < 7 and start_index > 0:
                start_index -= 1
            while end_index - start_index < 7 and end_index < len(strikes):
                end_index += 1
            
            selected_strikes = strikes[start_index:end_index]
            logger.info(f"  Selected {len(selected_strikes)} strikes: {selected_strikes}")
            
            # Filter data for selected strikes
            selected_data = fo_df[fo_df['Strike_price'].isin(selected_strikes)].copy()
            
            # Insert records
            records_inserted = insert_records(conn, trade_date, symbol, underlying_price, 
                                            selected_data, selected_strikes, nearest_strike)
            total_records += records_inserted
            
            logger.info(f"  {symbol}: {records_inserted} records inserted")
        
        logger.info(f"Day {trade_date} completed: {total_records} total records")
        return total_records
        
    except Exception as e:
        logger.error(f"Error processing day {trade_date}: {e}")
        return 0

def insert_records(conn, trade_date, symbol, underlying_price, fo_data, selected_strikes, nearest_strike):
    """Insert records for a symbol"""
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
    logger.info("Starting Step 5 Day-wise F&O Analysis (Robust Version)")
    
    conn = get_database_connection()
    if not conn:
        return
    
    try:
        # Create table
        if not create_table(conn):
            return
        
        # Get trading days (first 3 for testing)
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
        
        # Final summary
        summary_query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT Current_trade_date) as trading_days,
            COUNT(DISTINCT Symbol) as symbols
        FROM Step05_strikepriceAnalysisderived
        """
        
        summary_df = pd.read_sql(summary_query, conn)
        summary = summary_df.iloc[0]
        
        logger.info(f"=== ANALYSIS COMPLETED ===")
        logger.info(f"Total records: {summary['total_records']}")
        logger.info(f"Trading days: {summary['trading_days']}")
        logger.info(f"Symbols: {summary['symbols']}")
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        
    finally:
        conn.close()
        logger.info("Database connection closed")

if __name__ == "__main__":
    main()