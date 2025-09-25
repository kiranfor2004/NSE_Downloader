#!/usr/bin/env python3
"""
Step 5: Day-wise Comprehensive F&O Strike Price Analysis
Process one day at a time for February 2025 only
For each trading day, find 7 nearest strikes (3 up + 3 down + 1 nearest) for both PE and CE
"""

import pyodbc
import pandas as pd
import logging
from datetime import datetime
import sys

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('step05_daily_analysis.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Database connection
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

def create_step05_table(conn):
    """Create Step05_strikepriceAnalysisderived table"""
    create_table_query = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Step05_strikepriceAnalysisderived' AND xtype='U')
    BEGIN
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
            Analysis_timestamp DATETIME2 DEFAULT GETDATE(),
            INDEX IX_Step05_Symbol_Date (Symbol, Current_trade_date),
            INDEX IX_Step05_Strike (Strike_price, Option_type)
        )
        PRINT 'Step05_strikepriceAnalysisderived table created successfully'
    END
    ELSE
    BEGIN
        PRINT 'Step05_strikepriceAnalysisderived table already exists'
    END
    """
    
    try:
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
        logger.info("Step05_strikepriceAnalysisderived table ready")
        return True
    except Exception as e:
        logger.error(f"Error creating table: {e}")
        return False

def get_february_trading_days(conn):
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

def get_symbols_for_date(conn, trade_date):
    """Get symbols available for a specific trading date with their underlying prices"""
    query = """
    SELECT DISTINCT Symbol, AVG(UndrlygPric) as underlying_price
    FROM step04_fo_udiff_daily 
    WHERE Trade_date = ?
    GROUP BY Symbol
    ORDER BY Symbol
    """
    
    try:
        df = pd.read_sql(query, conn, params=[trade_date])
        logger.info(f"Found {len(df)} symbols for date {trade_date}")
        return df
    except Exception as e:
        logger.error(f"Error getting symbols for date {trade_date}: {e}")
        return pd.DataFrame()

def get_fo_data_for_symbol_date(conn, symbol, trade_date):
    """Get F&O data for specific symbol and date"""
    query = """
    SELECT 
        ID,
        Trade_date,
        Expiry_date,
        Strike_price,
        Option_type,
        Close_price,
        UndrlygPric
    FROM step04_fo_udiff_daily 
    WHERE Symbol = ? AND Trade_date = ?
    ORDER BY Option_type, Strike_price
    """
    
    try:
        df = pd.read_sql(query, conn, params=[symbol, trade_date])
        return df
    except Exception as e:
        logger.error(f"Error getting F&O data for {symbol} on {trade_date}: {e}")
        return pd.DataFrame()

def find_nearest_strikes(fo_data, underlying_price):
    """Find 7 nearest strikes (3 up + 3 down + 1 nearest) for both PE and CE"""
    if fo_data.empty:
        return pd.DataFrame()
    
    # Handle None or NaN underlying price
    if underlying_price is None or pd.isna(underlying_price):
        logger.warning("Underlying price is None or NaN, skipping strike selection")
        return pd.DataFrame()
    
    # Get unique strikes and sort them
    strikes = sorted(fo_data['Strike_price'].unique())
    
    if not strikes:
        return pd.DataFrame()
    
    # Find the nearest strike to underlying price
    nearest_strike = min(strikes, key=lambda x: abs(x - underlying_price))
    nearest_index = strikes.index(nearest_strike)
    
    # Get 3 strikes above, 3 below, and the nearest one (total 7)
    start_index = max(0, nearest_index - 3)
    end_index = min(len(strikes), nearest_index + 4)
    
    # Ensure we have exactly 7 strikes if possible
    while end_index - start_index < 7 and start_index > 0:
        start_index -= 1
    while end_index - start_index < 7 and end_index < len(strikes):
        end_index += 1
    
    selected_strikes = strikes[start_index:end_index]
    
    # Filter data for selected strikes
    selected_data = fo_data[fo_data['Strike_price'].isin(selected_strikes)].copy()
    
    # Add strike rank (relative to nearest)
    selected_data['Strike_rank'] = selected_data['Strike_price'].apply(
        lambda x: selected_strikes.index(x) - selected_strikes.index(nearest_strike)
    )
    
    return selected_data

def process_single_day(conn, trade_date):
    """Process all symbols for a single trading day"""
    logger.info(f"Processing trading day: {trade_date}")
    
    # Get symbols for this date
    symbols_df = get_symbols_for_date(conn, trade_date)
    if symbols_df.empty:
        logger.warning(f"No symbols found for date {trade_date}")
        return 0
    
    total_records = 0
    processed_symbols = 0
    
    for _, symbol_row in symbols_df.iterrows():
        symbol = symbol_row['Symbol']
        current_close_price = symbol_row['underlying_price']
        
        # Get F&O data for this symbol and date
        fo_data = get_fo_data_for_symbol_date(conn, symbol, trade_date)
        
        if fo_data.empty:
            continue
        
        # Find nearest strikes
        selected_data = find_nearest_strikes(fo_data, current_close_price)
        
        if selected_data.empty:
            continue
        
        # Insert records for this symbol
        records_inserted = insert_symbol_records(conn, trade_date, symbol, current_close_price, selected_data)
        total_records += records_inserted
        processed_symbols += 1
        
        if processed_symbols % 10 == 0:
            logger.info(f"  Processed {processed_symbols}/{len(symbols_df)} symbols, {total_records} records inserted")
    
    logger.info(f"Day {trade_date} completed: {processed_symbols} symbols, {total_records} total records")
    return total_records

def insert_symbol_records(conn, current_trade_date, symbol, current_close_price, fo_data):
    """Insert records for a single symbol"""
    insert_query = """
    INSERT INTO Step05_strikepriceAnalysisderived 
    (Current_trade_date, Symbol, Current_close_price, FO_Record_ID, Trade_date, 
     Expiry_date, Strike_price, Option_type, Close_price, Strike_rank)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    try:
        cursor = conn.cursor()
        records_inserted = 0
        
        for _, row in fo_data.iterrows():
            cursor.execute(insert_query, (
                current_trade_date,
                symbol,
                current_close_price,
                row['ID'],
                row['Trade_date'],
                row['Expiry_date'],
                row['Strike_price'],
                row['Option_type'],
                row['Close_price'],
                row['Strike_rank']
            ))
            records_inserted += 1
        
        conn.commit()
        return records_inserted
        
    except Exception as e:
        logger.error(f"Error inserting records for {symbol}: {e}")
        conn.rollback()
        return 0

def get_current_progress(conn):
    """Check how many days are already processed"""
    query = """
    SELECT DISTINCT Current_trade_date 
    FROM Step05_strikepriceAnalysisderived 
    WHERE Current_trade_date LIKE '202502%'
    ORDER BY Current_trade_date
    """
    
    try:
        df = pd.read_sql(query, conn)
        processed_days = df['Current_trade_date'].tolist()
        logger.info(f"Already processed {len(processed_days)} days")
        return processed_days
    except Exception as e:
        logger.info("No previous progress found (starting fresh)")
        return []

def main():
    """Main execution function"""
    logger.info("Starting Step 5 Day-wise Comprehensive F&O Analysis")
    
    # Connect to database
    conn = get_database_connection()
    if not conn:
        logger.error("Failed to connect to database")
        return
    
    try:
        # Create table
        if not create_step05_table(conn):
            logger.error("Failed to create table")
            return
        
        # Get all February trading days
        trading_days = get_february_trading_days(conn)
        if not trading_days:
            logger.error("No trading days found for February 2025")
            return
        
        # Check current progress
        processed_days = get_current_progress(conn)
        remaining_days = [day for day in trading_days if day not in processed_days]
        
        if not remaining_days:
            logger.info("All days already processed!")
            return
        
        logger.info(f"Processing {len(remaining_days)} remaining days out of {len(trading_days)} total days")
        
        # Process each day
        total_records = 0
        for i, trade_date in enumerate(remaining_days, 1):
            logger.info(f"=== Processing Day {i}/{len(remaining_days)}: {trade_date} ===")
            
            day_records = process_single_day(conn, trade_date)
            total_records += day_records
            
            logger.info(f"Day {trade_date} completed with {day_records} records")
            logger.info(f"Total progress: {i}/{len(remaining_days)} days, {total_records} total records")
            print(f"Progress: {i}/{len(remaining_days)} days completed")
        
        logger.info(f"=== ANALYSIS COMPLETED ===")
        logger.info(f"Total days processed: {len(remaining_days)}")
        logger.info(f"Total records inserted: {total_records}")
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        
    finally:
        conn.close()
        logger.info("Database connection closed")

if __name__ == "__main__":
    main()