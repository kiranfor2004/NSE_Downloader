#!/usr/bin/env python3
"""
Step 5: Complete February 2025 F&O Strike Price Analysis
Process all 19 trading days with all symbols using corrected logic
Exactly 14 records per symbol per day (7 strikes × 2 option types)
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
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('step05_complete_analysis.log'),
        logging.StreamHandler()
    ]
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

def create_fresh_table(conn):
    """Create a fresh analysis table"""
    try:
        cursor = conn.cursor()
        
        # Drop existing table
        cursor.execute("DROP TABLE IF EXISTS Step05_strikepriceAnalysisderived")
        conn.commit()
        logger.info("Dropped existing table")
        
        # Create new table with optimized structure
        create_table_query = """
        CREATE TABLE Step05_strikepriceAnalysisderived (
            analysis_id INT IDENTITY(1,1) PRIMARY KEY,
            Current_trade_date VARCHAR(10) NOT NULL,
            Symbol NVARCHAR(20) NOT NULL,
            Current_close_price DECIMAL(15,4) NOT NULL,
            ID INT NOT NULL,
            Trade_date VARCHAR(10) NOT NULL,
            expiry_date VARCHAR(10) NOT NULL,
            Strike_price DECIMAL(10,2) NOT NULL,
            option_type NVARCHAR(5) NOT NULL,
            close_price DECIMAL(15,4) NOT NULL,
            analysis_timestamp DATETIME2 DEFAULT GETDATE(),
            strike_position NVARCHAR(10) NOT NULL,
            INDEX IX_Step05_Symbol_Date (Symbol, Current_trade_date),
            INDEX IX_Step05_Strike (Strike_price, option_type),
            INDEX IX_Step05_Date (Current_trade_date)
        )
        """
        cursor.execute(create_table_query)
        conn.commit()
        logger.info("Created fresh Step05_strikepriceAnalysisderived table")
        return True
    except Exception as e:
        logger.error(f"Error creating table: {e}")
        return False

def get_all_trading_days(conn):
    """Get all 19 trading days in February 2025"""
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
    """Get all symbols with their underlying prices for a specific date"""
    query = """
    SELECT Symbol, AVG(UndrlygPric) as underlying_price, COUNT(*) as fo_count
    FROM step04_fo_udiff_daily 
    WHERE Trade_date = ? 
      AND UndrlygPric IS NOT NULL 
      AND Strike_price IS NOT NULL
      AND Close_price IS NOT NULL
      AND Strike_price > 0
    GROUP BY Symbol
    HAVING COUNT(*) >= 10  -- Ensure sufficient F&O records
    ORDER BY Symbol
    """
    
    try:
        df = pd.read_sql(query, conn, params=[trade_date])
        logger.info(f"Found {len(df)} symbols for date {trade_date}")
        return df
    except Exception as e:
        logger.error(f"Error getting symbols for date {trade_date}: {e}")
        return pd.DataFrame()

def get_fo_data_with_nearest_expiry(conn, symbol, trade_date):
    """Get F&O data with only nearest expiry for each strike-option combination"""
    query = """
    WITH RankedData AS (
        SELECT 
            ID,
            Trade_date,
            Expiry_date,
            Strike_price,
            Option_type,
            Close_price,
            ROW_NUMBER() OVER (
                PARTITION BY Strike_price, Option_type 
                ORDER BY ABS(DATEDIFF(day, CAST(Trade_date AS DATE), CAST(Expiry_date AS DATE)))
            ) as expiry_rank
        FROM step04_fo_udiff_daily 
        WHERE Symbol = ? AND Trade_date = ?
          AND Strike_price IS NOT NULL 
          AND Close_price IS NOT NULL
          AND Strike_price > 0
          AND Expiry_date IS NOT NULL
    )
    SELECT 
        ID,
        Trade_date,
        Expiry_date,
        Strike_price,
        Option_type,
        Close_price
    FROM RankedData 
    WHERE expiry_rank = 1
    ORDER BY Option_type, Strike_price
    """
    
    try:
        df = pd.read_sql(query, conn, params=[symbol, trade_date])
        return df
    except Exception as e:
        logger.error(f"Error getting F&O data for {symbol} on {trade_date}: {e}")
        return pd.DataFrame()

def select_7_nearest_strikes(fo_data, underlying_price):
    """Select exactly 7 nearest strikes (3 up + 3 down + 1 nearest)"""
    if fo_data.empty:
        return pd.DataFrame()
    
    # Handle None or NaN underlying price
    if underlying_price is None or pd.isna(underlying_price):
        return pd.DataFrame()
    
    # Get unique valid strikes
    strikes = sorted([s for s in fo_data['Strike_price'].unique() if not pd.isna(s) and s > 0])
    
    if len(strikes) < 3:
        return pd.DataFrame()
    
    # Find nearest strike
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
    
    # Filter data for selected strikes and ensure we have both CE and PE for each
    selected_data = fo_data[fo_data['Strike_price'].isin(selected_strikes)].copy()
    
    # Add strike position classification
    selected_data['strike_position'] = selected_data['Strike_price'].apply(
        lambda x: 'BELOW' if x < underlying_price else 'ABOVE'
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
    failed_symbols = 0
    
    for _, symbol_row in symbols_df.iterrows():
        symbol = symbol_row['Symbol']
        underlying_price = symbol_row['underlying_price']
        
        try:
            # Get F&O data with nearest expiry only
            fo_data = get_fo_data_with_nearest_expiry(conn, symbol, trade_date)
            
            if fo_data.empty:
                failed_symbols += 1
                continue
            
            # Select 7 nearest strikes
            selected_data = select_7_nearest_strikes(fo_data, underlying_price)
            
            if selected_data.empty or len(selected_data) > 14:
                failed_symbols += 1
                continue
            
            # Insert records for this symbol
            records_inserted = insert_symbol_records(conn, trade_date, symbol, underlying_price, selected_data)
            
            if records_inserted > 0:
                total_records += records_inserted
                processed_symbols += 1
            else:
                failed_symbols += 1
            
            # Progress reporting
            if (processed_symbols + failed_symbols) % 50 == 0:
                logger.info(f"  Progress: {processed_symbols + failed_symbols}/{len(symbols_df)} symbols")
                
        except Exception as e:
            logger.error(f"Error processing {symbol}: {e}")
            failed_symbols += 1
            continue
    
    logger.info(f"Day {trade_date}: {processed_symbols} symbols processed, {failed_symbols} failed, {total_records} records")
    return total_records

def insert_symbol_records(conn, current_trade_date, symbol, underlying_price, fo_data):
    """Insert records for a single symbol"""
    insert_query = """
    INSERT INTO Step05_strikepriceAnalysisderived 
    (Current_trade_date, Symbol, Current_close_price, ID, Trade_date, 
     expiry_date, Strike_price, option_type, close_price, strike_position)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    try:
        cursor = conn.cursor()
        records_inserted = 0
        
        for _, row in fo_data.iterrows():
            cursor.execute(insert_query, (
                current_trade_date,
                symbol,
                underlying_price,
                row['ID'],
                row['Trade_date'],
                row['Expiry_date'],
                row['Strike_price'],
                row['Option_type'],
                row['Close_price'],
                row['strike_position']
            ))
            records_inserted += 1
        
        conn.commit()
        return records_inserted
        
    except Exception as e:
        logger.error(f"Error inserting records for {symbol}: {e}")
        conn.rollback()
        return 0

def get_progress_status(conn):
    """Check current progress"""
    try:
        query = """
        SELECT 
            COUNT(DISTINCT Current_trade_date) as days_processed,
            COUNT(DISTINCT Symbol) as symbols_processed,
            COUNT(*) as total_records,
            MAX(Current_trade_date) as latest_date
        FROM Step05_strikepriceAnalysisderived
        """
        df = pd.read_sql(query, conn)
        return df.iloc[0] if not df.empty else None
    except:
        return None

def main():
    """Main execution function"""
    logger.info("Starting Complete February 2025 F&O Analysis")
    print("🚀 Starting Complete February 2025 F&O Analysis")
    
    # Connect to database
    conn = get_database_connection()
    if not conn:
        return
    
    try:
        # Create fresh table
        if not create_fresh_table(conn):
            return
        
        # Get all trading days
        trading_days = get_all_trading_days(conn)
        if not trading_days:
            logger.error("No trading days found")
            return
        
        logger.info(f"Processing {len(trading_days)} trading days")
        print(f"📅 Processing {len(trading_days)} trading days in February 2025")
        
        # Process each day
        total_records = 0
        start_time = datetime.now()
        
        for i, trade_date in enumerate(trading_days, 1):
            day_start = datetime.now()
            logger.info(f"=== Day {i}/{len(trading_days)}: {trade_date} ===")
            print(f"\n📊 Processing Day {i}/{len(trading_days)}: {trade_date}")
            
            day_records = process_single_day(conn, trade_date)
            total_records += day_records
            
            day_time = (datetime.now() - day_start).total_seconds()
            elapsed_time = (datetime.now() - start_time).total_seconds() / 60
            
            logger.info(f"Day {i} completed: {day_records} records in {day_time:.1f}s")
            print(f"✅ Day {i}: {day_records} records | Total: {total_records} | Time: {elapsed_time:.1f}min")
            
            # Progress update
            progress_pct = (i / len(trading_days)) * 100
            print(f"🔄 Progress: {progress_pct:.1f}% complete")
        
        # Final summary
        end_time = datetime.now()
        total_time = (end_time - start_time).total_seconds() / 60
        
        # Get final statistics
        final_stats = get_progress_status(conn)
        
        logger.info("=== ANALYSIS COMPLETED ===")
        logger.info(f"Total time: {total_time:.1f} minutes")
        logger.info(f"Total records: {total_records}")
        
        print("\n🎉 === ANALYSIS COMPLETED ===")
        print(f"⏱️  Total time: {total_time:.1f} minutes")
        print(f"📊 Total records: {total_records}")
        
        if final_stats is not None:
            print(f"📅 Days processed: {final_stats['days_processed']}")
            print(f"🏢 Symbols processed: {final_stats['symbols_processed']}")
            print(f"📈 Average records per day: {total_records / len(trading_days):.0f}")
        
        print("🗄️  Data stored in: Step05_strikepriceAnalysisderived")
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        print(f"❌ Error: {e}")
        
    finally:
        conn.close()
        logger.info("Database connection closed")

if __name__ == "__main__":
    main()