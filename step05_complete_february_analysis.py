#!/usr/bin/env python3
"""
Step 5: Complete February 2025 F&O Strike Price Analysis
Process ALL trading days with ALL symbols in February 2025
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
        logging.FileHandler('step05_complete_february_analysis.log'),
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

def create_table(conn):
    """Create the analysis table"""
    try:
        cursor = conn.cursor()
        
        # Drop existing table
        cursor.execute("DROP TABLE IF EXISTS Step05_strikepriceAnalysisderived")
        conn.commit()
        
        # Create new table with proper indexing
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
            Analysis_timestamp DATETIME2 DEFAULT GETDATE(),
            INDEX IX_Step05_Symbol_Date (Symbol, Current_trade_date),
            INDEX IX_Step05_Strike (Strike_price, Option_type),
            INDEX IX_Step05_Date (Current_trade_date)
        )
        """
        cursor.execute(create_table_query)
        conn.commit()
        logger.info("Created Step05_strikepriceAnalysisderived table with indexes")
        return True
    except Exception as e:
        logger.error(f"Error creating table: {e}")
        return False

def get_all_trading_days(conn):
    """Get ALL trading days in February 2025"""
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

def get_processed_days(conn):
    """Check which days are already processed"""
    try:
        query = """
        SELECT DISTINCT Current_trade_date 
        FROM Step05_strikepriceAnalysisderived 
        WHERE Current_trade_date LIKE '202502%'
        ORDER BY Current_trade_date
        """
        df = pd.read_sql(query, conn)
        processed_days = df['Current_trade_date'].tolist()
        logger.info(f"Already processed {len(processed_days)} days")
        return processed_days
    except Exception as e:
        logger.info("No previous progress found (starting fresh)")
        return []

def process_single_day(conn, trade_date):
    """Process ALL symbols for a single trading day"""
    logger.info(f"Processing trading day: {trade_date}")
    
    # Get ALL symbols for this date with valid data
    symbols_query = """
    SELECT Symbol, COUNT(*) as record_count, AVG(UndrlygPric) as avg_underlying
    FROM step04_fo_udiff_daily 
    WHERE Trade_date = ? 
      AND UndrlygPric IS NOT NULL 
      AND Strike_price IS NOT NULL
      AND Close_price IS NOT NULL
      AND Strike_price > 0
      AND Close_price > 0
    GROUP BY Symbol
    HAVING COUNT(*) >= 10
    ORDER BY Symbol
    """
    
    try:
        symbols_df = pd.read_sql(symbols_query, conn, params=[trade_date])
        logger.info(f"Found {len(symbols_df)} symbols for date {trade_date}")
        
        if symbols_df.empty:
            return 0
        
        total_records = 0
        successful_symbols = 0
        
        for i, symbol_row in symbols_df.iterrows():
            symbol = symbol_row['Symbol']
            underlying_price = symbol_row['avg_underlying']
            
            if (i + 1) % 20 == 0:
                logger.info(f"  Progress: {i+1}/{len(symbols_df)} symbols, {total_records} records so far")
            
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
              AND Close_price > 0
            ORDER BY Option_type, Strike_price
            """
            
            fo_df = pd.read_sql(fo_query, conn, params=[symbol, trade_date])
            
            if fo_df.empty:
                continue
            
            # Get unique valid strikes (no NaN)
            strikes = sorted([s for s in fo_df['Strike_price'].unique() if not pd.isna(s) and s > 0])
            
            if len(strikes) < 3:
                continue
            
            # Find nearest strike to underlying price
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
            
            # Filter data for selected strikes
            selected_data = fo_df[fo_df['Strike_price'].isin(selected_strikes)].copy()
            
            # Insert records
            records_inserted = insert_records(conn, trade_date, symbol, underlying_price, 
                                            selected_data, selected_strikes, nearest_strike)
            
            if records_inserted > 0:
                total_records += records_inserted
                successful_symbols += 1
        
        logger.info(f"Day {trade_date} completed: {successful_symbols} symbols, {total_records} records")
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

def generate_progress_report(conn):
    """Generate comprehensive progress report"""
    try:
        # Summary statistics
        summary_query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT Current_trade_date) as trading_days,
            COUNT(DISTINCT Symbol) as unique_symbols,
            MIN(Current_trade_date) as first_date,
            MAX(Current_trade_date) as last_date
        FROM Step05_strikepriceAnalysisderived
        """
        
        summary_df = pd.read_sql(summary_query, conn)
        summary = summary_df.iloc[0]
        
        # Daily breakdown
        daily_query = """
        SELECT 
            Current_trade_date,
            COUNT(*) as records,
            COUNT(DISTINCT Symbol) as symbols
        FROM Step05_strikepriceAnalysisderived
        GROUP BY Current_trade_date
        ORDER BY Current_trade_date
        """
        
        daily_df = pd.read_sql(daily_query, conn)
        
        logger.info(f"=== COMPREHENSIVE PROGRESS REPORT ===")
        logger.info(f"Total Records: {summary['total_records']:,}")
        logger.info(f"Trading Days Processed: {summary['trading_days']}")
        logger.info(f"Unique Symbols: {summary['unique_symbols']}")
        logger.info(f"Date Range: {summary['first_date']} to {summary['last_date']}")
        
        logger.info(f"\n=== DAILY BREAKDOWN ===")
        for _, day in daily_df.iterrows():
            logger.info(f"{day['Current_trade_date']}: {day['records']:,} records ({day['symbols']} symbols)")
        
        return summary['total_records']
        
    except Exception as e:
        logger.error(f"Error generating progress report: {e}")
        return 0

def main():
    """Main execution function"""
    logger.info("Starting Step 5 Complete February 2025 F&O Analysis")
    
    conn = get_database_connection()
    if not conn:
        return
    
    try:
        # Create table
        if not create_table(conn):
            return
        
        # Get all trading days
        all_trading_days = get_all_trading_days(conn)
        if not all_trading_days:
            logger.error("No trading days found")
            return
        
        # Check progress
        processed_days = get_processed_days(conn)
        remaining_days = [day for day in all_trading_days if day not in processed_days]
        
        if not remaining_days:
            logger.info("All days already processed!")
            generate_progress_report(conn)
            return
        
        logger.info(f"Processing {len(remaining_days)} remaining days out of {len(all_trading_days)} total")
        logger.info(f"Remaining days: {remaining_days}")
        
        # Process each day
        total_records = 0
        for i, trade_date in enumerate(remaining_days, 1):
            logger.info(f"=== Processing Day {i}/{len(remaining_days)}: {trade_date} ===")
            
            day_records = process_single_day(conn, trade_date)
            total_records += day_records
            
            completion_pct = (i / len(remaining_days)) * 100
            logger.info(f"Day {trade_date} completed with {day_records:,} records")
            logger.info(f"Overall Progress: {i}/{len(remaining_days)} days ({completion_pct:.1f}%), {total_records:,} total records")
            
            print(f"Progress: {i}/{len(remaining_days)} days completed ({completion_pct:.1f}%), {total_records:,} total records")
            
            # Generate interim report every 5 days
            if i % 5 == 0:
                logger.info(f"=== INTERIM REPORT AFTER {i} DAYS ===")
                generate_progress_report(conn)
        
        # Final comprehensive report
        logger.info(f"=== ANALYSIS COMPLETED ===")
        final_record_count = generate_progress_report(conn)
        
        logger.info(f"Successfully processed {len(remaining_days)} days")
        logger.info(f"Total new records inserted: {total_records:,}")
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        logger.info("Generating partial progress report...")
        generate_progress_report(conn)
        
    finally:
        conn.close()
        logger.info("Database connection closed")

if __name__ == "__main__":
    main()