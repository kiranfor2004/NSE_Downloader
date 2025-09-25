#!/usr/bin/env python3
"""
Step 5: FIXED - Day-wise F&O Analysis with single expiry per strike
Only select nearest expiry date to get exactly 14 records per symbol per day
"""

import pyodbc
import pandas as pd
import numpy as np
import logging
from datetime import datetime
import sys

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_database_connection():
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
    try:
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS Step05_strikepriceAnalysisderived")
        conn.commit()
        
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
    query = "SELECT DISTINCT Trade_date FROM step04_fo_udiff_daily WHERE Trade_date LIKE '202502%' ORDER BY Trade_date"
    try:
        df = pd.read_sql(query, conn)
        trading_days = df['Trade_date'].tolist()
        logger.info(f"Found {len(trading_days)} trading days in February 2025")
        return trading_days
    except Exception as e:
        logger.error(f"Error getting trading days: {e}")
        return []

def process_single_day(conn, trade_date):
    logger.info(f"Processing trading day: {trade_date}")
    
    # Get symbols with their underlying prices
    symbols_query = """
    SELECT DISTINCT Symbol, AVG(UndrlygPric) as avg_underlying
    FROM step04_fo_udiff_daily 
    WHERE Trade_date = ? 
      AND UndrlygPric IS NOT NULL 
      AND Strike_price IS NOT NULL
      AND Close_price IS NOT NULL
    GROUP BY Symbol
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
            
            # Get F&O data for this symbol with NEAREST EXPIRY ONLY
            fo_query = """
            WITH RankedExpiry AS (
                SELECT 
                    ID,
                    Trade_date,
                    Expiry_date,
                    Strike_price,
                    Option_type,
                    Close_price,
                    ROW_NUMBER() OVER (PARTITION BY Strike_price, Option_type ORDER BY Expiry_date) as expiry_rank
                FROM step04_fo_udiff_daily 
                WHERE Symbol = ? AND Trade_date = ?
                  AND Strike_price IS NOT NULL 
                  AND Close_price IS NOT NULL
                  AND Strike_price > 0
            )
            SELECT 
                ID,
                Trade_date,
                Expiry_date,
                Strike_price,
                Option_type,
                Close_price
            FROM RankedExpiry 
            WHERE expiry_rank = 1
            ORDER BY Option_type, Strike_price
            """
            
            fo_df = pd.read_sql(fo_query, conn, params=[symbol, trade_date])
            
            if fo_df.empty:
                continue
            
            # Get unique valid strikes
            strikes = sorted([s for s in fo_df['Strike_price'].unique() if not pd.isna(s) and s > 0])
            
            if len(strikes) < 3:
                continue
            
            # Find 7 nearest strikes
            nearest_strike = min(strikes, key=lambda x: abs(x - underlying_price))
            nearest_index = strikes.index(nearest_strike)
            
            start_index = max(0, nearest_index - 3)
            end_index = min(len(strikes), nearest_index + 4)
            
            while end_index - start_index < 7 and start_index > 0:
                start_index -= 1
            while end_index - start_index < 7 and end_index < len(strikes):
                end_index += 1
            
            selected_strikes = strikes[start_index:end_index]
            
            # Filter data for selected strikes
            selected_data = fo_df[fo_df['Strike_price'].isin(selected_strikes)].copy()
            
            # Verify we have exactly 14 records (7 strikes × 2 option types)
            expected_records = len(selected_strikes) * 2  # CE and PE
            if len(selected_data) != expected_records:
                logger.warning(f"{symbol}: Expected {expected_records} records, got {len(selected_data)}")
            
            # Insert records
            records_inserted = insert_records(conn, trade_date, symbol, underlying_price, 
                                            selected_data, selected_strikes, nearest_strike)
            total_records += records_inserted
            processed_symbols += 1
            
            if processed_symbols % 50 == 0:
                logger.info(f"  Processed {processed_symbols}/{len(symbols_df)} symbols")
        
        logger.info(f"Day {trade_date} completed: {processed_symbols} symbols, {total_records} records")
        return total_records
        
    except Exception as e:
        logger.error(f"Error processing day {trade_date}: {e}")
        return 0

def insert_records(conn, trade_date, symbol, underlying_price, fo_data, selected_strikes, nearest_strike):
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
    logger.info("Starting Step 5 FIXED Day-wise F&O Analysis")
    
    conn = get_database_connection()
    if not conn:
        return
    
    try:
        if not create_table(conn):
            return
        
        trading_days = get_trading_days(conn)
        if not trading_days:
            logger.error("No trading days found")
            return
        
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
            COUNT(DISTINCT Symbol) as symbols,
            AVG(CAST(records_per_symbol AS FLOAT)) as avg_records_per_symbol
        FROM (
            SELECT Symbol, Current_trade_date, COUNT(*) as records_per_symbol
            FROM Step05_strikepriceAnalysisderived
            GROUP BY Symbol, Current_trade_date
        ) t
        """
        
        summary_df = pd.read_sql(summary_query, conn)
        summary = summary_df.iloc[0]
        
        logger.info(f"=== ANALYSIS COMPLETED ===")
        logger.info(f"Total records: {summary['total_records']}")
        logger.info(f"Trading days: {summary['trading_days']}")
        logger.info(f"Symbols: {summary['symbols']}")
        logger.info(f"Average records per symbol: {summary['avg_records_per_symbol']:.1f}")
        
        # Test ABB specifically
        test_query = """
        SELECT COUNT(*) as abb_records 
        FROM Step05_strikepriceAnalysisderived 
        WHERE Symbol = 'ABB' AND Current_trade_date = '20250203'
        """
        test_df = pd.read_sql(test_query, conn)
        logger.info(f"ABB records on 2025-02-03: {test_df['abb_records'].iloc[0]} (should be ~14)")
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        
    finally:
        conn.close()
        logger.info("Database connection closed")

if __name__ == "__main__":
    main()