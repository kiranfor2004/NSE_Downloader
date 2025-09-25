#!/usr/bin/env python3
"""
Step 5: Day-wise F&O Strike Price Analysis - SIMPLIFIED VERSION
Test with just ABB symbol first
"""

import pyodbc
import pandas as pd
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

def main():
    """Main execution function"""
    logger.info("Starting simplified Step 5 analysis for ABB")
    
    conn = get_database_connection()
    if not conn:
        return
    
    try:
        # Drop existing table and recreate
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS Step05_strikepriceAnalysisderived")
        conn.commit()
        logger.info("Dropped existing table")
        
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
        logger.info("Created new table")
        
        # Test with ABB on 2025-02-03
        test_date = '20250203'
        test_symbol = 'ABB'
        
        # Get underlying price for ABB on this date
        price_query = """
        SELECT AVG(UndrlygPric) as underlying_price
        FROM step04_fo_udiff_daily 
        WHERE Trade_date = ? AND Symbol = ?
        """
        price_df = pd.read_sql(price_query, conn, params=[test_date, test_symbol])
        
        if price_df.empty or price_df['underlying_price'].iloc[0] is None:
            logger.error(f"No underlying price found for {test_symbol} on {test_date}")
            return
            
        underlying_price = float(price_df['underlying_price'].iloc[0])
        logger.info(f"Underlying price for {test_symbol} on {test_date}: {underlying_price}")
        
        # Get F&O data for ABB on this date
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
        ORDER BY Option_type, Strike_price
        """
        
        fo_df = pd.read_sql(fo_query, conn, params=[test_symbol, test_date])
        logger.info(f"Found {len(fo_df)} F&O records for {test_symbol}")
        
        if fo_df.empty:
            logger.error("No F&O data found")
            return
        
        # Get unique strikes and find 7 nearest
        strikes = sorted(fo_df['Strike_price'].unique())
        logger.info(f"Available strikes: {strikes}")
        
        # Find nearest strike
        nearest_strike = min(strikes, key=lambda x: abs(x - underlying_price))
        nearest_index = strikes.index(nearest_strike)
        logger.info(f"Nearest strike to {underlying_price}: {nearest_strike} (index {nearest_index})")
        
        # Get 7 strikes (3 up + 3 down + 1 nearest)
        start_index = max(0, nearest_index - 3)
        end_index = min(len(strikes), nearest_index + 4)
        
        # Ensure we have exactly 7 strikes if possible
        while end_index - start_index < 7 and start_index > 0:
            start_index -= 1
        while end_index - start_index < 7 and end_index < len(strikes):
            end_index += 1
        
        selected_strikes = strikes[start_index:end_index]
        logger.info(f"Selected {len(selected_strikes)} strikes: {selected_strikes}")
        
        # Filter data for selected strikes
        selected_data = fo_df[fo_df['Strike_price'].isin(selected_strikes)].copy()
        
        # Add strike rank
        for i, row in selected_data.iterrows():
            strike_rank = selected_strikes.index(row['Strike_price']) - selected_strikes.index(nearest_strike)
            selected_data.loc[i, 'Strike_rank'] = strike_rank
        
        logger.info(f"Total records to insert: {len(selected_data)}")
        
        # Insert records
        insert_query = """
        INSERT INTO Step05_strikepriceAnalysisderived 
        (Current_trade_date, Symbol, Current_close_price, FO_Record_ID, Trade_date, 
         Expiry_date, Strike_price, Option_type, Close_price, Strike_rank)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        inserted_count = 0
        for _, row in selected_data.iterrows():
            cursor.execute(insert_query, (
                test_date,
                test_symbol,
                underlying_price,
                row['ID'],
                row['Trade_date'],
                row['Expiry_date'],
                row['Strike_price'],
                row['Option_type'],
                row['Close_price'],
                row['Strike_rank']
            ))
            inserted_count += 1
        
        conn.commit()
        logger.info(f"Successfully inserted {inserted_count} records")
        
        # Verify the data
        verify_query = "SELECT COUNT(*) as count FROM Step05_strikepriceAnalysisderived"
        verify_df = pd.read_sql(verify_query, conn)
        logger.info(f"Total records in table: {verify_df['count'].iloc[0]}")
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        
    finally:
        conn.close()
        logger.info("Database connection closed")

if __name__ == "__main__":
    main()