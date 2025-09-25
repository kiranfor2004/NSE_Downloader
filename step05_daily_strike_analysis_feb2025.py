"""
Step 5: Daily Strike Price Analysis for February 2025
Creates Step05_strikepriceAnalysisderived table with 14 records per trading day
Processes every trading day in February 2025 for strike price analysis
"""

import pyodbc
import pandas as pd
import logging
from datetime import datetime, timedelta
import json

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('step05_daily_analysis_feb2025.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def load_database_config():
    """Load database configuration"""
    try:
        with open('database_config.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error("database_config.json not found")
        return None

def get_database_connection(config):
    """Create database connection"""
    try:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={config['server']};"
            f"DATABASE={config['database']};"
            f"Trusted_Connection=yes;"
        )
        conn = pyodbc.connect(conn_str)
        logger.info(f"Connected to database: {config['database']}")
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return None

def create_step05_daily_table(conn):
    """Create the Step05_strikepriceAnalysisderived table"""
    
    drop_table_query = """
    IF OBJECT_ID('Step05_strikepriceAnalysisderived', 'U') IS NOT NULL
        DROP TABLE Step05_strikepriceAnalysisderived
    """
    
    create_table_query = """
    CREATE TABLE Step05_strikepriceAnalysisderived (
        ID INT IDENTITY(1,1) PRIMARY KEY,
        
        -- From step04_fo_udiff_daily (current day data)
        Current_trade_date INT NOT NULL,
        Symbol NVARCHAR(20) NOT NULL,
        Current_close_price DECIMAL(15,4) NOT NULL,
        
        -- From step04_fo_udiff_daily (strike data)
        Strike_ID INT NOT NULL,
        Trade_date INT NOT NULL,
        Expiry_date INT NOT NULL,
        Strike_price DECIMAL(10,2) NOT NULL,
        Option_type NVARCHAR(5) NOT NULL,
        Close_price DECIMAL(15,4) NOT NULL,
        
        -- Analysis metadata
        Strike_position NVARCHAR(20) NOT NULL, -- 'ATM', 'OTM+1', 'OTM+2', 'OTM+3', 'ITM-1', 'ITM-2', 'ITM-3'
        Price_difference DECIMAL(15,4) NOT NULL, -- Difference from current close price
        Created_timestamp DATETIME2 DEFAULT GETDATE()
    )
    """
    
    try:
        cursor = conn.cursor()
        cursor.execute(drop_table_query)
        cursor.execute(create_table_query)
        conn.commit()
        logger.info("✅ Step05_strikepriceAnalysisderived table created successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to create table: {e}")
        return False

def get_february_trading_days(conn):
    """Get all unique trading days in February 2025 from step04_fo_udiff_daily"""
    
    query = """
    SELECT DISTINCT Trade_date 
    FROM step04_fo_udiff_daily 
    WHERE Trade_date >= 20250201 
      AND Trade_date < 20250301
      AND Trade_date IS NOT NULL
    ORDER BY Trade_date
    """
    
    try:
        df = pd.read_sql(query, conn)
        trading_days = df['Trade_date'].tolist()
        logger.info(f"Found {len(trading_days)} trading days in February 2025")
        return trading_days
    except Exception as e:
        logger.error(f"Failed to get February trading days: {e}")
        return []

def get_symbols_for_date(conn, trade_date):
    """Get all unique symbols available on a specific trading date"""
    
    query = """
    SELECT DISTINCT Symbol 
    FROM step04_fo_udiff_daily 
    WHERE Trade_date = ?
      AND Symbol IS NOT NULL
      AND Close_price IS NOT NULL
      AND Close_price > 0
    ORDER BY Symbol
    """
    
    try:
        df = pd.read_sql(query, conn, params=[trade_date])
        symbols = df['Symbol'].tolist()
        logger.info(f"Found {len(symbols)} symbols on {trade_date}")
        return symbols
    except Exception as e:
        logger.error(f"Failed to get symbols for {trade_date}: {e}")
        return []

def get_current_close_price(conn, symbol, trade_date):
    """Get the current close price for a symbol on a specific date"""
    
    query = """
    SELECT TOP 1 Close_price 
    FROM step04_fo_udiff_daily 
    WHERE Symbol = ? 
      AND Trade_date = ?
      AND Close_price IS NOT NULL
      AND Close_price > 0
    ORDER BY ID DESC
    """
    
    try:
        cursor = conn.cursor()
        cursor.execute(query, [symbol, trade_date])
        result = cursor.fetchone()
        if result:
            return float(result[0])
        return None
    except Exception as e:
        logger.error(f"Failed to get close price for {symbol} on {trade_date}: {e}")
        return None

def get_available_strikes_for_symbol_date(conn, symbol, trade_date):
    """Get all available strikes for a symbol on a specific date"""
    
    query = """
    SELECT DISTINCT Strike_price, Option_type
    FROM step04_fo_udiff_daily 
    WHERE Symbol = ? 
      AND Trade_date = ?
      AND Strike_price IS NOT NULL
      AND Close_price IS NOT NULL
      AND Close_price > 0
    ORDER BY Strike_price, Option_type
    """
    
    try:
        df = pd.read_sql(query, conn, params=[symbol, trade_date])
        return df
    except Exception as e:
        logger.error(f"Failed to get strikes for {symbol} on {trade_date}: {e}")
        return pd.DataFrame()

def find_nearest_strikes(available_strikes_df, current_price, num_strikes=7):
    """Find nearest strikes (3 up + 3 down + 1 nearest) for both PE and CE"""
    
    if available_strikes_df.empty:
        return pd.DataFrame()
    
    # Get unique strike prices
    unique_strikes = sorted(available_strikes_df['Strike_price'].unique())
    
    if len(unique_strikes) < num_strikes:
        logger.warning(f"Only {len(unique_strikes)} strikes available, need {num_strikes}")
        selected_strikes = unique_strikes
    else:
        # Find the closest strike to current price
        differences = [abs(strike - current_price) for strike in unique_strikes]
        closest_index = differences.index(min(differences))
        
        # Calculate range around closest strike
        start_index = max(0, closest_index - 3)
        end_index = min(len(unique_strikes), start_index + num_strikes)
        
        # Adjust start if we hit the end
        if end_index - start_index < num_strikes:
            start_index = max(0, end_index - num_strikes)
        
        selected_strikes = unique_strikes[start_index:end_index]
    
    # Filter available strikes to only include selected strikes for both PE and CE
    result_df = available_strikes_df[
        available_strikes_df['Strike_price'].isin(selected_strikes)
    ].copy()
    
    return result_df

def get_strike_details(conn, symbol, trade_date, strike_price, option_type):
    """Get detailed information for a specific strike"""
    
    query = """
    SELECT TOP 1 
        ID,
        Trade_date,
        Expiry_date,
        Strike_price,
        Option_type,
        Close_price
    FROM step04_fo_udiff_daily 
    WHERE Symbol = ? 
      AND Trade_date = ?
      AND Strike_price = ?
      AND Option_type = ?
      AND Close_price IS NOT NULL
      AND Close_price > 0
    ORDER BY ID DESC
    """
    
    try:
        df = pd.read_sql(query, conn, params=[symbol, trade_date, strike_price, option_type])
        if not df.empty:
            return df.iloc[0]
        return None
    except Exception as e:
        logger.error(f"Failed to get strike details: {e}")
        return None

def determine_strike_position(strike_price, current_price, all_selected_strikes):
    """Determine the position of strike relative to current price"""
    
    sorted_strikes = sorted(all_selected_strikes)
    
    try:
        strike_index = sorted_strikes.index(strike_price)
        middle_index = len(sorted_strikes) // 2
        
        if strike_index == middle_index:
            return "ATM"
        elif strike_index > middle_index:
            diff = strike_index - middle_index
            return f"OTM+{diff}"
        else:
            diff = middle_index - strike_index
            return f"ITM-{diff}"
    except ValueError:
        return "UNKNOWN"

def process_single_day_analysis(conn, trade_date):
    """Process analysis for a single trading day"""
    
    logger.info(f"🔍 Processing analysis for {trade_date}")
    
    # Get all symbols for this date
    symbols = get_symbols_for_date(conn, trade_date)
    
    if not symbols:
        logger.warning(f"No symbols found for {trade_date}")
        return 0
    
    total_records_inserted = 0
    
    for i, symbol in enumerate(symbols, 1):
        try:
            logger.info(f"Processing {symbol} ({i}/{len(symbols)}) for {trade_date}")
            
            # Get current close price
            current_price = get_current_close_price(conn, symbol, trade_date)
            if not current_price:
                logger.warning(f"No close price found for {symbol} on {trade_date}")
                continue
            
            # Get available strikes
            available_strikes = get_available_strikes_for_symbol_date(conn, symbol, trade_date)
            if available_strikes.empty:
                logger.warning(f"No strikes found for {symbol} on {trade_date}")
                continue
            
            # Find nearest strikes
            selected_strikes = find_nearest_strikes(available_strikes, current_price)
            if selected_strikes.empty:
                logger.warning(f"No selected strikes for {symbol} on {trade_date}")
                continue
            
            # Get unique selected strike prices for position calculation
            unique_selected_strikes = sorted(selected_strikes['Strike_price'].unique())
            
            # Process each selected strike
            for _, strike_row in selected_strikes.iterrows():
                strike_price = strike_row['Strike_price']
                option_type = strike_row['Option_type']
                
                # Get detailed strike information
                strike_details = get_strike_details(conn, symbol, trade_date, strike_price, option_type)
                if strike_details is None:
                    continue
                
                # Determine strike position
                strike_position = determine_strike_position(strike_price, current_price, unique_selected_strikes)
                price_difference = strike_price - current_price
                
                # Insert into database
                insert_query = """
                INSERT INTO Step05_strikepriceAnalysisderived (
                    Current_trade_date, Symbol, Current_close_price,
                    Strike_ID, Trade_date, Expiry_date, Strike_price, Option_type, Close_price,
                    Strike_position, Price_difference
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                cursor = conn.cursor()
                cursor.execute(insert_query, [
                    trade_date, symbol, current_price,
                    strike_details['ID'], strike_details['Trade_date'], strike_details['Expiry_date'],
                    strike_details['Strike_price'], strike_details['Option_type'], strike_details['Close_price'],
                    strike_position, price_difference
                ])
                
                total_records_inserted += 1
            
            # Commit after each symbol to avoid large transactions
            conn.commit()
            
        except Exception as e:
            logger.error(f"Error processing {symbol} on {trade_date}: {e}")
            continue
    
    logger.info(f"✅ Completed {trade_date}: {total_records_inserted} records inserted")
    return total_records_inserted

def main():
    """Main execution function"""
    
    logger.info("🚀 Starting Step 5 Daily Strike Price Analysis for February 2025")
    
    # Load database configuration
    config = load_database_config()
    if not config:
        logger.error("Failed to load database configuration")
        return
    
    # Connect to database
    conn = get_database_connection(config)
    if not conn:
        logger.error("Failed to connect to database")
        return
    
    try:
        # Create the table
        if not create_step05_daily_table(conn):
            logger.error("Failed to create table")
            return
        
        # Get February trading days
        trading_days = get_february_trading_days(conn)
        if not trading_days:
            logger.error("No trading days found for February 2025")
            return
        
        logger.info(f"📅 Processing {len(trading_days)} trading days in February 2025")
        
        total_records = 0
        
        # Process each trading day
        for day_num, trade_date in enumerate(trading_days, 1):
            logger.info(f"📊 Day {day_num}/{len(trading_days)}: {trade_date}")
            
            day_records = process_single_day_analysis(conn, trade_date)
            total_records += day_records
            
            # Progress update
            progress = (day_num / len(trading_days)) * 100
            logger.info(f"Progress: {progress:.1f}% - Total records so far: {total_records}")
        
        # Final summary
        logger.info(f"""
===========================================
✅ FEBRUARY 2025 ANALYSIS COMPLETED!
===========================================
📅 Trading days processed: {len(trading_days)}
📊 Total records created: {total_records}
📋 Table: Step05_strikepriceAnalysisderived
⏱️  Analysis completed at: {datetime.now()}
===========================================
        """)
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
    
    finally:
        conn.close()
        logger.info("Database connection closed")

if __name__ == "__main__":
    main()