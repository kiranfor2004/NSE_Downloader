import pyodbc
import pandas as pd
import logging
from datetime import datetime, timedelta
import numpy as np

# Setup logging without Unicode characters
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('step05_daily_analysis_feb2025.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_database_connection():
    """Establish database connection"""
    try:
        connection_string = (
            'DRIVER={ODBC Driver 17 for SQL Server};'
            'SERVER=SRIKIRANREDDY\\SQLEXPRESS;'
            'DATABASE=master;'
            'Trusted_Connection=yes;'
        )
        return pyodbc.connect(connection_string)
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        raise

def create_daily_analysis_table(conn):
    """Create the Step05_strikepriceAnalysisderived table"""
    
    drop_sql = "DROP TABLE IF EXISTS Step05_strikepriceAnalysisderived"
    
    create_sql = """
    CREATE TABLE Step05_strikepriceAnalysisderived (
        ID INT IDENTITY(1,1) PRIMARY KEY,
        Current_trade_date VARCHAR(8) NOT NULL,
        Symbol NVARCHAR(20) NOT NULL,
        Current_close_price DECIMAL(15,4) NOT NULL,
        Trade_date VARCHAR(8) NOT NULL,
        Expiry_date VARCHAR(8) NULL,
        Strike_price DECIMAL(10,2) NOT NULL,
        Option_type NVARCHAR(5) NOT NULL,
        Close_price DECIMAL(15,4) NOT NULL,
        Strike_rank INT NOT NULL,
        Days_from_current INT NOT NULL,
        Created_at DATETIME2 DEFAULT GETDATE()
    )
    """
    
    try:
        conn.execute(drop_sql)
        conn.execute(create_sql)
        conn.commit()
        logger.info("SUCCESS: Step05_strikepriceAnalysisderived table created successfully")
    except Exception as e:
        logger.error(f"ERROR creating table: {str(e)}")
        raise

def get_february_trading_dates(conn):
    """Get all trading dates in February 2025"""
    query = """
    SELECT DISTINCT trade_date 
    FROM step04_fo_udiff_daily 
    WHERE trade_date >= '20250201' AND trade_date <= '20250228'
    ORDER BY trade_date
    """
    
    try:
        df = pd.read_sql(query, conn)
        dates = df['trade_date'].tolist()
        logger.info(f"Found {len(dates)} trading dates in February 2025")
        return dates
    except Exception as e:
        logger.error(f"ERROR getting trading dates: {str(e)}")
        return []

def get_symbols_for_date(conn, trade_date):
    """Get unique symbols available for a specific trade date"""
    query = """
    SELECT DISTINCT symbol 
    FROM step04_fo_udiff_daily 
    WHERE trade_date = ? 
    AND option_type IN ('PE', 'CE')
    AND strike_price IS NOT NULL
    ORDER BY symbol
    """
    
    try:
        df = pd.read_sql(query, conn, params=[trade_date])
        symbols = df['symbol'].tolist()
        return symbols
    except Exception as e:
        logger.error(f"ERROR getting symbols for {trade_date}: {str(e)}")
        return []

def get_current_close_price(conn, symbol, trade_date):
    """Get current close price from step03 table using correct column names"""
    
    # Convert trade_date format from YYYYMMDD to YYYY-MM-DD for step03 query
    date_str = trade_date[:4] + '-' + trade_date[4:6] + '-' + trade_date[6:8]
    
    query = """
    SELECT current_close_price
    FROM step03_compare_monthvspreviousmonth 
    WHERE symbol = ? AND current_trade_date = ?
    """
    
    try:
        result = conn.execute(query, symbol, date_str).fetchone()
        if result and result[0] is not None:
            return float(result[0])
        else:
            # Fallback to step04 data using UndrlygPric
            fallback_query = """
            SELECT TOP 1 UndrlygPric 
            FROM step04_fo_udiff_daily 
            WHERE symbol = ? AND trade_date = ? 
            AND UndrlygPric IS NOT NULL
            """
            fallback_result = conn.execute(fallback_query, symbol, trade_date).fetchone()
            if fallback_result and fallback_result[0] is not None:
                return float(fallback_result[0])
            return None
    except Exception as e:
        logger.error(f"ERROR getting current price for {symbol} on {trade_date}: {str(e)}")
        return None

def get_nearest_strikes_for_symbol_date(conn, symbol, trade_date, current_price):
    """Get 7 nearest strikes (3 up + 3 down + 1 nearest) for both PE and CE"""
    
    # Get all available strikes for this symbol on this date
    query = """
    SELECT DISTINCT strike_price
    FROM step04_fo_udiff_daily 
    WHERE symbol = ? AND trade_date = ?
    AND option_type IN ('PE', 'CE')
    AND strike_price IS NOT NULL
    ORDER BY strike_price
    """
    
    try:
        df = pd.read_sql(query, conn, params=[symbol, trade_date])
        if df.empty:
            return []
        
        strikes = df['strike_price'].tolist()
        
        # Find nearest strike to current price
        nearest_strike = min(strikes, key=lambda x: abs(x - current_price))
        nearest_idx = strikes.index(nearest_strike)
        
        # Get 3 strikes above and 3 below + nearest (total 7)
        selected_strikes = set()
        
        # Add nearest strike
        selected_strikes.add(nearest_strike)
        
        # Add 3 strikes above
        for i in range(1, 4):
            if nearest_idx + i < len(strikes):
                selected_strikes.add(strikes[nearest_idx + i])
        
        # Add 3 strikes below
        for i in range(1, 4):
            if nearest_idx - i >= 0:
                selected_strikes.add(strikes[nearest_idx - i])
        
        return sorted(list(selected_strikes))
        
    except Exception as e:
        logger.error(f"ERROR getting strikes for {symbol} on {trade_date}: {str(e)}")
        return []

def get_option_data_for_strikes(conn, symbol, trade_date, strikes):
    """Get PE and CE data for selected strikes"""
    
    if not strikes:
        return []
    
    # Create placeholders for IN clause
    placeholders = ','.join(['?' for _ in strikes])
    
    query = f"""
    SELECT 
        trade_date,
        expiry_date,
        strike_price,
        option_type,
        close_price
    FROM step04_fo_udiff_daily 
    WHERE symbol = ? 
    AND trade_date = ?
    AND option_type IN ('PE', 'CE')
    AND strike_price IN ({placeholders})
    ORDER BY strike_price, option_type
    """
    
    try:
        params = [symbol, trade_date] + strikes
        df = pd.read_sql(query, conn, params=params)
        return df.to_dict('records')
    except Exception as e:
        logger.error(f"ERROR getting option data for {symbol} on {trade_date}: {str(e)}")
        return []

def process_single_date_symbol(conn, trade_date, symbol):
    """Process one symbol for one date"""
    
    try:
        # Get current close price
        current_price = get_current_close_price(conn, symbol, trade_date)
        if current_price is None:
            logger.warning(f"WARNING: No current price found for {symbol} on {trade_date}")
            return []
        
        # Get nearest strikes
        strikes = get_nearest_strikes_for_symbol_date(conn, symbol, trade_date, current_price)
        if not strikes:
            logger.warning(f"WARNING: No strikes found for {symbol} on {trade_date}")
            return []
        
        # Get option data for these strikes
        option_data = get_option_data_for_strikes(conn, symbol, trade_date, strikes)
        if not option_data:
            logger.warning(f"WARNING: No option data found for {symbol} on {trade_date}")
            return []
        
        # Prepare records for insertion
        records = []
        for option in option_data:
            record = {
                'Current_trade_date': trade_date,
                'Symbol': symbol,
                'Current_close_price': current_price,
                'Trade_date': option['trade_date'],
                'Expiry_date': option['expiry_date'],
                'Strike_price': option['strike_price'],
                'Option_type': option['option_type'],
                'Close_price': option['close_price'],
                'Strike_rank': strikes.index(option['strike_price']) + 1,
                'Days_from_current': 0  # Same day for now
            }
            records.append(record)
        
        logger.info(f"SUCCESS: Processed {symbol} on {trade_date}: {len(records)} records")
        return records
        
    except Exception as e:
        logger.error(f"ERROR processing {symbol} on {trade_date}: {str(e)}")
        return []

def insert_analysis_records(conn, records):
    """Insert records into the analysis table"""
    
    if not records:
        return 0
    
    insert_sql = """
    INSERT INTO Step05_strikepriceAnalysisderived 
    (Current_trade_date, Symbol, Current_close_price, Trade_date, Expiry_date, 
     Strike_price, Option_type, Close_price, Strike_rank, Days_from_current)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    try:
        cursor = conn.cursor()
        
        for record in records:
            cursor.execute(insert_sql, 
                record['Current_trade_date'],
                record['Symbol'],
                record['Current_close_price'],
                record['Trade_date'],
                record['Expiry_date'],
                record['Strike_price'],
                record['Option_type'],
                record['Close_price'],
                record['Strike_rank'],
                record['Days_from_current']
            )
        
        conn.commit()
        cursor.close()
        return len(records)
        
    except Exception as e:
        logger.error(f"ERROR inserting records: {str(e)}")
        conn.rollback()
        return 0

def generate_summary_report(conn):
    """Generate summary report"""
    
    summary_query = """
    SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT Current_trade_date) as unique_dates,
        COUNT(DISTINCT Symbol) as unique_symbols,
        COUNT(DISTINCT CONCAT(Symbol, Current_trade_date)) as symbol_date_combinations
    FROM Step05_strikepriceAnalysisderived
    """
    
    detailed_query = """
    SELECT 
        Current_trade_date,
        Symbol,
        COUNT(*) as records_count
    FROM Step05_strikepriceAnalysisderived
    GROUP BY Current_trade_date, Symbol
    ORDER BY Current_trade_date, Symbol
    """
    
    try:
        # Summary statistics
        summary_df = pd.read_sql(summary_query, conn)
        detailed_df = pd.read_sql(detailed_query, conn)
        
        print("\n" + "="*80)
        print("STEP 5: DAILY STRIKE PRICE ANALYSIS - FEBRUARY 2025 REPORT")
        print("="*80)
        
        print("\nSUMMARY STATISTICS:")
        for _, row in summary_df.iterrows():
            print(f"Total records: {row['total_records']}")
            print(f"Unique trading dates: {row['unique_dates']}")
            print(f"Unique symbols: {row['unique_symbols']}")
            print(f"Symbol-date combinations: {row['symbol_date_combinations']}")
        
        print("\nRECORDS BY DATE AND SYMBOL:")
        current_date = None
        for _, row in detailed_df.iterrows():
            if current_date != row['Current_trade_date']:
                current_date = row['Current_trade_date']
                date_str = f"{current_date[:4]}-{current_date[4:6]}-{current_date[6:8]}"
                print(f"\n{date_str} ({current_date}):")
            print(f"   {row['Symbol']}: {row['records_count']} records")
        
        print("\n" + "="*80)
        print("SUCCESS: Analysis completed successfully!")
        print("="*80)
        
    except Exception as e:
        logger.error(f"ERROR generating report: {str(e)}")

def main():
    """Main processing function"""
    
    logger.info("Starting Step 5 Daily Strike Price Analysis for February 2025")
    
    try:
        # Database connection
        conn = get_database_connection()
        logger.info("SUCCESS: Database connected")
        
        # Create table
        create_daily_analysis_table(conn)
        
        # Get February trading dates
        trading_dates = get_february_trading_dates(conn)
        if not trading_dates:
            logger.error("ERROR: No trading dates found for February 2025")
            return
        
        total_records = 0
        processed_combinations = 0
        
        # Process each date
        for i, trade_date in enumerate(trading_dates, 1):
            date_str = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:8]}"
            logger.info(f"Processing {date_str} ({i}/{len(trading_dates)})")
            
            # Get symbols for this date
            symbols = get_symbols_for_date(conn, trade_date)
            logger.info(f"Found {len(symbols)} symbols for {date_str}")
            
            date_records = []
            
            # Process each symbol for this date
            for j, symbol in enumerate(symbols, 1):
                if j % 50 == 0:  # Progress update every 50 symbols
                    logger.info(f"   Progress: {j}/{len(symbols)} symbols processed")
                
                symbol_records = process_single_date_symbol(conn, trade_date, symbol)
                if symbol_records:
                    date_records.extend(symbol_records)
                    processed_combinations += 1
            
            # Insert records for this date
            if date_records:
                inserted_count = insert_analysis_records(conn, date_records)
                total_records += inserted_count
                logger.info(f"SUCCESS: {date_str}: Inserted {inserted_count} records")
            else:
                logger.warning(f"WARNING: {date_str}: No records to insert")
        
        logger.info(f"PROCESSING COMPLETED!")
        logger.info(f"Total records inserted: {total_records}")
        logger.info(f"Processed combinations: {processed_combinations}")
        
        # Generate summary report
        generate_summary_report(conn)
        
        conn.close()
        logger.info("SUCCESS: Database connection closed")
        
    except Exception as e:
        logger.error(f"ERROR in main processing: {str(e)}")
        raise

if __name__ == "__main__":
    main()