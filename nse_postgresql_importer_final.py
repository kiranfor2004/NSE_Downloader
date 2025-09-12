#!/usr/bin/env python3
"""
🚀 NSE PostgreSQL Data Importer (Final Version)
Import NSE CSV data into PostgreSQL database with proper date handling
"""

import os
import json
import pandas as pd
import psycopg2
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_config():
    """Load database configuration"""
    try:
        with open('database_config.json', 'r') as f:
            config = json.load(f)
        return config
    except Exception as e:
        logger.error(f"Error loading config: {str(e)}")
        return None

def connect_to_database(config):
    """Connect to PostgreSQL database"""
    try:
        conn = psycopg2.connect(
            host=config['host'],
            port=config['port'],
            database=config['database'],
            user=config['username'],
            password=config['password']
        )
        logger.info("✅ Connected to PostgreSQL nse_data database")
        return conn
    except Exception as e:
        logger.error(f"❌ Database connection failed: {str(e)}")
        return None

def create_table_if_not_exists(conn):
    """Create the table with correct structure matching CSV data"""
    try:
        cursor = conn.cursor()
        
        # Drop table if exists to ensure clean structure
        cursor.execute("DROP TABLE IF EXISTS nse_stock_data")
        
        # Create table with proper data types
        create_table_sql = """
        CREATE TABLE nse_stock_data (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(50) NOT NULL,
            series VARCHAR(10) NOT NULL,
            trade_date DATE NOT NULL,
            prev_close DECIMAL(15,2),
            open_price DECIMAL(15,2),
            high_price DECIMAL(15,2),
            low_price DECIMAL(15,2),
            last_price DECIMAL(15,2),
            close_price DECIMAL(15,2),
            avg_price DECIMAL(15,2),
            total_traded_qty BIGINT,
            turnover_lacs DECIMAL(15,2),
            no_of_trades INTEGER,
            deliverable_qty BIGINT,
            delivery_percent DECIMAL(8,2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, series, trade_date)
        );
        
        CREATE INDEX IF NOT EXISTS idx_nse_symbol ON nse_stock_data(symbol);
        CREATE INDEX IF NOT EXISTS idx_nse_date ON nse_stock_data(trade_date);
        CREATE INDEX IF NOT EXISTS idx_nse_symbol_date ON nse_stock_data(symbol, trade_date);
        """
        
        cursor.execute(create_table_sql)
        conn.commit()
        cursor.close()
        logger.info("✅ Table nse_stock_data created successfully")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error creating table: {str(e)}")
        return False

def convert_date_format(date_str):
    """Convert date from '01-Aug-2025' format to datetime"""
    try:
        # Remove any leading/trailing spaces
        date_str = date_str.strip()
        # Parse the date with abbreviated month name
        return datetime.strptime(date_str, '%d-%b-%Y').date()
    except Exception as e:
        logger.error(f"Date conversion error: {date_str} -> {str(e)}")
        return None

def safe_float_convert(value):
    """Safely convert value to float, handling spaces and invalid values"""
    try:
        if pd.isna(value) or value == '' or str(value).strip() == '':
            return None
        return float(str(value).strip())
    except:
        return None

def safe_int_convert(value):
    """Safely convert value to int, handling spaces and invalid values"""
    try:
        if pd.isna(value) or value == '' or str(value).strip() == '':
            return None
        return int(float(str(value).strip()))
    except:
        return None

def process_csv_file(file_path, conn):
    """Process a single CSV file and import to database"""
    try:
        # Read CSV file
        df = pd.read_csv(file_path, skipinitialspace=True)
        
        # Clean column names (remove spaces)
        df.columns = df.columns.str.strip()
        
        # Expected columns
        expected_cols = ['SYMBOL', 'SERIES', 'DATE1', 'PREV_CLOSE', 'OPEN_PRICE', 
                        'HIGH_PRICE', 'LOW_PRICE', 'LAST_PRICE', 'CLOSE_PRICE', 
                        'AVG_PRICE', 'TTL_TRD_QNTY', 'TURNOVER_LACS', 'NO_OF_TRADES', 
                        'DELIV_QTY', 'DELIV_PER']
        
        # Check if all required columns exist
        missing_cols = [col for col in expected_cols if col not in df.columns]
        if missing_cols:
            logger.error(f"Missing columns: {missing_cols}")
            return False, 0
        
        records_imported = 0
        cursor = conn.cursor()
        
        for index, row in df.iterrows():
            try:
                # Convert and validate data
                symbol = str(row['SYMBOL']).strip()
                series = str(row['SERIES']).strip()
                trade_date = convert_date_format(str(row['DATE1']))
                
                if not trade_date:
                    continue
                
                # Convert numeric fields safely
                prev_close = safe_float_convert(row['PREV_CLOSE'])
                open_price = safe_float_convert(row['OPEN_PRICE'])
                high_price = safe_float_convert(row['HIGH_PRICE'])
                low_price = safe_float_convert(row['LOW_PRICE'])
                last_price = safe_float_convert(row['LAST_PRICE'])
                close_price = safe_float_convert(row['CLOSE_PRICE'])
                avg_price = safe_float_convert(row['AVG_PRICE'])
                total_traded_qty = safe_int_convert(row['TTL_TRD_QNTY'])
                turnover_lacs = safe_float_convert(row['TURNOVER_LACS'])
                no_of_trades = safe_int_convert(row['NO_OF_TRADES'])
                deliverable_qty = safe_int_convert(row['DELIV_QTY'])
                delivery_percent = safe_float_convert(row['DELIV_PER'])
                
                # Insert data using ON CONFLICT to handle duplicates
                insert_sql = """
                INSERT INTO nse_stock_data (
                    symbol, series, trade_date, prev_close, open_price, high_price,
                    low_price, last_price, close_price, avg_price, total_traded_qty,
                    turnover_lacs, no_of_trades, deliverable_qty, delivery_percent
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, series, trade_date) DO UPDATE SET
                    prev_close = EXCLUDED.prev_close,
                    open_price = EXCLUDED.open_price,
                    high_price = EXCLUDED.high_price,
                    low_price = EXCLUDED.low_price,
                    last_price = EXCLUDED.last_price,
                    close_price = EXCLUDED.close_price,
                    avg_price = EXCLUDED.avg_price,
                    total_traded_qty = EXCLUDED.total_traded_qty,
                    turnover_lacs = EXCLUDED.turnover_lacs,
                    no_of_trades = EXCLUDED.no_of_trades,
                    deliverable_qty = EXCLUDED.deliverable_qty,
                    delivery_percent = EXCLUDED.delivery_percent
                """
                
                cursor.execute(insert_sql, (
                    symbol, series, trade_date, prev_close, open_price, high_price,
                    low_price, last_price, close_price, avg_price, total_traded_qty,
                    turnover_lacs, no_of_trades, deliverable_qty, delivery_percent
                ))
                
                records_imported += 1
                
            except Exception as e:
                logger.error(f"Error processing row {index}: {str(e)}")
                continue
        
        conn.commit()
        cursor.close()
        return True, records_imported
        
    except Exception as e:
        logger.error(f"❌ Error processing file {file_path}: {str(e)}")
        return False, 0

def get_csv_files():
    """Get list of all CSV files in NSE data directories"""
    csv_files = []
    base_dirs = [
        'NSE_January_2025_Data', 'NSE_February_2025_Data', 'NSE_March_2025_Data',
        'NSE_April_2025_Data', 'NSE_May_2025_Data', 'NSE_June_2025_Data',
        'NSE_July_2025_Data', 'NSE_August_2025_Data'
    ]
    
    for base_dir in base_dirs:
        if os.path.exists(base_dir):
            files = [f for f in os.listdir(base_dir) if f.endswith('.csv')]
            dir_files = [(base_dir, f) for f in files]
            csv_files.extend(dir_files)
            logger.info(f"📂 {base_dir}: {len(files)} CSV files")
    
    return csv_files

def get_current_record_count(conn):
    """Get current number of records in database"""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM nse_stock_data")
        count = cursor.fetchone()[0]
        cursor.close()
        return count
    except:
        return 0

def main():
    """Main import function"""
    logger.info("📊 NSE PostgreSQL Data Importer (Final)")
    logger.info("=" * 50)
    
    # Load configuration
    config = load_config()
    if not config:
        logger.error("❌ Could not load database configuration")
        return
    
    # Connect to database
    conn = connect_to_database(config)
    if not conn:
        logger.error("❌ Could not connect to database")
        return
    
    # Create table
    if not create_table_if_not_exists(conn):
        logger.error("❌ Could not create table")
        conn.close()
        return
    
    # Get CSV files
    csv_files = get_csv_files()
    if not csv_files:
        logger.error("❌ No CSV files found")
        conn.close()
        return
    
    logger.info(f"🎯 Total CSV files to import: {len(csv_files)}")
    
    # Get initial record count
    initial_count = get_current_record_count(conn)
    logger.info(f"📊 Current records in database: {initial_count}")
    
    # Confirm before proceeding
    response = input("\n🚀 Proceed with import? (yes/no): ").strip().lower()
    if response != 'yes':
        logger.info("❌ Import cancelled by user")
        conn.close()
        return
    
    # Process files
    total_imported = 0
    successful_files = 0
    failed_files = 0
    
    for directory, filename in csv_files:
        file_path = os.path.join(directory, filename)
        logger.info(f"   📄 Processing {filename}...")
        
        success, records = process_csv_file(file_path, conn)
        if success:
            logger.info(f"      ✅ Imported {records} records")
            total_imported += records
            successful_files += 1
        else:
            logger.error(f"      ❌ Failed to import")
            failed_files += 1
    
    # Final summary
    final_count = get_current_record_count(conn)
    logger.info("\n📊 Import Summary:")
    logger.info("=" * 30)
    logger.info(f"✅ Files imported: {successful_files}")
    logger.info(f"❌ Files with errors: {failed_files}")
    logger.info(f"📈 Total records imported: {total_imported}")
    logger.info(f"📊 Final database count: {final_count}")
    
    conn.close()
    logger.info("🎉 Import process completed!")

if __name__ == "__main__":
    main()
