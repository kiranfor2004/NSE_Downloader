#!/usr/bin/env python3
"""
Step 4 Data Correction: Monthly F&O Data Loader (Feb-Aug 2025)
============================================================

Purpose: Delete existing February 2025 data and reload complete F&O data
         from February 2025 to August 2025, month by month.

Process:
1. Delete all existing data from step04_fo_udiff_daily
2. Download F&O data for each month (Feb-Aug 2025)
3. Load data month by month into step04_fo_udiff_daily
4. Validate data integrity after each month

Months to load:
- February 2025 (202502)
- March 2025 (202503)  
- April 2025 (202504)
- May 2025 (202505)
- June 2025 (202506)
- July 2025 (202507)
- August 2025 (202508)
"""

import requests
import pandas as pd
import pyodbc
import json
import time
from datetime import datetime, timedelta
import logging
import os
import io
import zipfile

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('step04_monthly_fo_loader.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Database connection
def get_connection():
    """Get database connection"""
    connection_string = (
        "Driver={ODBC Driver 17 for SQL Server};"
        "Server=SRIKIRANREDDY\\SQLEXPRESS;"
        "Database=master;"
        "Trusted_Connection=yes;"
    )
    return pyodbc.connect(connection_string)

def clear_existing_data():
    """
    Delete all existing data from step04_fo_udiff_daily table
    """
    logger.info("Clearing all existing data from step04_fo_udiff_daily...")
    
    with get_connection() as conn:
        cursor = conn.cursor()
        
        # Get current record count
        cursor.execute("SELECT COUNT(*) FROM step04_fo_udiff_daily")
        current_count = cursor.fetchone()[0]
        logger.info(f"Current records in table: {current_count:,}")
        
        # Delete all records
        cursor.execute("DELETE FROM step04_fo_udiff_daily")
        conn.commit()
        
        # Verify deletion
        cursor.execute("SELECT COUNT(*) FROM step04_fo_udiff_daily")
        remaining_count = cursor.fetchone()[0]
        
        logger.info(f"Records deleted: {current_count:,}")
        logger.info(f"Remaining records: {remaining_count}")
        
        if remaining_count == 0:
            logger.info("All existing data cleared successfully")
        else:
            logger.error(f"Data clearing failed - {remaining_count} records remain")
            raise Exception("Failed to clear existing data")

def get_month_dates(year, month):
    """
    Get all trading dates for a specific month
    """
    logger.info(f"Generating dates for {year}-{month:02d}")
    
    # Generate all dates in the month
    start_date = datetime(year, month, 1)
    if month == 12:
        end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
    else:
        end_date = datetime(year, month + 1, 1) - timedelta(days=1)
    
    dates = []
    current_date = start_date
    while current_date <= end_date:
        # Skip weekends (Monday=0, Sunday=6)
        if current_date.weekday() < 5:  # Monday to Friday
            date_str = current_date.strftime('%d-%m-%Y')
            dates.append(date_str)
        current_date += timedelta(days=1)
    
    logger.info(f"Generated {len(dates)} potential trading dates for {year}-{month:02d}")
    return dates

def download_fo_data_for_date(date_str):
    """
    Download F&O data for a specific date using correct UDiFF format
    """
    logger.info(f"Downloading F&O data for {date_str}")
    
    # Convert date format: DD-MM-YYYY to DDMMYYYY
    date_formatted = date_str.replace('-', '')
    
    # Use correct UDiFF URL format
    url = f"https://archives.nseindia.com/products/content/derivatives/equities/udiff_{date_formatted}.zip"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Referer': 'https://www.nseindia.com/'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code == 200:
            # Handle ZIP file
            try:
                import zipfile
                with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
                    # Get the CSV file from zip
                    csv_files = [f for f in zip_file.namelist() if f.endswith('.csv')]
                    if csv_files:
                        csv_filename = csv_files[0]
                        with zip_file.open(csv_filename) as csv_file:
                            df = pd.read_csv(csv_file)
                            if not df.empty and len(df.columns) >= 12:
                                logger.info(f"Downloaded {len(df)} records for {date_str}")
                                return df
                            else:
                                logger.warning(f"Empty or invalid data for {date_str}")
                                return None
                    else:
                        logger.warning(f"No CSV file found in ZIP for {date_str}")
                        return None
            except Exception as e:
                logger.warning(f"ZIP processing failed for {date_str}: {e}")
                return None
        else:
            logger.warning(f"HTTP {response.status_code} for {date_str}")
            return None
    except Exception as e:
        logger.warning(f"Request failed for {date_str}: {e}")
        return None

def process_fo_data(df, trade_date):
    """
    Process and clean F&O data using correct UDiFF column mapping
    """
    if df is None or df.empty:
        return None
    
    # UDiFF column mapping (based on your existing working downloader)
    column_mapping = {
        'INSTRUMENT': 'instrument',
        'SYMBOL': 'symbol',
        'EXPIRY_DT': 'expiry_date',
        'STRIKE_PR': 'strike_price',
        'OPTION_TYP': 'option_type',
        'OPEN': 'open_price',
        'HIGH': 'high_price',
        'LOW': 'low_price',
        'CLOSE': 'close_price',
        'SETTLE_PR': 'settle_price',
        'CONTRACTS': 'contracts_traded',
        'VAL_INLAKH': 'value_in_lakh',
        'OPEN_INT': 'open_interest',
        'CHG_IN_OI': 'change_in_oi',
        'UNDERLYING': 'underlying'
    }
    
    # Rename columns if they exist
    for old_col, new_col in column_mapping.items():
        if old_col in df.columns:
            df = df.rename(columns={old_col: new_col})
    
    # Add trade_date
    df['trade_date'] = trade_date.replace('-', '')
    
    # Filter for F&O instruments only
    if 'instrument' in df.columns:
        df = df[df['instrument'].isin(['FUTSTK', 'OPTSTK', 'FUTIDX', 'OPTIDX'])]
    
    # Clean numeric columns
    numeric_columns = ['strike_price', 'open_price', 'high_price', 'low_price', 
                      'close_price', 'settle_price', 'contracts_traded', 
                      'value_in_lakh', 'open_interest', 'change_in_oi']
    
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Remove rows with missing essential data (symbol is required)
    if 'symbol' in df.columns:
        df = df.dropna(subset=['symbol'])
    else:
        logger.warning("No 'symbol' column found in data")
        return None
    
    logger.info(f"Processed data: {len(df)} records for {trade_date}")
    return df

def save_fo_data_to_db(df, trade_date):
    """
    Save F&O data to step04_fo_udiff_daily table
    """
    if df is None or df.empty:
        logger.warning(f"No data to save for {trade_date}")
        return 0
    
    logger.info(f"Saving {len(df)} records to database for {trade_date}")
    
    # Prepare data for insertion
    records_saved = 0
    
    with get_connection() as conn:
        cursor = conn.cursor()
        
        insert_query = """
        INSERT INTO step04_fo_udiff_daily (
            instrument, symbol, expiry_date, strike_price, option_type,
            open_price, high_price, low_price, close_price, settle_price,
            contracts_traded, value_in_lakh, open_interest, change_in_oi,
            trade_date, underlying
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        for _, row in df.iterrows():
            try:
                cursor.execute(insert_query, (
                    str(row.get('instrument', '')),
                    str(row.get('symbol', '')),
                    str(row.get('expiry_date', '')),
                    float(row.get('strike_price', 0)) if pd.notna(row.get('strike_price')) else None,
                    str(row.get('option_type', '')),
                    float(row.get('open_price', 0)) if pd.notna(row.get('open_price')) else None,
                    float(row.get('high_price', 0)) if pd.notna(row.get('high_price')) else None,
                    float(row.get('low_price', 0)) if pd.notna(row.get('low_price')) else None,
                    float(row.get('close_price', 0)) if pd.notna(row.get('close_price')) else None,
                    float(row.get('settle_price', 0)) if pd.notna(row.get('settle_price')) else None,
                    int(row.get('contracts_traded', 0)) if pd.notna(row.get('contracts_traded')) else None,
                    float(row.get('value_in_lakh', 0)) if pd.notna(row.get('value_in_lakh')) else None,
                    int(row.get('open_interest', 0)) if pd.notna(row.get('open_interest')) else None,
                    int(row.get('change_in_oi', 0)) if pd.notna(row.get('change_in_oi')) else None,
                    str(row.get('trade_date', '')),
                    str(row.get('underlying', ''))
                ))
                records_saved += 1
            except Exception as e:
                logger.warning(f"Failed to insert record: {e}")
                continue
        
        conn.commit()
        
    logger.info(f"Saved {records_saved} records for {trade_date}")
    return records_saved

def load_month_data(year, month):
    """
    Load all F&O data for a specific month
    """
    month_name = {
        2: 'February', 3: 'March', 4: 'April', 5: 'May',
        6: 'June', 7: 'July', 8: 'August'
    }[month]
    
    logger.info(f"="*60)
    logger.info(f"LOADING {month_name.upper()} {year} DATA")
    logger.info(f"="*60)
    
    dates = get_month_dates(year, month)
    total_records = 0
    successful_dates = 0
    
    for i, date_str in enumerate(dates, 1):
        logger.info(f"Processing date {i}/{len(dates)}: {date_str}")
        
        # Download data
        raw_data = download_fo_data_for_date(date_str)
        
        if raw_data is not None:
            # Process data
            processed_data = process_fo_data(raw_data, date_str)
            
            # Save to database
            records_saved = save_fo_data_to_db(processed_data, date_str)
            total_records += records_saved
            successful_dates += 1
        
        # Small delay to be respectful to the server
        time.sleep(1)
    
    logger.info(f"{month_name} {year} COMPLETED")
    logger.info(f"  Successful dates: {successful_dates}/{len(dates)}")
    logger.info(f"  Total records: {total_records:,}")
    
    return total_records

def validate_month_data(year, month):
    """
    Validate loaded data for a month
    """
    month_str = f"{year}{month:02d}"
    
    with get_connection() as conn:
        query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT symbol) as unique_symbols,
            COUNT(DISTINCT trade_date) as unique_dates,
            MIN(trade_date) as first_date,
            MAX(trade_date) as last_date
        FROM step04_fo_udiff_daily
        WHERE LEFT(trade_date, 6) = ?
        """
        
        df = pd.read_sql(query, conn, params=[month_str])
        
        if not df.empty:
            row = df.iloc[0]
            logger.info(f"Validation for {year}-{month:02d}:")
            logger.info(f"  Records: {row['total_records']:,}")
            logger.info(f"  Symbols: {row['unique_symbols']}")
            logger.info(f"  Dates: {row['unique_dates']}")
            logger.info(f"  Range: {row['first_date']} to {row['last_date']}")
            return row['total_records']
        else:
            logger.warning(f"No data found for {year}-{month:02d}")
            return 0

def main():
    """
    Main execution function
    """
    logger.info("="*80)
    logger.info("STEP 4 DATA CORRECTION: MONTHLY F&O LOADER (FEB-AUG 2025)")
    logger.info("="*80)
    
    try:
        # Step 1: Clear existing data
        clear_existing_data()
        
        # Step 2: Load data month by month
        months_to_load = [
            (2025, 2, 'February'),
            (2025, 3, 'March'), 
            (2025, 4, 'April'),
            (2025, 5, 'May'),
            (2025, 6, 'June'),
            (2025, 7, 'July'),
            (2025, 8, 'August')
        ]
        
        total_all_records = 0
        
        for year, month, month_name in months_to_load:
            try:
                records = load_month_data(year, month)
                validate_month_data(year, month)
                total_all_records += records
                logger.info(f"{month_name} {year} completed successfully")
            except Exception as e:
                logger.error(f"Failed to load {month_name} {year}: {e}")
                continue
        
        # Final validation
        logger.info("="*80)
        logger.info("FINAL SUMMARY")
        logger.info("="*80)
        
        with get_connection() as conn:
            summary_query = """
            SELECT 
                LEFT(trade_date, 6) as year_month,
                COUNT(*) as record_count,
                COUNT(DISTINCT symbol) as unique_symbols,
                MIN(trade_date) as first_date,
                MAX(trade_date) as last_date
            FROM step04_fo_udiff_daily
            GROUP BY LEFT(trade_date, 6)
            ORDER BY LEFT(trade_date, 6)
            """
            
            summary_df = pd.read_sql(summary_query, conn)
            
            if not summary_df.empty:
                print("\nMONTH-WISE LOADED DATA:")
                for _, row in summary_df.iterrows():
                    year_month = row['year_month']
                    month_name = {
                        '202502': 'February 2025',
                        '202503': 'March 2025',
                        '202504': 'April 2025', 
                        '202505': 'May 2025',
                        '202506': 'June 2025',
                        '202507': 'July 2025',
                        '202508': 'August 2025'
                    }.get(year_month, year_month)
                    
                    print(f"{month_name}: {row['record_count']:,} records | {row['unique_symbols']} symbols | {row['first_date']} to {row['last_date']}")
                
                total_final = summary_df['record_count'].sum()
                print(f"\nTOTAL RECORDS LOADED: {total_final:,}")
                logger.info(f"Step 4 data correction completed successfully!")
                logger.info(f"  Total records: {total_final:,}")
                logger.info(f"  Months loaded: {len(summary_df)}")
            else:
                logger.error("No data loaded - process failed")
        
    except Exception as e:
        logger.error(f"Step 4 data correction failed: {e}")
        raise

if __name__ == "__main__":
    main()