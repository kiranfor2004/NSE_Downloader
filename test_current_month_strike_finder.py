#!/usr/bin/env python3
"""
Test Script: Current Month Strike Price Finder
==============================================

Purpose: Test script to get current month cost price from step03_compare_monthvspreviousmonth
         and find nearest 3 strike prices above and below (total 14 records: 7 strikes × 2 option types)
         from step04_fo_udiff_daily for testing purposes.

Process:
1. Get current month cost price for a symbol from step03_compare_monthvspreviousmonth
2. Use that price to find 7 nearest strikes from step04_fo_udiff_daily (3 above + 3 below + 1 nearest)
3. Get both PE and CE options for each strike (7 × 2 = 14 records)
4. Create test table and load the 14 records
5. Validate the results
"""

import pyodbc
import pandas as pd
from decimal import Decimal
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('test_current_month_strike_finder.log'),
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

def get_current_month_data(test_symbol=None):
    """
    Get current month cost price from step03_compare_monthvspreviousmonth
    """
    logger.info("Getting current month data from step03_compare_monthvspreviousmonth...")
    
    # Base query to get current month data with correct column names
    base_query = """
    SELECT 
        symbol,
        current_trade_date,
        current_close_price,
        previous_close_price,
        delivery_increase_abs,
        delivery_increase_pct
    FROM step03_compare_monthvspreviousmonth
    WHERE current_close_price IS NOT NULL
    """
    
    # Add symbol filter if specified
    if test_symbol:
        query = base_query + f" AND symbol = '{test_symbol}'"
        logger.info(f"Testing with specific symbol: {test_symbol}")
    else:
        query = base_query + " ORDER BY symbol"
        logger.info("Getting first available symbol for testing")
    
    with get_connection() as conn:
        df = pd.read_sql(query, conn)
    
    if df.empty:
        logger.error("No current month data found")
        return None
    
    # Use the first record for testing
    record = df.iloc[0]
    logger.info(f"Found current month data:")
    logger.info(f"   Symbol: {record['symbol']}")
    logger.info(f"   Date: {record['current_trade_date']}")
    logger.info(f"   Current Close Price: Rs.{record['current_close_price']:.2f}")
    logger.info(f"   Delivery Change: {record['delivery_increase_abs']} ({record['delivery_increase_pct']:.2f}%)")
    
    return record

def find_nearest_strikes_for_month(symbol, target_price, target_date):
    """
    Find nearest 7 strikes (3 above + 3 below + 1 nearest) for the given month
    Returns exactly 14 records (7 strikes × 2 option types)
    """
    logger.info(f"Finding nearest strikes for {symbol} with target price Rs.{target_price:.2f}")
    
    # Convert date format from YYYY-MM-DD to YYYYMMDD if needed
    if isinstance(target_date, str) and '-' in target_date:
        target_month = target_date[:7]  # YYYY-MM
        trade_date_pattern = target_date.replace('-', '')[:6] + '%'  # YYYYMM%
    else:
        target_month = str(target_date)[:7]
        trade_date_pattern = str(target_date).replace('-', '')[:6] + '%'
    
    logger.info(f"Looking for F&O data in month: {target_month}")
    
    # Get all available strikes for this symbol in the target month with ALL columns
    strikes_query = """
    SELECT * 
    FROM step04_fo_udiff_daily 
    WHERE symbol = ?
    AND trade_date LIKE ?
    AND strike_price IS NOT NULL
    AND option_type IN ('PE', 'CE')
    ORDER BY trade_date DESC, strike_price
    """
    
    with get_connection() as conn:
        strikes_df = pd.read_sql(strikes_query, conn, params=[symbol, trade_date_pattern])
    
    if strikes_df.empty:
        logger.error(f"No F&O data found for {symbol} in month {target_month}")
        return []
    
    # Get the latest trade date available
    latest_trade_date = strikes_df['trade_date'].max()
    latest_data = strikes_df[strikes_df['trade_date'] == latest_trade_date]
    
    logger.info(f"Found {len(latest_data)} F&O records on latest date: {latest_trade_date}")
    
    # Log available columns for debugging
    logger.info(f"Available columns in F&O data: {list(latest_data.columns)}")
    
    # Get unique strikes
    available_strikes = sorted(latest_data['strike_price'].unique())
    logger.info(f"Available strikes: {len(available_strikes)} total")
    logger.info(f"   Strike range: Rs.{min(available_strikes):.0f} - Rs.{max(available_strikes):.0f}")
    
    # Separate strikes above and below target
    strikes_above = [s for s in available_strikes if s > target_price]
    strikes_below = [s for s in available_strikes if s < target_price]
    strikes_exact = [s for s in available_strikes if s == target_price]
    
    logger.info(f"   Strikes above target: {len(strikes_above)}")
    logger.info(f"   Strikes below target: {len(strikes_below)}")
    logger.info(f"   Strikes at target: {len(strikes_exact)}")
    
    # Select strikes using the enhanced logic
    selected_strikes = []
    
    # Get 3 nearest strikes ABOVE target
    nearest_above = sorted(strikes_above)[:3]  # Closest above first
    selected_strikes.extend(nearest_above)
    logger.info(f"3 Strikes ABOVE target: {nearest_above}")
    
    # Get 3 nearest strikes BELOW target
    nearest_below = sorted(strikes_below, reverse=True)[:3]  # Closest below first
    selected_strikes.extend(nearest_below)
    logger.info(f"3 Strikes BELOW target: {nearest_below}")
    
    # Get 1 overall nearest strike (if not already selected)
    all_distances = [(s, abs(s - target_price)) for s in available_strikes]
    all_distances.sort(key=lambda x: x[1])
    nearest_overall = all_distances[0][0]
    
    if nearest_overall not in selected_strikes:
        selected_strikes.append(nearest_overall)
        logger.info(f"1 NEAREST overall: {nearest_overall} (added)")
    else:
        logger.info(f"1 NEAREST overall: {nearest_overall} (already included)")
        # Add the next nearest that's not already selected
        for strike, distance in all_distances[1:]:
            if strike not in selected_strikes:
                selected_strikes.append(strike)
                logger.info(f"1 Additional strike: {strike} (to make 7 total)")
                break
    
    logger.info(f"Final selected strikes ({len(selected_strikes)}): {sorted(selected_strikes)}")
    
    # Get F&O data for all selected strikes (both PE and CE)
    final_records = []
    
    for strike in selected_strikes:
        for option_type in ['PE', 'CE']:
            strike_data = latest_data[
                (latest_data['strike_price'] == strike) & 
                (latest_data['option_type'] == option_type)
            ]
            
            if not strike_data.empty:
                record = strike_data.iloc[0]
                
                # Determine strike position
                if strike > target_price:
                    position = 'above'
                elif strike < target_price:
                    position = 'below'
                else:
                    position = 'exact'
                
                # Create comprehensive record with all F&O columns plus our analysis fields
                record_dict = {
                    # Analysis fields
                    'symbol': symbol,
                    'target_price': float(target_price),
                    'target_date': target_date,
                    'strike_position': position,
                    'price_difference': float(strike) - float(target_price),
                    'percentage_difference': ((float(strike) - float(target_price)) / float(target_price)) * 100,
                }
                
                # Add ALL F&O columns from step04_fo_udiff_daily
                for col in record.index:
                    if col not in ['symbol']:  # Avoid duplicate symbol
                        value = record[col]
                        # Convert data types appropriately
                        if pd.isna(value):
                            record_dict[f'fo_{col}'] = None
                        elif isinstance(value, (int, float, Decimal)):
                            record_dict[f'fo_{col}'] = float(value) if value is not None else 0.0
                        else:
                            record_dict[f'fo_{col}'] = str(value) if value is not None else ''
                
                final_records.append(record_dict)
    
    pe_count = len([r for r in final_records if r.get('fo_option_type') == 'PE'])
    ce_count = len([r for r in final_records if r.get('fo_option_type') == 'CE'])
    
    logger.info(f"Generated {len(final_records)} records: {pe_count} PE + {ce_count} CE")
    
    # Validate we have exactly 14 records
    if len(final_records) == 14 and pe_count == 7 and ce_count == 7:
        logger.info("Perfect! Exactly 14 records as expected")
    else:
        logger.warning(f"Expected 14 records (7 PE + 7 CE), got {len(final_records)} ({pe_count} PE + {ce_count} CE)")
    
    return final_records

def create_test_table(sample_record=None):
    """
    Create test table to store the 14 F&O records with ALL columns from step04_fo_udiff_daily
    """
    logger.info("Creating test table: test_current_month_strikes...")
    
    # Drop existing table first
    drop_table_query = """
    IF OBJECT_ID('test_current_month_strikes', 'U') IS NOT NULL
        DROP TABLE test_current_month_strikes
    """
    
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(drop_table_query)
        conn.commit()
    
    # If we have a sample record, create table dynamically
    if sample_record:
        logger.info("Creating table dynamically based on data structure...")
        
        create_table_query = """
        CREATE TABLE test_current_month_strikes (
            id INT IDENTITY(1,1) PRIMARY KEY,
            symbol NVARCHAR(50),
            target_price DECIMAL(10,2),
            target_date DATE,
            strike_position NVARCHAR(10),
            price_difference DECIMAL(10,2),
            percentage_difference DECIMAL(8,4),
        """
        
        # Add columns for all F&O fields
        fo_columns = []
        for key, value in sample_record.items():
            if key.startswith('fo_'):
                col_name = key
                if isinstance(value, (int, float)):
                    if 'price' in key.lower() or 'value' in key.lower():
                        fo_columns.append(f"    {col_name} DECIMAL(18,4)")
                    else:
                        fo_columns.append(f"    {col_name} BIGINT")
                elif isinstance(value, str):
                    if len(value) <= 10:
                        fo_columns.append(f"    {col_name} NVARCHAR(20)")
                    else:
                        fo_columns.append(f"    {col_name} NVARCHAR(100)")
                else:
                    fo_columns.append(f"    {col_name} NVARCHAR(50)")
        
        create_table_query += ",\n".join(fo_columns)
        create_table_query += ",\n    created_at DATETIME DEFAULT GETDATE()\n)"
        
    else:
        # Fallback to basic table structure
        create_table_query = """
        CREATE TABLE test_current_month_strikes (
            id INT IDENTITY(1,1) PRIMARY KEY,
            symbol NVARCHAR(50),
            target_price DECIMAL(10,2),
            target_date DATE,
            strike_position NVARCHAR(10),
            price_difference DECIMAL(10,2),
            percentage_difference DECIMAL(8,4),
            fo_trade_date NVARCHAR(8),
            fo_strike_price DECIMAL(10,2),
            fo_option_type NVARCHAR(2),
            fo_close_price DECIMAL(10,2),
            fo_open_interest BIGINT,
            fo_contracts_traded BIGINT,
            fo_expiry_date NVARCHAR(8),
            created_at DATETIME DEFAULT GETDATE()
        )
        """
    
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
    
    logger.info("Test table created successfully")

def save_test_records(records):
    """
    Save test records to the test table with dynamic column handling
    """
    if not records:
        logger.warning("No records to save")
        return
    
    logger.info(f"Saving {len(records)} test records with all F&O columns...")
    
    # Get all column names from the first record
    if records:
        sample_record = records[0]
        columns = list(sample_record.keys())
        
        # Create placeholders for the INSERT statement
        placeholders = ", ".join(["?" for _ in columns])
        column_names = ", ".join(columns)
        
        insert_query = f"""
        INSERT INTO test_current_month_strikes ({column_names})
        VALUES ({placeholders})
        """
        
        logger.info(f"Inserting {len(columns)} columns per record")
        
        with get_connection() as conn:
            cursor = conn.cursor()
            for record in records:
                values = [record[col] for col in columns]
                cursor.execute(insert_query, values)
            conn.commit()
        
        logger.info("Test records saved successfully")
    else:
        logger.warning("No records to save")

def validate_test_results():
    """
    Validate the test results from the database
    """
    logger.info("Validating test results...")
    
    validation_query = """
    SELECT 
        symbol,
        target_price,
        COUNT(*) as total_records,
        COUNT(CASE WHEN fo_option_type = 'PE' THEN 1 END) as pe_count,
        COUNT(CASE WHEN fo_option_type = 'CE' THEN 1 END) as ce_count,
        COUNT(DISTINCT fo_strike_price) as unique_strikes,
        MIN(fo_strike_price) as min_strike,
        MAX(fo_strike_price) as max_strike,
        AVG(ABS(percentage_difference)) as avg_percentage_diff
    FROM test_current_month_strikes
    GROUP BY symbol, target_price
    """
    
    with get_connection() as conn:
        validation_df = pd.read_sql(validation_query, conn)
    
    if validation_df.empty:
        logger.error("No test data found in database")
        return False
    
    result = validation_df.iloc[0]
    
    logger.info("Validation Results:")
    logger.info(f"   Symbol: {result['symbol']}")
    logger.info(f"   Target Price: Rs.{result['target_price']:.2f}")
    logger.info(f"   Total Records: {result['total_records']}")
    logger.info(f"   PE Count: {result['pe_count']}")
    logger.info(f"   CE Count: {result['ce_count']}")
    logger.info(f"   Unique Strikes: {result['unique_strikes']}")
    logger.info(f"   Strike Range: Rs.{result['min_strike']:.0f} - Rs.{result['max_strike']:.0f}")
    logger.info(f"   Avg % Difference: {result['avg_percentage_diff']:.2f}%")
    
    # Check if results are as expected
    success = True
    
    if result['total_records'] != 14:
        logger.error(f"Expected 14 total records, got {result['total_records']}")
        success = False
    
    if result['pe_count'] != 7:
        logger.error(f"Expected 7 PE records, got {result['pe_count']}")
        success = False
    
    if result['ce_count'] != 7:
        logger.error(f"Expected 7 CE records, got {result['ce_count']}")
        success = False
    
    if result['unique_strikes'] != 7:
        logger.error(f"Expected 7 unique strikes, got {result['unique_strikes']}")
        success = False
    
    if success:
        logger.info("Perfect! All validation checks passed")
        
        # Show detailed breakdown
        detail_query = """
        SELECT 
            fo_strike_price as strike_price,
            strike_position,
            fo_option_type as option_type,
            fo_close_price as close_price,
            percentage_difference,
            fo_open_interest as open_interest,
            fo_contracts_traded as contracts_traded
        FROM test_current_month_strikes
        ORDER BY fo_strike_price, fo_option_type
        """
        
        with get_connection() as conn:
            detail_df = pd.read_sql(detail_query, conn)
        
        logger.info("Detailed Strike Breakdown:")
        print(detail_df.to_string(index=False))
        
    else:
        logger.error("Validation failed - check the data")
    
    return success

def main():
    """
    Main test execution function
    """
    logger.info("Starting Current Month Strike Finder Test")
    
    try:
        # Step 1: Get current month data from step03
        current_month_data = get_current_month_data(test_symbol='ABB')  # You can change this symbol
        
        if current_month_data is None:
            logger.error("Failed to get current month data")
            return
        
        symbol = current_month_data['symbol']
        target_price = current_month_data['current_close_price']
        target_date = current_month_data['current_trade_date']
        
        logger.info(f"Test Parameters:")
        logger.info(f"   Symbol: {symbol}")
        logger.info(f"   Target Price: Rs.{target_price:.2f}")
        logger.info(f"   Target Date: {target_date}")
        
        # Step 2: Find nearest strikes from step04
        test_records = find_nearest_strikes_for_month(symbol, target_price, target_date)
        
        if not test_records:
            logger.error("Failed to find F&O data")
            return
        
        # Step 3: Create test table based on actual data structure
        sample_record = test_records[0] if test_records else None
        create_test_table(sample_record)
        
        # Step 4: Save test records
        save_test_records(test_records)
        
        # Step 5: Validate results
        validation_success = validate_test_results()
        
        if validation_success:
            logger.info("Test completed successfully! Ready for complete analysis.")
            logger.info("You can now run the complete analysis on all symbols.")
        else:
            logger.error("Test failed. Please check the logic before proceeding.")
        
    except Exception as e:
        logger.error(f"Test failed with error: {str(e)}")
        raise

if __name__ == "__main__":
    main()