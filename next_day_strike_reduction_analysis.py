#!/usr/bin/env python3
"""
Next Day Strike Price Reduction Analysis
========================================

Purpose: Analyze strike price reductions from current trade date to the next available trade date.
         For example, if trade date is March 3rd, 2025, check March 4th, 2025 onwards for 50% reductions.

Process:
1. Get data from test_current_month_strikes table (current trade date)
2. Find the next available trade date(s) in step04_fo_udiff_daily for the same symbols
3. Compare strike prices between current and next trade dates
4. Identify records with 50% or more reduction in strike prices
5. Store qualifying records in: next_day_strike_reduction_analysis

Logic:
- Current trade date: From test_current_month_strikes
- Next trade date: Next available date in step04_fo_udiff_daily for same symbol
- Calculate: (Previous Strike - Current Strike) / Previous Strike * 100 >= 50%
"""

import pyodbc
import pandas as pd
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('next_day_strike_reduction_analysis.log'),
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

def get_current_test_data():
    """
    Get current trade date data from test_current_month_strikes table
    """
    logger.info("Reading current trade date data from test_current_month_strikes...")
    
    query = """
    SELECT DISTINCT
        symbol,
        fo_trade_date as current_trade_date,
        target_price,
        target_date
    FROM test_current_month_strikes
    ORDER BY symbol, fo_trade_date
    """
    
    with get_connection() as conn:
        df = pd.read_sql(query, conn)
    
    logger.info(f"Found {len(df)} symbols with current trade dates")
    return df

def get_next_trade_dates(symbol, current_trade_date):
    """
    Find next available trade dates for a symbol after the current trade date
    """
    logger.info(f"Finding next trade dates for {symbol} after {current_trade_date}")
    
    # Convert current trade date to integer for comparison
    current_date_int = int(current_trade_date)
    
    query = """
    SELECT DISTINCT trade_date
    FROM step04_fo_udiff_daily
    WHERE symbol = ?
    AND CAST(trade_date AS INT) > ?
    ORDER BY trade_date
    """
    
    with get_connection() as conn:
        df = pd.read_sql(query, conn, params=[symbol, current_date_int])
    
    logger.info(f"Found {len(df)} future trade dates for {symbol}")
    return df['trade_date'].tolist() if not df.empty else []

def get_fo_data_for_date(symbol, trade_date):
    """
    Get F&O data for a specific symbol and trade date
    """
    query = """
    SELECT *
    FROM step04_fo_udiff_daily
    WHERE symbol = ?
    AND trade_date = ?
    AND strike_price IS NOT NULL
    AND option_type IN ('PE', 'CE')
    ORDER BY strike_price, option_type
    """
    
    with get_connection() as conn:
        df = pd.read_sql(query, conn, params=[symbol, trade_date])
    
    return df

def analyze_next_day_reduction(current_data):
    """
    Analyze strike price reductions from current to next trade dates
    """
    logger.info("Analyzing next day strike price reductions...")
    
    reduction_records = []
    
    for idx, current_record in current_data.iterrows():
        symbol = current_record['symbol']
        current_trade_date = current_record['current_trade_date']
        target_price = current_record['target_price']
        
        logger.info(f"Processing {symbol} - Current trade date: {current_trade_date}")
        
        # Get current trade date F&O data
        current_fo_data = get_fo_data_for_date(symbol, current_trade_date)
        
        if current_fo_data.empty:
            logger.warning(f"No current F&O data found for {symbol} on {current_trade_date}")
            continue
        
        # Get next available trade dates
        next_trade_dates = get_next_trade_dates(symbol, current_trade_date)
        
        if not next_trade_dates:
            logger.warning(f"No future trade dates found for {symbol} after {current_trade_date}")
            continue
        
        # Check each next trade date
        for next_trade_date in next_trade_dates[:3]:  # Check up to 3 next dates
            logger.info(f"  Comparing {current_trade_date} vs {next_trade_date}")
            
            # Get next trade date F&O data
            next_fo_data = get_fo_data_for_date(symbol, next_trade_date)
            
            if next_fo_data.empty:
                logger.warning(f"    No F&O data found for {symbol} on {next_trade_date}")
                continue
            
            # Compare strike prices between current and next dates
            current_strikes = {}
            for _, row in current_fo_data.iterrows():
                key = (row['strike_price'], row['option_type'])
                current_strikes[key] = row
            
            # Check for reductions in next date data
            for _, next_row in next_fo_data.iterrows():
                next_strike = next_row['strike_price']
                next_option_type = next_row['option_type']
                
                # Find matching option types in current data
                for (current_strike, current_option_type), current_row in current_strikes.items():
                    if current_option_type == next_option_type:
                        # Calculate strike price reduction
                        if current_strike > 0:
                            reduction_amount = current_strike - next_strike
                            reduction_percentage = (reduction_amount / current_strike) * 100
                            
                            # Check if reduction is 50% or more
                            if reduction_percentage >= 50:
                                logger.info(f"    *** 50%+ REDUCTION FOUND ***")
                                logger.info(f"    {current_option_type}: {current_strike} -> {next_strike}")
                                logger.info(f"    Reduction: {reduction_amount:.2f} ({reduction_percentage:.2f}%)")
                                
                                # Create reduction record
                                reduction_record = {
                                    'symbol': symbol,
                                    'target_price': target_price,
                                    'current_trade_date': current_trade_date,
                                    'next_trade_date': next_trade_date,
                                    'option_type': next_option_type,
                                    
                                    # Current date data
                                    'current_strike_price': current_strike,
                                    'current_close_price': current_row['close_price'],
                                    'current_open_interest': current_row['open_interest'],
                                    'current_contracts_traded': current_row['contracts_traded'],
                                    
                                    # Next date data
                                    'next_strike_price': next_strike,
                                    'next_close_price': next_row['close_price'],
                                    'next_open_interest': next_row['open_interest'],
                                    'next_contracts_traded': next_row['contracts_traded'],
                                    'next_expiry_date': next_row['expiry_date'],
                                    
                                    # Reduction analysis
                                    'strike_reduction_amount': reduction_amount,
                                    'strike_reduction_percentage': reduction_percentage,
                                    'analysis_type': 'next_day_50_percent_reduction',
                                    'days_difference': 1,  # Will calculate properly if needed
                                }
                                
                                # Add all other fields from next_row
                                for col in next_row.index:
                                    if col not in reduction_record:
                                        reduction_record[f'next_{col}'] = next_row[col]
                                
                                reduction_records.append(reduction_record)
    
    logger.info(f"Found {len(reduction_records)} records with next day 50%+ strike reductions")
    return reduction_records

def create_next_day_reduction_table():
    """
    Create table for next day strike reduction analysis
    """
    logger.info("Creating next_day_strike_reduction_analysis table...")
    
    create_table_query = """
    IF OBJECT_ID('next_day_strike_reduction_analysis', 'U') IS NOT NULL
        DROP TABLE next_day_strike_reduction_analysis
    
    CREATE TABLE next_day_strike_reduction_analysis (
        id INT IDENTITY(1,1) PRIMARY KEY,
        
        -- Symbol and date info
        symbol NVARCHAR(50),
        target_price DECIMAL(10,2),
        current_trade_date NVARCHAR(8),
        next_trade_date NVARCHAR(8),
        option_type NVARCHAR(2),
        
        -- Current date F&O data
        current_strike_price DECIMAL(10,2),
        current_close_price DECIMAL(10,2),
        current_open_interest BIGINT,
        current_contracts_traded BIGINT,
        
        -- Next date F&O data
        next_strike_price DECIMAL(10,2),
        next_close_price DECIMAL(10,2),
        next_open_interest BIGINT,
        next_contracts_traded BIGINT,
        next_expiry_date NVARCHAR(8),
        
        -- Reduction analysis
        strike_reduction_amount DECIMAL(10,2),
        strike_reduction_percentage DECIMAL(8,4),
        analysis_type NVARCHAR(50),
        days_difference INT,
        
        -- Additional next day fields
        next_id BIGINT,
        next_instrument NVARCHAR(50),
        next_open_price DECIMAL(10,2),
        next_high_price DECIMAL(10,2),
        next_low_price DECIMAL(10,2),
        next_settle_price DECIMAL(10,2),
        next_value_in_lakh DECIMAL(18,4),
        next_change_in_oi BIGINT,
        next_underlying NVARCHAR(50),
        
        created_at DATETIME DEFAULT GETDATE()
    )
    """
    
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
    
    logger.info("Next day strike reduction analysis table created successfully")

def save_reduction_records(reduction_records):
    """
    Save next day reduction records to the table
    """
    if not reduction_records:
        logger.warning("No reduction records to save")
        return
    
    logger.info(f"Saving {len(reduction_records)} next day reduction records...")
    
    insert_query = """
    INSERT INTO next_day_strike_reduction_analysis (
        symbol, target_price, current_trade_date, next_trade_date, option_type,
        current_strike_price, current_close_price, current_open_interest, current_contracts_traded,
        next_strike_price, next_close_price, next_open_interest, next_contracts_traded, next_expiry_date,
        strike_reduction_amount, strike_reduction_percentage, analysis_type, days_difference
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    with get_connection() as conn:
        cursor = conn.cursor()
        for record in reduction_records:
            cursor.execute(insert_query, (
                record.get('symbol', ''),
                record.get('target_price', 0),
                record.get('current_trade_date', ''),
                record.get('next_trade_date', ''),
                record.get('option_type', ''),
                record.get('current_strike_price', 0),
                record.get('current_close_price', 0),
                record.get('current_open_interest', 0),
                record.get('current_contracts_traded', 0),
                record.get('next_strike_price', 0),
                record.get('next_close_price', 0),
                record.get('next_open_interest', 0),
                record.get('next_contracts_traded', 0),
                record.get('next_expiry_date', ''),
                record.get('strike_reduction_amount', 0),
                record.get('strike_reduction_percentage', 0),
                record.get('analysis_type', ''),
                record.get('days_difference', 1)
            ))
        conn.commit()
    
    logger.info("Next day reduction records saved successfully")

def generate_reduction_summary():
    """
    Generate summary of next day strike reductions
    """
    logger.info("Generating next day reduction summary...")
    
    summary_query = """
    SELECT 
        symbol,
        COUNT(*) as reduction_count,
        AVG(strike_reduction_percentage) as avg_reduction_pct,
        MIN(strike_reduction_percentage) as min_reduction_pct,
        MAX(strike_reduction_percentage) as max_reduction_pct,
        COUNT(DISTINCT current_trade_date) as current_dates,
        COUNT(DISTINCT next_trade_date) as next_dates
    FROM next_day_strike_reduction_analysis
    GROUP BY symbol
    ORDER BY symbol
    """
    
    with get_connection() as conn:
        summary_df = pd.read_sql(summary_query, conn)
    
    if not summary_df.empty:
        print("\n" + "="*80)
        print("NEXT DAY STRIKE PRICE REDUCTION ANALYSIS SUMMARY")
        print("="*80)
        print(f"Symbols with next day reductions: {len(summary_df)}")
        print(f"Total reduction records: {summary_df['reduction_count'].sum()}")
        
        print("\nDetailed Summary:")
        print(summary_df.to_string(index=False))
        
        # Show individual records
        detail_query = """
        SELECT TOP 10
            symbol, current_trade_date, next_trade_date, option_type,
            current_strike_price, next_strike_price, strike_reduction_percentage
        FROM next_day_strike_reduction_analysis
        ORDER BY strike_reduction_percentage DESC
        """
        
        detail_df = pd.read_sql(detail_query, conn)
        print("\nTop 10 Reductions:")
        print(detail_df.to_string(index=False))
        print("="*80)
    else:
        print("\nNo next day strike price reductions (50%+) found.")

def main():
    """
    Main execution function
    """
    logger.info("Starting Next Day Strike Price Reduction Analysis")
    
    try:
        # Step 1: Get current test data
        current_data = get_current_test_data()
        
        if current_data.empty:
            logger.error("No current trade date data found")
            return
        
        # Step 2: Analyze next day reductions
        reduction_records = analyze_next_day_reduction(current_data)
        
        # Step 3: Create table
        create_next_day_reduction_table()
        
        # Step 4: Save records
        save_reduction_records(reduction_records)
        
        # Step 5: Generate summary
        generate_reduction_summary()
        
        if reduction_records:
            logger.info("Next day strike reduction analysis completed successfully!")
        else:
            logger.info("Analysis completed - no 50%+ next day strike reductions found")
        
    except Exception as e:
        logger.error(f"Error in next day reduction analysis: {str(e)}")
        raise

if __name__ == "__main__":
    main()