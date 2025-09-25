#!/usr/bin/env python3
"""
Strike Price Analysis - Alternative Approach
===========================================

Purpose: Since we have single trade date data, analyze strike price reductions relative to target price
         and find options that are significantly below the target price (50% or more discount)

Process:
1. Read data from test_current_month_strikes table
2. Calculate how much each strike price is below the target price
3. Find strikes that are 50% or more below the target price
4. Store qualifying records in a new table: significant_strike_discounts

Logic:
- Compare each strike price against the target price (current close price)
- Calculate percentage discount from target price
- Flag records where strike price is 50%+ below target price
"""

import pyodbc
import pandas as pd
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('significant_strike_discounts_analysis.log'),
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

def get_test_data():
    """
    Get all data from test_current_month_strikes table
    """
    logger.info("Reading data from test_current_month_strikes table...")
    
    query = """
    SELECT *
    FROM test_current_month_strikes
    ORDER BY symbol, fo_strike_price
    """
    
    with get_connection() as conn:
        df = pd.read_sql(query, conn)
    
    logger.info(f"Retrieved {len(df)} records from test_current_month_strikes")
    return df

def analyze_significant_strike_discounts(df):
    """
    Analyze strikes that are significantly below target price (50%+ discount)
    """
    logger.info("Analyzing significant strike price discounts...")
    
    discount_records = []
    
    # Group by symbol to analyze each symbol separately
    for symbol, symbol_group in df.groupby('symbol'):
        target_price = symbol_group['target_price'].iloc[0]
        logger.info(f"Analyzing {symbol} - Target price: Rs.{target_price:.2f}")
        
        # Analyze each record
        for idx, record in symbol_group.iterrows():
            strike_price = record['fo_strike_price']
            
            # Calculate discount from target price
            if target_price > 0:
                discount_amount = target_price - strike_price
                discount_percentage = (discount_amount / target_price) * 100
                
                logger.info(f"  Strike {strike_price}: Discount {discount_amount:.2f} ({discount_percentage:.2f}%)")
                
                # Check if discount is 50% or more (strike is 50%+ below target)
                if discount_percentage >= 50:
                    logger.info(f"    *** SIGNIFICANT DISCOUNT FOUND: {discount_percentage:.2f}% ***")
                    
                    # Create discount analysis record
                    discount_record = record.copy()
                    discount_record['discount_from_target_amount'] = discount_amount
                    discount_record['discount_from_target_percentage'] = discount_percentage
                    discount_record['analysis_type'] = 'significant_discount_50_percent'
                    discount_record['analysis_description'] = f'Strike price is {discount_percentage:.2f}% below target price'
                    
                    discount_records.append(discount_record)
                
                # Also check for extreme discounts (70%+)
                elif discount_percentage >= 70:
                    logger.info(f"    *** EXTREME DISCOUNT FOUND: {discount_percentage:.2f}% ***")
                    
                    discount_record = record.copy()
                    discount_record['discount_from_target_amount'] = discount_amount
                    discount_record['discount_from_target_percentage'] = discount_percentage
                    discount_record['analysis_type'] = 'extreme_discount_70_percent'
                    discount_record['analysis_description'] = f'Strike price is {discount_percentage:.2f}% below target price'
                    
                    discount_records.append(discount_record)
    
    logger.info(f"Found {len(discount_records)} records with significant discounts")
    return discount_records

def create_discount_analysis_table():
    """
    Create table to store significant strike discount analysis
    """
    logger.info("Creating significant_strike_discounts table...")
    
    create_table_query = """
    IF OBJECT_ID('significant_strike_discounts', 'U') IS NOT NULL
        DROP TABLE significant_strike_discounts
    
    CREATE TABLE significant_strike_discounts (
        id INT IDENTITY(1,1) PRIMARY KEY,
        
        -- Original analysis fields
        original_id INT,
        symbol NVARCHAR(50),
        target_price DECIMAL(10,2),
        target_date DATE,
        strike_position NVARCHAR(10),
        price_difference DECIMAL(10,2),
        percentage_difference DECIMAL(8,4),
        
        -- Key F&O fields
        fo_trade_date NVARCHAR(8),
        fo_symbol NVARCHAR(50),
        fo_strike_price DECIMAL(10,2),
        fo_option_type NVARCHAR(2),
        fo_close_price DECIMAL(10,2),
        fo_open_interest BIGINT,
        fo_contracts_traded BIGINT,
        fo_expiry_date NVARCHAR(8),
        
        -- Discount analysis fields
        discount_from_target_amount DECIMAL(10,2),
        discount_from_target_percentage DECIMAL(8,4),
        analysis_type NVARCHAR(50),
        analysis_description NVARCHAR(200),
        
        created_at DATETIME DEFAULT GETDATE()
    )
    """
    
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
    
    logger.info("Significant strike discounts table created successfully")

def save_discount_records(discount_records):
    """
    Save discount analysis records to the new table
    """
    if not discount_records:
        logger.warning("No discount records to save")
        return
    
    logger.info(f"Saving {len(discount_records)} discount analysis records...")
    
    insert_query = """
    INSERT INTO significant_strike_discounts (
        original_id, symbol, target_price, target_date, strike_position,
        price_difference, percentage_difference, fo_trade_date, fo_symbol,
        fo_strike_price, fo_option_type, fo_close_price, fo_open_interest,
        fo_contracts_traded, fo_expiry_date, discount_from_target_amount,
        discount_from_target_percentage, analysis_type, analysis_description
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    with get_connection() as conn:
        cursor = conn.cursor()
        for record in discount_records:
            cursor.execute(insert_query, (
                record.get('id', 0),
                record.get('symbol', ''),
                record.get('target_price', 0),
                record.get('target_date', None),
                record.get('strike_position', ''),
                record.get('price_difference', 0),
                record.get('percentage_difference', 0),
                record.get('fo_trade_date', ''),
                record.get('fo_symbol', ''),
                record.get('fo_strike_price', 0),
                record.get('fo_option_type', ''),
                record.get('fo_close_price', 0),
                record.get('fo_open_interest', 0),
                record.get('fo_contracts_traded', 0),
                record.get('fo_expiry_date', ''),
                record.get('discount_from_target_amount', 0),
                record.get('discount_from_target_percentage', 0),
                record.get('analysis_type', ''),
                record.get('analysis_description', '')
            ))
        conn.commit()
    
    logger.info("Discount analysis records saved successfully")

def generate_discount_summary():
    """
    Generate summary report of significant strike discounts
    """
    logger.info("Generating discount analysis summary...")
    
    summary_query = """
    SELECT 
        symbol,
        analysis_type,
        COUNT(*) as record_count,
        AVG(discount_from_target_percentage) as avg_discount_pct,
        MIN(discount_from_target_percentage) as min_discount_pct,
        MAX(discount_from_target_percentage) as max_discount_pct,
        MIN(fo_strike_price) as lowest_strike,
        MAX(fo_strike_price) as highest_strike,
        AVG(target_price) as target_price
    FROM significant_strike_discounts
    GROUP BY symbol, analysis_type
    ORDER BY symbol, analysis_type
    """
    
    with get_connection() as conn:
        summary_df = pd.read_sql(summary_query, conn)
    
    if not summary_df.empty:
        print("\n" + "="*80)
        print("SIGNIFICANT STRIKE PRICE DISCOUNTS ANALYSIS SUMMARY")
        print("="*80)
        print(f"Total symbols with significant discounts: {summary_df['symbol'].nunique()}")
        print(f"Total discount records: {summary_df['record_count'].sum()}")
        
        print("\nDetailed Analysis by Symbol:")
        print(summary_df.to_string(index=False))
        
        # Show individual records
        detail_query = """
        SELECT 
            symbol, fo_option_type, fo_strike_price, target_price,
            discount_from_target_percentage, analysis_type, fo_close_price
        FROM significant_strike_discounts
        ORDER BY symbol, discount_from_target_percentage DESC
        """
        
        detail_df = pd.read_sql(detail_query, conn)
        print("\nDetailed Records:")
        print(detail_df.to_string(index=False))
        print("="*80)
    else:
        print("\nNo significant strike price discounts found in the analysis.")

def main():
    """
    Main execution function
    """
    logger.info("Starting Significant Strike Price Discount Analysis")
    
    try:
        # Step 1: Get test data
        test_data = get_test_data()
        
        if test_data.empty:
            logger.error("No data found in test_current_month_strikes table")
            return
        
        # Step 2: Analyze significant discounts
        discount_records = analyze_significant_strike_discounts(test_data)
        
        # Step 3: Create discount analysis table
        create_discount_analysis_table()
        
        # Step 4: Save discount records (even if empty for demonstration)
        save_discount_records(discount_records)
        
        # Step 5: Generate summary
        generate_discount_summary()
        
        if discount_records:
            logger.info("Significant strike price discount analysis completed successfully!")
        else:
            logger.info("Analysis completed - no strikes found with 50%+ discount from target price")
            logger.info("This is normal as options strikes are typically within reasonable ranges of the underlying price")
        
    except Exception as e:
        logger.error(f"Error in discount analysis: {str(e)}")
        raise

if __name__ == "__main__":
    main()