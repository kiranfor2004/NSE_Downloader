#!/usr/bin/env python3
"""
Post Trade Date Strike Price Reduction Analysis
==============================================

Purpose: Analyze strike price reductions after the trade date from test_current_month_strikes.
         Take symbol, strike_price, and trade_date from test_current_month_strikes,
         then check step04_fo_udiff_daily for dates AFTER the trade_date to find 50% reductions.

Process:
1. Get symbol, strike_price, and trade_date from test_current_month_strikes
2. For each record, search step04_fo_udiff_daily for the same symbol and strike_price 
   on dates AFTER the trade_date
3. Compare prices to find 50% or more reduction
4. Store complete columns from step04_fo_udiff_daily in new table

Logic:
- Base data: test_current_month_strikes (reference trade_date and strike_price)
- Search data: step04_fo_udiff_daily (dates after trade_date)
- Calculate: (Base Price - Future Price) / Base Price * 100 >= 50%
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
        logging.FileHandler('post_trade_date_reduction_analysis.log'),
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

def get_test_strikes_data():
    """
    Get symbol, strike_price, and trade_date from test_current_month_strikes
    """
    logger.info("Reading test strikes data from test_current_month_strikes...")
    
    query = """
    SELECT DISTINCT
        symbol,
        fo_strike_price as strike_price,
        fo_trade_date as trade_date,
        fo_option_type as option_type,
        fo_close_price as base_close_price,
        target_price,
        target_date
    FROM test_current_month_strikes
    WHERE fo_strike_price IS NOT NULL 
    AND fo_trade_date IS NOT NULL
    AND fo_option_type IS NOT NULL
    ORDER BY symbol, fo_trade_date, fo_strike_price, fo_option_type
    """
    
    with get_connection() as conn:
        df = pd.read_sql(query, conn)
    
    logger.info(f"Found {len(df)} test strike records to analyze")
    if not df.empty:
        logger.info(f"Sample data: {df.head(3).to_dict('records')}")
    
    return df

def find_future_fo_data(symbol, strike_price, option_type, base_trade_date):
    """
    Find F&O data for the same symbol, strike_price, and option_type 
    on dates AFTER the base_trade_date
    """
    logger.info(f"Searching for {symbol} {option_type} {strike_price} after {base_trade_date}")
    
    # Convert base_trade_date to integer for comparison
    base_date_int = int(base_trade_date)
    
    query = """
    SELECT *
    FROM step04_fo_udiff_daily
    WHERE symbol = ?
    AND strike_price = ?
    AND option_type = ?
    AND CAST(trade_date AS INT) > ?
    ORDER BY trade_date
    """
    
    with get_connection() as conn:
        df = pd.read_sql(query, conn, params=[symbol, strike_price, option_type, base_date_int])
    
    logger.info(f"  Found {len(df)} future records for {symbol} {option_type} {strike_price}")
    return df

def analyze_post_trade_reductions():
    """
    Analyze strike price reductions after trade dates
    """
    logger.info("Starting post trade date reduction analysis...")
    
    # Get test strikes data
    test_data = get_test_strikes_data()
    
    if test_data.empty:
        logger.error("No test strikes data found")
        return []
    
    reduction_records = []
    
    for idx, test_record in test_data.iterrows():
        symbol = test_record['symbol']
        strike_price = test_record['strike_price']
        option_type = test_record['option_type']
        base_trade_date = test_record['trade_date']
        base_close_price = test_record['base_close_price']
        target_price = test_record['target_price']
        target_date = test_record['target_date']
        
        logger.info(f"Processing {symbol} {option_type} {strike_price} from {base_trade_date}")
        
        # Find future F&O data for this symbol/strike/option combination
        future_data = find_future_fo_data(symbol, strike_price, option_type, base_trade_date)
        
        if future_data.empty:
            logger.warning(f"  No future data found for {symbol} {option_type} {strike_price}")
            continue
        
        # Check each future record for 50% reduction
        for _, future_record in future_data.iterrows():
            future_trade_date = future_record['trade_date']
            future_close_price = float(future_record['close_price']) if future_record['close_price'] else 0
            
            # Calculate price reduction
            if base_close_price and base_close_price > 0:
                price_reduction_amount = base_close_price - future_close_price
                price_reduction_percentage = (price_reduction_amount / base_close_price) * 100
                
                # Check if reduction is 50% or more
                if price_reduction_percentage >= 50:
                    logger.info(f"  *** 50%+ REDUCTION FOUND ***")
                    logger.info(f"  {base_trade_date} -> {future_trade_date}: {base_close_price} -> {future_close_price}")
                    logger.info(f"  Reduction: {price_reduction_amount:.2f} ({price_reduction_percentage:.2f}%)")
                    
                    # Create reduction record with all columns from step04_fo_udiff_daily
                    reduction_record = {
                        # Reference information
                        'base_symbol': symbol,
                        'base_strike_price': strike_price,
                        'base_option_type': option_type,
                        'base_trade_date': base_trade_date,
                        'base_close_price': base_close_price,
                        'target_price': target_price,
                        'target_date': target_date,
                        
                        # Reduction analysis
                        'price_reduction_amount': price_reduction_amount,
                        'price_reduction_percentage': price_reduction_percentage,
                        'analysis_type': 'post_trade_date_50_percent_reduction',
                        
                        # All columns from future F&O data (step04_fo_udiff_daily)
                        'future_id': future_record.get('id'),
                        'future_instrument': future_record.get('instrument'),
                        'future_symbol': future_record.get('symbol'),
                        'future_expiry_date': future_record.get('expiry_date'),
                        'future_strike_price': future_record.get('strike_price'),
                        'future_option_type': future_record.get('option_type'),
                        'future_open_price': future_record.get('open_price'),
                        'future_high_price': future_record.get('high_price'),
                        'future_low_price': future_record.get('low_price'),
                        'future_close_price': future_record.get('close_price'),
                        'future_settle_price': future_record.get('settle_price'),
                        'future_contracts_traded': future_record.get('contracts_traded'),
                        'future_value_in_lakh': future_record.get('value_in_lakh'),
                        'future_open_interest': future_record.get('open_interest'),
                        'future_change_in_oi': future_record.get('change_in_oi'),
                        'future_trade_date': future_record.get('trade_date'),
                        'future_underlying': future_record.get('underlying'),
                    }
                    
                    reduction_records.append(reduction_record)
    
    logger.info(f"Found {len(reduction_records)} records with post-trade-date 50%+ reductions")
    return reduction_records

def create_post_trade_reduction_table():
    """
    Create table for post trade date reduction analysis
    """
    logger.info("Creating post_trade_date_reduction_analysis table...")
    
    create_table_query = """
    IF OBJECT_ID('post_trade_date_reduction_analysis', 'U') IS NOT NULL
        DROP TABLE post_trade_date_reduction_analysis
    
    CREATE TABLE post_trade_date_reduction_analysis (
        id INT IDENTITY(1,1) PRIMARY KEY,
        
        -- Reference information from test_current_month_strikes
        base_symbol NVARCHAR(50),
        base_strike_price DECIMAL(10,2),
        base_option_type NVARCHAR(2),
        base_trade_date NVARCHAR(8),
        base_close_price DECIMAL(10,2),
        target_price DECIMAL(10,2),
        target_date NVARCHAR(8),
        
        -- Reduction analysis
        price_reduction_amount DECIMAL(10,2),
        price_reduction_percentage DECIMAL(8,4),
        analysis_type NVARCHAR(50),
        
        -- Complete columns from step04_fo_udiff_daily (future data)
        future_id BIGINT,
        future_instrument NVARCHAR(50),
        future_symbol NVARCHAR(50),
        future_expiry_date NVARCHAR(8),
        future_strike_price DECIMAL(10,2),
        future_option_type NVARCHAR(2),
        future_open_price DECIMAL(10,2),
        future_high_price DECIMAL(10,2),
        future_low_price DECIMAL(10,2),
        future_close_price DECIMAL(10,2),
        future_settle_price DECIMAL(10,2),
        future_contracts_traded BIGINT,
        future_value_in_lakh DECIMAL(18,4),
        future_open_interest BIGINT,
        future_change_in_oi BIGINT,
        future_trade_date NVARCHAR(8),
        future_underlying NVARCHAR(50),
        
        created_at DATETIME DEFAULT GETDATE()
    )
    """
    
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
    
    logger.info("Post trade date reduction analysis table created successfully")

def save_reduction_records(reduction_records):
    """
    Save post trade date reduction records to the table
    """
    if not reduction_records:
        logger.warning("No reduction records to save")
        return
    
    logger.info(f"Saving {len(reduction_records)} post trade date reduction records...")
    
    insert_query = """
    INSERT INTO post_trade_date_reduction_analysis (
        base_symbol, base_strike_price, base_option_type, base_trade_date, base_close_price,
        target_price, target_date, price_reduction_amount, price_reduction_percentage, analysis_type,
        future_id, future_instrument, future_symbol, future_expiry_date, future_strike_price,
        future_option_type, future_open_price, future_high_price, future_low_price, future_close_price,
        future_settle_price, future_contracts_traded, future_value_in_lakh, future_open_interest,
        future_change_in_oi, future_trade_date, future_underlying
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    with get_connection() as conn:
        cursor = conn.cursor()
        for record in reduction_records:
            cursor.execute(insert_query, (
                record.get('base_symbol', ''),
                record.get('base_strike_price', 0),
                record.get('base_option_type', ''),
                record.get('base_trade_date', ''),
                record.get('base_close_price', 0),
                record.get('target_price', 0),
                record.get('target_date', ''),
                record.get('price_reduction_amount', 0),
                record.get('price_reduction_percentage', 0),
                record.get('analysis_type', ''),
                record.get('future_id', None),
                record.get('future_instrument', ''),
                record.get('future_symbol', ''),
                record.get('future_expiry_date', ''),
                record.get('future_strike_price', 0),
                record.get('future_option_type', ''),
                record.get('future_open_price', 0),
                record.get('future_high_price', 0),
                record.get('future_low_price', 0),
                record.get('future_close_price', 0),
                record.get('future_settle_price', 0),
                record.get('future_contracts_traded', 0),
                record.get('future_value_in_lakh', 0),
                record.get('future_open_interest', 0),
                record.get('future_change_in_oi', 0),
                record.get('future_trade_date', ''),
                record.get('future_underlying', '')
            ))
        conn.commit()
    
    logger.info("Post trade date reduction records saved successfully")

def generate_reduction_summary():
    """
    Generate summary of post trade date reductions
    """
    logger.info("Generating post trade date reduction summary...")
    
    summary_query = """
    SELECT 
        base_symbol,
        COUNT(*) as reduction_count,
        AVG(price_reduction_percentage) as avg_reduction_pct,
        MIN(price_reduction_percentage) as min_reduction_pct,
        MAX(price_reduction_percentage) as max_reduction_pct,
        COUNT(DISTINCT base_trade_date) as base_dates,
        COUNT(DISTINCT future_trade_date) as future_dates
    FROM post_trade_date_reduction_analysis
    GROUP BY base_symbol
    ORDER BY base_symbol
    """
    
    with get_connection() as conn:
        summary_df = pd.read_sql(summary_query, conn)
    
    if not summary_df.empty:
        print("\n" + "="*80)
        print("POST TRADE DATE STRIKE PRICE REDUCTION ANALYSIS SUMMARY")
        print("="*80)
        print(f"Symbols with post-trade-date reductions: {len(summary_df)}")
        print(f"Total reduction records: {summary_df['reduction_count'].sum()}")
        
        print("\nDetailed Summary:")
        print(summary_df.to_string(index=False))
        
        # Show individual records
        detail_query = """
        SELECT TOP 10
            base_symbol, base_trade_date, future_trade_date, base_option_type, base_strike_price,
            base_close_price, future_close_price, price_reduction_percentage
        FROM post_trade_date_reduction_analysis
        ORDER BY price_reduction_percentage DESC
        """
        
        detail_df = pd.read_sql(detail_query, conn)
        print("\nTop 10 Reductions:")
        print(detail_df.to_string(index=False))
        print("="*80)
    else:
        print("\nNo post trade date strike price reductions (50%+) found.")

def main():
    """
    Main execution function
    """
    logger.info("Starting Post Trade Date Strike Price Reduction Analysis")
    
    try:
        # Step 1: Analyze post trade date reductions
        reduction_records = analyze_post_trade_reductions()
        
        # Step 2: Create table
        create_post_trade_reduction_table()
        
        # Step 3: Save records
        save_reduction_records(reduction_records)
        
        # Step 4: Generate summary
        generate_reduction_summary()
        
        if reduction_records:
            logger.info("Post trade date reduction analysis completed successfully!")
        else:
            logger.info("Analysis completed - no 50%+ post trade date reductions found")
        
    except Exception as e:
        logger.error(f"Error in post trade date reduction analysis: {str(e)}")
        raise

if __name__ == "__main__":
    main()