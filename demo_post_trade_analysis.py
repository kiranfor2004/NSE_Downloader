#!/usr/bin/env python3
"""
Demo Post Trade Date Analysis with February Data
===============================================

Purpose: Demonstrate the 50% reduction analysis by using earlier February dates
         as "base" dates and later February dates as "future" dates.

This will show how the analysis works when proper future data is available.
"""

import pyodbc
import pandas as pd
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_connection():
    """Get database connection"""
    connection_string = (
        "Driver={ODBC Driver 17 for SQL Server};"
        "Server=SRIKIRANREDDY\\SQLEXPRESS;"
        "Database=master;"
        "Trusted_Connection=yes;"
    )
    return pyodbc.connect(connection_string)

def create_demo_test_data():
    """
    Create demo test data using earlier February dates so we can find future data
    """
    logger.info("Creating demo test data with earlier February dates...")
    
    # Use February 3rd as the "test trade date" so we can find future data from Feb 4th onwards
    demo_query = """
    IF OBJECT_ID('demo_test_strikes', 'U') IS NOT NULL
        DROP TABLE demo_test_strikes
    
    -- Create demo table with earlier trade dates
    SELECT TOP 7
        symbol,
        fo_strike_price as strike_price,
        '20250203' as trade_date,  -- Use Feb 3rd instead of Feb 28th
        fo_option_type as option_type,
        fo_close_price as base_close_price,
        target_price,
        target_date
    INTO demo_test_strikes
    FROM test_current_month_strikes
    WHERE fo_strike_price IS NOT NULL 
    AND fo_option_type = 'CE'  -- Just CE options for demo
    """
    
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(demo_query)
        conn.commit()
    
    logger.info("Demo test data created successfully")

def run_demo_analysis():
    """
    Run the post trade date analysis using demo data
    """
    logger.info("Running demo post trade date analysis...")
    
    # Get demo test data
    query = """
    SELECT symbol, strike_price, trade_date, option_type, base_close_price, target_price
    FROM demo_test_strikes
    """
    
    with get_connection() as conn:
        demo_data = pd.read_sql(query, conn)
    
    logger.info(f"Demo data: {len(demo_data)} records")
    print("Demo Test Data:")
    print(demo_data.to_string(index=False))
    
    reduction_records = []
    
    for idx, test_record in demo_data.iterrows():
        symbol = test_record['symbol']
        strike_price = test_record['strike_price']
        option_type = test_record['option_type']
        base_trade_date = test_record['trade_date']  # 20250203
        base_close_price = test_record['base_close_price']
        
        logger.info(f"Checking {symbol} {option_type} {strike_price} from {base_trade_date}")
        
        # Find future data after Feb 3rd
        future_query = """
        SELECT TOP 5 *
        FROM step04_fo_udiff_daily
        WHERE symbol = ?
        AND strike_price = ?
        AND option_type = ?
        AND CAST(trade_date AS INT) > 20250203  -- After Feb 3rd
        ORDER BY trade_date
        """
        
        with get_connection() as conn:
            future_data = pd.read_sql(future_query, conn, params=[symbol, strike_price, option_type])
        
        logger.info(f"  Found {len(future_data)} future records")
        
        if not future_data.empty:
            print(f"\nFuture data for {symbol} {option_type} {strike_price}:")
            print(future_data[['trade_date', 'close_price', 'open_interest']].to_string(index=False))
            
            # Check for price reductions
            for _, future_record in future_data.iterrows():
                future_close_price = float(future_record['close_price']) if future_record['close_price'] else 0
                
                if base_close_price and base_close_price > 0 and future_close_price > 0:
                    reduction_amount = base_close_price - future_close_price
                    reduction_percentage = (reduction_amount / base_close_price) * 100
                    
                    print(f"  {base_trade_date} -> {future_record['trade_date']}: {base_close_price} -> {future_close_price} = {reduction_percentage:.2f}%")
                    
                    if reduction_percentage >= 50:
                        logger.info(f"    *** 50%+ REDUCTION FOUND: {reduction_percentage:.2f}% ***")
                        reduction_records.append({
                            'symbol': symbol,
                            'strike_price': strike_price,
                            'option_type': option_type,
                            'base_date': base_trade_date,
                            'future_date': future_record['trade_date'],
                            'base_price': base_close_price,
                            'future_price': future_close_price,
                            'reduction_pct': reduction_percentage
                        })
                    elif reduction_percentage >= 20:
                        logger.info(f"    20%+ reduction found: {reduction_percentage:.2f}%")
    
    return reduction_records

def main():
    """
    Main demo function
    """
    logger.info("Starting Demo Analysis")
    
    try:
        # Step 1: Create demo test data with earlier dates
        create_demo_test_data()
        
        # Step 2: Run analysis
        reduction_records = run_demo_analysis()
        
        print(f"\n" + "="*60)
        print("DEMO ANALYSIS RESULTS")
        print("="*60)
        print(f"Total reductions found: {len(reduction_records)}")
        
        if reduction_records:
            for record in reduction_records:
                print(f"50%+ Reduction: {record['symbol']} {record['option_type']} {record['strike_price']}")
                print(f"  {record['base_date']} -> {record['future_date']}: {record['base_price']} -> {record['future_price']} ({record['reduction_pct']:.2f}%)")
        else:
            print("No 50%+ reductions found in demo data")
            print("This is normal - 50% option price drops are rare in short timeframes")
        
        print("="*60)
        
    except Exception as e:
        logger.error(f"Demo analysis error: {str(e)}")
        raise

if __name__ == "__main__":
    main()