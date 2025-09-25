#!/usr/bin/env python3
"""
Intramonth Strike Price Reduction Analysis
==========================================

Purpose: Analyze strike price reductions within available trade dates.
         Since we only have February 2025 data, we'll check for 50% reductions
         from earlier dates to later dates within the same month.

Process:
1. Get data from test_current_month_strikes table (base reference)
2. Take earlier dates in the same month as "current" dates
3. Compare with later dates in the same month as "next" dates
4. Identify records with 50% or more reduction in strike prices
5. Store qualifying records in: intramonth_strike_reduction_analysis

Logic:
- Current trade date: Earlier dates in February 2025
- Next trade date: Later dates in February 2025
- Calculate: (Earlier Strike - Later Strike) / Earlier Strike * 100 >= 50%
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
        logging.FileHandler('intramonth_strike_reduction_analysis.log'),
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

def get_available_dates_for_symbol(symbol):
    """
    Get all available trade dates for a symbol
    """
    logger.info(f"Getting all available trade dates for {symbol}...")
    
    query = """
    SELECT DISTINCT trade_date
    FROM step04_fo_udiff_daily
    WHERE symbol = ?
    ORDER BY trade_date
    """
    
    with get_connection() as conn:
        df = pd.read_sql(query, conn, params=[symbol])
    
    dates = df['trade_date'].tolist() if not df.empty else []
    logger.info(f"Found {len(dates)} trade dates for {symbol}: {dates[:5]}{'...' if len(dates) > 5 else ''}")
    return dates

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

def analyze_intramonth_reduction():
    """
    Analyze strike price reductions within the available month data
    """
    logger.info("Analyzing intramonth strike price reductions...")
    
    # Get symbol from test table
    query = """
    SELECT DISTINCT symbol, target_price
    FROM test_current_month_strikes
    """
    
    with get_connection() as conn:
        symbols_df = pd.read_sql(query, conn)
    
    if symbols_df.empty:
        logger.error("No symbols found in test_current_month_strikes")
        return []
    
    reduction_records = []
    
    for _, symbol_record in symbols_df.iterrows():
        symbol = symbol_record['symbol']
        target_price = symbol_record['target_price']
        
        logger.info(f"Processing {symbol}...")
        
        # Get all available dates for this symbol
        available_dates = get_available_dates_for_symbol(symbol)
        
        if len(available_dates) < 2:
            logger.warning(f"Not enough dates for {symbol} to compare")
            continue
        
        # Compare each earlier date with each later date
        for i, earlier_date in enumerate(available_dates[:-1]):  # All except last
            for later_date in available_dates[i+1:]:  # All dates after earlier_date
                
                logger.info(f"  Comparing {earlier_date} vs {later_date}")
                
                # Get F&O data for both dates
                earlier_fo_data = get_fo_data_for_date(symbol, earlier_date)
                later_fo_data = get_fo_data_for_date(symbol, later_date)
                
                if earlier_fo_data.empty or later_fo_data.empty:
                    logger.warning(f"    Missing F&O data for comparison")
                    continue
                
                # Create lookup for earlier data
                earlier_strikes = {}
                for _, row in earlier_fo_data.iterrows():
                    key = (row['strike_price'], row['option_type'])
                    earlier_strikes[key] = row
                
                # Check for reductions in later data
                for _, later_row in later_fo_data.iterrows():
                    later_strike = later_row['strike_price']
                    later_option_type = later_row['option_type']
                    
                    # Find matching strike and option type in earlier data
                    for (earlier_strike, earlier_option_type), earlier_row in earlier_strikes.items():
                        if (earlier_option_type == later_option_type and 
                            earlier_strike == later_strike and
                            earlier_strike > 0):
                            
                            # Calculate price reduction (close price comparison)
                            earlier_close = float(earlier_row['close_price']) if earlier_row['close_price'] else 0
                            later_close = float(later_row['close_price']) if later_row['close_price'] else 0
                            
                            if earlier_close > 0:
                                price_reduction_amount = earlier_close - later_close
                                price_reduction_percentage = (price_reduction_amount / earlier_close) * 100
                                
                                # Check if price reduction is 50% or more
                                if price_reduction_percentage >= 50:
                                    logger.info(f"    *** 50%+ PRICE REDUCTION FOUND ***")
                                    logger.info(f"    {earlier_option_type} Strike {earlier_strike}: {earlier_close} -> {later_close}")
                                    logger.info(f"    Price Reduction: {price_reduction_amount:.2f} ({price_reduction_percentage:.2f}%)")
                                    
                                    # Create reduction record
                                    reduction_record = {
                                        'symbol': symbol,
                                        'target_price': target_price,
                                        'earlier_trade_date': earlier_date,
                                        'later_trade_date': later_date,
                                        'option_type': later_option_type,
                                        'strike_price': later_strike,
                                        
                                        # Earlier date data
                                        'earlier_close_price': earlier_close,
                                        'earlier_open_interest': earlier_row['open_interest'],
                                        'earlier_contracts_traded': earlier_row['contracts_traded'],
                                        'earlier_open_price': earlier_row['open_price'],
                                        'earlier_high_price': earlier_row['high_price'],
                                        'earlier_low_price': earlier_row['low_price'],
                                        
                                        # Later date data
                                        'later_close_price': later_close,
                                        'later_open_interest': later_row['open_interest'],
                                        'later_contracts_traded': later_row['contracts_traded'],
                                        'later_open_price': later_row['open_price'],
                                        'later_high_price': later_row['high_price'],
                                        'later_low_price': later_row['low_price'],
                                        'later_expiry_date': later_row['expiry_date'],
                                        
                                        # Reduction analysis
                                        'price_reduction_amount': price_reduction_amount,
                                        'price_reduction_percentage': price_reduction_percentage,
                                        'analysis_type': 'intramonth_50_percent_price_reduction',
                                        'days_difference': len([d for d in available_dates if earlier_date < d <= later_date]),
                                    }
                                    
                                    reduction_records.append(reduction_record)
    
    logger.info(f"Found {len(reduction_records)} records with intramonth 50%+ price reductions")
    return reduction_records

def create_intramonth_reduction_table():
    """
    Create table for intramonth strike reduction analysis
    """
    logger.info("Creating intramonth_strike_reduction_analysis table...")
    
    create_table_query = """
    IF OBJECT_ID('intramonth_strike_reduction_analysis', 'U') IS NOT NULL
        DROP TABLE intramonth_strike_reduction_analysis
    
    CREATE TABLE intramonth_strike_reduction_analysis (
        id INT IDENTITY(1,1) PRIMARY KEY,
        
        -- Symbol and date info
        symbol NVARCHAR(50),
        target_price DECIMAL(10,2),
        earlier_trade_date NVARCHAR(8),
        later_trade_date NVARCHAR(8),
        option_type NVARCHAR(2),
        strike_price DECIMAL(10,2),
        
        -- Earlier date F&O data
        earlier_close_price DECIMAL(10,2),
        earlier_open_interest BIGINT,
        earlier_contracts_traded BIGINT,
        earlier_open_price DECIMAL(10,2),
        earlier_high_price DECIMAL(10,2),
        earlier_low_price DECIMAL(10,2),
        
        -- Later date F&O data
        later_close_price DECIMAL(10,2),
        later_open_interest BIGINT,
        later_contracts_traded BIGINT,
        later_open_price DECIMAL(10,2),
        later_high_price DECIMAL(10,2),
        later_low_price DECIMAL(10,2),
        later_expiry_date NVARCHAR(8),
        
        -- Reduction analysis
        price_reduction_amount DECIMAL(10,2),
        price_reduction_percentage DECIMAL(8,4),
        analysis_type NVARCHAR(50),
        days_difference INT,
        
        created_at DATETIME DEFAULT GETDATE()
    )
    """
    
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
    
    logger.info("Intramonth strike reduction analysis table created successfully")

def save_reduction_records(reduction_records):
    """
    Save intramonth reduction records to the table
    """
    if not reduction_records:
        logger.warning("No reduction records to save")
        return
    
    logger.info(f"Saving {len(reduction_records)} intramonth reduction records...")
    
    insert_query = """
    INSERT INTO intramonth_strike_reduction_analysis (
        symbol, target_price, earlier_trade_date, later_trade_date, option_type, strike_price,
        earlier_close_price, earlier_open_interest, earlier_contracts_traded, 
        earlier_open_price, earlier_high_price, earlier_low_price,
        later_close_price, later_open_interest, later_contracts_traded,
        later_open_price, later_high_price, later_low_price, later_expiry_date,
        price_reduction_amount, price_reduction_percentage, analysis_type, days_difference
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    with get_connection() as conn:
        cursor = conn.cursor()
        for record in reduction_records:
            cursor.execute(insert_query, (
                record.get('symbol', ''),
                record.get('target_price', 0),
                record.get('earlier_trade_date', ''),
                record.get('later_trade_date', ''),
                record.get('option_type', ''),
                record.get('strike_price', 0),
                record.get('earlier_close_price', 0),
                record.get('earlier_open_interest', 0),
                record.get('earlier_contracts_traded', 0),
                record.get('earlier_open_price', 0),
                record.get('earlier_high_price', 0),
                record.get('earlier_low_price', 0),
                record.get('later_close_price', 0),
                record.get('later_open_interest', 0),
                record.get('later_contracts_traded', 0),
                record.get('later_open_price', 0),
                record.get('later_high_price', 0),
                record.get('later_low_price', 0),
                record.get('later_expiry_date', ''),
                record.get('price_reduction_amount', 0),
                record.get('price_reduction_percentage', 0),
                record.get('analysis_type', ''),
                record.get('days_difference', 0)
            ))
        conn.commit()
    
    logger.info("Intramonth reduction records saved successfully")

def generate_reduction_summary():
    """
    Generate summary of intramonth strike reductions
    """
    logger.info("Generating intramonth reduction summary...")
    
    summary_query = """
    SELECT 
        symbol,
        COUNT(*) as reduction_count,
        AVG(price_reduction_percentage) as avg_reduction_pct,
        MIN(price_reduction_percentage) as min_reduction_pct,
        MAX(price_reduction_percentage) as max_reduction_pct,
        COUNT(DISTINCT earlier_trade_date) as earlier_dates,
        COUNT(DISTINCT later_trade_date) as later_dates,
        AVG(days_difference) as avg_days_diff
    FROM intramonth_strike_reduction_analysis
    GROUP BY symbol
    ORDER BY symbol
    """
    
    with get_connection() as conn:
        summary_df = pd.read_sql(summary_query, conn)
    
    if not summary_df.empty:
        print("\n" + "="*80)
        print("INTRAMONTH STRIKE PRICE REDUCTION ANALYSIS SUMMARY")
        print("="*80)
        print(f"Symbols with intramonth reductions: {len(summary_df)}")
        print(f"Total reduction records: {summary_df['reduction_count'].sum()}")
        
        print("\nDetailed Summary:")
        print(summary_df.to_string(index=False))
        
        # Show individual records
        detail_query = """
        SELECT TOP 10
            symbol, earlier_trade_date, later_trade_date, option_type, strike_price,
            earlier_close_price, later_close_price, price_reduction_percentage
        FROM intramonth_strike_reduction_analysis
        ORDER BY price_reduction_percentage DESC
        """
        
        detail_df = pd.read_sql(detail_query, conn)
        print("\nTop 10 Reductions:")
        print(detail_df.to_string(index=False))
        print("="*80)
    else:
        print("\nNo intramonth strike price reductions (50%+) found.")

def main():
    """
    Main execution function
    """
    logger.info("Starting Intramonth Strike Price Reduction Analysis")
    
    try:
        # Step 1: Analyze intramonth reductions
        reduction_records = analyze_intramonth_reduction()
        
        # Step 2: Create table
        create_intramonth_reduction_table()
        
        # Step 3: Save records
        save_reduction_records(reduction_records)
        
        # Step 4: Generate summary
        generate_reduction_summary()
        
        if reduction_records:
            logger.info("Intramonth strike reduction analysis completed successfully!")
        else:
            logger.info("Analysis completed - no 50%+ intramonth price reductions found")
        
    except Exception as e:
        logger.error(f"Error in intramonth reduction analysis: {str(e)}")
        raise

if __name__ == "__main__":
    main()