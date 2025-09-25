#!/usr/bin/env python3
"""
Step 5: Strike Price Analysis Derived Table
===========================================

This script implements the exact requirements for Step 5:

1. Get symbol and first Current_trade_date from step03_compare_monthvspreviousmonth
2. Get Current_close_price for that symbol and date
3. Find nearest 3 strikes up and below for both PE and CE (14 records total)
4. Store results in Step05_strikepriceAnalysisderived table
5. Test with symbol = 'ABB'

Table Structure:
From step03: Current_trade_date, Symbol, Current_close_price  
From step04: ID, Trade_date, expiry_date, Strike_price, option_type, close_price

Author: NSE Data Analysis Team
Date: September 2025
"""

import pyodbc
import pandas as pd
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('step05_strike_analysis_derived.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_database_connection():
    """Create database connection."""
    connection_string = (
        'Driver={ODBC Driver 17 for SQL Server};'
        'Server=SRIKIRANREDDY\\SQLEXPRESS;'
        'Database=master;'
        'Trusted_Connection=yes;'
    )
    return pyodbc.connect(connection_string)

def create_step05_derived_table(conn):
    """Create Step05_strikepriceAnalysisderived table."""
    
    # Drop table if exists
    drop_sql = "IF OBJECT_ID('Step05_strikepriceAnalysisderived', 'U') IS NOT NULL DROP TABLE Step05_strikepriceAnalysisderived"
    
    # Create table with specified columns
    create_sql = """
    CREATE TABLE Step05_strikepriceAnalysisderived (
        -- Primary key
        analysis_id BIGINT IDENTITY(1,1) PRIMARY KEY,
        
        -- From step03_compare_monthvspreviousmonth
        Current_trade_date DATE NOT NULL,
        Symbol NVARCHAR(50) NOT NULL,
        Current_close_price DECIMAL(18,4) NOT NULL,
        
        -- From step04_fo_udiff_daily
        ID INT NOT NULL,
        Trade_date VARCHAR(10) NOT NULL,
        expiry_date VARCHAR(10),
        Strike_price DECIMAL(18,4) NOT NULL,
        option_type VARCHAR(5) NOT NULL,
        close_price DECIMAL(18,4),
        
        -- Analysis metadata
        analysis_timestamp DATETIME2 DEFAULT GETDATE(),
        strike_position VARCHAR(10), -- 'ABOVE', 'BELOW', 'EXACT'
        strike_rank INT, -- 1=nearest, 2=second nearest, etc.
        
        -- Indexes for performance
        INDEX IX_step05_derived_symbol (Symbol),
        INDEX IX_step05_derived_strike (Strike_price),
        INDEX IX_step05_derived_option_type (option_type)
    )
    """
    
    cursor = conn.cursor()
    
    try:
        logger.info("Dropping existing Step05_strikepriceAnalysisderived table if exists...")
        cursor.execute(drop_sql)
        
        logger.info("Creating Step05_strikepriceAnalysisderived table...")
        cursor.execute(create_sql)
        
        conn.commit()
        logger.info("✅ Step05_strikepriceAnalysisderived table created successfully")
        
    except Exception as e:
        logger.error(f"Error creating table: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()

def get_symbol_first_date_data(conn, symbol):
    """
    Get first Current_trade_date and Current_close_price for a symbol 
    from step03_compare_monthvspreviousmonth.
    """
    logger.info(f"Getting first date data for symbol: {symbol}")
    
    query = """
    SELECT TOP 1
        Current_trade_date,
        symbol,
        Current_close_price
    FROM step03_compare_monthvspreviousmonth
    WHERE symbol = ?
    ORDER BY Current_trade_date ASC
    """
    
    df = pd.read_sql(query, conn, params=[symbol])
    
    if df.empty:
        logger.warning(f"No data found for symbol {symbol} in step03_compare_monthvspreviousmonth")
        return None
    
    result = df.iloc[0]
    logger.info(f"Found data for {symbol}: Date={result['Current_trade_date']}, Close={result['Current_close_price']}")
    
    return result

def get_fo_data_for_symbol_date(conn, symbol, trade_date):
    """
    Get F&O data for symbol on the specified trade date from step04_fo_udiff_daily.
    """
    logger.info(f"Getting F&O data for {symbol} on {trade_date}")
    
    # Convert date format if needed (YYYY-MM-DD to YYYYMMDD)
    if isinstance(trade_date, str) and '-' in trade_date:
        formatted_date = trade_date.replace('-', '')
    else:
        # If it's a datetime object
        formatted_date = trade_date.strftime('%Y%m%d')
    
    query = """
    SELECT 
        id,
        trade_date,
        symbol,
        expiry_date,
        strike_price,
        option_type,
        close_price
    FROM step04_fo_udiff_daily
    WHERE symbol = ?
    AND trade_date = ?
    AND strike_price IS NOT NULL
    AND option_type IN ('CE', 'PE')
    ORDER BY strike_price, option_type
    """
    
    df = pd.read_sql(query, conn, params=[symbol, formatted_date])
    
    if df.empty:
        logger.warning(f"No F&O data found for {symbol} on {formatted_date}")
        return pd.DataFrame()
    
    logger.info(f"Found {len(df)} F&O records for {symbol} on {formatted_date}")
    return df

def find_nearest_strikes_3_up_3_down(fo_data, current_close_price):
    """
    Find nearest 3 strikes above and 3 strikes below the current close price.
    For both PE and CE, total 14 records (7 strikes × 2 option types).
    """
    logger.info(f"Finding nearest strikes around close price: ₹{current_close_price:.2f}")
    
    if fo_data.empty:
        logger.warning("No F&O data provided")
        return pd.DataFrame()
    
    # Get unique strike prices
    unique_strikes = sorted(fo_data['strike_price'].unique())
    logger.info(f"Available strikes: {len(unique_strikes)} total")
    
    # Find strikes above and below current close price
    strikes_above = [s for s in unique_strikes if s > current_close_price]
    strikes_below = [s for s in unique_strikes if s < current_close_price]
    strikes_exact = [s for s in unique_strikes if s == current_close_price]
    
    logger.info(f"Strikes above close: {len(strikes_above)}")
    logger.info(f"Strikes below close: {len(strikes_below)}")
    logger.info(f"Strikes at close: {len(strikes_exact)}")
    
    # Select 3 nearest strikes above (ascending order = nearest first)
    selected_above = sorted(strikes_above)[:3]
    
    # Select 3 nearest strikes below (descending order = nearest first) 
    selected_below = sorted(strikes_below, reverse=True)[:3]
    
    # Include exact match if available
    selected_exact = strikes_exact[:1] if strikes_exact else []
    
    # Combine all selected strikes
    all_selected_strikes = selected_above + selected_below + selected_exact
    
    # If we don't have exactly 7 strikes, adjust
    if len(all_selected_strikes) < 7:
        # Add more strikes to reach 7
        remaining_strikes = [s for s in unique_strikes if s not in all_selected_strikes]
        # Sort by distance from close price
        remaining_by_distance = sorted(remaining_strikes, key=lambda x: abs(x - current_close_price))
        needed = 7 - len(all_selected_strikes)
        all_selected_strikes.extend(remaining_by_distance[:needed])
    elif len(all_selected_strikes) > 7:
        # Take only 7 closest strikes
        all_selected_strikes = sorted(all_selected_strikes, key=lambda x: abs(x - current_close_price))[:7]
    
    logger.info(f"Selected strikes ({len(all_selected_strikes)}): {sorted(all_selected_strikes)}")
    
    # Get all records (both PE and CE) for selected strikes
    selected_data = fo_data[fo_data['strike_price'].isin(all_selected_strikes)].copy()
    
    # Select only one record per strike-option combination (take the first one)
    # This ensures we get exactly 14 records (7 strikes × 2 option types)
    selected_data = selected_data.groupby(['strike_price', 'option_type']).first().reset_index()
    
    # Add strike position and rank
    selected_data['strike_position'] = selected_data['strike_price'].apply(
        lambda x: 'ABOVE' if x > current_close_price 
                 else 'BELOW' if x < current_close_price 
                 else 'EXACT'
    )
    
    # Add strike rank (1 = nearest to close price)
    strike_distances = {strike: abs(strike - current_close_price) for strike in all_selected_strikes}
    sorted_strikes = sorted(strike_distances.items(), key=lambda x: x[1])
    strike_rank_map = {strike: rank + 1 for rank, (strike, _) in enumerate(sorted_strikes)}
    selected_data['strike_rank'] = selected_data['strike_price'].map(strike_rank_map)
    
    # Sort by strike price and option type
    selected_data = selected_data.sort_values(['strike_price', 'option_type'])
    
    pe_count = len(selected_data[selected_data['option_type'] == 'PE'])
    ce_count = len(selected_data[selected_data['option_type'] == 'CE'])
    
    logger.info(f"Final selection: {len(selected_data)} records ({pe_count} PE + {ce_count} CE)")
    
    return selected_data

def insert_derived_analysis_data(conn, step03_data, fo_selected_data):
    """Insert analysis data into Step05_strikepriceAnalysisderived table."""
    
    if fo_selected_data.empty:
        logger.warning("No F&O data to insert")
        return 0
    
    # Prepare step03 data
    current_trade_date = step03_data['Current_trade_date']
    symbol = step03_data['symbol']
    current_close_price = step03_data['Current_close_price']
    
    logger.info(f"Inserting {len(fo_selected_data)} records for {symbol}")
    
    insert_sql = """
    INSERT INTO Step05_strikepriceAnalysisderived (
        Current_trade_date, Symbol, Current_close_price,
        ID, Trade_date, expiry_date, Strike_price, option_type, close_price,
        strike_position, strike_rank
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    cursor = conn.cursor()
    inserted_count = 0
    
    try:
        for _, row in fo_selected_data.iterrows():
            values = (
                current_trade_date,
                symbol,
                float(current_close_price),
                int(row['id']),
                row['trade_date'],
                row['expiry_date'],
                float(row['strike_price']),
                row['option_type'],
                float(row['close_price']) if pd.notna(row['close_price']) else None,
                row['strike_position'],
                int(row['strike_rank'])
            )
            
            cursor.execute(insert_sql, values)
            inserted_count += 1
        
        conn.commit()
        logger.info(f"✅ Successfully inserted {inserted_count} records")
        
    except Exception as e:
        logger.error(f"Error inserting data: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
    
    return inserted_count

def analyze_symbol_step05(conn, symbol):
    """
    Complete Step 5 analysis for a single symbol.
    """
    logger.info(f"Starting Step 5 analysis for symbol: {symbol}")
    
    # Step 1: Get first date data from step03
    step03_data = get_symbol_first_date_data(conn, symbol)
    if step03_data is None:
        logger.error(f"No step03 data found for {symbol}")
        return None
    
    # Step 2: Get F&O data for that symbol and date
    fo_data = get_fo_data_for_symbol_date(conn, symbol, step03_data['Current_trade_date'])
    if fo_data.empty:
        logger.error(f"No F&O data found for {symbol}")
        return None
    
    # Step 3: Find nearest 3 up and 3 down strikes for both PE and CE
    selected_fo_data = find_nearest_strikes_3_up_3_down(fo_data, step03_data['Current_close_price'])
    if selected_fo_data.empty:
        logger.error(f"No strikes selected for {symbol}")
        return None
    
    # Step 4: Insert into derived table
    inserted_count = insert_derived_analysis_data(conn, step03_data, selected_fo_data)
    
    return {
        'symbol': symbol,
        'step03_data': step03_data,
        'fo_records': len(fo_data),
        'selected_records': len(selected_fo_data),
        'inserted_records': inserted_count
    }

def generate_summary_report(conn):
    """Generate summary report of the analysis."""
    logger.info("Generating summary report...")
    
    query = """
    SELECT 
        Symbol,
        Current_trade_date,
        Current_close_price,
        COUNT(*) as total_records,
        COUNT(DISTINCT Strike_price) as unique_strikes,
        COUNT(CASE WHEN option_type = 'PE' THEN 1 END) as pe_records,
        COUNT(CASE WHEN option_type = 'CE' THEN 1 END) as ce_records,
        MIN(Strike_price) as min_strike,
        MAX(Strike_price) as max_strike
    FROM Step05_strikepriceAnalysisderived
    GROUP BY Symbol, Current_trade_date, Current_close_price
    ORDER BY Symbol
    """
    
    summary_df = pd.read_sql(query, conn)
    
    print("\n" + "="*80)
    print("STEP 5: STRIKE PRICE ANALYSIS DERIVED - SUMMARY REPORT")
    print("="*80)
    
    if not summary_df.empty:
        print(summary_df.to_string(index=False))
        
        total_records = summary_df['total_records'].sum()
        print(f"\nTotal records generated: {total_records}")
        print(f"Expected records per symbol: 14 (7 strikes × 2 option types)")
        
        for _, row in summary_df.iterrows():
            symbol = row['Symbol']
            actual_records = row['total_records']
            if actual_records == 14:
                print(f"✅ {symbol}: Perfect - {actual_records} records")
            else:
                print(f"⚠️  {symbol}: {actual_records} records (expected 14)")
    else:
        print("No data found in derived table")
    
    print("="*80)

def main():
    """Main function to execute Step 5 analysis."""
    print("🎯 STEP 5: STRIKE PRICE ANALYSIS DERIVED")
    print("="*60)
    print("Requirements:")
    print("1. Get symbol first date from step03_compare_monthvspreviousmonth")
    print("2. Find 3 strikes up + 3 strikes down for PE and CE (14 records)")
    print("3. Store in Step05_strikepriceAnalysisderived table")
    print("4. Test with symbol = 'ABB'")
    print("="*60)
    
    # Test symbol as specified
    test_symbol = 'ABB'
    
    try:
        conn = get_database_connection()
        logger.info("Database connection established")
        
        # Create the derived analysis table
        create_step05_derived_table(conn)
        
        # Analyze the test symbol
        result = analyze_symbol_step05(conn, test_symbol)
        
        if result:
            print(f"\n📊 ANALYSIS RESULTS FOR {test_symbol}:")
            print(f"Symbol: {result['symbol']}")
            print(f"Step03 Date: {result['step03_data']['Current_trade_date']}")
            print(f"Close Price: ₹{result['step03_data']['Current_close_price']:.2f}")
            print(f"Available F&O Records: {result['fo_records']}")
            print(f"Selected Records: {result['selected_records']}")
            print(f"Inserted Records: {result['inserted_records']}")
            
            if result['inserted_records'] == 14:
                print("✅ Perfect! Generated exactly 14 records as required")
            else:
                print(f"⚠️  Expected 14 records, got {result['inserted_records']}")
        else:
            print(f"\n❌ Analysis failed for {test_symbol}")
        
        # Generate summary report
        generate_summary_report(conn)
        
        print(f"\n✅ Step 5 analysis completed!")
        print(f"📋 Results stored in Step05_strikepriceAnalysisderived table")
        
    except Exception as e:
        logger.error(f"Step 5 analysis error: {e}")
        print(f"❌ Error: {e}")
    
    finally:
        if 'conn' in locals():
            conn.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    main()
