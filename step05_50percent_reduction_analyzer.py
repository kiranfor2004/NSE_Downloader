#!/usr/bin/env python3
"""
Step 5 Extension: 50% Strike Price Reduction Analysis
====================================================

This script analyzes the Step05_strikepriceAnalysisderived table to find
50% or more reduction in strike prices by comparing with subsequent
trading data from step04_fo_udiff_daily table.

Logic:
1. Take each record from Step05_strikepriceAnalysisderived (base data from 3rd Feb 2025)
2. For each strike price, find subsequent trading data from 4th Feb 2025 onwards
3. Calculate percentage reduction: ((Base_Price - Current_Price) / Base_Price) * 100
4. Identify when 50% or more reduction occurs
5. Store analysis results in a new table

Author: NSE Data Analysis Team
Date: September 2025
"""

import pyodbc
import pandas as pd
import logging
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('step05_50percent_reduction_analysis.log'),
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

def create_50percent_reduction_table(conn):
    """Create table to store 50% reduction analysis results."""
    
    drop_sql = "IF OBJECT_ID('Step05_50percent_reduction_analysis', 'U') IS NOT NULL DROP TABLE Step05_50percent_reduction_analysis"
    
    create_sql = """
    CREATE TABLE Step05_50percent_reduction_analysis (
        -- Primary key
        reduction_id BIGINT IDENTITY(1,1) PRIMARY KEY,
        
        -- Source data from Step05_strikepriceAnalysisderived
        source_analysis_id BIGINT NOT NULL,
        symbol NVARCHAR(50) NOT NULL,
        base_trade_date DATE NOT NULL,
        strike_price DECIMAL(18,4) NOT NULL,
        option_type VARCHAR(5) NOT NULL,
        base_close_price DECIMAL(18,4) NOT NULL,
        
        -- Reduction analysis results
        reduction_found BIT NOT NULL DEFAULT 0,
        reduction_date DATE NULL,
        reduced_price DECIMAL(18,4) NULL,
        reduction_percentage DECIMAL(10,4) NULL,
        days_to_reduction INT NULL,
        
        -- Additional analysis data
        max_reduction_percentage DECIMAL(10,4) NULL,
        max_reduction_date DATE NULL,
        max_reduction_price DECIMAL(18,4) NULL,
        total_trading_days_analyzed INT NULL,
        
        -- Metadata
        analysis_timestamp DATETIME2 DEFAULT GETDATE(),
        
        -- Indexes
        INDEX IX_50percent_symbol (symbol),
        INDEX IX_50percent_strike (strike_price),
        INDEX IX_50percent_option (option_type),
        INDEX IX_50percent_reduction_found (reduction_found)
    )
    """
    
    cursor = conn.cursor()
    
    try:
        logger.info("Dropping existing Step05_50percent_reduction_analysis table if exists...")
        cursor.execute(drop_sql)
        
        logger.info("Creating Step05_50percent_reduction_analysis table...")
        cursor.execute(create_sql)
        
        conn.commit()
        logger.info("✅ Step05_50percent_reduction_analysis table created successfully")
        
    except Exception as e:
        logger.error(f"Error creating table: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()

def get_step05_base_data(conn):
    """Get all records from Step05_strikepriceAnalysisderived as base data."""
    logger.info("Getting base data from Step05_strikepriceAnalysisderived...")
    
    query = """
    SELECT 
        analysis_id,
        Symbol,
        Current_trade_date,
        Strike_price,
        option_type,
        close_price as base_close_price
    FROM Step05_strikepriceAnalysisderived
    ORDER BY Strike_price, option_type
    """
    
    df = pd.read_sql(query, conn)
    logger.info(f"Retrieved {len(df)} base records for analysis")
    
    return df

def get_subsequent_trading_data(conn, symbol, strike_price, option_type, start_date):
    """
    Get subsequent trading data from step04_fo_udiff_daily starting from start_date onwards.
    """
    # Convert start_date to YYYYMMDD format for comparison
    if isinstance(start_date, str):
        # Parse date string and add 1 day
        from datetime import datetime
        date_obj = datetime.strptime(start_date, '%Y-%m-%d')
        next_day = date_obj + timedelta(days=1)
        start_date_formatted = next_day.strftime('%Y%m%d')
    else:
        # start_date is already a date object
        next_day = start_date + timedelta(days=1)
        start_date_formatted = next_day.strftime('%Y%m%d')
    
    logger.info(f"Getting subsequent data for {symbol} {strike_price} {option_type} from {start_date_formatted}")
    
    query = """
    SELECT 
        trade_date,
        close_price
    FROM step04_fo_udiff_daily
    WHERE symbol = ?
    AND strike_price = ?
    AND option_type = ?
    AND trade_date >= ?
    AND close_price IS NOT NULL
    ORDER BY trade_date
    """
    
    df = pd.read_sql(query, conn, params=[symbol, strike_price, option_type, start_date_formatted])
    
    if not df.empty:
        logger.info(f"Found {len(df)} subsequent trading records")
    else:
        logger.warning(f"No subsequent trading data found for {symbol} {strike_price} {option_type}")
    
    return df

def analyze_50percent_reduction(base_price, subsequent_data):
    """
    Analyze subsequent trading data to find 50% reduction.
    Returns analysis results.
    """
    if subsequent_data.empty:
        return {
            'reduction_found': False,
            'reduction_date': None,
            'reduced_price': None,
            'reduction_percentage': None,
            'days_to_reduction': None,
            'max_reduction_percentage': None,
            'max_reduction_date': None,
            'max_reduction_price': None,
            'total_trading_days_analyzed': 0
        }
    
    # Calculate reduction percentages for all subsequent prices
    subsequent_data = subsequent_data.copy()
    subsequent_data['reduction_percentage'] = ((base_price - subsequent_data['close_price']) / base_price) * 100
    
    # Find first occurrence of 50% or more reduction
    fifty_percent_reductions = subsequent_data[subsequent_data['reduction_percentage'] >= 50.0]
    
    # Find maximum reduction
    max_reduction_row = subsequent_data.loc[subsequent_data['reduction_percentage'].idxmax()]
    
    result = {
        'total_trading_days_analyzed': len(subsequent_data),
        'max_reduction_percentage': max_reduction_row['reduction_percentage'],
        'max_reduction_date': max_reduction_row['trade_date'],
        'max_reduction_price': max_reduction_row['close_price']
    }
    
    if not fifty_percent_reductions.empty:
        # 50% reduction found
        first_50_percent = fifty_percent_reductions.iloc[0]
        
        # Calculate days to reduction
        base_date = datetime.strptime(subsequent_data.iloc[0]['trade_date'], '%Y%m%d') - timedelta(days=1)
        reduction_date = datetime.strptime(first_50_percent['trade_date'], '%Y%m%d')
        days_to_reduction = (reduction_date - base_date).days
        
        result.update({
            'reduction_found': True,
            'reduction_date': first_50_percent['trade_date'],
            'reduced_price': first_50_percent['close_price'],
            'reduction_percentage': first_50_percent['reduction_percentage'],
            'days_to_reduction': days_to_reduction
        })
        
        logger.info(f"✅ 50% reduction found! {first_50_percent['reduction_percentage']:.2f}% on {first_50_percent['trade_date']}")
    else:
        # No 50% reduction found
        result.update({
            'reduction_found': False,
            'reduction_date': None,
            'reduced_price': None,
            'reduction_percentage': None,
            'days_to_reduction': None
        })
        
        logger.info(f"❌ No 50% reduction found. Max reduction: {max_reduction_row['reduction_percentage']:.2f}%")
    
    return result

def process_single_strike_analysis(conn, base_record):
    """Process 50% reduction analysis for a single strike record."""
    
    symbol = base_record['Symbol']
    strike_price = base_record['Strike_price']
    option_type = base_record['option_type']
    base_date = base_record['Current_trade_date']
    base_price = base_record['base_close_price']
    
    logger.info(f"Analyzing {symbol} {strike_price} {option_type} - Base: ₹{base_price:.2f}")
    
    # Get subsequent trading data
    subsequent_data = get_subsequent_trading_data(conn, symbol, strike_price, option_type, base_date)
    
    # Analyze for 50% reduction
    analysis_result = analyze_50percent_reduction(base_price, subsequent_data)
    
    # Prepare data for database insertion
    result = {
        'source_analysis_id': base_record['analysis_id'],
        'symbol': symbol,
        'base_trade_date': base_date,
        'strike_price': strike_price,
        'option_type': option_type,
        'base_close_price': base_price,
        **analysis_result
    }
    
    return result

def insert_reduction_analysis_results(conn, analysis_results):
    """Insert all analysis results into the reduction analysis table."""
    
    if not analysis_results:
        logger.warning("No analysis results to insert")
        return 0
    
    logger.info(f"Inserting {len(analysis_results)} analysis results...")
    
    insert_sql = """
    INSERT INTO Step05_50percent_reduction_analysis (
        source_analysis_id, symbol, base_trade_date, strike_price, option_type, base_close_price,
        reduction_found, reduction_date, reduced_price, reduction_percentage, days_to_reduction,
        max_reduction_percentage, max_reduction_date, max_reduction_price, total_trading_days_analyzed
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    cursor = conn.cursor()
    inserted_count = 0
    
    try:
        for result in analysis_results:
            values = (
                result['source_analysis_id'],
                result['symbol'],
                result['base_trade_date'],
                result['strike_price'],
                result['option_type'],
                result['base_close_price'],
                result['reduction_found'],
                result['reduction_date'],
                result['reduced_price'],
                result['reduction_percentage'],
                result['days_to_reduction'],
                result['max_reduction_percentage'],
                result['max_reduction_date'],
                result['max_reduction_price'],
                result['total_trading_days_analyzed']
            )
            
            cursor.execute(insert_sql, values)
            inserted_count += 1
        
        conn.commit()
        logger.info(f"✅ Successfully inserted {inserted_count} analysis results")
        
    except Exception as e:
        logger.error(f"Error inserting analysis results: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
    
    return inserted_count

def generate_reduction_summary_report(conn):
    """Generate comprehensive summary report of 50% reduction analysis."""
    logger.info("Generating 50% reduction analysis summary report...")
    
    # Summary statistics
    summary_query = """
    SELECT 
        COUNT(*) as total_strikes_analyzed,
        COUNT(CASE WHEN reduction_found = 1 THEN 1 END) as strikes_with_50percent_reduction,
        COUNT(CASE WHEN reduction_found = 0 THEN 1 END) as strikes_without_50percent_reduction,
        AVG(CASE WHEN reduction_found = 1 THEN reduction_percentage END) as avg_reduction_percentage,
        AVG(CASE WHEN reduction_found = 1 THEN days_to_reduction END) as avg_days_to_reduction,
        MIN(CASE WHEN reduction_found = 1 THEN days_to_reduction END) as min_days_to_reduction,
        MAX(CASE WHEN reduction_found = 1 THEN days_to_reduction END) as max_days_to_reduction
    FROM Step05_50percent_reduction_analysis
    """
    
    summary_df = pd.read_sql(summary_query, conn)
    
    # Detailed results
    detailed_query = """
    SELECT 
        symbol,
        strike_price,
        option_type,
        base_close_price,
        reduction_found,
        reduction_percentage,
        reduction_date,
        reduced_price,
        days_to_reduction,
        max_reduction_percentage
    FROM Step05_50percent_reduction_analysis
    ORDER BY reduction_found DESC, reduction_percentage DESC, strike_price, option_type
    """
    
    detailed_df = pd.read_sql(detailed_query, conn)
    
    print("\n" + "="*100)
    print("STEP 5: 50% STRIKE PRICE REDUCTION ANALYSIS - COMPREHENSIVE REPORT")
    print("="*100)
    
    # Print summary statistics
    if not summary_df.empty:
        summary = summary_df.iloc[0]
        print(f"📊 SUMMARY STATISTICS:")
        print(f"Total strikes analyzed: {summary['total_strikes_analyzed']}")
        print(f"Strikes with 50%+ reduction: {summary['strikes_with_50percent_reduction']}")
        print(f"Strikes without 50%+ reduction: {summary['strikes_without_50percent_reduction']}")
        
        if summary['strikes_with_50percent_reduction'] > 0:
            success_rate = (summary['strikes_with_50percent_reduction'] / summary['total_strikes_analyzed']) * 100
            print(f"Success rate: {success_rate:.1f}%")
            print(f"Average reduction percentage: {summary['avg_reduction_percentage']:.2f}%")
            print(f"Average days to 50% reduction: {summary['avg_days_to_reduction']:.1f} days")
            print(f"Fastest 50% reduction: {summary['min_days_to_reduction']} days")
            print(f"Slowest 50% reduction: {summary['max_days_to_reduction']} days")
        else:
            print("❌ No strikes achieved 50% reduction")
    
    print(f"\n📋 DETAILED RESULTS:")
    if not detailed_df.empty:
        print(detailed_df.to_string(index=False))
        
        # Separate winners and losers
        winners = detailed_df[detailed_df['reduction_found'] == True]
        losers = detailed_df[detailed_df['reduction_found'] == False]
        
        if not winners.empty:
            print(f"\n🎯 WINNERS (50%+ Reduction Achieved): {len(winners)} strikes")
            for _, row in winners.iterrows():
                print(f"  ✅ {row['symbol']} {row['strike_price']:.0f} {row['option_type']}: "
                      f"{row['reduction_percentage']:.1f}% reduction in {row['days_to_reduction']} days "
                      f"(₹{row['base_close_price']:.2f} → ₹{row['reduced_price']:.2f})")
        
        if not losers.empty:
            print(f"\n❌ NO 50% REDUCTION: {len(losers)} strikes")
            for _, row in losers.iterrows():
                print(f"  ❌ {row['symbol']} {row['strike_price']:.0f} {row['option_type']}: "
                      f"Max reduction {row['max_reduction_percentage']:.1f}% "
                      f"(₹{row['base_close_price']:.2f})")
    else:
        print("No detailed results found")
    
    print("="*100)

def main():
    """Main function to execute 50% reduction analysis."""
    print("🎯 STEP 5 EXTENSION: 50% STRIKE PRICE REDUCTION ANALYSIS")
    print("="*70)
    print("Analyzing Step05_strikepriceAnalysisderived records for 50% price reduction")
    print("Comparing base prices (3rd Feb 2025) with subsequent trading data (4th Feb onwards)")
    print("="*70)
    
    try:
        conn = get_database_connection()
        logger.info("Database connection established")
        
        # Create the reduction analysis table
        create_50percent_reduction_table(conn)
        
        # Get base data from Step05 table
        base_data = get_step05_base_data(conn)
        
        if base_data.empty:
            logger.error("No base data found in Step05_strikepriceAnalysisderived table")
            return
        
        print(f"\n📊 PROCESSING {len(base_data)} STRIKE RECORDS FOR 50% REDUCTION ANALYSIS")
        
        # Process each strike record
        all_analysis_results = []
        
        for index, base_record in base_data.iterrows():
            try:
                result = process_single_strike_analysis(conn, base_record)
                all_analysis_results.append(result)
                
                # Progress indicator
                progress = ((index + 1) / len(base_data)) * 100
                symbol = base_record['Symbol']
                strike = base_record['Strike_price']
                option_type = base_record['option_type']
                status = "✅" if result['reduction_found'] else "❌"
                
                print(f"Progress: {progress:.1f}% - {symbol} {strike:.0f} {option_type}: {status}")
                
            except Exception as e:
                logger.error(f"Error processing record {index}: {e}")
                continue
        
        # Insert all results into database
        inserted_count = insert_reduction_analysis_results(conn, all_analysis_results)
        
        print(f"\n🎯 ANALYSIS COMPLETED!")
        print(f"Records processed: {len(base_data)}")
        print(f"Results stored: {inserted_count}")
        
        # Generate comprehensive summary report
        generate_reduction_summary_report(conn)
        
        print(f"\n✅ 50% reduction analysis completed successfully!")
        print(f"📋 Results stored in Step05_50percent_reduction_analysis table")
        
    except Exception as e:
        logger.error(f"50% reduction analysis error: {e}")
        print(f"❌ Error: {e}")
    
    finally:
        if 'conn' in locals():
            conn.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    main()