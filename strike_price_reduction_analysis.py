#!/usr/bin/env python3
"""
Strike Price Reduction Analysis
===============================

Purpose: Analyze test_current_month_strikes table to find records where there's a 50% reduction
         in strike_price for each symbol based on trade date, and store those records in a new table.

Process:
1. Read data from test_current_month_strikes table
2. For each symbol, analyze strike prices across trade dates
3. Identify records with 50% or more reduction in strike price
4. Store the qualifying records in a new table: strike_price_reduction_analysis

Logic:
- Compare strike prices across different trade dates for the same symbol
- Calculate percentage reduction from previous/highest strike price
- Flag records with >= 50% reduction
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
        logging.FileHandler('strike_price_reduction_analysis.log'),
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
    ORDER BY symbol, fo_trade_date, fo_strike_price
    """
    
    with get_connection() as conn:
        df = pd.read_sql(query, conn)
    
    logger.info(f"Retrieved {len(df)} records from test_current_month_strikes")
    return df

def analyze_strike_price_reduction(df):
    """
    Analyze strike price patterns to find 50% reductions
    """
    logger.info("Analyzing strike price reductions...")
    
    reduction_records = []
    
    # Group by symbol to analyze each symbol separately
    for symbol, symbol_group in df.groupby('symbol'):
        logger.info(f"Analyzing strike price reduction for {symbol}")
        
        # Sort by trade date and strike price
        symbol_data = symbol_group.sort_values(['fo_trade_date', 'fo_strike_price'])
        
        # Get unique trade dates for this symbol
        trade_dates = sorted(symbol_data['fo_trade_date'].unique())
        
        if len(trade_dates) < 2:
            logger.info(f"  {symbol}: Only one trade date found, skipping reduction analysis")
            continue
        
        logger.info(f"  {symbol}: Analyzing across {len(trade_dates)} trade dates")
        
        # For each trade date, compare with previous trade dates
        for i in range(1, len(trade_dates)):
            current_date = trade_dates[i]
            previous_date = trade_dates[i-1]
            
            # Get records for current and previous dates
            current_records = symbol_data[symbol_data['fo_trade_date'] == current_date]
            previous_records = symbol_data[symbol_data['fo_trade_date'] == previous_date]
            
            # Get the strike price ranges
            current_strikes = set(current_records['fo_strike_price'].values)
            previous_strikes = set(previous_records['fo_strike_price'].values)
            
            # Find the maximum previous strike price as baseline
            max_previous_strike = previous_records['fo_strike_price'].max()
            min_current_strike = current_records['fo_strike_price'].min()
            
            # Calculate reduction percentage
            if max_previous_strike > 0:
                reduction_percentage = ((max_previous_strike - min_current_strike) / max_previous_strike) * 100
                
                logger.info(f"    {current_date} vs {previous_date}: Max previous strike: {max_previous_strike}, Min current strike: {min_current_strike}")
                logger.info(f"    Reduction: {reduction_percentage:.2f}%")
                
                # Check if reduction is 50% or more
                if reduction_percentage >= 50:
                    logger.info(f"    *** SIGNIFICANT REDUCTION FOUND: {reduction_percentage:.2f}% ***")
                    
                    # Add all current records for this symbol/date combination
                    for idx, record in current_records.iterrows():
                        reduction_record = record.copy()
                        # Add reduction analysis fields
                        reduction_record['reduction_analysis_date'] = current_date
                        reduction_record['baseline_date'] = previous_date
                        reduction_record['baseline_max_strike'] = max_previous_strike
                        reduction_record['current_min_strike'] = min_current_strike
                        reduction_record['reduction_percentage'] = reduction_percentage
                        reduction_record['reduction_amount'] = max_previous_strike - min_current_strike
                        reduction_record['analysis_type'] = '50_percent_reduction'
                        
                        reduction_records.append(reduction_record)
            
            # Also check individual strike-to-strike reductions
            for current_strike in current_strikes:
                for previous_strike in previous_strikes:
                    if previous_strike > 0:
                        individual_reduction = ((previous_strike - current_strike) / previous_strike) * 100
                        
                        if individual_reduction >= 50:
                            logger.info(f"    Individual strike reduction: {previous_strike} -> {current_strike} ({individual_reduction:.2f}%)")
                            
                            # Get records for this specific strike comparison
                            current_strike_records = current_records[current_records['fo_strike_price'] == current_strike]
                            
                            for idx, record in current_strike_records.iterrows():
                                reduction_record = record.copy()
                                # Add individual reduction analysis fields
                                reduction_record['reduction_analysis_date'] = current_date
                                reduction_record['baseline_date'] = previous_date
                                reduction_record['baseline_strike'] = previous_strike
                                reduction_record['current_strike'] = current_strike
                                reduction_record['individual_reduction_percentage'] = individual_reduction
                                reduction_record['individual_reduction_amount'] = previous_strike - current_strike
                                reduction_record['analysis_type'] = 'individual_strike_reduction'
                                
                                reduction_records.append(reduction_record)
    
    logger.info(f"Found {len(reduction_records)} records with significant strike price reductions")
    return reduction_records

def create_reduction_analysis_table():
    """
    Create table to store strike price reduction analysis
    """
    logger.info("Creating strike_price_reduction_analysis table...")
    
    create_table_query = """
    IF OBJECT_ID('strike_price_reduction_analysis', 'U') IS NOT NULL
        DROP TABLE strike_price_reduction_analysis
    
    CREATE TABLE strike_price_reduction_analysis (
        id INT IDENTITY(1,1) PRIMARY KEY,
        
        -- Original fields from test_current_month_strikes
        original_id INT,
        symbol NVARCHAR(50),
        target_price DECIMAL(10,2),
        target_date DATE,
        strike_position NVARCHAR(10),
        price_difference DECIMAL(10,2),
        percentage_difference DECIMAL(8,4),
        
        -- All F&O fields
        fo_id BIGINT,
        fo_trade_date NVARCHAR(8),
        fo_symbol NVARCHAR(50),
        fo_instrument NVARCHAR(50),
        fo_expiry_date NVARCHAR(8),
        fo_strike_price DECIMAL(10,2),
        fo_option_type NVARCHAR(2),
        fo_open_price DECIMAL(10,2),
        fo_high_price DECIMAL(10,2),
        fo_low_price DECIMAL(10,2),
        fo_close_price DECIMAL(10,2),
        fo_settle_price DECIMAL(10,2),
        fo_contracts_traded BIGINT,
        fo_value_in_lakh DECIMAL(18,4),
        fo_open_interest BIGINT,
        fo_change_in_oi BIGINT,
        fo_underlying NVARCHAR(50),
        fo_source_file NVARCHAR(100),
        fo_created_at DATETIME,
        
        -- Additional F&O fields
        fo_BizDt NVARCHAR(20),
        fo_Sgmt NVARCHAR(20),
        fo_Src NVARCHAR(20),
        fo_FininstrmActlXpryDt NVARCHAR(20),
        fo_FinInstrmId NVARCHAR(50),
        fo_ISIN NVARCHAR(50),
        fo_SctySrs NVARCHAR(20),
        fo_FinInstrmNm NVARCHAR(100),
        fo_LastPric DECIMAL(10,2),
        fo_PrvsClsgPric DECIMAL(10,2),
        fo_UndrlygPric DECIMAL(10,2),
        fo_TtlNbOfTxsExctd BIGINT,
        fo_SsnId NVARCHAR(20),
        fo_NewBrdLotQty BIGINT,
        fo_Rmks NVARCHAR(100),
        fo_Rsvd1 NVARCHAR(50),
        fo_Rsvd2 NVARCHAR(50),
        fo_Rsvd3 NVARCHAR(50),
        fo_Rsvd4 NVARCHAR(50),
        
        -- Reduction analysis fields
        reduction_analysis_date NVARCHAR(8),
        baseline_date NVARCHAR(8),
        baseline_max_strike DECIMAL(10,2),
        current_min_strike DECIMAL(10,2),
        reduction_percentage DECIMAL(8,4),
        reduction_amount DECIMAL(10,2),
        baseline_strike DECIMAL(10,2),
        current_strike DECIMAL(10,2),
        individual_reduction_percentage DECIMAL(8,4),
        individual_reduction_amount DECIMAL(10,2),
        analysis_type NVARCHAR(50),
        
        created_at DATETIME DEFAULT GETDATE()
    )
    """
    
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
    
    logger.info("Strike price reduction analysis table created successfully")

def save_reduction_records(reduction_records):
    """
    Save reduction analysis records to the new table
    """
    if not reduction_records:
        logger.warning("No reduction records to save")
        return
    
    logger.info(f"Saving {len(reduction_records)} reduction analysis records...")
    
    # Convert to DataFrame for easier handling
    df = pd.DataFrame(reduction_records)
    
    # Define column mapping (original table columns to new table columns)
    column_mapping = {
        'id': 'original_id',
        # Keep all other columns as they are, they should match the new table structure
    }
    
    # Prepare insert query with all columns
    insert_columns = [
        'original_id', 'symbol', 'target_price', 'target_date', 'strike_position',
        'price_difference', 'percentage_difference',
        'fo_id', 'fo_trade_date', 'fo_symbol', 'fo_instrument', 'fo_expiry_date',
        'fo_strike_price', 'fo_option_type', 'fo_open_price', 'fo_high_price',
        'fo_low_price', 'fo_close_price', 'fo_settle_price', 'fo_contracts_traded',
        'fo_value_in_lakh', 'fo_open_interest', 'fo_change_in_oi', 'fo_underlying',
        'fo_source_file', 'fo_created_at', 'fo_BizDt', 'fo_Sgmt', 'fo_Src',
        'fo_FininstrmActlXpryDt', 'fo_FinInstrmId', 'fo_ISIN', 'fo_SctySrs',
        'fo_FinInstrmNm', 'fo_LastPric', 'fo_PrvsClsgPric', 'fo_UndrlygPric',
        'fo_TtlNbOfTxsExctd', 'fo_SsnId', 'fo_NewBrdLotQty', 'fo_Rmks',
        'fo_Rsvd1', 'fo_Rsvd2', 'fo_Rsvd3', 'fo_Rsvd4',
        'reduction_analysis_date', 'baseline_date', 'baseline_max_strike',
        'current_min_strike', 'reduction_percentage', 'reduction_amount',
        'baseline_strike', 'current_strike', 'individual_reduction_percentage',
        'individual_reduction_amount', 'analysis_type'
    ]
    
    # Create placeholders
    placeholders = ", ".join(["?" for _ in column_mapping])
    column_names = ", ".join(column_mapping.keys())
    
    # Simple insert for available fields
    insert_query = """
    INSERT INTO strike_price_reduction_analysis (
        symbol, fo_trade_date, fo_strike_price, fo_option_type, fo_close_price,
        reduction_analysis_date, baseline_date, reduction_percentage, 
        reduction_amount, analysis_type
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    with get_connection() as conn:
        cursor = conn.cursor()
        for record in reduction_records:
            cursor.execute(insert_query, (
                record.get('symbol', ''),
                record.get('fo_trade_date', ''),
                record.get('fo_strike_price', 0),
                record.get('fo_option_type', ''),
                record.get('fo_close_price', 0),
                record.get('reduction_analysis_date', ''),
                record.get('baseline_date', ''),
                record.get('reduction_percentage', 0),
                record.get('reduction_amount', 0),
                record.get('analysis_type', '')
            ))
        conn.commit()
    
    logger.info("Reduction analysis records saved successfully")

def generate_reduction_summary():
    """
    Generate summary report of strike price reductions
    """
    logger.info("Generating reduction analysis summary...")
    
    summary_query = """
    SELECT 
        symbol,
        analysis_type,
        COUNT(*) as record_count,
        AVG(reduction_percentage) as avg_reduction_pct,
        MIN(reduction_percentage) as min_reduction_pct,
        MAX(reduction_percentage) as max_reduction_pct,
        COUNT(DISTINCT fo_trade_date) as trade_dates_affected
    FROM strike_price_reduction_analysis
    GROUP BY symbol, analysis_type
    ORDER BY symbol, analysis_type
    """
    
    with get_connection() as conn:
        summary_df = pd.read_sql(summary_query, conn)
    
    print("\n" + "="*80)
    print("STRIKE PRICE REDUCTION ANALYSIS SUMMARY")
    print("="*80)
    print(f"Total symbols with 50%+ reductions: {summary_df['symbol'].nunique()}")
    print(f"Total reduction records: {summary_df['record_count'].sum()}")
    
    print("\nDetailed Analysis by Symbol:")
    print(summary_df.to_string(index=False))
    print("="*80)

def main():
    """
    Main execution function
    """
    logger.info("Starting Strike Price Reduction Analysis")
    
    try:
        # Step 1: Get test data
        test_data = get_test_data()
        
        if test_data.empty:
            logger.error("No data found in test_current_month_strikes table")
            return
        
        # Step 2: Analyze strike price reductions
        reduction_records = analyze_strike_price_reduction(test_data)
        
        if not reduction_records:
            logger.info("No significant strike price reductions (50%+) found")
            return
        
        # Step 3: Create reduction analysis table
        create_reduction_analysis_table()
        
        # Step 4: Save reduction records
        save_reduction_records(reduction_records)
        
        # Step 5: Generate summary
        generate_reduction_summary()
        
        logger.info("Strike price reduction analysis completed successfully!")
        
    except Exception as e:
        logger.error(f"Error in strike price reduction analysis: {str(e)}")
        raise

if __name__ == "__main__":
    main()