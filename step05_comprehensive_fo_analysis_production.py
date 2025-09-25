#!/usr/bin/env python3
"""
Step 05: Comprehensive Current Month Strike Price Analysis (Production Version)
=============================================================================

Purpose: Get current month cost price from step03_compare_monthvspreviousmonth
         and find nearest strike prices from step04_fo_udiff_daily with ALL F&O columns
         
Process:
1. Get current month data from step03_compare_monthvspreviousmonth  
2. For each symbol, find 7 nearest strikes (3 above + 3 below + 1 nearest)
3. Get both PE and CE options for each strike (7 × 2 = 14 records per symbol)
4. Include ALL columns from step04_fo_udiff_daily in the output
5. Store in production table: step05_comprehensive_fo_analysis

Expected Output: 14 records per symbol with ALL F&O data columns
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
        logging.FileHandler('step05_comprehensive_fo_analysis.log'),
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

def get_all_symbols_current_month_data():
    """
    Get current month cost price from step03_compare_monthvspreviousmonth for all symbols
    """
    logger.info("Getting current month data for all symbols from step03_compare_monthvspreviousmonth...")
    
    query = """
    SELECT 
        symbol,
        current_trade_date,
        current_close_price,
        previous_close_price,
        delivery_increase_abs,
        delivery_increase_pct,
        category,
        index_name
    FROM step03_compare_monthvspreviousmonth
    WHERE current_close_price IS NOT NULL
    ORDER BY symbol
    """
    
    with get_connection() as conn:
        df = pd.read_sql(query, conn)
    
    logger.info(f"Found current month data for {len(df)} symbols")
    return df

def find_comprehensive_strikes_for_symbol(symbol, target_price, target_date):
    """
    Find nearest 7 strikes with ALL F&O columns for the given symbol and month
    Returns exactly 14 records (7 strikes × 2 option types) with all F&O data
    """
    logger.info(f"Finding comprehensive strikes for {symbol} with target price Rs.{target_price:.2f}")
    
    # Convert date format from YYYY-MM-DD to YYYYMMDD if needed
    if isinstance(target_date, str) and '-' in target_date:
        target_month = target_date[:7]  # YYYY-MM
        trade_date_pattern = target_date.replace('-', '')[:6] + '%'  # YYYYMM%
    else:
        target_month = str(target_date)[:7]
        trade_date_pattern = str(target_date).replace('-', '')[:6] + '%'
    
    # Get ALL available F&O data for this symbol in the target month
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
        logger.warning(f"No F&O data found for {symbol} in month {target_month}")
        return []
    
    # Get the latest trade date available
    latest_trade_date = strikes_df['trade_date'].max()
    latest_data = strikes_df[strikes_df['trade_date'] == latest_trade_date]
    
    logger.info(f"Found {len(latest_data)} F&O records on latest date: {latest_trade_date}")
    
    # Get unique strikes
    available_strikes = sorted(latest_data['strike_price'].unique())
    
    if len(available_strikes) < 7:
        logger.warning(f"Only {len(available_strikes)} strikes available for {symbol}, need at least 7")
        
    # Separate strikes above and below target
    strikes_above = [s for s in available_strikes if s > target_price]
    strikes_below = [s for s in available_strikes if s < target_price]
    
    # Select strikes using the enhanced logic
    selected_strikes = []
    
    # Get 3 nearest strikes ABOVE target
    nearest_above = sorted(strikes_above)[:3]  # Closest above first
    selected_strikes.extend(nearest_above)
    
    # Get 3 nearest strikes BELOW target
    nearest_below = sorted(strikes_below, reverse=True)[:3]  # Closest below first
    selected_strikes.extend(nearest_below)
    
    # Get 1 overall nearest strike (if not already selected)
    all_distances = [(s, abs(s - target_price)) for s in available_strikes]
    all_distances.sort(key=lambda x: x[1])
    nearest_overall = all_distances[0][0]
    
    if nearest_overall not in selected_strikes:
        selected_strikes.append(nearest_overall)
    else:
        # Add the next nearest that's not already selected
        for strike, distance in all_distances[1:]:
            if strike not in selected_strikes:
                selected_strikes.append(strike)
                break
    
    logger.info(f"Selected {len(selected_strikes)} strikes: {sorted(selected_strikes)}")
    
    # Get comprehensive F&O data for all selected strikes (both PE and CE)
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
                    'analysis_symbol': symbol,
                    'target_price': float(target_price),
                    'target_date': target_date,
                    'strike_position': position,
                    'price_difference': float(strike) - float(target_price),
                    'percentage_difference': ((float(strike) - float(target_price)) / float(target_price)) * 100,
                }
                
                # Add ALL F&O columns from step04_fo_udiff_daily
                for col in record.index:
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
    
    logger.info(f"Generated {len(final_records)} comprehensive records: {pe_count} PE + {ce_count} CE")
    
    return final_records

def create_comprehensive_table(sample_record=None):
    """
    Create comprehensive production table with ALL F&O columns
    """
    logger.info("Creating comprehensive production table: step05_comprehensive_fo_analysis...")
    
    # Drop existing table first
    drop_table_query = """
    IF OBJECT_ID('step05_comprehensive_fo_analysis', 'U') IS NOT NULL
        DROP TABLE step05_comprehensive_fo_analysis
    """
    
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(drop_table_query)
        conn.commit()
    
    # Create table dynamically based on data structure
    if sample_record:
        logger.info("Creating comprehensive table based on actual data structure...")
        
        create_table_query = """
        CREATE TABLE step05_comprehensive_fo_analysis (
            id INT IDENTITY(1,1) PRIMARY KEY,
            analysis_symbol NVARCHAR(50),
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
                    if 'price' in key.lower() or 'value' in key.lower() or 'pric' in key.lower():
                        fo_columns.append(f"    {col_name} DECIMAL(18,4)")
                    else:
                        fo_columns.append(f"    {col_name} BIGINT")
                elif isinstance(value, str):
                    if len(value) <= 10:
                        fo_columns.append(f"    {col_name} NVARCHAR(20)")
                    elif len(value) <= 50:
                        fo_columns.append(f"    {col_name} NVARCHAR(50)")
                    else:
                        fo_columns.append(f"    {col_name} NVARCHAR(100)")
                else:
                    fo_columns.append(f"    {col_name} NVARCHAR(50)")
        
        create_table_query += ",\n".join(fo_columns)
        create_table_query += ",\n    created_at DATETIME DEFAULT GETDATE()\n)"
        
    else:
        # Fallback to basic comprehensive table structure
        create_table_query = """
        CREATE TABLE step05_comprehensive_fo_analysis (
            id INT IDENTITY(1,1) PRIMARY KEY,
            analysis_symbol NVARCHAR(50),
            target_price DECIMAL(10,2),
            target_date DATE,
            strike_position NVARCHAR(10),
            price_difference DECIMAL(10,2),
            percentage_difference DECIMAL(8,4),
            fo_trade_date NVARCHAR(8),
            fo_symbol NVARCHAR(50),
            fo_strike_price DECIMAL(10,2),
            fo_option_type NVARCHAR(2),
            fo_close_price DECIMAL(10,2),
            fo_open_interest BIGINT,
            fo_contracts_traded BIGINT,
            fo_expiry_date NVARCHAR(8),
            fo_open_price DECIMAL(10,2),
            fo_high_price DECIMAL(10,2),
            fo_low_price DECIMAL(10,2),
            fo_settle_price DECIMAL(10,2),
            fo_value_in_lakh DECIMAL(18,4),
            fo_change_in_oi BIGINT,
            fo_underlying NVARCHAR(50),
            created_at DATETIME DEFAULT GETDATE()
        )
        """
    
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
    
    logger.info("Comprehensive production table created successfully")

def save_comprehensive_records(records):
    """
    Save comprehensive records to the production table
    """
    if not records:
        logger.warning("No records to save")
        return
    
    logger.info(f"Saving {len(records)} comprehensive records to production table...")
    
    # Get all column names from the first record
    if records:
        sample_record = records[0]
        columns = list(sample_record.keys())
        
        # Create placeholders for the INSERT statement
        placeholders = ", ".join(["?" for _ in columns])
        column_names = ", ".join(columns)
        
        insert_query = f"""
        INSERT INTO step05_comprehensive_fo_analysis ({column_names})
        VALUES ({placeholders})
        """
        
        logger.info(f"Inserting {len(columns)} columns per record")
        
        with get_connection() as conn:
            cursor = conn.cursor()
            for record in records:
                values = [record[col] for col in columns]
                cursor.execute(insert_query, values)
            conn.commit()
        
        logger.info("Comprehensive records saved successfully to production table")

def generate_comprehensive_summary():
    """
    Generate comprehensive summary report
    """
    logger.info("Generating comprehensive analysis summary...")
    
    summary_query = """
    SELECT 
        analysis_symbol,
        COUNT(*) as total_records,
        COUNT(CASE WHEN fo_option_type = 'PE' THEN 1 END) as pe_count,
        COUNT(CASE WHEN fo_option_type = 'CE' THEN 1 END) as ce_count,
        COUNT(DISTINCT fo_strike_price) as unique_strikes,
        MIN(fo_strike_price) as min_strike,
        MAX(fo_strike_price) as max_strike,
        AVG(target_price) as avg_target_price,
        AVG(ABS(percentage_difference)) as avg_percentage_diff
    FROM step05_comprehensive_fo_analysis
    GROUP BY analysis_symbol
    ORDER BY analysis_symbol
    """
    
    with get_connection() as conn:
        summary_df = pd.read_sql(summary_query, conn)
    
    print("\n" + "="*100)
    print("STEP 05 COMPREHENSIVE F&O ANALYSIS SUMMARY REPORT")
    print("="*100)
    print(f"Total symbols analyzed: {len(summary_df)}")
    print(f"Total records generated: {summary_df['total_records'].sum()}")
    print(f"Expected records per symbol: 14 (7 PE + 7 CE)")
    
    # Check success rate
    perfect_symbols = len(summary_df[summary_df['total_records'] == 14])
    success_rate = (perfect_symbols / len(summary_df) * 100) if len(summary_df) > 0 else 0
    
    print(f"\nPerformance Metrics:")
    print(f"  Symbols with exactly 14 records: {perfect_symbols}/{len(summary_df)}")
    print(f"  Success rate: {success_rate:.1f}%")
    
    # Show sample of results
    print(f"\nSample Results (first 10 symbols):")
    print(summary_df.head(10).to_string(index=False))
    print("="*100)

def main():
    """
    Main execution function for comprehensive F&O analysis
    """
    logger.info("Starting Step 05: Comprehensive Current Month F&O Analysis")
    
    try:
        # Get current month data for all symbols
        symbols_data = get_all_symbols_current_month_data()
        
        if symbols_data.empty:
            logger.error("No current month data found. Exiting.")
            return
        
        logger.info(f"Processing {len(symbols_data)} symbols with comprehensive F&O analysis")
        
        # Process each symbol
        all_comprehensive_records = []
        
        for index, symbol_record in symbols_data.iterrows():
            try:
                symbol = symbol_record['symbol']
                target_price = symbol_record['current_close_price']
                target_date = symbol_record['current_trade_date']
                
                logger.info(f"Processing {symbol} ({index + 1}/{len(symbols_data)}) - Target: Rs.{target_price:.2f}")
                
                # Find comprehensive strikes with all F&O data
                comprehensive_records = find_comprehensive_strikes_for_symbol(symbol, target_price, target_date)
                all_comprehensive_records.extend(comprehensive_records)
                
                # Progress update
                if len(comprehensive_records) == 14:
                    status = "SUCCESS"
                else:
                    status = f"PARTIAL ({len(comprehensive_records)} records)"
                
                logger.info(f"Progress: {index + 1}/{len(symbols_data)} - {symbol}: {status}")
                
            except Exception as e:
                logger.error(f"Error processing {symbol_record['symbol']}: {str(e)}")
                continue
        
        # Create comprehensive table and save results
        if all_comprehensive_records:
            sample_record = all_comprehensive_records[0]
            create_comprehensive_table(sample_record)
            save_comprehensive_records(all_comprehensive_records)
            generate_comprehensive_summary()
            
            logger.info(f"Comprehensive analysis completed successfully!")
            logger.info(f"Total records generated: {len(all_comprehensive_records)}")
            logger.info(f"Stored in table: step05_comprehensive_fo_analysis")
        else:
            logger.error("No comprehensive records generated")
        
    except Exception as e:
        logger.error(f"Critical error in comprehensive analysis: {str(e)}")
        raise

if __name__ == "__main__":
    main()