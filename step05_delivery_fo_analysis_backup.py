#!/usr/bin/env python3
"""
Step 05: Delivery-based F&O Strike Price Analysis
==================================================

Purpose: For each symbol with highest delivery quantity data, find the 3 nearest 
         strike prices (up and down) from F&O data for both CE and PE options.

Logic:
1. Get highest delivery day data from step02_monthly_analysis
2. For each record, get the closing price on that delivery date
3. Find corresponding F&O data for that symbol and date (or nearest available date)
4. Find 3 strike prices above and below the closing price for both CE and PE
5. Store 14 records per symbol (7 CE + 7 PE)

Expected Output: 14 records per symbol per month
"""

import pyodbc
import pandas as pd
from decimal import Decimal
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('step05_delivery_fo_analysis.log'),
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

def get_highest_delivery_data():
    """
    Get highest delivery quantity data from step02_monthly_analysis
    """
    logger.info("📊 Fetching highest delivery data from step02_monthly_analysis...")
    
    query = """
    SELECT 
        symbol,
        analysis_month,
        peak_date as highest_delivery_date,
        deliv_qty as highest_delivery_qty,
        close_price as closing_price_on_delivery_date
    FROM step02_monthly_analysis
    WHERE analysis_type = 'DELIVERY'
    ORDER BY symbol, analysis_month
    """
    
    with get_connection() as conn:
        df = pd.read_sql(query, conn)
    
    logger.info(f"✅ Found {len(df)} highest delivery records")
    return df

def find_fo_data_for_date(symbol, target_date):
    """
    Find F&O data for a symbol on a specific date or nearest available date
    """
    logger.info(f"Finding F&O data for {symbol} on {target_date}")
    
    # Convert target_date to proper format (YYYYMMDD)
    if isinstance(target_date, str):
        if '-' in target_date:
            target_date = target_date.replace('-', '')
    else:
        # target_date is a datetime.date object
        target_date = target_date.strftime('%Y%m%d')
    
    query = """
    SELECT 
        trade_date,
        symbol,
        strike_price,
        option_type,
        close_price as fo_close_price
    FROM step04_fo_udiff_daily
    WHERE symbol = ?
        AND strike_price IS NOT NULL
        AND option_type IN ('CE', 'PE')
        AND trade_date >= ?
    ORDER BY trade_date, strike_price
    """
    
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query, (symbol, target_date))
        results = cursor.fetchall()
    
    if results:
        # Convert to DataFrame
        columns = ['trade_date', 'symbol', 'strike_price', 'option_type', 'fo_close_price']
        df = pd.DataFrame(results, columns=columns)
        
        # Get the earliest available date (closest to target date)
        earliest_date = df['trade_date'].min()
        df_filtered = df[df['trade_date'] == earliest_date]
        
        logger.info(f"✅ Found {len(df_filtered)} F&O records for {symbol} on {earliest_date}")
        return df_filtered
    else:
        logger.warning(f"⚠️ No F&O data found for {symbol} on or after {target_date}")
        return pd.DataFrame()

def find_nearest_strikes(closing_price, fo_data, option_type):
    """
    Find 3 nearest strike prices above and below the closing price for given option type
    """
    # Filter by option type
    option_data = fo_data[fo_data['option_type'] == option_type].copy()
    
    if option_data.empty:
        logger.warning(f"⚠️ No {option_type} options found")
        return []
    
    # Remove duplicates and sort by strike price
    option_data = option_data.drop_duplicates(subset=['strike_price']).sort_values('strike_price')
    
    strikes = option_data['strike_price'].tolist()
    closing_price = float(closing_price)
    
    results = []
    
    # Find exact match
    exact_strikes = [s for s in strikes if abs(float(s) - closing_price) < 0.01]
    if exact_strikes:
        exact_strike = exact_strikes[0]
        strike_data = option_data[option_data['strike_price'] == exact_strike].iloc[0]
        results.append({
            'strike_price': exact_strike,
            'strike_position': 'exact',
            'fo_close_price': strike_data['fo_close_price'],
            'trade_date': strike_data['trade_date']
        })
    
    # Find strikes below closing price (down_1, down_2, down_3)
    lower_strikes = [s for s in strikes if float(s) < closing_price]
    lower_strikes.sort(reverse=True)  # Sort descending to get nearest first
    
    for i, strike in enumerate(lower_strikes[:3]):
        strike_data = option_data[option_data['strike_price'] == strike].iloc[0]
        results.append({
            'strike_price': strike,
            'strike_position': f'down_{i+1}',
            'fo_close_price': strike_data['fo_close_price'],
            'trade_date': strike_data['trade_date']
        })
    
    # Find strikes above closing price (up_1, up_2, up_3)
    upper_strikes = [s for s in strikes if float(s) > closing_price]
    upper_strikes.sort()  # Sort ascending to get nearest first
    
    for i, strike in enumerate(upper_strikes[:3]):
        strike_data = option_data[option_data['strike_price'] == strike].iloc[0]
        results.append({
            'strike_price': strike,
            'strike_position': f'up_{i+1}',
            'fo_close_price': strike_data['fo_close_price'],
            'trade_date': strike_data['trade_date']
        })
    
    logger.info(f"✅ Found {len(results)} {option_type} strikes for closing price {closing_price}")
    return results

def process_symbol_delivery_data(delivery_record):
    """
    Process a single delivery record and find all F&O strikes
    """
    symbol = delivery_record['symbol']
    analysis_month = delivery_record['analysis_month']
    delivery_date = delivery_record['highest_delivery_date']
    delivery_qty = delivery_record['highest_delivery_qty']
    closing_price = delivery_record['closing_price_on_delivery_date']
    
    logger.info(f"Processing {symbol} - {analysis_month} (Delivery: {delivery_qty:,} on {delivery_date})")
    
    # Find F&O data for this symbol and date
    fo_data = find_fo_data_for_date(symbol, delivery_date)
    
    if fo_data.empty:
        logger.warning(f"⚠️ No F&O data found for {symbol} on {delivery_date}")
        return []
    
    all_records = []
    
    # Process CE options
    ce_strikes = find_nearest_strikes(closing_price, fo_data, 'CE')
    for strike_info in ce_strikes:
        record = {
            'source_symbol': symbol,
            'analysis_month': analysis_month,
            'highest_delivery_date': delivery_date,
            'highest_delivery_qty': delivery_qty,
            'closing_price_on_delivery_date': closing_price,
            'fo_trade_date': strike_info['trade_date'],
            'fo_symbol': symbol,
            'strike_price': strike_info['strike_price'],
            'option_type': 'CE',
            'fo_close_price': strike_info['fo_close_price'],
            'strike_position': strike_info['strike_position'],
            'price_difference': float(strike_info['strike_price']) - float(closing_price),
            'percentage_difference': ((float(strike_info['strike_price']) - float(closing_price)) / float(closing_price)) * 100
        }
        all_records.append(record)
    
    # Process PE options
    pe_strikes = find_nearest_strikes(closing_price, fo_data, 'PE')
    for strike_info in pe_strikes:
        record = {
            'source_symbol': symbol,
            'analysis_month': analysis_month,
            'highest_delivery_date': delivery_date,
            'highest_delivery_qty': delivery_qty,
            'closing_price_on_delivery_date': closing_price,
            'fo_trade_date': strike_info['trade_date'],
            'fo_symbol': symbol,
            'strike_price': strike_info['strike_price'],
            'option_type': 'PE',
            'fo_close_price': strike_info['fo_close_price'],
            'strike_position': strike_info['strike_position'],
            'price_difference': float(strike_info['strike_price']) - float(closing_price),
            'percentage_difference': ((float(strike_info['strike_price']) - float(closing_price)) / float(closing_price)) * 100
        }
        all_records.append(record)
    
    logger.info(f"✅ Generated {len(all_records)} records for {symbol} (CE: {len(ce_strikes)}, PE: {len(pe_strikes)})")
    return all_records

def save_to_database(records):
    """
    Save analysis records to step05_delivery_fo_analysis table
    """
    if not records:
        logger.warning("No records to save")
        return
    
    logger.info(f"💾 Saving {len(records)} records to step05_delivery_fo_analysis table...")
    
    # Clear existing data
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM step05_delivery_fo_analysis")
        conn.commit()
        logger.info("🗑️ Cleared existing data from step05_delivery_fo_analysis")
    
    # Insert new records
    insert_query = """
    INSERT INTO step05_delivery_fo_analysis (
        source_symbol, analysis_month, highest_delivery_date, highest_delivery_qty,
        closing_price_on_delivery_date, fo_trade_date, fo_symbol, strike_price,
        option_type, fo_close_price, strike_position, price_difference, percentage_difference
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    with get_connection() as conn:
        cursor = conn.cursor()
        for record in records:
            cursor.execute(insert_query, (
                record['source_symbol'],
                record['analysis_month'],
                record['highest_delivery_date'],
                record['highest_delivery_qty'],
                record['closing_price_on_delivery_date'],
                record['fo_trade_date'],
                record['fo_symbol'],
                record['strike_price'],
                record['option_type'],
                record['fo_close_price'],
                record['strike_position'],
                record['price_difference'],
                record['percentage_difference']
            ))
        conn.commit()
    
    logger.info(f"✅ Successfully saved {len(records)} records!")

def generate_summary_report():
    """
    Generate summary report of the analysis
    """
    logger.info("Generating summary report...")
    
    query = """
    SELECT 
        source_symbol,
        analysis_month,
        option_type,
        COUNT(*) as strike_count,
        AVG(ABS(percentage_difference)) as avg_percentage_diff
    FROM step05_delivery_fo_analysis
    GROUP BY source_symbol, analysis_month, option_type
    ORDER BY source_symbol, analysis_month, option_type
    """
    
    with get_connection() as conn:
        summary_df = pd.read_sql(query, conn)
    
    print("\n" + "="*80)
    print("📊 STEP 05 ANALYSIS SUMMARY REPORT")
    print("="*80)
    print(f"Total symbols analyzed: {summary_df['source_symbol'].nunique()}")
    print(f"Total months analyzed: {summary_df['analysis_month'].nunique()}")
    print(f"Total records generated: {summary_df['strike_count'].sum()}")
    print("\nBreakdown by Symbol and Option Type:")
    print(summary_df.to_string(index=False))
    print("="*80)

def main():
    """
    Main execution function
    """
    logger.info("🚀 Starting Step 05: Delivery-based F&O Strike Price Analysis")
    
    try:
        # Get highest delivery data
        delivery_data = get_highest_delivery_data()
        
        if delivery_data.empty:
            logger.error("❌ No delivery data found. Exiting.")
            return
        
        # Process each delivery record
        all_analysis_records = []
        
        for index, delivery_record in delivery_data.iterrows():
            try:
                records = process_symbol_delivery_data(delivery_record)
                all_analysis_records.extend(records)
                
                # Progress indicator
                progress = (index + 1) / len(delivery_data) * 100
                logger.info(f"📈 Progress: {progress:.1f}% ({index + 1}/{len(delivery_data)})")
                
            except Exception as e:
                logger.error(f"Error processing {delivery_record['symbol']}: {str(e)}")
                continue
        
        # Save results to database
        save_to_database(all_analysis_records)
        
        # Generate summary report
        generate_summary_report()
        
        logger.info("Step 05 analysis completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Critical error in Step 05 analysis: {str(e)}")
        raise

if __name__ == "__main__":
    main()