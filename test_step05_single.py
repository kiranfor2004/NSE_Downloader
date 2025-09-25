"""
Test step05 analysis with one symbol to debug issues
"""
import pyodbc
import pandas as pd
import logging
from datetime import datetime

# Setup logging without emojis
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('step05_test.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Database connection
CONNECTION_STRING = (
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=SRIKIRANREDDY\\SQLEXPRESS;'
    'DATABASE=master;'
    'Trusted_Connection=yes;'
)

def get_connection():
    """Get database connection"""
    return pyodbc.connect(CONNECTION_STRING, timeout=30)

def test_single_symbol():
    """Test with one symbol that has both delivery and F&O data"""
    
    # Get one delivery record from February
    query_delivery = """
    SELECT TOP 1
        symbol,
        analysis_month,
        peak_date,
        close_price
    FROM step02_monthly_analysis 
    WHERE analysis_type = 'DELIVERY' 
    AND analysis_month >= '2025-02'
    AND symbol = 'RELIANCE'
    ORDER BY peak_date
    """
    
    conn = get_connection()
    delivery_df = pd.read_sql(query_delivery, conn)
    
    if len(delivery_df) == 0:
        logger.error("No delivery data found")
        return
    
    record = delivery_df.iloc[0]
    symbol = record['symbol']
    delivery_date = record['peak_date']
    closing_price = record['close_price']
    
    logger.info(f"Testing symbol: {symbol}")
    logger.info(f"Delivery date: {delivery_date}")
    logger.info(f"Closing price: {closing_price}")
    
    # Convert date format for F&O query
    if isinstance(delivery_date, str):
        if '-' in delivery_date:
            target_date = delivery_date.replace('-', '')
        else:
            target_date = delivery_date
    else:
        # delivery_date is a datetime.date object
        target_date = delivery_date.strftime('%Y%m%d')
    
    logger.info(f"Target F&O date: {target_date}")
    
    # Query F&O data
    query_fo = """
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
    
    fo_df = pd.read_sql(query_fo, conn, params=[symbol, target_date])
    
    if len(fo_df) == 0:
        logger.warning(f"No F&O data found for {symbol} on or after {target_date}")
        return
    
    # Get data for the earliest available date
    earliest_date = fo_df['trade_date'].min()
    df_filtered = fo_df[fo_df['trade_date'] == earliest_date]
    
    logger.info(f"Found {len(df_filtered)} F&O records for {symbol} on {earliest_date}")
    
    # Test finding nearest strikes for CE options
    ce_options = df_filtered[df_filtered['option_type'] == 'CE']
    
    if len(ce_options) == 0:
        logger.warning("No CE options found")
        return
    
    logger.info(f"CE options count: {len(ce_options)}")
    logger.info(f"Strike price range: {ce_options['strike_price'].min()} - {ce_options['strike_price'].max()}")
    logger.info(f"Target closing price: {closing_price}")
    
    # Sort by strike price for proximity calculation
    ce_sorted = ce_options.sort_values('strike_price')
    
    # Find strikes around the closing price
    above_strikes = ce_sorted[ce_sorted['strike_price'] >= closing_price].head(3)
    below_strikes = ce_sorted[ce_sorted['strike_price'] < closing_price].tail(3)
    
    logger.info(f"Strikes above closing price: {len(above_strikes)}")
    if len(above_strikes) > 0:
        logger.info(f"Above strikes: {above_strikes['strike_price'].tolist()}")
    
    logger.info(f"Strikes below closing price: {len(below_strikes)}")
    if len(below_strikes) > 0:
        logger.info(f"Below strikes: {below_strikes['strike_price'].tolist()}")
    
    conn.close()
    logger.info("Test completed successfully!")

if __name__ == "__main__":
    test_single_symbol()