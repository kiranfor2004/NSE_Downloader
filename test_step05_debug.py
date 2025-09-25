import pyodbc
import pandas as pd
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_database_connection():
    """Get database connection"""
    try:
        conn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            'SERVER=SRIKIRANREDDY\\SQLEXPRESS;'
            'DATABASE=master;'
            'Trusted_Connection=yes;',
            timeout=30
        )
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        return None

def test_fo_query():
    """Test F&O data query for a specific symbol and date"""
    conn = get_database_connection()
    if not conn:
        return
    
    symbol = 'RELIANCE'
    target_date = '20250221'
    
    logger.info(f"Testing F&O data query for {symbol} on {target_date}")
    
    query = """
    SELECT 
        trade_date,
        symbol,
        strike_price,
        option_type,
        close_price,
        expiry_date
    FROM step04_fo_udiff_daily 
    WHERE symbol = ? 
    AND trade_date >= ?
    ORDER BY trade_date ASC, option_type, strike_price
    """
    
    try:
        # Test the raw query first
        df = pd.read_sql(query, conn, params=[symbol, target_date])
        logger.info(f"Raw query returned {len(df)} rows")
        logger.info(f"Columns: {df.columns.tolist()}")
        
        if len(df) > 0:
            logger.info(f"Sample data:\n{df.head()}")
            
            # Test specific option type filtering
            ce_options = df[df['option_type'] == 'CE']
            pe_options = df[df['option_type'] == 'PE']
            
            logger.info(f"CE options: {len(ce_options)}, PE options: {len(pe_options)}")
            
            if len(ce_options) > 0:
                logger.info(f"CE strikes range: {ce_options['strike_price'].min()} - {ce_options['strike_price'].max()}")
            
            if len(pe_options) > 0:
                logger.info(f"PE strikes range: {pe_options['strike_price'].min()} - {pe_options['strike_price'].max()}")
        
    except Exception as e:
        logger.error(f"Query failed: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    test_fo_query()