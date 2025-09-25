"""
January-February 2025 F&O Analysis
Complete strike price and reduction analysis using available January and February data
"""
import pyodbc
import pandas as pd
import json
import logging
from datetime import datetime, timedelta

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('jan_feb_fo_analysis.log'),
        logging.StreamHandler()
    ]
)

class JanuaryFebruaryFOAnalysis:
    def __init__(self):
        # Load database configuration
        with open('database_config.json', 'r') as f:
            self.db_config = json.load(f)
        
        self.conn_string = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={self.db_config['server']};"
            f"DATABASE={self.db_config['database']};"
            f"Trusted_Connection=yes;"
        )
        
        logging.info("January-February F&O Analysis initialized")
    
    def get_connection(self):
        """Get database connection"""
        return pyodbc.connect(self.conn_string)
    
    def analyze_jan_feb_equity_changes(self):
        """Analyze equity price changes from January to February"""
        logging.info("Analyzing January to February equity price changes...")
        
        conn = self.get_connection()
        
        # Get equity comparison data (Jan vs Feb)
        query = """
        SELECT 
            symbol,
            current_trade_date as feb_date,
            current_close_price as feb_close,
            previous_baseline_date as jan_date,
            previous_close_price as jan_close,
            delivery_increase_pct as delivery_change_pct,
            category,
            index_name,
            (current_close_price - previous_close_price) as price_change,
            ((current_close_price - previous_close_price) / previous_close_price) * 100 as price_change_pct
        FROM step03_compare_monthvspreviousmonth
        WHERE comparison_type = 'FEB2025_vs_JAN2025_DELIVERY'
        ORDER BY ((current_close_price - previous_close_price) / previous_close_price) * 100 DESC
        """
        
        df = pd.read_sql(query, conn)
        conn.close()
        
        logging.info(f"Found {len(df)} symbols with January-February comparison data")
        
        # Show top gainers and losers
        print("\n📈 TOP 10 GAINERS (Jan to Feb 2025):")
        print("=" * 80)
        top_gainers = df.head(10)
        for _, row in top_gainers.iterrows():
            print(f"{row['symbol']:12} | Jan: ₹{row['jan_close']:8.2f} → Feb: ₹{row['feb_close']:8.2f} | Change: {row['price_change_pct']:+6.2f}% | Delivery: {row['delivery_change_pct']:+6.2f}%")
        
        print("\n📉 TOP 10 LOSERS (Jan to Feb 2025):")
        print("=" * 80)
        top_losers = df.tail(10)
        for _, row in top_losers.iterrows():
            print(f"{row['symbol']:12} | Jan: ₹{row['jan_close']:8.2f} → Feb: ₹{row['feb_close']:8.2f} | Change: {row['price_change_pct']:+6.2f}% | Delivery: {row['delivery_change_pct']:+6.2f}%")
        
        return df
    
    def get_fo_symbols_with_jan_feb_data(self):
        """Get symbols that have both equity changes and F&O data"""
        logging.info("Finding symbols with both equity and F&O data...")
        
        conn = self.get_connection()
        
        query = """
        SELECT DISTINCT 
            e.symbol,
            e.current_close_price as feb_equity_price,
            e.previous_close_price as jan_equity_price,
            e.price_change_pct,
            e.delivery_change_pct,
            COUNT(f.symbol) as fo_contracts_count
        FROM (
            SELECT 
                symbol,
                current_close_price,
                previous_close_price,
                ((current_close_price - previous_close_price) / previous_close_price) * 100 as price_change_pct,
                delivery_increase_pct as delivery_change_pct
            FROM step03_compare_monthvspreviousmonth
            WHERE comparison_type = 'FEB2025_vs_JAN2025_DELIVERY'
        ) e
        INNER JOIN step04_fo_udiff_daily f ON e.symbol = f.symbol
        WHERE LEFT(f.trade_date, 6) = '202502'  -- February F&O data
        GROUP BY e.symbol, e.current_close_price, e.previous_close_price, e.price_change_pct, e.delivery_change_pct
        HAVING COUNT(f.symbol) >= 10  -- At least 10 F&O contracts
        ORDER BY e.price_change_pct DESC
        """
        
        df = pd.read_sql(query, conn)
        conn.close()
        
        logging.info(f"Found {len(df)} symbols with both equity and F&O data")
        
        print(f"\n🎯 SYMBOLS WITH BOTH EQUITY & F&O DATA (Jan-Feb 2025):")
        print("=" * 100)
        print(f"{'Symbol':12} | {'Jan Price':>10} | {'Feb Price':>10} | {'Price Δ%':>8} | {'Delivery Δ%':>11} | {'F&O Contracts':>12}")
        print("=" * 100)
        
        for _, row in df.head(20).iterrows():
            print(f"{row['symbol']:12} | {row['jan_equity_price']:10.2f} | {row['feb_equity_price']:10.2f} | {row['price_change_pct']:+7.2f}% | {row['delivery_change_pct']:+10.2f}% | {row['fo_contracts_count']:12}")
        
        return df
    
    def analyze_jan_feb_fo_strikes(self, symbol_list=None):
        """Analyze F&O strike patterns for January-February period"""
        logging.info("Analyzing F&O strike patterns for Jan-Feb period...")
        
        if symbol_list is None:
            # Get top symbols with significant price changes
            equity_df = self.get_fo_symbols_with_jan_feb_data()
            symbol_list = equity_df.head(5)['symbol'].tolist()
        
        conn = self.get_connection()
        
        results = []
        
        for symbol in symbol_list:
            logging.info(f"Analyzing F&O strikes for {symbol}...")
            
            # Get equity price info
            equity_query = """
            SELECT 
                symbol,
                current_close_price as feb_price,
                previous_close_price as jan_price,
                ((current_close_price - previous_close_price) / previous_close_price) * 100 as price_change_pct
            FROM step03_compare_monthvspreviousmonth
            WHERE symbol = ? AND comparison_type = 'FEB2025_vs_JAN2025_DELIVERY'
            """
            
            equity_df = pd.read_sql(equity_query, conn, params=[symbol])
            
            if equity_df.empty:
                continue
                
            equity_info = equity_df.iloc[0]
            
            # Get F&O strike distribution around these prices
            fo_query = """
            SELECT 
                symbol,
                strike_price,
                option_type,
                instrument,
                COUNT(*) as trading_days,
                AVG(close_price) as avg_close_price,
                MIN(close_price) as min_close_price,
                MAX(close_price) as max_close_price,
                SUM(contracts_traded) as total_volume,
                AVG(open_interest) as avg_open_interest
            FROM step04_fo_udiff_daily
            WHERE symbol = ?
              AND LEFT(trade_date, 6) = '202502'  -- February data
              AND instrument IN ('OPTIDX', 'OPTSTK')
              AND strike_price > 0
            GROUP BY symbol, strike_price, option_type, instrument
            ORDER BY strike_price, option_type
            """
            
            fo_df = pd.read_sql(fo_query, conn, params=[symbol])
            
            if not fo_df.empty:
                # Analyze strikes around Jan and Feb prices
                jan_price = float(equity_info['jan_price'])
                feb_price = float(equity_info['feb_price'])
                price_change_pct = float(equity_info['price_change_pct'])
                
                # Find strikes near Jan price
                jan_strikes = fo_df[
                    (fo_df['strike_price'] >= jan_price * 0.95) & 
                    (fo_df['strike_price'] <= jan_price * 1.05)
                ].copy()
                
                # Find strikes near Feb price  
                feb_strikes = fo_df[
                    (fo_df['strike_price'] >= feb_price * 0.95) & 
                    (fo_df['strike_price'] <= feb_price * 1.05)
                ].copy()
                
                result = {
                    'symbol': symbol,
                    'jan_price': jan_price,
                    'feb_price': feb_price,
                    'price_change_pct': price_change_pct,
                    'total_fo_strikes': len(fo_df),
                    'jan_area_strikes': len(jan_strikes),
                    'feb_area_strikes': len(feb_strikes),
                    'jan_strikes_data': jan_strikes,
                    'feb_strikes_data': feb_strikes,
                    'all_strikes_data': fo_df
                }
                
                results.append(result)
                
                print(f"\n📊 {symbol} F&O Analysis (Jan-Feb 2025):")
                print(f"   Jan Price: ₹{jan_price:.2f} → Feb Price: ₹{feb_price:.2f} ({price_change_pct:+.2f}%)")
                print(f"   Total F&O strikes: {len(fo_df)}")
                print(f"   Strikes near Jan price: {len(jan_strikes)}")
                print(f"   Strikes near Feb price: {len(feb_strikes)}")
                
                if not jan_strikes.empty:
                    print(f"   Jan area volume: {jan_strikes['total_volume'].sum():,}")
                if not feb_strikes.empty:
                    print(f"   Feb area volume: {feb_strikes['total_volume'].sum():,}")
        
        conn.close()
        return results
    
    def create_jan_feb_strike_analysis_table(self):
        """Create table for January-February strike analysis results"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Drop table if exists
        try:
            cursor.execute("DROP TABLE IF EXISTS jan_feb_strike_analysis")
            logging.info("Dropped existing jan_feb_strike_analysis table")
        except:
            pass
        
        # Create new table
        create_table_sql = """
        CREATE TABLE jan_feb_strike_analysis (
            id INT IDENTITY(1,1) PRIMARY KEY,
            symbol VARCHAR(50),
            jan_equity_price DECIMAL(10,2),
            feb_equity_price DECIMAL(10,2),
            equity_price_change_pct DECIMAL(8,2),
            strike_price DECIMAL(10,2),
            option_type VARCHAR(5),
            instrument VARCHAR(10),
            feb_avg_close_price DECIMAL(10,2),
            feb_total_volume BIGINT,
            feb_avg_open_interest DECIMAL(15,2),
            strike_position VARCHAR(20),  -- 'near_jan_price', 'near_feb_price', 'between', 'outside'
            distance_from_jan_pct DECIMAL(8,2),
            distance_from_feb_pct DECIMAL(8,2),
            analysis_type VARCHAR(50),
            created_at DATETIME DEFAULT GETDATE()
        )
        """
        
        cursor.execute(create_table_sql)
        conn.commit()
        conn.close()
        
        logging.info("Created jan_feb_strike_analysis table")
    
    def run_comprehensive_jan_feb_analysis(self):
        """Run complete January-February F&O analysis"""
        logging.info("Starting comprehensive January-February F&O analysis...")
        
        print("🎯 JANUARY-FEBRUARY 2025 F&O ANALYSIS")
        print("=" * 60)
        
        # 1. Analyze equity changes
        equity_df = self.analyze_jan_feb_equity_changes()
        
        # 2. Get symbols with F&O data
        fo_symbols_df = self.get_fo_symbols_with_jan_feb_data()
        
        # 3. Analyze F&O strikes for top movers
        fo_results = self.analyze_jan_feb_fo_strikes()
        
        # 4. Create summary table
        self.create_jan_feb_strike_analysis_table()
        
        # 5. Save detailed results
        if fo_results:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            insert_sql = """
            INSERT INTO jan_feb_strike_analysis 
            (symbol, jan_equity_price, feb_equity_price, equity_price_change_pct,
             strike_price, option_type, instrument, feb_avg_close_price, 
             feb_total_volume, feb_avg_open_interest, strike_position,
             distance_from_jan_pct, distance_from_feb_pct, analysis_type)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            records_inserted = 0
            
            for result in fo_results:
                symbol = result['symbol']
                jan_price = result['jan_price']
                feb_price = result['feb_price']
                price_change_pct = result['price_change_pct']
                
                # Insert all strikes for this symbol
                for _, strike_row in result['all_strikes_data'].iterrows():
                    strike_price = float(strike_row['strike_price'])
                    
                    # Determine position relative to Jan/Feb prices
                    jan_distance_pct = ((strike_price - jan_price) / jan_price) * 100
                    feb_distance_pct = ((strike_price - feb_price) / feb_price) * 100
                    
                    if abs(jan_distance_pct) <= 5:
                        position = 'near_jan_price'
                    elif abs(feb_distance_pct) <= 5:
                        position = 'near_feb_price'
                    elif min(jan_price, feb_price) <= strike_price <= max(jan_price, feb_price):
                        position = 'between_jan_feb'
                    else:
                        position = 'outside_range'
                    
                    record = (
                        symbol, jan_price, feb_price, price_change_pct,
                        strike_price, strike_row['option_type'], strike_row['instrument'],
                        float(strike_row['avg_close_price']), int(strike_row['total_volume']),
                        float(strike_row['avg_open_interest']), position,
                        jan_distance_pct, feb_distance_pct, 'JAN_FEB_2025_COMPREHENSIVE'
                    )
                    
                    cursor.execute(insert_sql, record)
                    records_inserted += 1
            
            conn.commit()
            conn.close()
            
            logging.info(f"Inserted {records_inserted} strike analysis records")
            print(f"\n✅ Inserted {records_inserted} detailed strike records into jan_feb_strike_analysis table")
        
        # 6. Generate summary report
        self.generate_summary_report()
        
        print(f"\n🎉 January-February 2025 F&O analysis completed!")
        print(f"📊 Check jan_feb_strike_analysis table for detailed results")
        print(f"📝 Check jan_feb_fo_analysis.log for detailed logs")
    
    def generate_summary_report(self):
        """Generate summary report of Jan-Feb analysis"""
        conn = self.get_connection()
        
        try:
            # Summary statistics
            summary_query = """
            SELECT 
                COUNT(DISTINCT symbol) as total_symbols,
                COUNT(*) as total_strike_records,
                AVG(equity_price_change_pct) as avg_equity_change_pct,
                COUNT(CASE WHEN strike_position = 'near_jan_price' THEN 1 END) as strikes_near_jan,
                COUNT(CASE WHEN strike_position = 'near_feb_price' THEN 1 END) as strikes_near_feb,
                COUNT(CASE WHEN strike_position = 'between_jan_feb' THEN 1 END) as strikes_between,
                SUM(feb_total_volume) as total_feb_volume
            FROM jan_feb_strike_analysis
            WHERE analysis_type = 'JAN_FEB_2025_COMPREHENSIVE'
            """
            
            summary_df = pd.read_sql(summary_query, conn)
            
            if not summary_df.empty:
                summary = summary_df.iloc[0]
                
                print(f"\n📈 JANUARY-FEBRUARY 2025 ANALYSIS SUMMARY")
                print(f"=" * 50)
                print(f"Total symbols analyzed: {summary['total_symbols']}")
                print(f"Total F&O strike records: {summary['total_strike_records']:,}")
                print(f"Average equity price change: {summary['avg_equity_change_pct']:.2f}%")
                print(f"Strikes near Jan prices: {summary['strikes_near_jan']:,}")
                print(f"Strikes near Feb prices: {summary['strikes_near_feb']:,}")
                print(f"Strikes between Jan-Feb: {summary['strikes_between']:,}")
                print(f"Total Feb F&O volume: {summary['total_feb_volume']:,}")
        
        except Exception as e:
            logging.error(f"Error generating summary: {e}")
        finally:
            conn.close()

def main():
    analyzer = JanuaryFebruaryFOAnalysis()
    analyzer.run_comprehensive_jan_feb_analysis()

if __name__ == "__main__":
    main()