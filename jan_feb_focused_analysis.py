"""
January-February 2025 F&O Analysis - Focused on Available Data
Analysis using actual F&O data structure (STO, IDO, STF, IDF)
"""
import pyodbc
import pandas as pd
import json
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('jan_feb_focused_analysis.log'),
        logging.StreamHandler()
    ]
)

class JanFebFocusedAnalysis:
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
        
        logging.info("January-February Focused F&O Analysis initialized")
    
    def get_connection(self):
        """Get database connection"""
        return pyodbc.connect(self.conn_string)
    
    def analyze_top_equity_movers(self):
        """Find top equity movers from January to February"""
        logging.info("Finding top equity movers Jan-Feb...")
        
        conn = self.get_connection()
        
        query = """
        SELECT TOP 20
            symbol,
            current_close_price as feb_price,
            previous_close_price as jan_price,
            ((current_close_price - previous_close_price) / previous_close_price) * 100 as price_change_pct,
            delivery_increase_pct,
            current_trade_date as feb_date,
            previous_baseline_date as jan_date
        FROM step03_compare_monthvspreviousmonth
        WHERE comparison_type = 'FEB2025_vs_JAN2025_DELIVERY'
          AND ABS((current_close_price - previous_close_price) / previous_close_price) * 100 > 5  -- At least 5% change
        ORDER BY ABS((current_close_price - previous_close_price) / previous_close_price) DESC
        """
        
        df = pd.read_sql(query, conn)
        conn.close()
        
        print(f"\n🎯 TOP 20 EQUITY MOVERS (Jan-Feb 2025)")
        print("=" * 90)
        print(f"{'Symbol':12} | {'Jan Price':>10} | {'Feb Price':>10} | {'Change %':>8} | {'Delivery Δ%':>12}")
        print("=" * 90)
        
        for _, row in df.iterrows():
            print(f"{row['symbol']:12} | {row['jan_price']:10.2f} | {row['feb_price']:10.2f} | {row['price_change_pct']:+7.2f}% | {row['delivery_increase_pct']:+11.2f}%")
        
        return df
    
    def analyze_fo_coverage(self):
        """Analyze which equity symbols have F&O coverage"""
        logging.info("Analyzing F&O coverage for equity symbols...")
        
        conn = self.get_connection()
        
        query = """
        SELECT 
            e.symbol,
            e.feb_price,
            e.jan_price,
            e.price_change_pct,
            e.delivery_change_pct,
            COUNT(f.symbol) as fo_records,
            COUNT(CASE WHEN f.instrument = 'STO' THEN 1 END) as stock_options,
            COUNT(CASE WHEN f.instrument = 'STF' THEN 1 END) as stock_futures,
            SUM(f.contracts_traded) as total_volume,
            AVG(f.close_price) as avg_option_price
        FROM (
            SELECT 
                symbol,
                current_close_price as feb_price,
                previous_close_price as jan_price,
                ((current_close_price - previous_close_price) / previous_close_price) * 100 as price_change_pct,
                delivery_increase_pct as delivery_change_pct
            FROM step03_compare_monthvspreviousmonth
            WHERE comparison_type = 'FEB2025_vs_JAN2025_DELIVERY'
              AND ABS((current_close_price - previous_close_price) / previous_close_price) * 100 > 5
        ) e
        INNER JOIN step04_fo_udiff_daily f ON e.symbol = f.symbol
        WHERE LEFT(f.trade_date, 6) = '202502'  -- February F&O data
        GROUP BY e.symbol, e.feb_price, e.jan_price, e.price_change_pct, e.delivery_change_pct
        HAVING COUNT(f.symbol) >= 10  -- At least 10 F&O records
        ORDER BY ABS(e.price_change_pct) DESC
        """
        
        df = pd.read_sql(query, conn)
        conn.close()
        
        print(f"\n🎯 EQUITY MOVERS WITH F&O COVERAGE")
        print("=" * 120)
        print(f"{'Symbol':12} | {'Jan→Feb':>10} | {'Change%':>8} | {'FO Recs':>8} | {'Stock Opts':>10} | {'Stock Futs':>10} | {'Total Vol':>12}")
        print("=" * 120)
        
        for _, row in df.iterrows():
            print(f"{row['symbol']:12} | {row['jan_price']:5.0f}→{row['feb_price']:4.0f} | {row['price_change_pct']:+7.2f}% | {row['fo_records']:8} | {row['stock_options']:10} | {row['stock_futures']:10} | {row['total_volume']:12,.0f}")
        
        return df
    
    def analyze_strike_patterns_detailed(self, symbol):
        """Detailed strike analysis for a specific symbol"""
        logging.info(f"Detailed strike analysis for {symbol}...")
        
        conn = self.get_connection()
        
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
            print(f"❌ No equity data found for {symbol}")
            return None
            
        equity_info = equity_df.iloc[0]
        jan_price = float(equity_info['jan_price'])
        feb_price = float(equity_info['feb_price'])
        change_pct = float(equity_info['price_change_pct'])
        
        # Get all F&O data for this symbol
        fo_query = """
        SELECT 
            trade_date,
            instrument,
            strike_price,
            option_type,
            close_price,
            contracts_traded,
            open_interest,
            value_in_lakh
        FROM step04_fo_udiff_daily
        WHERE symbol = ?
          AND LEFT(trade_date, 6) = '202502'  -- February data
          AND strike_price > 0  -- Only records with strikes
        ORDER BY trade_date, instrument, strike_price, option_type
        """
        
        fo_df = pd.read_sql(fo_query, conn, params=[symbol])
        conn.close()
        
        if fo_df.empty:
            print(f"❌ No F&O data found for {symbol}")
            return None
        
        print(f"\n📊 DETAILED F&O ANALYSIS: {symbol}")
        print("=" * 80)
        print(f"Jan Price: ₹{jan_price:.2f} → Feb Price: ₹{feb_price:.2f} ({change_pct:+.2f}%)")
        print(f"F&O Records: {len(fo_df):,}")
        
        # Group by strike and option type
        strike_summary = fo_df.groupby(['strike_price', 'option_type']).agg({
            'close_price': ['mean', 'min', 'max', 'std'],
            'contracts_traded': 'sum',
            'open_interest': 'mean',
            'value_in_lakh': 'sum'
        }).round(2)
        
        strike_summary.columns = ['Avg_Price', 'Min_Price', 'Max_Price', 'Price_Std', 'Total_Volume', 'Avg_OI', 'Total_Value_Lakh']
        strike_summary = strike_summary.reset_index()
        
        # Categorize strikes relative to Jan/Feb prices
        strike_summary['Jan_Distance_Pct'] = ((strike_summary['strike_price'] - jan_price) / jan_price * 100).round(2)
        strike_summary['Feb_Distance_Pct'] = ((strike_summary['strike_price'] - feb_price) / feb_price * 100).round(2)
        
        # Focus on strikes within ±20% of equity prices
        relevant_strikes = strike_summary[
            (abs(strike_summary['Jan_Distance_Pct']) <= 20) | 
            (abs(strike_summary['Feb_Distance_Pct']) <= 20)
        ].copy()
        
        if not relevant_strikes.empty:
            print(f"\n📈 RELEVANT STRIKES (within ±20% of Jan/Feb prices):")
            print("=" * 120)
            print(f"{'Strike':>8} | {'Type':>4} | {'Avg Price':>9} | {'Volume':>10} | {'Value(L)':>9} | {'Jan Δ%':>7} | {'Feb Δ%':>7}")
            print("=" * 120)
            
            for _, row in relevant_strikes.head(15).iterrows():
                print(f"{row['strike_price']:8.0f} | {row['option_type']:>4} | {row['Avg_Price']:9.2f} | {row['Total_Volume']:10,.0f} | {row['Total_Value_Lakh']:9.2f} | {row['Jan_Distance_Pct']:+6.1f}% | {row['Feb_Distance_Pct']:+6.1f}%")
        
        return {
            'symbol': symbol,
            'equity_info': equity_info,
            'fo_summary': strike_summary,
            'relevant_strikes': relevant_strikes
        }
    
    def find_potential_reductions(self):
        """Find potential strike price reductions from Jan to Feb"""
        logging.info("Finding potential strike price reductions...")
        
        conn = self.get_connection()
        
        # Look for cases where stock price increased significantly 
        # and put options might have decreased in value
        query = """
        WITH equity_movers AS (
            SELECT 
                symbol,
                current_close_price as feb_price,
                previous_close_price as jan_price,
                ((current_close_price - previous_close_price) / previous_close_price) * 100 as price_change_pct
            FROM step03_compare_monthvspreviousmonth
            WHERE comparison_type = 'FEB2025_vs_JAN2025_DELIVERY'
              AND ((current_close_price - previous_close_price) / previous_close_price) * 100 > 10  -- Stock went up >10%
        ),
        feb_options AS (
            SELECT 
                symbol,
                strike_price,
                option_type,
                AVG(close_price) as avg_feb_price,
                MIN(close_price) as min_feb_price,
                MAX(close_price) as max_feb_price,
                SUM(contracts_traded) as total_volume
            FROM step04_fo_udiff_daily
            WHERE LEFT(trade_date, 6) = '202502'  
              AND instrument = 'STO'  -- Stock options
              AND option_type = 'PE'  -- Put options (should decrease when stock rises)
              AND strike_price > 0
            GROUP BY symbol, strike_price, option_type
        )
        SELECT 
            e.symbol,
            e.jan_price,
            e.feb_price,
            e.price_change_pct,
            f.strike_price,
            f.option_type,
            f.avg_feb_price as put_avg_price,
            f.min_feb_price as put_min_price,
            f.total_volume,
            ((f.strike_price - e.feb_price) / e.feb_price * 100) as strike_vs_feb_pct,
            CASE 
                WHEN f.strike_price < e.feb_price * 0.9 THEN 'Deep OTM'
                WHEN f.strike_price < e.feb_price THEN 'OTM'
                WHEN f.strike_price > e.feb_price * 1.1 THEN 'Deep ITM'
                ELSE 'Near ATM'
            END as moneyness
        FROM equity_movers e
        INNER JOIN feb_options f ON e.symbol = f.symbol
        WHERE f.total_volume > 100  -- Some trading activity
        ORDER BY e.price_change_pct DESC, f.strike_price
        """
        
        df = pd.read_sql(query, conn)
        conn.close()
        
        if not df.empty:
            print(f"\n🎯 POTENTIAL PUT OPTION SCENARIOS (Stock Price Increased >10%)")
            print("=" * 130)
            print(f"{'Symbol':10} | {'Jan→Feb':>10} | {'Chg%':>6} | {'Strike':>7} | {'Put Avg':>8} | {'Put Min':>8} | {'Volume':>8} | {'Moneyness':>10}")
            print("=" * 130)
            
            for _, row in df.head(20).iterrows():
                print(f"{row['symbol']:10} | {row['jan_price']:4.0f}→{row['feb_price']:4.0f} | {row['price_change_pct']:+5.1f}% | {row['strike_price']:7.0f} | {row['put_avg_price']:8.2f} | {row['put_min_price']:8.2f} | {row['total_volume']:8,.0f} | {row['moneyness']:>10}")
        
        return df
    
    def run_focused_analysis(self):
        """Run focused January-February analysis"""
        print("\n🎯 JANUARY-FEBRUARY 2025 FOCUSED F&O ANALYSIS")
        print("=" * 80)
        print("Using actual data structure: STO (Stock Options), IDO (Index Options), STF/IDF (Futures)")
        
        # 1. Top equity movers
        equity_movers = self.analyze_top_equity_movers()
        
        # 2. F&O coverage analysis
        fo_coverage = self.analyze_fo_coverage()
        
        # 3. Detailed analysis for top symbols with F&O
        if not fo_coverage.empty:
            print(f"\n🔍 DETAILED ANALYSIS FOR TOP F&O SYMBOLS:")
            top_symbols = fo_coverage.head(3)['symbol'].tolist()
            
            for symbol in top_symbols:
                self.analyze_strike_patterns_detailed(symbol)
        
        # 4. Look for potential reductions
        reductions = self.find_potential_reductions()
        
        print(f"\n✅ January-February focused analysis completed!")
        print(f"📊 Key findings:")
        print(f"   • {len(equity_movers)} significant equity movers (>5% change)")
        print(f"   • {len(fo_coverage)} symbols with both equity movement and F&O data")
        print(f"   • {len(reductions)} potential put option scenarios analyzed")

def main():
    analyzer = JanFebFocusedAnalysis()
    analyzer.run_focused_analysis()

if __name__ == "__main__":
    main()