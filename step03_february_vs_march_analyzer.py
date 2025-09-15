#!/usr/bin/env python3
"""
Step 03: February vs March 2025 Delivery Exceedance Analysis

PURPOSE:
========
Identify symbols where March 2025 daily delivery quantities exceed February 2025 peak values.
This analysis helps detect stocks with significantly increased investor delivery activity from February to March.

BUSINESS LOGIC:
==============
1. Calculate February 2025 peak delivery values for each symbol (series='EQ')
2. Process March 2025 daily data day-by-day
3. For each symbol, check if daily delivery > February peak delivery
4. Record complete transaction details when exceedance occurs
5. Provide comprehensive analytics and reporting

TARGET TABLE: step03_february_vs_march_analysis
"""

import pandas as pd
from datetime import datetime, date
from nse_database_integration import NSEDatabaseManager

class Step03FebruaryVsMarchAnalyzer:
    def __init__(self):
        """Initialize the Step 3 analyzer for February vs March delivery exceedance detection"""
        self.db = NSEDatabaseManager()
        
        print("🚀 STEP 03: February vs March 2025 Delivery Exceedance Analysis")
        print("=" * 70)
        print("📋 Logic: March 2025 daily delivery vs February 2025 peak delivery")
        print("🎯 Target: Record symbols where March delivery > February peak")
        print("📊 Focus: Complete transaction details for delivery exceedances")
        print("🗓️  Baseline: February 2025 | Comparison: March 2025")
        print()
        
        self.create_analysis_table()
        
    def create_analysis_table(self):
        """Create step03_february_vs_march_analysis table"""
        print("🔧 Creating step03_february_vs_march_analysis table...")
        
        cursor = self.db.connection.cursor()
        
        # Drop existing table if it exists to ensure clean structure
        cursor.execute("IF EXISTS (SELECT * FROM sysobjects WHERE name='step03_february_vs_march_analysis' AND xtype='U') DROP TABLE step03_february_vs_march_analysis")
        
        create_table_sql = """
        CREATE TABLE step03_february_vs_march_analysis (
            id BIGINT IDENTITY(1,1) PRIMARY KEY,
            trade_date DATE NOT NULL,
            symbol NVARCHAR(50) NOT NULL,
            series NVARCHAR(10) NOT NULL,
            
            -- March 2025 daily actual values (complete transaction record)
            march_prev_close DECIMAL(18,4),
            march_open_price DECIMAL(18,4),
            march_high_price DECIMAL(18,4),
            march_low_price DECIMAL(18,4),
            march_last_price DECIMAL(18,4),
            march_close_price DECIMAL(18,4),
            march_avg_price DECIMAL(18,4),
            march_ttl_trd_qnty BIGINT,
            march_turnover_lacs DECIMAL(18,4),
            march_no_of_trades BIGINT,
            march_deliv_qty BIGINT,
            march_deliv_per DECIMAL(8,4),
            march_source_file NVARCHAR(255),
            
            -- February 2025 baseline values for comparison
            feb_peak_delivery BIGINT,
            feb_peak_delivery_date DATE,
            feb_peak_delivery_source NVARCHAR(255),
            
            -- Exceedance calculations
            delivery_increase_abs BIGINT,
            delivery_increase_pct DECIMAL(8,2),
            
            -- Price performance metrics
            price_change_pct DECIMAL(8,2),      -- (close - prev_close) / prev_close * 100
            price_volatility_pct DECIMAL(8,2),  -- (high - low) / prev_close * 100
            
            -- Trading intensity metrics
            volume_to_delivery_ratio DECIMAL(8,4),  -- ttl_trd_qnty / deliv_qty
            turnover_per_share DECIMAL(8,4),        -- turnover_lacs / ttl_trd_qnty
            avg_trade_size DECIMAL(18,2),           -- ttl_trd_qnty / no_of_trades
            
            -- Classification and metadata
            exceedance_tier NVARCHAR(20),       -- MODERATE, SIGNIFICANT, EXCEPTIONAL, EXPLOSIVE
            analysis_type NVARCHAR(30) DEFAULT 'FEB_VS_MAR_DELIVERY',
            created_at DATETIME2 DEFAULT GETDATE(),
            
            -- Performance indexes
            INDEX IX_step03_feb_mar_symbol_date (symbol, trade_date),
            INDEX IX_step03_feb_mar_date (trade_date),
            INDEX IX_step03_feb_mar_symbol (symbol),
            INDEX IX_step03_feb_mar_tier (exceedance_tier),
            INDEX IX_step03_feb_mar_increase_pct (delivery_increase_pct DESC)
        )
        """
        
        cursor.execute(create_table_sql)
        self.db.connection.commit()
        print("✅ step03_february_vs_march_analysis table created successfully")
        
    def get_february_peak_delivery_baselines(self):
        """Calculate February 2025 peak delivery values for each EQ symbol"""
        print("📊 Calculating February 2025 peak delivery baselines...")
        
        cursor = self.db.connection.cursor()
        
        baseline_query = """
        SELECT 
            ranked.symbol,
            ranked.deliv_qty as peak_delivery,
            ranked.trade_date as peak_date,
            ranked.source_file as peak_source
        FROM (
            SELECT 
                symbol,
                deliv_qty,
                trade_date,
                source_file,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY deliv_qty DESC) as rn
            FROM step01_equity_daily
            WHERE YEAR(trade_date) = 2025 
                AND MONTH(trade_date) = 2
                AND series = 'EQ'
                AND deliv_qty IS NOT NULL
                AND deliv_qty > 0
        ) ranked
        WHERE ranked.rn = 1
        ORDER BY ranked.symbol
        """
        
        cursor.execute(baseline_query)
        baselines = {}
        
        for row in cursor.fetchall():
            symbol = row[0]
            baselines[symbol] = {
                'peak_delivery': int(row[1]) if row[1] else 0,
                'peak_date': row[2],
                'peak_source': row[3]
            }
        
        print(f"   ✅ Calculated February peak delivery baselines for {len(baselines):,} symbols")
        print(f"   📊 February 2025 peak delivery values established")
        
        # Show some sample baselines
        print(f"   🔍 Sample February peak deliveries:")
        sample_symbols = list(baselines.keys())[:5]
        for symbol in sample_symbols:
            baseline = baselines[symbol]
            print(f"      {symbol}: {baseline['peak_delivery']:,} shares on {baseline['peak_date']}")
        
        return baselines
        
    def get_march_daily_data(self):
        """Get March 2025 daily trading data for EQ series"""
        print("📈 Loading March 2025 daily trading data...")
        
        cursor = self.db.connection.cursor()
        
        march_query = """
        SELECT 
            trade_date, symbol, series, prev_close, open_price, high_price, 
            low_price, last_price, close_price, avg_price, ttl_trd_qnty, 
            turnover_lacs, no_of_trades, deliv_qty, deliv_per, source_file
        FROM step01_equity_daily
        WHERE YEAR(trade_date) = 2025 
            AND MONTH(trade_date) = 3
            AND series = 'EQ'
            AND deliv_qty IS NOT NULL
            AND deliv_qty > 0
        ORDER BY trade_date, symbol
        """
        
        cursor.execute(march_query)
        march_data = cursor.fetchall()
        
        print(f"   ✅ Loaded {len(march_data):,} March 2025 daily records")
        print(f"   📅 March data ready for comparison against February baselines")
        
        return march_data
        
    def process_february_vs_march_analysis(self):
        """Main analysis: Compare March daily delivery vs February peak delivery"""
        print("🔍 Processing February vs March delivery exceedance analysis...")
        print("=" * 70)
        
        # Get February baselines
        feb_baselines = self.get_february_peak_delivery_baselines()
        
        # Get March daily data
        march_data = self.get_march_daily_data()
        
        exceedance_records = []
        processed_count = 0
        exceedance_count = 0
        
        print("📊 Processing March daily records against February peak baselines...")
        
        for record in march_data:
            processed_count += 1
            
            if processed_count % 5000 == 0:
                print(f"   📊 Processed {processed_count:,} records, found {exceedance_count:,} exceedances...")
            
            (trade_date, symbol, series, prev_close, open_price, high_price, low_price,
             last_price, close_price, avg_price, ttl_trd_qnty, turnover_lacs, 
             no_of_trades, deliv_qty, deliv_per, source_file) = record
            
            # Skip if no February baseline available for this symbol
            if symbol not in feb_baselines:
                continue
                
            baseline = feb_baselines[symbol]
            feb_peak_delivery = baseline['peak_delivery']
            
            # Check for delivery exceedance: March daily delivery > February peak delivery
            if deliv_qty > feb_peak_delivery:
                exceedance_count += 1
                
                # Calculate metrics
                delivery_increase_abs = deliv_qty - feb_peak_delivery
                delivery_increase_pct = ((deliv_qty / feb_peak_delivery) - 1) * 100 if feb_peak_delivery > 0 else 0
                
                # Price performance
                price_change_pct = 0
                price_volatility_pct = 0
                if prev_close and prev_close > 0:
                    if close_price:
                        price_change_pct = ((close_price / prev_close) - 1) * 100
                    if high_price and low_price:
                        price_volatility_pct = ((high_price - low_price) / prev_close) * 100
                
                # Trading intensity
                volume_to_delivery_ratio = (ttl_trd_qnty / deliv_qty) if deliv_qty > 0 else 0
                turnover_per_share = (turnover_lacs / ttl_trd_qnty) if ttl_trd_qnty > 0 else 0
                avg_trade_size = (ttl_trd_qnty / no_of_trades) if no_of_trades > 0 else 0
                
                # Classify exceedance tier based on percentage increase
                if delivery_increase_pct >= 300:
                    exceedance_tier = 'EXPLOSIVE'      # 300%+ increase
                elif delivery_increase_pct >= 200:
                    exceedance_tier = 'EXCEPTIONAL'    # 200%+ increase
                elif delivery_increase_pct >= 100:
                    exceedance_tier = 'SIGNIFICANT'    # 100%+ increase
                elif delivery_increase_pct >= 50:
                    exceedance_tier = 'MODERATE'       # 50%+ increase
                else:
                    exceedance_tier = 'MINOR'          # Less than 50% increase
                
                exceedance_record = (
                    trade_date, symbol, series,
                    prev_close, open_price, high_price, low_price, last_price, close_price, avg_price,
                    ttl_trd_qnty, turnover_lacs, no_of_trades, deliv_qty, deliv_per, source_file,
                    feb_peak_delivery, baseline['peak_date'], baseline['peak_source'],
                    delivery_increase_abs, delivery_increase_pct,
                    price_change_pct, price_volatility_pct,
                    volume_to_delivery_ratio, turnover_per_share, avg_trade_size,
                    exceedance_tier
                )
                
                exceedance_records.append(exceedance_record)
        
        print(f"\n✅ February vs March Analysis Complete!")
        print(f"   📊 Processed: {processed_count:,} March daily records")
        print(f"   🎯 Found: {exceedance_count:,} delivery exceedances")
        print(f"   📈 Exceedance Rate: {(exceedance_count/processed_count)*100:.2f}%")
        print(f"   🔍 March delivery exceeded February peak in {exceedance_count:,} cases")
        
        return exceedance_records
        
    def insert_exceedance_records(self, exceedance_records):
        """Insert exceedance records into database"""
        if not exceedance_records:
            print("   ⚠️ No exceedance records to insert")
            return
            
        print(f"💾 Inserting {len(exceedance_records):,} February vs March exceedance records...")
        
        cursor = self.db.connection.cursor()
        
        # Clear existing analysis data
        cursor.execute("DELETE FROM step03_february_vs_march_analysis")
        print(f"   🗑️ Cleared existing February vs March analysis data")
        
        insert_sql = """
        INSERT INTO step03_february_vs_march_analysis (
            trade_date, symbol, series, march_prev_close, march_open_price, march_high_price, 
            march_low_price, march_last_price, march_close_price, march_avg_price,
            march_ttl_trd_qnty, march_turnover_lacs, march_no_of_trades, march_deliv_qty, 
            march_deliv_per, march_source_file, feb_peak_delivery, feb_peak_delivery_date, 
            feb_peak_delivery_source, delivery_increase_abs, delivery_increase_pct,
            price_change_pct, price_volatility_pct, volume_to_delivery_ratio, 
            turnover_per_share, avg_trade_size, exceedance_tier
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        # Insert in batches
        batch_size = 1000
        for i in range(0, len(exceedance_records), batch_size):
            batch = exceedance_records[i:i+batch_size]
            cursor.executemany(insert_sql, batch)
            
            if (i + batch_size) % 5000 == 0 or i + batch_size >= len(exceedance_records):
                self.db.connection.commit()
                print(f"   📊 Inserted {min(i + batch_size, len(exceedance_records)):,} records...")
        
        print(f"   ✅ Successfully inserted {len(exceedance_records):,} records")
        
    def show_comprehensive_results(self):
        """Show comprehensive February vs March analysis results"""
        print("\n📊 STEP 03 FEBRUARY VS MARCH DELIVERY EXCEEDANCE RESULTS")
        print("=" * 80)
        
        cursor = self.db.connection.cursor()
        
        # Overall summary
        cursor.execute("SELECT COUNT(*) FROM step03_february_vs_march_analysis")
        total_exceedances = cursor.fetchone()[0]
        print(f"📈 Total delivery exceedances found: {total_exceedances:,}")
        print(f"🎯 March daily delivery exceeded February peak delivery in {total_exceedances:,} cases")
        
        # Tier distribution
        print(f"\n🏆 EXCEEDANCE TIER DISTRIBUTION:")
        cursor.execute("""
            SELECT exceedance_tier, COUNT(*) as count, AVG(delivery_increase_pct) as avg_increase
            FROM step03_february_vs_march_analysis
            GROUP BY exceedance_tier
            ORDER BY AVG(delivery_increase_pct) DESC
        """)
        
        tier_results = cursor.fetchall()
        for row in tier_results:
            print(f"   🔥 {row[0]}: {row[1]:,} symbols (avg +{row[2]:.1f}%)")
        
        # Top delivery exceedances
        print(f"\n🔝 TOP 15 DELIVERY EXCEEDANCES (March vs February):")
        cursor.execute("""
            SELECT TOP 15 symbol, trade_date, march_deliv_qty, feb_peak_delivery, 
                   delivery_increase_pct, exceedance_tier
            FROM step03_february_vs_march_analysis
            ORDER BY delivery_increase_pct DESC
        """)
        
        for row in cursor.fetchall():
            print(f"   📊 {row[0]} ({row[1]}) | March: {row[2]:,} | Feb Peak: {row[3]:,} | +{row[4]:.1f}% [{row[5]}]")
        
        # Date-wise distribution
        print(f"\n📅 EXCEEDANCES BY MARCH TRADING DATE:")
        cursor.execute("""
            SELECT trade_date, COUNT(*) as exceedances, AVG(delivery_increase_pct) as avg_increase
            FROM step03_february_vs_march_analysis
            GROUP BY trade_date
            ORDER BY trade_date
        """)
        
        date_results = cursor.fetchall()
        for row in date_results:
            print(f"   📅 {row[0]}: {row[1]:,} exceedances (avg +{row[2]:.1f}%)")
            
        # Symbols with multiple exceedances
        print(f"\n🔄 SYMBOLS WITH MULTIPLE EXCEEDANCES:")
        cursor.execute("""
            SELECT TOP 10 symbol, COUNT(*) as exceedance_days, AVG(delivery_increase_pct) as avg_increase,
                   MAX(delivery_increase_pct) as max_increase
            FROM step03_february_vs_march_analysis
            GROUP BY symbol
            HAVING COUNT(*) > 1
            ORDER BY COUNT(*) DESC, AVG(delivery_increase_pct) DESC
        """)
        
        for row in cursor.fetchall():
            print(f"   🔄 {row[0]}: {row[1]} days | Avg +{row[2]:.1f}% | Max +{row[3]:.1f}%")
            
        # Statistical summary
        print(f"\n📊 STATISTICAL SUMMARY:")
        cursor.execute("""
            SELECT 
                MIN(delivery_increase_pct) as min_increase,
                AVG(delivery_increase_pct) as avg_increase,
                MAX(delivery_increase_pct) as max_increase,
                COUNT(DISTINCT symbol) as unique_symbols,
                COUNT(DISTINCT trade_date) as trading_days
            FROM step03_february_vs_march_analysis
        """)
        
        stats = cursor.fetchone()
        print(f"   📈 Min Increase: +{stats[0]:.1f}%")
        print(f"   📊 Avg Increase: +{stats[1]:.1f}%") 
        print(f"   🚀 Max Increase: +{stats[2]:.1f}%")
        print(f"   🏢 Unique Symbols: {stats[3]:,}")
        print(f"   📅 Trading Days: {stats[4]}")
            
    def export_to_excel(self):
        """Export results to Excel for detailed analysis"""
        print(f"\n📄 Exporting February vs March results to Excel...")
        
        try:
            cursor = self.db.connection.cursor()
            
            # Get all results
            cursor.execute("""
                SELECT 
                    trade_date, symbol, march_deliv_qty, feb_peak_delivery,
                    delivery_increase_abs, delivery_increase_pct, exceedance_tier,
                    march_close_price, price_change_pct, march_ttl_trd_qnty,
                    volume_to_delivery_ratio, avg_trade_size, feb_peak_delivery_date
                FROM step03_february_vs_march_analysis
                ORDER BY delivery_increase_pct DESC
            """)
            
            results = cursor.fetchall()
            
            if results:
                df = pd.DataFrame(results, columns=[
                    'Trade Date', 'Symbol', 'March Delivery', 'Feb Peak Delivery',
                    'Delivery Increase (Abs)', 'Delivery Increase (%)', 'Exceedance Tier',
                    'March Close Price', 'Price Change (%)', 'March Volume',
                    'Volume/Delivery Ratio', 'Avg Trade Size', 'Feb Peak Date'
                ])
                
                filename = f"step03_february_vs_march_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                df.to_excel(filename, index=False, sheet_name='February vs March Analysis')
                print(f"   ✅ Exported to {filename}")
            else:
                print(f"   ⚠️ No data to export")
        except Exception as e:
            print(f"   ⚠️ Excel export failed: {e}")
            print(f"   📊 Analysis results are available in database table: step03_february_vs_march_analysis")
            
    def run_complete_analysis(self):
        """Run complete Step 3 February vs March analysis"""
        print("🚀 Starting Step 3 February vs March Analysis...")
        
        # Process exceedances
        exceedance_records = self.process_february_vs_march_analysis()
        
        # Insert into database
        self.insert_exceedance_records(exceedance_records)
        
        # Show comprehensive results
        self.show_comprehensive_results()
        
        # Export to Excel
        self.export_to_excel()
        
        print(f"\n✅ STEP 03 FEBRUARY VS MARCH DELIVERY EXCEEDANCE ANALYSIS COMPLETE!")
        print(f"📊 Table: step03_february_vs_march_analysis")
        print(f"🎯 Analysis: March 2025 daily delivery vs February 2025 peak delivery")
        print(f"📈 Business Value: Identify stocks with increased delivery activity from Feb to Mar")
        print(f"🚀 Ready for business intelligence and investment decision support")
        
    def close(self):
        """Close database connection"""
        self.db.close()

def main():
    analyzer = Step03FebruaryVsMarchAnalyzer()
    try:
        analyzer.run_complete_analysis()
    finally:
        analyzer.close()

if __name__ == '__main__':
    main()