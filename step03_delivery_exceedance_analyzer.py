#!/usr/bin/env python3
"""
Step 03: Delivery Exceedance Analysis - January vs March 2025

PURPOSE:
========
Identify symbols where March 2025 daily delivery quantities exceed January 2025 peak values.
This analysis helps detect stocks with significantly increased investor delivery activity.

BUSINESS LOGIC:
==============
1. Calculate January 2025 peak delivery values for each symbol (series='EQ')
2. Process March 2025 daily data day-by-day
3. For each symbol, check if daily delivery > January peak delivery
4. Record complete transaction details when exceedance occurs
5. Provide comprehensive analytics and reporting

TARGET TABLE: step03_delivery_exceedance_analysis
"""

import pandas as pd
from datetime import datetime, date
from nse_database_integration import NSEDatabaseManager

class Step03DeliveryExceedanceAnalyzer:
    def __init__(self):
        """Initialize the Step 3 analyzer for delivery exceedance detection"""
        self.db = NSEDatabaseManager()
        
        print("🚀 STEP 03: Delivery Exceedance Analysis")
        print("=" * 60)
        print("📋 Logic: March 2025 daily delivery vs January 2025 peak delivery")
        print("🎯 Target: Record symbols where March delivery > January peak")
        print("📊 Focus: Complete transaction details for delivery exceedances")
        print()
        
        self.create_analysis_table()
        
    def create_analysis_table(self):
        """Create step03_delivery_exceedance_analysis table"""
        print("🔧 Creating step03_delivery_exceedance_analysis table...")
        
        cursor = self.db.connection.cursor()
        
        create_table_sql = """
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='step03_delivery_exceedance_analysis' AND xtype='U')
        CREATE TABLE step03_delivery_exceedance_analysis (
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
            
            -- January 2025 baseline values for comparison
            jan_peak_delivery BIGINT,
            jan_peak_delivery_date DATE,
            jan_peak_delivery_source NVARCHAR(255),
            
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
            exceedance_tier NVARCHAR(20),       -- MODERATE, SIGNIFICANT, EXCEPTIONAL
            analysis_month NVARCHAR(10) DEFAULT '2025-03',
            created_at DATETIME2 DEFAULT GETDATE(),
            
            -- Performance indexes
            INDEX IX_step03_symbol_date (symbol, trade_date),
            INDEX IX_step03_date (trade_date),
            INDEX IX_step03_symbol (symbol),
            INDEX IX_step03_tier (exceedance_tier),
            INDEX IX_step03_increase_pct (delivery_increase_pct DESC)
        )
        """
        
        cursor.execute(create_table_sql)
        self.db.connection.commit()
        print("✅ step03_delivery_exceedance_analysis table created/verified")
        
    def get_january_peak_delivery_baselines(self):
        """Calculate January 2025 peak delivery values for each EQ symbol"""
        print("📊 Calculating January 2025 peak delivery baselines...")
        
        cursor = self.db.connection.cursor()
        
        baseline_query = """
        SELECT 
            symbol,
            MAX(deliv_qty) as peak_delivery,
            trade_date as peak_date,
            source_file as peak_source
        FROM (
            SELECT 
                symbol,
                deliv_qty,
                trade_date,
                source_file,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY deliv_qty DESC) as rn
            FROM step01_equity_daily
            WHERE YEAR(trade_date) = 2025 
                AND MONTH(trade_date) = 1
                AND series = 'EQ'
                AND deliv_qty IS NOT NULL
                AND deliv_qty > 0
        ) ranked
        WHERE rn = 1
        ORDER BY symbol
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
        
        print(f"   ✅ Calculated delivery baselines for {len(baselines):,} symbols")
        print(f"   📊 January 2025 peak delivery values established")
        
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
        return march_data
        
    def process_delivery_exceedance_analysis(self):
        """Main analysis: Compare March daily delivery vs January peak delivery"""
        print("🔍 Processing delivery exceedance analysis...")
        print("=" * 60)
        
        # Get January baselines
        jan_baselines = self.get_january_peak_delivery_baselines()
        
        # Get March daily data
        march_data = self.get_march_daily_data()
        
        exceedance_records = []
        processed_count = 0
        exceedance_count = 0
        
        print("📊 Processing March daily records against January baselines...")
        
        for record in march_data:
            processed_count += 1
            
            if processed_count % 5000 == 0:
                print(f"   📊 Processed {processed_count:,} records, found {exceedance_count:,} exceedances...")
            
            (trade_date, symbol, series, prev_close, open_price, high_price, low_price,
             last_price, close_price, avg_price, ttl_trd_qnty, turnover_lacs, 
             no_of_trades, deliv_qty, deliv_per, source_file) = record
            
            # Skip if no baseline available for this symbol
            if symbol not in jan_baselines:
                continue
                
            baseline = jan_baselines[symbol]
            jan_peak_delivery = baseline['peak_delivery']
            
            # Check for delivery exceedance
            if deliv_qty > jan_peak_delivery:
                exceedance_count += 1
                
                # Calculate metrics
                delivery_increase_abs = deliv_qty - jan_peak_delivery
                delivery_increase_pct = ((deliv_qty / jan_peak_delivery) - 1) * 100 if jan_peak_delivery > 0 else 0
                
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
                
                # Classify exceedance tier
                if delivery_increase_pct >= 200:
                    exceedance_tier = 'EXCEPTIONAL'
                elif delivery_increase_pct >= 100:
                    exceedance_tier = 'SIGNIFICANT'
                elif delivery_increase_pct >= 50:
                    exceedance_tier = 'MODERATE'
                else:
                    exceedance_tier = 'MINOR'
                
                exceedance_record = (
                    trade_date, symbol, series,
                    prev_close, open_price, high_price, low_price, last_price, close_price, avg_price,
                    ttl_trd_qnty, turnover_lacs, no_of_trades, deliv_qty, deliv_per, source_file,
                    jan_peak_delivery, baseline['peak_date'], baseline['peak_source'],
                    delivery_increase_abs, delivery_increase_pct,
                    price_change_pct, price_volatility_pct,
                    volume_to_delivery_ratio, turnover_per_share, avg_trade_size,
                    exceedance_tier
                )
                
                exceedance_records.append(exceedance_record)
        
        print(f"\n✅ Analysis Complete!")
        print(f"   📊 Processed: {processed_count:,} March daily records")
        print(f"   🎯 Found: {exceedance_count:,} delivery exceedances")
        print(f"   📈 Exceedance Rate: {(exceedance_count/processed_count)*100:.2f}%")
        
        return exceedance_records
        
    def insert_exceedance_records(self, exceedance_records):
        """Insert exceedance records into database"""
        if not exceedance_records:
            print("   ⚠️ No exceedance records to insert")
            return
            
        print(f"💾 Inserting {len(exceedance_records):,} exceedance records...")
        
        cursor = self.db.connection.cursor()
        
        # Clear existing data for March 2025
        cursor.execute("DELETE FROM step03_delivery_exceedance_analysis WHERE analysis_month = '2025-03'")
        print(f"   🗑️ Cleared existing March 2025 analysis data")
        
        insert_sql = """
        INSERT INTO step03_delivery_exceedance_analysis (
            trade_date, symbol, series, march_prev_close, march_open_price, march_high_price, 
            march_low_price, march_last_price, march_close_price, march_avg_price,
            march_ttl_trd_qnty, march_turnover_lacs, march_no_of_trades, march_deliv_qty, 
            march_deliv_per, march_source_file, jan_peak_delivery, jan_peak_delivery_date, 
            jan_peak_delivery_source, delivery_increase_abs, delivery_increase_pct,
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
        """Show comprehensive analysis results"""
        print("\n📊 STEP 03 DELIVERY EXCEEDANCE ANALYSIS RESULTS")
        print("=" * 70)
        
        cursor = self.db.connection.cursor()
        
        # Overall summary
        cursor.execute("SELECT COUNT(*) FROM step03_delivery_exceedance_analysis")
        total_exceedances = cursor.fetchone()[0]
        print(f"📈 Total delivery exceedances found: {total_exceedances:,}")
        
        # Tier distribution
        print(f"\n🎯 EXCEEDANCE TIER DISTRIBUTION:")
        cursor.execute("""
            SELECT exceedance_tier, COUNT(*) as count, AVG(delivery_increase_pct) as avg_increase
            FROM step03_delivery_exceedance_analysis
            GROUP BY exceedance_tier
            ORDER BY AVG(delivery_increase_pct) DESC
        """)
        
        for row in cursor.fetchall():
            print(f"   {row[0]}: {row[1]:,} symbols (avg +{row[2]:.1f}%)")
        
        # Top delivery exceedances
        print(f"\n🔝 TOP 10 DELIVERY EXCEEDANCES:")
        cursor.execute("""
            SELECT TOP 10 symbol, trade_date, march_deliv_qty, jan_peak_delivery, 
                   delivery_increase_pct, exceedance_tier
            FROM step03_delivery_exceedance_analysis
            ORDER BY delivery_increase_pct DESC
        """)
        
        for row in cursor.fetchall():
            print(f"   {row[0]} ({row[1]}) | March: {row[2]:,} | Jan Peak: {row[3]:,} | +{row[4]:.1f}% [{row[5]}]")
        
        # Date-wise distribution
        print(f"\n📅 EXCEEDANCES BY TRADING DATE:")
        cursor.execute("""
            SELECT trade_date, COUNT(*) as exceedances, AVG(delivery_increase_pct) as avg_increase
            FROM step03_delivery_exceedance_analysis
            GROUP BY trade_date
            ORDER BY trade_date
        """)
        
        for row in cursor.fetchall()[:15]:  # Show first 15 dates
            print(f"   {row[0]}: {row[1]:,} exceedances (avg +{row[2]:.1f}%)")
            
        # Symbols with multiple exceedances
        print(f"\n🔄 SYMBOLS WITH MULTIPLE EXCEEDANCES:")
        cursor.execute("""
            SELECT TOP 10 symbol, COUNT(*) as exceedance_days, AVG(delivery_increase_pct) as avg_increase,
                   MAX(delivery_increase_pct) as max_increase
            FROM step03_delivery_exceedance_analysis
            GROUP BY symbol
            HAVING COUNT(*) > 1
            ORDER BY COUNT(*) DESC, AVG(delivery_increase_pct) DESC
        """)
        
        for row in cursor.fetchall():
            print(f"   {row[0]}: {row[1]} days | Avg +{row[2]:.1f}% | Max +{row[3]:.1f}%")
            
    def export_to_excel(self):
        """Export results to Excel for detailed analysis"""
        print(f"\n📄 Exporting results to Excel...")
        
        cursor = self.db.connection.cursor()
        
        # Get all results
        cursor.execute("""
            SELECT 
                trade_date, symbol, march_deliv_qty, jan_peak_delivery,
                delivery_increase_abs, delivery_increase_pct, exceedance_tier,
                march_close_price, price_change_pct, march_ttl_trd_qnty,
                volume_to_delivery_ratio, avg_trade_size
            FROM step03_delivery_exceedance_analysis
            ORDER BY delivery_increase_pct DESC
        """)
        
        results = cursor.fetchall()
        
        if results:
            df = pd.DataFrame(results, columns=[
                'Trade Date', 'Symbol', 'March Delivery', 'Jan Peak Delivery',
                'Delivery Increase (Abs)', 'Delivery Increase (%)', 'Exceedance Tier',
                'March Close Price', 'Price Change (%)', 'March Volume',
                'Volume/Delivery Ratio', 'Avg Trade Size'
            ])
            
            filename = f"step03_delivery_exceedance_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            df.to_excel(filename, index=False, sheet_name='Delivery Exceedance Analysis')
            print(f"   ✅ Exported to {filename}")
        else:
            print(f"   ⚠️ No data to export")
            
    def run_complete_analysis(self):
        """Run complete Step 3 delivery exceedance analysis"""
        print("🚀 Starting Step 3 Analysis...")
        
        # Process exceedances
        exceedance_records = self.process_delivery_exceedance_analysis()
        
        # Insert into database
        self.insert_exceedance_records(exceedance_records)
        
        # Show comprehensive results
        self.show_comprehensive_results()
        
        # Export to Excel
        self.export_to_excel()
        
        print(f"\n✅ STEP 03 DELIVERY EXCEEDANCE ANALYSIS COMPLETE!")
        print(f"📊 Table: step03_delivery_exceedance_analysis")
        print(f"🎯 Analysis: March 2025 delivery vs January 2025 peak delivery")
        print(f"🚀 Ready for business intelligence and further analysis")
        
    def close(self):
        """Close database connection"""
        self.db.close()

def main():
    analyzer = Step03DeliveryExceedanceAnalyzer()
    try:
        analyzer.run_complete_analysis()
    finally:
        analyzer.close()

if __name__ == '__main__':
    main()