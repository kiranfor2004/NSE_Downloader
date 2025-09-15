#!/usr/bin/env python3
"""
Step 03: February Daily vs January Monthly Baseline Comparison

New Logic:
1. Take February 2025 daily data from step01_equity_daily (series='EQ')
2. Compare with January 2025 monthly baselines from step02_monthly_analysis
3. Find February daily records where delivery_qty OR ttl_trd_qnty > January baseline
4. Store complete line items in step03_compare_monthvspreviousmonth table

Business Logic:
- February daily activity that exceeds January monthly peaks indicates strong momentum
- Captures specific dates and complete transaction details for analysis
"""

import pandas as pd
import sys
import os
from datetime import datetime

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from nse_database_integration import NSEDatabaseManager

class Step03NewLogic:
    def __init__(self):
        self.db = NSEDatabaseManager()
        self.create_step03_table()
        
    def create_step03_table(self):
        """Create step03_compare_monthvspreviousmonth table"""
        print("🔧 Creating step03_compare_monthvspreviousmonth table...")
        
        cursor = self.db.connection.cursor()
        
        # Drop table if exists (for fresh start)
        cursor.execute("DROP TABLE IF EXISTS step03_compare_monthvspreviousmonth")
        
        # Create new table
        create_table_sql = """
        CREATE TABLE step03_compare_monthvspreviousmonth (
            id BIGINT IDENTITY(1,1) PRIMARY KEY,
            trade_date DATE NOT NULL,
            symbol NVARCHAR(50) NOT NULL,
            series NVARCHAR(10) NOT NULL,
            prev_close DECIMAL(18,4),
            open_price DECIMAL(18,4),
            high_price DECIMAL(18,4),
            low_price DECIMAL(18,4),
            last_price DECIMAL(18,4),
            close_price DECIMAL(18,4),
            avg_price DECIMAL(18,4),
            ttl_trd_qnty BIGINT,
            turnover_lacs DECIMAL(18,4),
            no_of_trades BIGINT,
            deliv_qty BIGINT,
            deliv_per DECIMAL(8,4),
            source_file NVARCHAR(255),
            
            -- January baseline comparison fields
            jan_baseline_volume BIGINT,
            jan_baseline_delivery BIGINT,
            jan_baseline_volume_date DATE,
            jan_baseline_delivery_date DATE,
            
            -- Exceedance indicators
            exceeds_volume BIT,
            exceeds_delivery BIT,
            volume_increase_abs BIGINT,
            delivery_increase_abs BIGINT,
            volume_increase_pct DECIMAL(10,2),
            delivery_increase_pct DECIMAL(10,2),
            
            -- Analysis metadata
            comparison_type NVARCHAR(50),
            created_at DATETIME2 DEFAULT GETDATE(),
            
            INDEX IX_step03_symbol_date (symbol, trade_date),
            INDEX IX_step03_date (trade_date),
            INDEX IX_step03_exceeds (exceeds_volume, exceeds_delivery)
        )
        """
        
        cursor.execute(create_table_sql)
        self.db.connection.commit()
        print("✅ step03_compare_monthvspreviousmonth table created")
        
    def get_january_baselines(self):
        """Get January 2025 monthly baselines from step02_monthly_analysis"""
        print("📊 Loading January 2025 baselines...")
        
        cursor = self.db.connection.cursor()
        
        # Get January volume baselines
        cursor.execute("""
            SELECT symbol, peak_value, peak_date
            FROM step02_monthly_analysis
            WHERE analysis_month = '2025-01' AND analysis_type = 'VOLUME'
        """)
        
        volume_baselines = {row[0]: {'baseline': row[1], 'date': row[2]} 
                          for row in cursor.fetchall()}
        
        # Get January delivery baselines
        cursor.execute("""
            SELECT symbol, peak_value, peak_date
            FROM step02_monthly_analysis
            WHERE analysis_month = '2025-01' AND analysis_type = 'DELIVERY'
        """)
        
        delivery_baselines = {row[0]: {'baseline': row[1], 'date': row[2]} 
                            for row in cursor.fetchall()}
        
        print(f"   ✅ Volume baselines: {len(volume_baselines)} symbols")
        print(f"   ✅ Delivery baselines: {len(delivery_baselines)} symbols")
        
        return volume_baselines, delivery_baselines
        
    def get_february_daily_data(self):
        """Get February 2025 daily data from step01_equity_daily"""
        print("📅 Loading February 2025 daily data...")
        
        cursor = self.db.connection.cursor()
        cursor.execute("""
            SELECT 
                trade_date, symbol, series, prev_close, open_price, high_price, 
                low_price, last_price, close_price, avg_price, ttl_trd_qnty, 
                turnover_lacs, no_of_trades, deliv_qty, deliv_per, source_file
            FROM step01_equity_daily
            WHERE series = 'EQ' 
            AND FORMAT(trade_date, 'yyyy-MM') = '2025-02'
            ORDER BY trade_date, symbol
        """)
        
        february_data = cursor.fetchall()
        print(f"   ✅ Loaded {len(february_data)} February daily records")
        
        return february_data
        
    def process_comparisons(self):
        """Main comparison logic: February daily vs January baselines"""
        print("🔍 Processing February daily vs January baseline comparisons...")
        
        # Get data
        volume_baselines, delivery_baselines = self.get_january_baselines()
        february_data = self.get_february_daily_data()
        
        # Process comparisons
        exceedances = []
        total_processed = 0
        
        for row in february_data:
            total_processed += 1
            
            (trade_date, symbol, series, prev_close, open_price, high_price, 
             low_price, last_price, close_price, avg_price, ttl_trd_qnty, 
             turnover_lacs, no_of_trades, deliv_qty, deliv_per, source_file) = row
            
            # Get baselines for this symbol
            volume_baseline = volume_baselines.get(symbol, {})
            delivery_baseline = delivery_baselines.get(symbol, {})
            
            # Check exceedances
            exceeds_volume = False
            exceeds_delivery = False
            volume_increase_abs = 0
            delivery_increase_abs = 0
            volume_increase_pct = 0
            delivery_increase_pct = 0
            
            # Volume comparison
            if volume_baseline and ttl_trd_qnty:
                jan_vol = volume_baseline.get('baseline', 0)
                if jan_vol and ttl_trd_qnty > jan_vol:
                    exceeds_volume = True
                    volume_increase_abs = ttl_trd_qnty - jan_vol
                    volume_increase_pct = round((volume_increase_abs / jan_vol) * 100, 2)
            
            # Delivery comparison
            if delivery_baseline and deliv_qty:
                jan_del = delivery_baseline.get('baseline', 0)
                if jan_del and deliv_qty > jan_del:
                    exceeds_delivery = True
                    delivery_increase_abs = deliv_qty - jan_del
                    delivery_increase_pct = round((delivery_increase_abs / jan_del) * 100, 2)
            
            # If either volume or delivery exceeds baseline, record it
            if exceeds_volume or exceeds_delivery:
                exceedance_record = (
                    trade_date, symbol, series, prev_close, open_price, high_price,
                    low_price, last_price, close_price, avg_price, ttl_trd_qnty,
                    turnover_lacs, no_of_trades, deliv_qty, deliv_per, source_file,
                    
                    # January baselines
                    volume_baseline.get('baseline') if volume_baseline else None,
                    delivery_baseline.get('baseline') if delivery_baseline else None,
                    volume_baseline.get('date') if volume_baseline else None,
                    delivery_baseline.get('date') if delivery_baseline else None,
                    
                    # Exceedance indicators
                    exceeds_volume,
                    exceeds_delivery,
                    volume_increase_abs,
                    delivery_increase_abs,
                    volume_increase_pct,
                    delivery_increase_pct,
                    
                    # Analysis metadata
                    'FEB2025_vs_JAN2025_DAILY'
                )
                
                exceedances.append(exceedance_record)
        
        print(f"   📊 Processed {total_processed} February daily records")
        print(f"   🎯 Found {len(exceedances)} exceedances")
        
        return exceedances
        
    def insert_exceedances(self, exceedances):
        """Insert exceedance records into step03 table"""
        if not exceedances:
            print("⚠️ No exceedances to insert")
            return
            
        print(f"💾 Inserting {len(exceedances)} exceedance records...")
        
        cursor = self.db.connection.cursor()
        
        insert_sql = """
        INSERT INTO step03_compare_monthvspreviousmonth (
            trade_date, symbol, series, prev_close, open_price, high_price,
            low_price, last_price, close_price, avg_price, ttl_trd_qnty,
            turnover_lacs, no_of_trades, deliv_qty, deliv_per, source_file,
            jan_baseline_volume, jan_baseline_delivery, jan_baseline_volume_date, 
            jan_baseline_delivery_date, exceeds_volume, exceeds_delivery,
            volume_increase_abs, delivery_increase_abs, volume_increase_pct, 
            delivery_increase_pct, comparison_type
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        cursor.executemany(insert_sql, exceedances)
        self.db.connection.commit()
        
        print(f"✅ Successfully inserted {len(exceedances)} records")
        
    def show_results_summary(self):
        """Show summary of Step 3 results"""
        print("\n📊 STEP 03 RESULTS SUMMARY")
        print("=" * 50)
        
        cursor = self.db.connection.cursor()
        
        # Total counts
        cursor.execute("SELECT COUNT(*) FROM step03_compare_monthvspreviousmonth")
        total_records = cursor.fetchone()[0]
        print(f"Total exceedance records: {total_records:,}")
        
        # Volume vs delivery exceedances
        cursor.execute("""
            SELECT 
                SUM(CASE WHEN exceeds_volume = 1 THEN 1 ELSE 0 END) as volume_exceeds,
                SUM(CASE WHEN exceeds_delivery = 1 THEN 1 ELSE 0 END) as delivery_exceeds,
                SUM(CASE WHEN exceeds_volume = 1 AND exceeds_delivery = 1 THEN 1 ELSE 0 END) as both_exceed
            FROM step03_compare_monthvspreviousmonth
        """)
        
        counts = cursor.fetchone()
        print(f"Volume exceedances: {counts[0]:,}")
        print(f"Delivery exceedances: {counts[1]:,}")
        print(f"Both volume & delivery: {counts[2]:,}")
        
        # Top performers
        print(f"\n🏆 TOP VOLUME EXCEEDANCES:")
        cursor.execute("""
            SELECT TOP 5 trade_date, symbol, ttl_trd_qnty, jan_baseline_volume, volume_increase_pct
            FROM step03_compare_monthvspreviousmonth
            WHERE exceeds_volume = 1
            ORDER BY volume_increase_pct DESC
        """)
        
        for row in cursor.fetchall():
            print(f"  {row[0]} | {row[1]} | Feb: {row[2]:,} > Jan: {row[3]:,} (+{row[4]:.1f}%)")
            
        print(f"\n🏆 TOP DELIVERY EXCEEDANCES:")
        cursor.execute("""
            SELECT TOP 5 trade_date, symbol, deliv_qty, jan_baseline_delivery, delivery_increase_pct
            FROM step03_compare_monthvspreviousmonth
            WHERE exceeds_delivery = 1
            ORDER BY delivery_increase_pct DESC
        """)
        
        for row in cursor.fetchall():
            print(f"  {row[0]} | {row[1]} | Feb: {row[2]:,} > Jan: {row[3]:,} (+{row[4]:.1f}%)")
            
        # Date distribution
        print(f"\n📅 EXCEEDANCES BY DATE:")
        cursor.execute("""
            SELECT trade_date, COUNT(*) as count
            FROM step03_compare_monthvspreviousmonth
            GROUP BY trade_date
            ORDER BY count DESC
        """)
        
        for i, row in enumerate(cursor.fetchall()):
            if i < 5:  # Top 5 dates
                print(f"  {row[0]}: {row[1]:,} exceedances")
                
    def run_step03_analysis(self):
        """Run complete Step 3 analysis"""
        print("🚀 STEP 03: February Daily vs January Monthly Baseline Analysis")
        print("=" * 70)
        print("📊 Logic: February daily records > January monthly baselines")
        print("📅 Source: step01_equity_daily (Feb 2025, series='EQ')")
        print("📈 Baseline: step02_monthly_analysis (Jan 2025 peaks)")
        print("💾 Output: step03_compare_monthvspreviousmonth table")
        print()
        
        # Process comparisons
        exceedances = self.process_comparisons()
        
        # Insert results
        self.insert_exceedances(exceedances)
        
        # Show summary
        self.show_results_summary()
        
        print(f"\n✅ STEP 03 ANALYSIS COMPLETE!")
        print(f"📊 Results available in: step03_compare_monthvspreviousmonth table")
        
    def close(self):
        """Close database connection"""
        self.db.close()

def main():
    analyzer = Step03NewLogic()
    try:
        analyzer.run_step03_analysis()
    finally:
        analyzer.close()

if __name__ == '__main__':
    main()
