#!/usr/bin/env python3
"""
Step 03: February vs January Delivery Quantity Comparison (Revised Logic)

New Logic (Delivery-Only Focus):
1. Compare ONLY delivery_qty between February daily vs January monthly baseline
2. Source: February 2025 daily data from step01_equity_daily (series='EQ')
3. Baseline: January 2025 delivery peaks from step02_monthly_analysis
4. Output: When February daily delivery > January baseline:
   - Complete February daily line item
   - Complete January baseline line item
   - Multiple records per symbol if delivery exceeds on multiple February days

Business Logic:
- Focus on delivery quantity as indicator of investor commitment
- Track multiple exceedance days per symbol for trend analysis
- Provide complete transaction context for both months
"""

import pandas as pd
import sys
import os
from datetime import datetime

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from nse_database_integration import NSEDatabaseManager

class Step03DeliveryComparison:
    def __init__(self):
        self.db = NSEDatabaseManager()
        self.create_step03_table()
        
    def create_step03_table(self):
        """Create step03_compare_monthvspreviousmonth table for delivery-only comparison"""
        print("🔧 Creating step03_compare_monthvspreviousmonth table (Delivery Focus)...")
        
        cursor = self.db.connection.cursor()
        
        # Drop table if exists (for fresh start)
        cursor.execute("DROP TABLE IF EXISTS step03_compare_monthvspreviousmonth")
        
        # Create new table with dual record structure
        create_table_sql = """
        CREATE TABLE step03_compare_monthvspreviousmonth (
            id BIGINT IDENTITY(1,1) PRIMARY KEY,
            
            -- Record metadata
            record_type NVARCHAR(20) NOT NULL, -- 'FEBRUARY_DAILY' or 'JANUARY_BASELINE'
            symbol NVARCHAR(50) NOT NULL,
            comparison_pair_id NVARCHAR(100), -- Links February and January records
            
            -- Transaction data (from step01_equity_daily)
            trade_date DATE NOT NULL,
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
            
            -- Comparison analysis (only for February records)
            jan_baseline_delivery BIGINT,
            jan_baseline_date DATE,
            delivery_increase_abs BIGINT,
            delivery_increase_pct DECIMAL(10,2),
            
            -- Analysis metadata
            created_at DATETIME2 DEFAULT GETDATE(),
            
            INDEX IX_step03_symbol (symbol),
            INDEX IX_step03_record_type (record_type),
            INDEX IX_step03_trade_date (trade_date),
            INDEX IX_step03_comparison_pair (comparison_pair_id)
        )
        """
        
        cursor.execute(create_table_sql)
        self.db.connection.commit()
        print("✅ step03_compare_monthvspreviousmonth table created (Delivery Focus)")
        
    def get_january_delivery_baselines(self):
        """Get January 2025 delivery baselines with complete line items"""
        print("📊 Loading January 2025 delivery baselines...")
        
        cursor = self.db.connection.cursor()
        
        # Get January delivery baselines with their peak dates
        cursor.execute("""
            SELECT symbol, peak_value, peak_date
            FROM step02_monthly_analysis
            WHERE analysis_month = '2025-01' AND analysis_type = 'DELIVERY'
        """)
        
        baselines = {}
        for row in cursor.fetchall():
            symbol, peak_value, peak_date = row
            baselines[symbol] = {
                'baseline_delivery': peak_value,
                'baseline_date': peak_date
            }
        
        print(f"   ✅ Loaded delivery baselines for {len(baselines)} symbols")
        return baselines
        
    def get_january_baseline_records(self, baselines):
        """Get complete January line items for baseline dates"""
        print("📅 Loading complete January baseline records...")
        
        baseline_records = {}
        cursor = self.db.connection.cursor()
        
        for symbol, baseline_info in baselines.items():
            baseline_date = baseline_info['baseline_date']
            
            # Get complete January record for this symbol on baseline date
            cursor.execute("""
                SELECT 
                    trade_date, symbol, series, prev_close, open_price, high_price,
                    low_price, last_price, close_price, avg_price, ttl_trd_qnty,
                    turnover_lacs, no_of_trades, deliv_qty, deliv_per, source_file
                FROM step01_equity_daily
                WHERE symbol = ? AND trade_date = ? AND series = 'EQ'
            """, (symbol, baseline_date))
            
            record = cursor.fetchone()
            if record:
                baseline_records[symbol] = record
                
        print(f"   ✅ Loaded complete baseline records for {len(baseline_records)} symbols")
        return baseline_records
        
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
        
    def process_delivery_comparisons(self):
        """Main comparison logic: February daily delivery vs January baselines"""
        print("🔍 Processing February daily delivery vs January baseline comparisons...")
        
        # Get data
        baselines = self.get_january_delivery_baselines()
        baseline_records = self.get_january_baseline_records(baselines)
        february_data = self.get_february_daily_data()
        
        # Process comparisons
        output_records = []
        exceedance_count = 0
        total_processed = 0
        
        for feb_row in february_data:
            total_processed += 1
            
            (trade_date, symbol, series, prev_close, open_price, high_price,
             low_price, last_price, close_price, avg_price, ttl_trd_qnty,
             turnover_lacs, no_of_trades, deliv_qty, deliv_per, source_file) = feb_row
            
            # Check if symbol has January baseline
            if symbol in baselines and deliv_qty:
                jan_baseline_delivery = baselines[symbol]['baseline_delivery']
                jan_baseline_date = baselines[symbol]['baseline_date']
                
                # Check if February delivery exceeds January baseline
                if jan_baseline_delivery and deliv_qty > jan_baseline_delivery:
                    exceedance_count += 1
                    
                    # Calculate increases
                    delivery_increase_abs = deliv_qty - jan_baseline_delivery
                    delivery_increase_pct = round((delivery_increase_abs / jan_baseline_delivery) * 100, 2)
                    
                    # Create comparison pair ID
                    comparison_pair_id = f"{symbol}_{trade_date.strftime('%Y%m%d')}_vs_JAN2025"
                    
                    # February record (with comparison data)
                    feb_record = (
                        'FEBRUARY_DAILY', symbol, comparison_pair_id,
                        trade_date, series, prev_close, open_price, high_price,
                        low_price, last_price, close_price, avg_price, ttl_trd_qnty,
                        turnover_lacs, no_of_trades, deliv_qty, deliv_per, source_file,
                        jan_baseline_delivery, jan_baseline_date,
                        delivery_increase_abs, delivery_increase_pct
                    )
                    
                    output_records.append(feb_record)
                    
                    # January baseline record (if available)
                    if symbol in baseline_records:
                        jan_row = baseline_records[symbol]
                        jan_record = (
                            'JANUARY_BASELINE', symbol, comparison_pair_id,
                            jan_row[0], jan_row[2], jan_row[3], jan_row[4], jan_row[5],
                            jan_row[6], jan_row[7], jan_row[8], jan_row[9], jan_row[10],
                            jan_row[11], jan_row[12], jan_row[13], jan_row[14], jan_row[15],
                            None, None, None, None  # No comparison data for baseline records
                        )
                        
                        output_records.append(jan_record)
        
        print(f"   📊 Processed {total_processed} February daily records")
        print(f"   🎯 Found {exceedance_count} delivery exceedances")
        print(f"   📋 Generated {len(output_records)} total output records")
        
        return output_records
        
    def insert_comparison_records(self, records):
        """Insert comparison records into step03 table"""
        if not records:
            print("⚠️ No records to insert")
            return
            
        print(f"💾 Inserting {len(records)} comparison records...")
        
        cursor = self.db.connection.cursor()
        
        insert_sql = """
        INSERT INTO step03_compare_monthvspreviousmonth (
            record_type, symbol, comparison_pair_id, trade_date, series,
            prev_close, open_price, high_price, low_price, last_price,
            close_price, avg_price, ttl_trd_qnty, turnover_lacs, no_of_trades,
            deliv_qty, deliv_per, source_file, jan_baseline_delivery, jan_baseline_date,
            delivery_increase_abs, delivery_increase_pct
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        cursor.executemany(insert_sql, records)
        self.db.connection.commit()
        
        print(f"✅ Successfully inserted {len(records)} records")
        
    def show_results_summary(self):
        """Show summary of Step 3 delivery comparison results"""
        print("\n📊 STEP 03 DELIVERY COMPARISON RESULTS")
        print("=" * 60)
        
        cursor = self.db.connection.cursor()
        
        # Total counts by record type
        cursor.execute("""
            SELECT record_type, COUNT(*) as count
            FROM step03_compare_monthvspreviousmonth
            GROUP BY record_type
        """)
        
        print("📋 Record Counts:")
        for row in cursor.fetchall():
            print(f"  {row[0]}: {row[1]:,} records")
            
        # Exceedance summary
        cursor.execute("""
            SELECT COUNT(DISTINCT symbol) as unique_symbols,
                   COUNT(*) as total_exceedances
            FROM step03_compare_monthvspreviousmonth
            WHERE record_type = 'FEBRUARY_DAILY'
        """)
        
        result = cursor.fetchone()
        print(f"\n🎯 Delivery Exceedances:")
        print(f"  Unique symbols with exceedances: {result[0]:,}")
        print(f"  Total February exceedance days: {result[1]:,}")
        
        # Top delivery performers
        print(f"\n🏆 TOP DELIVERY EXCEEDANCES:")
        cursor.execute("""
            SELECT TOP 5 symbol, trade_date, deliv_qty, jan_baseline_delivery, delivery_increase_pct
            FROM step03_compare_monthvspreviousmonth
            WHERE record_type = 'FEBRUARY_DAILY'
            ORDER BY delivery_increase_pct DESC
        """)
        
        for row in cursor.fetchall():
            print(f"  {row[1]} | {row[0]} | Feb: {row[2]:,} > Jan: {row[3]:,} (+{row[4]:.1f}%)")
            
        # Symbols with multiple exceedance days
        print(f"\n📅 SYMBOLS WITH MULTIPLE EXCEEDANCE DAYS:")
        cursor.execute("""
            SELECT symbol, COUNT(*) as exceedance_days
            FROM step03_compare_monthvspreviousmonth
            WHERE record_type = 'FEBRUARY_DAILY'
            GROUP BY symbol
            HAVING COUNT(*) > 1
            ORDER BY COUNT(*) DESC
        """)
        
        multiple_days = cursor.fetchall()
        print(f"  Symbols with multiple days: {len(multiple_days)}")
        for i, row in enumerate(multiple_days[:5]):
            print(f"  {row[0]}: {row[1]} exceedance days")
            
    def run_step03_delivery_analysis(self):
        """Run complete Step 3 delivery-focused analysis"""
        print("🚀 STEP 03: February Daily vs January Baseline - Delivery Quantity Focus")
        print("=" * 80)
        print("📊 Logic: February daily delivery_qty > January monthly baseline delivery")
        print("📅 Source: step01_equity_daily (Feb 2025, series='EQ')")
        print("📈 Baseline: step02_monthly_analysis (Jan 2025 delivery peaks)")
        print("💾 Output: Paired records (February + January baseline) per exceedance")
        print("🔢 Multiple: One pair per symbol per exceedance day")
        print()
        
        # Process comparisons
        records = self.process_delivery_comparisons()
        
        # Insert results
        self.insert_comparison_records(records)
        
        # Show summary
        self.show_results_summary()
        
        print(f"\n✅ STEP 03 DELIVERY ANALYSIS COMPLETE!")
        print(f"📊 Results: step03_compare_monthvspreviousmonth table")
        print(f"🎯 Focus: Delivery quantity exceedances with complete line items")
        
    def close(self):
        """Close database connection"""
        self.db.close()

def main():
    analyzer = Step03DeliveryComparison()
    try:
        analyzer.run_step03_delivery_analysis()
    finally:
        analyzer.close()

if __name__ == '__main__':
    main()
