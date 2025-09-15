#!/usr/bin/env python3
"""
Step 03: February vs January Delivery Comparison (Single Line Item Fix)

Fixed Logic:
1. Compare February daily delivery_qty vs January monthly baseline
2. When February daily delivery > January baseline:
   - Create ONE line item combining February daily + January baseline data
   - No separate records, all data in single row
3. Fill all January baseline fields properly (no NULLs)
4. Multiple records only when same symbol exceeds on multiple February days

Business Logic:
- Single comprehensive record per exceedance
- Complete February transaction data + January baseline reference
- Clean data with no NULL values in baseline fields
"""

import pandas as pd
import sys
import os
from datetime import datetime

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from nse_database_integration import NSEDatabaseManager

class Step03SingleLineComparison:
    def __init__(self):
        self.db = NSEDatabaseManager()
        self.create_step03_table()
        
    def create_step03_table(self):
        """Create step03_compare_monthvspreviousmonth table for single line items"""
        print("🔧 Creating step03_compare_monthvspreviousmonth table (Single Line Item)...")
        
        cursor = self.db.connection.cursor()
        
        # Drop table if exists (for fresh start)
        cursor.execute("DROP TABLE IF EXISTS step03_compare_monthvspreviousmonth")
        
        # Create new table with single record structure
        create_table_sql = """
        CREATE TABLE step03_compare_monthvspreviousmonth (
            id BIGINT IDENTITY(1,1) PRIMARY KEY,
            
            -- February transaction data (main record)
            feb_trade_date DATE NOT NULL,
            symbol NVARCHAR(50) NOT NULL,
            series NVARCHAR(10) NOT NULL,
            feb_prev_close DECIMAL(18,4),
            feb_open_price DECIMAL(18,4),
            feb_high_price DECIMAL(18,4),
            feb_low_price DECIMAL(18,4),
            feb_last_price DECIMAL(18,4),
            feb_close_price DECIMAL(18,4),
            feb_avg_price DECIMAL(18,4),
            feb_ttl_trd_qnty BIGINT,
            feb_turnover_lacs DECIMAL(18,4),
            feb_no_of_trades BIGINT,
            feb_deliv_qty BIGINT,
            feb_deliv_per DECIMAL(8,4),
            feb_source_file NVARCHAR(255),
            
            -- January baseline data (reference)
            jan_baseline_date DATE NOT NULL,
            jan_prev_close DECIMAL(18,4),
            jan_open_price DECIMAL(18,4),
            jan_high_price DECIMAL(18,4),
            jan_low_price DECIMAL(18,4),
            jan_last_price DECIMAL(18,4),
            jan_close_price DECIMAL(18,4),
            jan_avg_price DECIMAL(18,4),
            jan_ttl_trd_qnty BIGINT,
            jan_turnover_lacs DECIMAL(18,4),
            jan_no_of_trades BIGINT,
            jan_deliv_qty BIGINT,
            jan_deliv_per DECIMAL(8,4),
            jan_source_file NVARCHAR(255),
            
            -- Comparison analysis
            delivery_increase_abs BIGINT,
            delivery_increase_pct DECIMAL(10,2),
            
            -- Analysis metadata
            comparison_type NVARCHAR(50),
            created_at DATETIME2 DEFAULT GETDATE(),
            
            INDEX IX_step03_symbol (symbol),
            INDEX IX_step03_feb_date (feb_trade_date),
            INDEX IX_step03_jan_date (jan_baseline_date),
            INDEX IX_step03_delivery_pct (delivery_increase_pct)
        )
        """
        
        cursor.execute(create_table_sql)
        self.db.connection.commit()
        print("✅ step03_compare_monthvspreviousmonth table created (Single Line Item)")
        
    def get_january_delivery_baselines_with_records(self):
        """Get January delivery baselines with complete transaction records"""
        print("📊 Loading January 2025 delivery baselines with complete records...")
        
        cursor = self.db.connection.cursor()
        
        # Get January delivery baselines with peak dates
        cursor.execute("""
            SELECT symbol, peak_value, peak_date
            FROM step02_monthly_analysis
            WHERE analysis_month = '2025-01' AND analysis_type = 'DELIVERY'
        """)
        
        baselines = {}
        baseline_dates = {}
        
        for row in cursor.fetchall():
            symbol, peak_value, peak_date = row
            baselines[symbol] = peak_value
            baseline_dates[symbol] = peak_date
        
        # Get complete January records for all baseline dates
        january_records = {}
        for symbol, baseline_date in baseline_dates.items():
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
                january_records[symbol] = record
        
        print(f"   ✅ Loaded baselines: {len(baselines)} symbols")
        print(f"   ✅ Loaded complete records: {len(january_records)} symbols")
        
        return baselines, january_records
        
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
        
    def process_single_line_comparisons(self):
        """Main comparison logic: Single line items with February + January data"""
        print("🔍 Processing single line delivery comparisons...")
        
        # Get data
        jan_baselines, jan_records = self.get_january_delivery_baselines_with_records()
        february_data = self.get_february_daily_data()
        
        # Process comparisons
        single_line_records = []
        exceedance_count = 0
        total_processed = 0
        
        for feb_row in february_data:
            total_processed += 1
            
            (feb_date, symbol, series, feb_prev_close, feb_open, feb_high,
             feb_low, feb_last, feb_close, feb_avg, feb_volume,
             feb_turnover, feb_trades, feb_delivery, feb_deliv_per, feb_source) = feb_row
            
            # Check if symbol has January baseline and delivery data
            if (symbol in jan_baselines and symbol in jan_records and 
                feb_delivery and jan_baselines[symbol]):
                
                jan_baseline_delivery = jan_baselines[symbol]
                jan_record = jan_records[symbol]
                
                # Check if February delivery exceeds January baseline
                if feb_delivery > jan_baseline_delivery:
                    exceedance_count += 1
                    
                    # Calculate increases
                    delivery_increase_abs = feb_delivery - jan_baseline_delivery
                    delivery_increase_pct = round((delivery_increase_abs / jan_baseline_delivery) * 100, 2)
                    
                    # Extract January record data
                    (jan_date, jan_symbol, jan_series, jan_prev_close, jan_open, jan_high,
                     jan_low, jan_last, jan_close, jan_avg, jan_volume,
                     jan_turnover, jan_trades, jan_delivery, jan_deliv_per, jan_source) = jan_record
                    
                    # Create single combined record
                    combined_record = (
                        # February data (prefixed with feb_)
                        feb_date, symbol, series,
                        feb_prev_close, feb_open, feb_high, feb_low, feb_last, feb_close, feb_avg,
                        feb_volume, feb_turnover, feb_trades, feb_delivery, feb_deliv_per, feb_source,
                        
                        # January baseline data (prefixed with jan_)
                        jan_date,
                        jan_prev_close, jan_open, jan_high, jan_low, jan_last, jan_close, jan_avg,
                        jan_volume, jan_turnover, jan_trades, jan_delivery, jan_deliv_per, jan_source,
                        
                        # Comparison analysis
                        delivery_increase_abs, delivery_increase_pct,
                        
                        # Metadata
                        'FEB2025_vs_JAN2025_DELIVERY'
                    )
                    
                    single_line_records.append(combined_record)
        
        print(f"   📊 Processed {total_processed} February daily records")
        print(f"   🎯 Found {exceedance_count} delivery exceedances")
        print(f"   📋 Generated {len(single_line_records)} single line records")
        
        return single_line_records
        
    def insert_single_line_records(self, records):
        """Insert single line records into step03 table"""
        if not records:
            print("⚠️ No records to insert")
            return
            
        print(f"💾 Inserting {len(records)} single line records...")
        
        cursor = self.db.connection.cursor()
        
        insert_sql = """
        INSERT INTO step03_compare_monthvspreviousmonth (
            feb_trade_date, symbol, series,
            feb_prev_close, feb_open_price, feb_high_price, feb_low_price, 
            feb_last_price, feb_close_price, feb_avg_price,
            feb_ttl_trd_qnty, feb_turnover_lacs, feb_no_of_trades, 
            feb_deliv_qty, feb_deliv_per, feb_source_file,
            jan_baseline_date,
            jan_prev_close, jan_open_price, jan_high_price, jan_low_price,
            jan_last_price, jan_close_price, jan_avg_price,
            jan_ttl_trd_qnty, jan_turnover_lacs, jan_no_of_trades,
            jan_deliv_qty, jan_deliv_per, jan_source_file,
            delivery_increase_abs, delivery_increase_pct, comparison_type
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        cursor.executemany(insert_sql, records)
        self.db.connection.commit()
        
        print(f"✅ Successfully inserted {len(records)} single line records")
        
    def show_results_summary(self):
        """Show summary of Step 3 single line results"""
        print("\n📊 STEP 03 SINGLE LINE DELIVERY COMPARISON RESULTS")
        print("=" * 70)
        
        cursor = self.db.connection.cursor()
        
        # Total count
        cursor.execute("SELECT COUNT(*) FROM step03_compare_monthvspreviousmonth")
        total_records = cursor.fetchone()[0]
        print(f"Total single line records: {total_records:,}")
        
        # Unique symbols
        cursor.execute("SELECT COUNT(DISTINCT symbol) FROM step03_compare_monthvspreviousmonth")
        unique_symbols = cursor.fetchone()[0]
        print(f"Unique symbols with exceedances: {unique_symbols:,}")
        
        # Top performers
        print(f"\n🏆 TOP DELIVERY EXCEEDANCES (Single Line View):")
        cursor.execute("""
            SELECT TOP 5 
                symbol, feb_trade_date, feb_deliv_qty, jan_deliv_qty, delivery_increase_pct
            FROM step03_compare_monthvspreviousmonth
            ORDER BY delivery_increase_pct DESC
        """)
        
        for row in cursor.fetchall():
            print(f"  {row[1]} | {row[0]} | Feb: {row[2]:,} > Jan: {row[3]:,} (+{row[4]:.1f}%)")
            
        # Multiple exceedance days
        print(f"\n📅 SYMBOLS WITH MULTIPLE EXCEEDANCE DAYS:")
        cursor.execute("""
            SELECT symbol, COUNT(*) as exceedance_days
            FROM step03_compare_monthvspreviousmonth
            GROUP BY symbol
            HAVING COUNT(*) > 1
            ORDER BY COUNT(*) DESC
        """)
        
        multiple_days = cursor.fetchall()
        print(f"  Symbols with multiple days: {len(multiple_days)}")
        for i, row in enumerate(multiple_days[:5]):
            print(f"  {row[0]}: {row[1]} exceedance days")
            
        # Sample record structure
        print(f"\n📋 SAMPLE RECORD STRUCTURE:")
        cursor.execute("""
            SELECT TOP 1 
                symbol, feb_trade_date, feb_deliv_qty, feb_close_price,
                jan_baseline_date, jan_deliv_qty, jan_close_price,
                delivery_increase_pct
            FROM step03_compare_monthvspreviousmonth
            ORDER BY delivery_increase_pct DESC
        """)
        
        sample = cursor.fetchone()
        if sample:
            print(f"  Symbol: {sample[0]}")
            print(f"  February: {sample[1]} | Delivery: {sample[2]:,} | Close: ₹{sample[3]}")
            print(f"  January:  {sample[4]} | Delivery: {sample[5]:,} | Close: ₹{sample[6]}")
            print(f"  Increase: +{sample[7]:.1f}%")
            
    def run_step03_single_line_analysis(self):
        """Run complete Step 3 single line analysis"""
        print("🚀 STEP 03: February vs January Delivery Comparison (Single Line)")
        print("=" * 80)
        print("📊 Logic: ONE record per exceedance with Feb + Jan data combined")
        print("📅 Source: step01_equity_daily (Feb 2025, series='EQ')")
        print("📈 Baseline: step02_monthly_analysis (Jan 2025 delivery peaks)")
        print("💾 Output: Single line items with complete February + January data")
        print("🎯 Focus: No NULL values, no separate records")
        print()
        
        # Process comparisons
        records = self.process_single_line_comparisons()
        
        # Insert results
        self.insert_single_line_records(records)
        
        # Show summary
        self.show_results_summary()
        
        print(f"\n✅ STEP 03 SINGLE LINE ANALYSIS COMPLETE!")
        print(f"📊 Results: step03_compare_monthvspreviousmonth table")
        print(f"🎯 Structure: One record per exceedance with all data")
        
    def close(self):
        """Close database connection"""
        self.db.close()

def main():
    analyzer = Step03SingleLineComparison()
    try:
        analyzer.run_step03_single_line_analysis()
    finally:
        analyzer.close()

if __name__ == '__main__':
    main()
