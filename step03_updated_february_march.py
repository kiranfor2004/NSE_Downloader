#!/usr/bin/env python3
"""
Step 03: February vs March 2025 Delivery Comparison - Using Existing Table Structure

PURPOSE:
========
Update the existing step03_compare_monthvspreviousmonth table with February vs March comparison.
Use February 2025 peak delivery values as baselines and March 2025 daily data for comparison.

LOGIC:
======
1. Calculate February 2025 peak delivery values for each symbol (series='EQ')
2. Proces        print(f"\n✅ Analysis complete! {total_records:,} exceedance records saved to step03_compare_monthvspreviousmonth table")
        print(f"📊 Data stored with comparison_type = 'MAR_VS_FEB_2025'")
        print("🎯 Column names updated: current_ = March data, previous_ = February baselines")
        print("🎯 Use this table for Step 3 analysis and reporting!")arch 2025 daily data day-by-day  
3. For each symbol, check if daily delivery > February peak delivery
4. Record complete transaction details in existing step03_compare_monthvspreviousmonth table

TARGET TABLE: step03_compare_monthvspreviousmonth (existing table)
"""

import pandas as pd
from datetime import datetime, date
from nse_database_integration import NSEDatabaseManager

class Step03FebruaryMarchComparison:
    def __init__(self):
        """Initialize the Step 3 analyzer using existing table structure"""
        self.db = NSEDatabaseManager()
        
        print("🚀 STEP 03: February vs March 2025 Delivery Comparison")
        print("=" * 70)
        print("📋 Logic: March 2025 daily delivery vs February 2025 peak delivery")
        print("🎯 Target: Update step03_compare_monthvspreviousmonth table")
        print("📊 Focus: Delivery exceedances where March > February peak")
        print("🗓️  Baseline: February 2025 | Comparison: March 2025")
        print()
        
        self.ensure_table_exists()
        
    def ensure_table_exists(self):
        """Ensure step03_compare_monthvspreviousmonth table exists"""
        print("🔧 Verifying step03_compare_monthvspreviousmonth table...")
        
        cursor = self.db.connection.cursor()
        
        create_table_sql = """
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='step03_compare_monthvspreviousmonth' AND xtype='U')
        CREATE TABLE step03_compare_monthvspreviousmonth (
            id BIGINT IDENTITY(1,1) PRIMARY KEY,
            trade_date DATE NOT NULL,
            symbol NVARCHAR(50) NOT NULL,
            series NVARCHAR(10) NOT NULL,
            
            -- Daily actual values (March 2025 data)
            daily_ttl_trd_qnty BIGINT,
            daily_deliv_qty BIGINT,
            prev_close DECIMAL(18,4),
            open_price DECIMAL(18,4),
            high_price DECIMAL(18,4),
            low_price DECIMAL(18,4),
            last_price DECIMAL(18,4),
            close_price DECIMAL(18,4),
            avg_price DECIMAL(18,4),
            turnover_lacs DECIMAL(18,4),
            no_of_trades BIGINT,
            deliv_per DECIMAL(8,4),
            
            -- Monthly baseline values for comparison (February 2025 peaks)
            monthly_volume_baseline BIGINT,
            monthly_delivery_baseline BIGINT,
            baseline_volume_date DATE,
            baseline_delivery_date DATE,
            
            -- Exceedance flags and calculations
            volume_exceeded BIT,
            delivery_exceeded BIT,
            volume_increase_abs BIGINT,
            delivery_increase_abs BIGINT,
            volume_increase_pct DECIMAL(8,2),
            delivery_increase_pct DECIMAL(8,2),
            
            -- Metadata
            analysis_month NVARCHAR(10),
            source_file NVARCHAR(255),
            created_at DATETIME2 DEFAULT GETDATE(),
            
            INDEX IX_step03_symbol_date (symbol, trade_date),
            INDEX IX_step03_date (trade_date),
            INDEX IX_step03_volume_exceeded (volume_exceeded),
            INDEX IX_step03_delivery_exceeded (delivery_exceeded)
        )
        """
        
        cursor.execute(create_table_sql)
        self.db.connection.commit()
        print("✅ step03_compare_monthvspreviousmonth table verified/created")
        
    def rename_table_columns(self):
        """Rename columns from feb_/jan_ to current_/previous_"""
        print("🔄 Renaming table columns to more meaningful names...")
        
        cursor = self.db.connection.cursor()
        
        # Check if columns need renaming
        cursor.execute("""
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'step03_compare_monthvspreviousmonth' 
        AND COLUMN_NAME LIKE 'feb_%'
        """)
        feb_count = cursor.fetchone()[0]
        
        if feb_count == 0:
            print("   ✅ Columns already have updated names")
            return
            
        print(f"   🔍 Found {feb_count} columns to rename")
        
        # Define column mappings
        feb_to_current = [
            'feb_trade_date', 'feb_prev_close', 'feb_open_price', 'feb_high_price', 
            'feb_low_price', 'feb_last_price', 'feb_close_price', 'feb_avg_price',
            'feb_ttl_trd_qnty', 'feb_turnover_lacs', 'feb_no_of_trades', 
            'feb_deliv_qty', 'feb_deliv_per', 'feb_source_file'
        ]
        
        jan_to_previous = [
            'jan_baseline_date', 'jan_prev_close', 'jan_open_price', 'jan_high_price',
            'jan_low_price', 'jan_last_price', 'jan_close_price', 'jan_avg_price',
            'jan_ttl_trd_qnty', 'jan_turnover_lacs', 'jan_no_of_trades',
            'jan_deliv_qty', 'jan_deliv_per', 'jan_source_file'
        ]

        # Rename feb_ columns to current_
        print('   🔄 Renaming feb_ → current_...')
        for old_col in feb_to_current:
            new_col = old_col.replace('feb_', 'current_')
            try:
                sql = f"EXEC sp_rename 'step03_compare_monthvspreviousmonth.{old_col}', '{new_col}', 'COLUMN'"
                cursor.execute(sql)
                print(f"      ✅ {old_col} → {new_col}")
            except Exception as e:
                if "does not exist" in str(e):
                    print(f"      ⚠️ {old_col} not found")
                else:
                    print(f"      ❌ Error renaming {old_col}: {e}")

        # Rename jan_ columns to previous_
        print('   🔄 Renaming jan_ → previous_...')
        for old_col in jan_to_previous:
            new_col = old_col.replace('jan_', 'previous_')
            try:
                sql = f"EXEC sp_rename 'step03_compare_monthvspreviousmonth.{old_col}', '{new_col}', 'COLUMN'"
                cursor.execute(sql)
                print(f"      ✅ {old_col} → {new_col}")
            except Exception as e:
                if "does not exist" in str(e):
                    print(f"      ⚠️ {old_col} not found")
                else:
                    print(f"      ❌ Error renaming {old_col}: {e}")

        self.db.connection.commit()
        print("   🎉 Column renaming completed!")
        
    def get_february_baselines(self):
        """Calculate February 2025 peak delivery and volume values for each EQ symbol"""
        print("📊 Calculating February 2025 peak baselines...")
        
        cursor = self.db.connection.cursor()
        
        # Get peak delivery and volume for each symbol in February
        baseline_query = """
        SELECT 
            symbol,
            MAX(deliv_qty) as peak_delivery,
            MAX(ttl_trd_qnty) as peak_volume
        FROM step01_equity_daily
        WHERE YEAR(trade_date) = 2025 
            AND MONTH(trade_date) = 2
            AND series = 'EQ'
            AND deliv_qty IS NOT NULL
            AND deliv_qty > 0
        GROUP BY symbol
        ORDER BY symbol
        """
        
        cursor.execute(baseline_query)
        baselines = {}
        
        for row in cursor.fetchall():
            symbol = row[0]
            baselines[symbol] = {
                'peak_delivery': int(row[1]) if row[1] else 0,
                'peak_volume': int(row[2]) if row[2] else 0
            }
        
        print(f"   ✅ Calculated February baselines for {len(baselines):,} symbols")
        print(f"   📊 February 2025 peak delivery and volume values established")
        
        # Show sample baselines
        sample_symbols = list(baselines.keys())[:5]
        for symbol in sample_symbols:
            baseline = baselines[symbol]
            print(f"      {symbol}: Delivery {baseline['peak_delivery']:,}, Volume {baseline['peak_volume']:,}")
        
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
        
    def process_february_march_comparison(self):
        """Main analysis: Compare March daily data vs February baselines"""
        print("🔍 Processing February vs March comparison analysis...")
        print("=" * 70)
        
        # Get February baselines
        feb_baselines = self.get_february_baselines()
        
        # Get March daily data
        march_data = self.get_march_daily_data()
        
        exceedance_records = []
        processed_count = 0
        volume_exceeds = 0
        delivery_exceeds = 0
        both_exceed = 0
        
        print("📊 Processing March daily records against February baselines...")
        
        for record in march_data:
            processed_count += 1
            
            if processed_count % 5000 == 0:
                print(f"   📊 Processed {processed_count:,} records, delivery exceeds: {delivery_exceeds:,}, volume exceeds: {volume_exceeds:,}...")
            
            (trade_date, symbol, series, prev_close, open_price, high_price, low_price,
             last_price, close_price, avg_price, ttl_trd_qnty, turnover_lacs, 
             no_of_trades, deliv_qty, deliv_per, source_file) = record
            
            # Skip if no February baseline available for this symbol
            if symbol not in feb_baselines:
                continue
                
            baseline = feb_baselines[symbol]
            feb_peak_delivery = baseline['peak_delivery']
            feb_peak_volume = baseline['peak_volume']
            
            # Check for exceedances
            volume_exceeded = bool(ttl_trd_qnty and feb_peak_volume and ttl_trd_qnty > feb_peak_volume)
            delivery_exceeded = bool(deliv_qty and feb_peak_delivery and deliv_qty > feb_peak_delivery)
            
            # Only record if at least one exceeds baseline (focusing on delivery as requested)
            if delivery_exceeded or volume_exceeded:
                # Calculate increases
                vol_increase_abs = (ttl_trd_qnty - feb_peak_volume) if volume_exceeded else 0
                del_increase_abs = (deliv_qty - feb_peak_delivery) if delivery_exceeded else 0
                
                vol_increase_pct = (vol_increase_abs / feb_peak_volume * 100) if feb_peak_volume > 0 and volume_exceeded else 0
                del_increase_pct = (del_increase_abs / feb_peak_delivery * 100) if feb_peak_delivery > 0 and delivery_exceeded else 0
                
                # Create record for existing table structure
                exceedance_record = (
                    trade_date, symbol, series,
                    ttl_trd_qnty, deliv_qty,
                    prev_close, open_price, high_price, low_price,
                    last_price, close_price, avg_price,
                    turnover_lacs, no_of_trades, deliv_per,
                    feb_peak_volume, feb_peak_delivery,
                    None, None,  # baseline dates (not tracking specific dates for peaks)
                    volume_exceeded, delivery_exceeded,
                    vol_increase_abs, del_increase_abs,
                    round(vol_increase_pct, 2), round(del_increase_pct, 2),
                    '2025-03', source_file
                )
                
                exceedance_records.append(exceedance_record)
                
                # Count exceedances
                if volume_exceeded:
                    volume_exceeds += 1
                if delivery_exceeded:
                    delivery_exceeds += 1
                if volume_exceeded and delivery_exceeded:
                    both_exceed += 1
        
        print(f"\n✅ February vs March Analysis Complete!")
        print(f"   📊 Processed: {processed_count:,} March daily records")
        print(f"   📈 Volume exceedances: {volume_exceeds:,}")
        print(f"   📦 Delivery exceedances: {delivery_exceeds:,}")
        print(f"   🎯 Both exceeded: {both_exceed:,}")
        print(f"   💾 Total records to store: {len(exceedance_records):,}")
        
        return exceedance_records
        
    def insert_exceedance_records(self, exceedance_records):
        """Insert exceedance records into step03_compare_monthvspreviousmonth table"""
        if not exceedance_records:
            print("   ⚠️ No exceedance records to insert")
            return
            
        print(f"💾 Inserting {len(exceedance_records):,} records into step03_compare_monthvspreviousmonth...")
        
        cursor = self.db.connection.cursor()
        
        # Clear existing data - use a different approach since analysis_month doesn't exist
        cursor.execute("DELETE FROM step03_compare_monthvspreviousmonth WHERE comparison_type = 'MAR_VS_FEB_2025'")
        print(f"   🗑️ Cleared existing March vs February analysis data")
        
        # Use the existing table structure but adapt our data to fit (with new column names)
        insert_sql = """
        INSERT INTO step03_compare_monthvspreviousmonth 
        (current_trade_date, symbol, series,
         current_prev_close, current_open_price, current_high_price, current_low_price,
         current_last_price, current_close_price, current_avg_price,
         current_ttl_trd_qnty, current_turnover_lacs, current_no_of_trades, 
         current_deliv_qty, current_deliv_per, current_source_file,
         previous_baseline_date, previous_prev_close, previous_open_price, previous_high_price, previous_low_price,
         previous_last_price, previous_close_price, previous_avg_price,
         previous_ttl_trd_qnty, previous_turnover_lacs, previous_no_of_trades,
         previous_deliv_qty, previous_deliv_per, previous_source_file,
         delivery_increase_abs, delivery_increase_pct, comparison_type)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        # Adapt our records to fit the existing table structure
        adapted_records = []
        for record in exceedance_records:
            (trade_date, symbol, series, ttl_trd_qnty, deliv_qty, prev_close, open_price, 
             high_price, low_price, last_price, close_price, avg_price, turnover_lacs, 
             no_of_trades, deliv_per, feb_peak_volume, feb_peak_delivery, baseline_vol_date, 
             baseline_del_date, volume_exceeded, delivery_exceeded, vol_increase_abs, 
             del_increase_abs, vol_increase_pct, del_increase_pct, analysis_month, source_file) = record
            
            # Use baseline dates from February peaks (not None)
            jan_baseline_date = baseline_del_date if baseline_del_date else trade_date
            
            adapted_record = (
                trade_date, symbol, series,
                # March data goes in "feb" columns (current month data)
                prev_close or 0.0, open_price or 0.0, high_price or 0.0, low_price or 0.0, 
                last_price or 0.0, close_price or 0.0, avg_price or 0.0,
                ttl_trd_qnty or 0, turnover_lacs or 0.0, no_of_trades or 0, 
                deliv_qty or 0, deliv_per or 0.0, source_file or '',
                # February baseline data goes in "jan" columns (baseline data)  
                jan_baseline_date,  # jan_baseline_date - use baseline date
                0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,  # jan price data (default to 0)
                feb_peak_volume or 0, 0.0, 0,  # jan volume data
                feb_peak_delivery or 0, 0.0, '',  # jan delivery data  
                del_increase_abs or 0, del_increase_pct or 0.0, 'MAR_VS_FEB_2025'
            )
            adapted_records.append(adapted_record)
        
        # Insert in batches
        batch_size = 1000
        for i in range(0, len(adapted_records), batch_size):
            batch = adapted_records[i:i+batch_size]
            cursor.executemany(insert_sql, batch)
            
            if (i + batch_size) % 5000 == 0 or i + batch_size >= len(adapted_records):
                self.db.connection.commit()
                print(f"   📊 Inserted {min(i + batch_size, len(adapted_records)):,} records...")
        
        print(f"   ✅ Successfully inserted {len(adapted_records):,} records")
        
    def show_results_summary(self):
        """Show summary of analysis results from step03_compare_monthvspreviousmonth table"""
        cursor = self.db.connection.cursor()
        
        print("\n📊 STEP 03 FEBRUARY vs MARCH ANALYSIS RESULTS")
        print("=" * 55)
        
        # Count total records for this analysis (using comparison_type instead of analysis_month)
        cursor.execute("SELECT COUNT(*) FROM step03_compare_monthvspreviousmonth WHERE comparison_type = 'MAR_VS_FEB_2025'")
        total_records = cursor.fetchone()[0]
        print(f"� Total exceedance records: {total_records:,}")
        
        # Show top delivery increases using new column names
        cursor.execute("""
        SELECT TOP 10 symbol, current_trade_date, current_deliv_qty, previous_deliv_qty, 
               delivery_increase_abs, delivery_increase_pct 
        FROM step03_compare_monthvspreviousmonth 
        WHERE comparison_type = 'MAR_VS_FEB_2025' AND delivery_increase_pct > 0
        ORDER BY delivery_increase_pct DESC
        """)
        
        print("\n🚀 TOP 10 DELIVERY INCREASES:")
        print(f"{'Symbol':<15} {'Date':<12} {'March Deliv':<15} {'Feb Peak':<15} {'Increase':<15} {'% Inc':<10}")
        print("-" * 90)
        
        for row in cursor.fetchall():
            symbol, trade_date, march_deliv, feb_peak, abs_inc, pct_inc = row
            print(f"{symbol:<15} {trade_date.strftime('%Y-%m-%d'):<12} {march_deliv:>14,} {feb_peak:>14,} {abs_inc:>14,} {pct_inc:>9.1f}%")
        
        # Show symbols with most frequent exceedances
        cursor.execute("""
        SELECT symbol, COUNT(*) as exceedance_days,
               MAX(delivery_increase_pct) as max_increase,
               AVG(delivery_increase_pct) as avg_increase
        FROM step03_compare_monthvspreviousmonth 
        WHERE comparison_type = 'MAR_VS_FEB_2025' AND delivery_increase_pct > 0
        GROUP BY symbol
        HAVING COUNT(*) >= 5
        ORDER BY COUNT(*) DESC, MAX(delivery_increase_pct) DESC
        """)
        
        print(f"\n📈 SYMBOLS WITH MOST FREQUENT EXCEEDANCES (5+ days):")
        print(f"{'Symbol':<15} {'Days':<8} {'Max %':<12} {'Avg %':<12}")
        print("-" * 50)
        
        frequent_performers = cursor.fetchall()
        for row in frequent_performers[:15]:  # Top 15
            symbol, days, max_pct, avg_pct = row
            print(f"{symbol:<15} {days:<8} {max_pct:>11.1f}% {avg_pct:>11.1f}%")
        
        print(f"\n✅ Analysis complete! {total_records:,} exceedance records saved to step03_compare_monthvspreviousmonth table")
        print(f"� Data stored with comparison_type = 'MAR_VS_FEB_2025'")
        print("🎯 Use this table for Step 3 analysis and reporting!")
        
        # Top delivery exceedances
        print(f"\n🏆 TOP 10 DELIVERY EXCEEDANCES (March vs February):")
        cursor.execute("""
            SELECT TOP 10 trade_date, symbol, daily_deliv_qty, monthly_delivery_baseline, delivery_increase_pct
            FROM step03_compare_monthvspreviousmonth
            WHERE delivery_exceeded = 1 AND analysis_month = '2025-03'
            ORDER BY delivery_increase_pct DESC
        """)
        
        for row in cursor.fetchall():
            print(f"   {row[0]} | {row[1]} | March: {row[2]:,} | Feb Peak: {row[3]:,} | +{row[4]:.1f}%")
            
        # Top volume exceedances
        print(f"\n📈 TOP 10 VOLUME EXCEEDANCES (March vs February):")
        cursor.execute("""
            SELECT TOP 10 trade_date, symbol, daily_ttl_trd_qnty, monthly_volume_baseline, volume_increase_pct
            FROM step03_compare_monthvspreviousmonth
            WHERE volume_exceeded = 1 AND analysis_month = '2025-03'
            ORDER BY volume_increase_pct DESC
        """)
        
        for row in cursor.fetchall():
            print(f"   {row[0]} | {row[1]} | March: {row[2]:,} | Feb Peak: {row[3]:,} | +{row[4]:.1f}%")
        
        # Date distribution
        print(f"\n📅 EXCEEDANCES BY DATE:")
        cursor.execute("""
            SELECT trade_date, COUNT(*) as exceedances
            FROM step03_compare_monthvspreviousmonth
            WHERE analysis_month = '2025-03'
            GROUP BY trade_date
            ORDER BY trade_date
        """)
        
        for row in cursor.fetchall()[:15]:  # Show first 15 dates
            print(f"   {row[0]}: {row[1]:,} exceedances")
            
    def run_complete_analysis(self):
        """Run complete Step 3 February vs March analysis using existing table"""
        print("🚀 Starting Step 3 Analysis with existing table structure...")
        
        # Rename columns to more meaningful names
        self.rename_table_columns()
        
        # Process comparisons
        exceedance_records = self.process_february_march_comparison()
        
        # Insert into existing table
        self.insert_exceedance_records(exceedance_records)
        
        # Show summary
        self.show_results_summary()
        
        print(f"\n✅ STEP 03 FEBRUARY vs MARCH ANALYSIS COMPLETE!")
        print(f"📊 Table: step03_compare_monthvspreviousmonth")
        print(f"🎯 Analysis: March 2025 daily vs February 2025 peaks")
        print(f"📈 Focus: Delivery exceedances where March > February")
        print(f"🚀 Ready for business intelligence and decision support")
        
    def close(self):
        """Close database connection"""
        self.db.close()

def main():
    analyzer = Step03FebruaryMarchComparison()
    try:
        analyzer.run_complete_analysis()
    finally:
        analyzer.close()

if __name__ == '__main__':
    main()