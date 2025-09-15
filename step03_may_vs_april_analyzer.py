#!/usr/bin/env python3
"""
Step 03: May vs April 2025 Delivery Comparison - Using Updated Column Names

PURPOSE:
========
Update the existing step03_compare_monthvspreviousmonth table with May vs April comparison.
Use April 2025 peak delivery values as baselines and May 2025 daily data for comparison.

LOGIC:
======
1. Calculate April 2025 peak delivery values for each symbol (series='EQ')
2. Process May 2025 daily data day-by-day  
3. For each symbol, check if daily delivery > April peak delivery
4. Record complete transaction details in existing step03_compare_monthvspreviousmonth table

COLUMN MAPPING:
===============
- current_* columns = May 2025 data (current analysis month)
- previous_* columns = April 2025 baselines (previous comparison month)

TARGET TABLE: step03_compare_monthvspreviousmonth (with meaningful column names)
"""

import pandas as pd
from datetime import datetime, date
from nse_database_integration import NSEDatabaseManager

class Step03MayAprilComparison:
    def __init__(self):
        """Initialize the Step 3 analyzer for May vs April comparison"""
        self.db = NSEDatabaseManager()
        
        print("🚀 STEP 03: May vs April 2025 Delivery Comparison")
        print("=" * 60)
        print("📋 Logic: May 2025 daily delivery vs April 2025 peak delivery")
        print("🎯 Target: Update step03_compare_monthvspreviousmonth table")
        print("📊 Focus: Delivery exceedances where May > April peak")
        print("🗓️  Baseline: April 2025 | Comparison: May 2025")
        print("💡 Column names: current_ = May data, previous_ = April baselines")
        print()
        
    def get_april_baselines(self):
        """Calculate April 2025 peak delivery and volume values for each EQ symbol"""
        print("📊 Calculating April 2025 peak baselines...")
        
        cursor = self.db.connection.cursor()
        cursor.execute("""
        SELECT symbol, 
               MAX(deliv_qty) as peak_delivery,
               MAX(ttl_trd_qnty) as peak_volume,
               MAX(trade_date) as last_april_date
        FROM step01_equity_daily 
        WHERE series = 'EQ' 
          AND YEAR(trade_date) = 2025 
          AND MONTH(trade_date) = 4
        GROUP BY symbol
        HAVING MAX(deliv_qty) > 0
        """)
        
        baselines = {}
        for row in cursor.fetchall():
            symbol, peak_delivery, peak_volume, last_date = row
            baselines[symbol] = {
                'peak_delivery': peak_delivery,
                'peak_volume': peak_volume,
                'baseline_date': last_date
            }
        
        print(f"   ✅ Calculated April baselines for {len(baselines):,} symbols")
        print(f"   📊 April 2025 peak delivery and volume values established")
        
        # Show some examples
        sample_symbols = list(baselines.keys())[:5]
        for symbol in sample_symbols:
            data = baselines[symbol]
            print(f"      {symbol}: Delivery {data['peak_delivery']:,}, Volume {data['peak_volume']:,}")
        
        return baselines
    
    def get_may_daily_data(self):
        """Get May 2025 daily trading data for EQ series"""
        print("📈 Loading May 2025 daily trading data...")
        
        cursor = self.db.connection.cursor()
        cursor.execute("""
        SELECT trade_date, symbol, series, ttl_trd_qnty, deliv_qty,
               prev_close, open_price, high_price, low_price, last_price,
               close_price, avg_price, turnover_lacs, no_of_trades, deliv_per,
               source_file
        FROM step01_equity_daily 
        WHERE series = 'EQ' 
          AND YEAR(trade_date) = 2025 
          AND MONTH(trade_date) = 5
        ORDER BY trade_date, symbol
        """)
        
        may_data = cursor.fetchall()
        print(f"   ✅ Loaded {len(may_data):,} May 2025 daily records")
        return may_data

    def analyze_exceedances(self, baselines, may_data):
        """Analyze May data vs April baselines for exceedances"""
        print("📊 Processing May daily records against April baselines...")
        
        exceedance_records = []
        vol_exceeds = 0
        del_exceeds = 0
        both_exceed = 0
        processed = 0
        
        for record in may_data:
            processed += 1
            (trade_date, symbol, series, ttl_trd_qnty, deliv_qty, prev_close, 
             open_price, high_price, low_price, last_price, close_price, avg_price, 
             turnover_lacs, no_of_trades, deliv_per, source_file) = record
            
            # Check if we have April baseline for this symbol
            if symbol not in baselines:
                continue
                
            april_data = baselines[symbol]
            april_peak_delivery = april_data['peak_delivery']
            april_peak_volume = april_data['peak_volume']
            baseline_date = april_data['baseline_date']
            
            # Check for exceedances
            volume_exceeded = 1 if ttl_trd_qnty > april_peak_volume else 0
            delivery_exceeded = 1 if deliv_qty > april_peak_delivery else 0
            
            # Calculate increases
            vol_increase_abs = ttl_trd_qnty - april_peak_volume if volume_exceeded else 0
            del_increase_abs = deliv_qty - april_peak_delivery if delivery_exceeded else 0
            
            vol_increase_pct = (vol_increase_abs / april_peak_volume * 100) if april_peak_volume > 0 and volume_exceeded else 0
            del_increase_pct = (del_increase_abs / april_peak_delivery * 100) if april_peak_delivery > 0 and delivery_exceeded else 0
            
            # Count exceedances
            if volume_exceeded:
                vol_exceeds += 1
            if delivery_exceeded:
                del_exceeds += 1
            if volume_exceeded and delivery_exceeded:
                both_exceed += 1
            
            # Store records where either volume or delivery exceeded
            if volume_exceeded or delivery_exceeded:
                exceedance_record = (
                    trade_date, symbol, series, ttl_trd_qnty, deliv_qty, prev_close, 
                    open_price, high_price, low_price, last_price, close_price, avg_price, 
                    turnover_lacs, no_of_trades, deliv_per, april_peak_volume, april_peak_delivery, 
                    baseline_date, baseline_date, volume_exceeded, delivery_exceeded, 
                    vol_increase_abs, del_increase_abs, vol_increase_pct, del_increase_pct, 
                    '2025-05', source_file
                )
                exceedance_records.append(exceedance_record)
            
            if processed % 5000 == 0:
                print(f"   📊 Processed {processed:,} records, delivery exceeds: {del_exceeds:,}, volume exceeds: {vol_exceeds:,}...")
        
        print(f"\n✅ May vs April Analysis Complete!")
        print(f"   📊 Processed: {processed:,} May daily records")
        print(f"   📈 Volume exceedances: {vol_exceeds:,}")
        print(f"   📦 Delivery exceedances: {del_exceeds:,}")
        print(f"   🎯 Both exceeded: {both_exceed:,}")
        print(f"   💾 Total records to store: {len(exceedance_records):,}")
        
        return exceedance_records

    def insert_exceedance_records(self, exceedance_records):
        """Insert exceedance records into step03_compare_monthvspreviousmonth table with meaningful column names"""
        if not exceedance_records:
            print("   ⚠️ No exceedance records to insert")
            return
            
        print(f"💾 Inserting {len(exceedance_records):,} records into step03_compare_monthvspreviousmonth...")
        
        cursor = self.db.connection.cursor()
        
        # Clear existing May vs April analysis data
        cursor.execute("DELETE FROM step03_compare_monthvspreviousmonth WHERE comparison_type = 'MAY_VS_APR_2025'")
        print(f"   🗑️ Cleared existing May vs April analysis data")
        
        # Use the meaningful column names (current_ = May, previous_ = April)
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
        
        # Adapt our records to fit the table structure with meaningful names
        adapted_records = []
        for record in exceedance_records:
            (trade_date, symbol, series, ttl_trd_qnty, deliv_qty, prev_close, open_price, 
             high_price, low_price, last_price, close_price, avg_price, turnover_lacs, 
             no_of_trades, deliv_per, april_peak_volume, april_peak_delivery, baseline_vol_date, 
             baseline_del_date, volume_exceeded, delivery_exceeded, vol_increase_abs, 
             del_increase_abs, vol_increase_pct, del_increase_pct, analysis_month, source_file) = record
            
            # Use baseline dates from April peaks
            previous_baseline_date = baseline_del_date if baseline_del_date else trade_date
            
            adapted_record = (
                trade_date, symbol, series,
                # May data goes in "current" columns (current analysis month)
                prev_close or 0.0, open_price or 0.0, high_price or 0.0, low_price or 0.0, 
                last_price or 0.0, close_price or 0.0, avg_price or 0.0,
                ttl_trd_qnty or 0, turnover_lacs or 0.0, no_of_trades or 0, 
                deliv_qty or 0, deliv_per or 0.0, source_file or '',
                # April baseline data goes in "previous" columns (baseline comparison month)  
                previous_baseline_date,  # previous_baseline_date - use baseline date
                0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,  # previous price data (default to 0)
                april_peak_volume or 0, 0.0, 0,  # previous volume data
                april_peak_delivery or 0, 0.0, '',  # previous delivery data  
                del_increase_abs or 0, del_increase_pct or 0.0, 'MAY_VS_APR_2025'
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
        
        print(f"   ✅ Successfully inserted {len(adapted_records):,} records with meaningful column names")

    def show_results_summary(self):
        """Show summary of May vs April analysis results using meaningful column names"""
        cursor = self.db.connection.cursor()
        
        print("\n📊 STEP 03 MAY vs APRIL ANALYSIS RESULTS")
        print("=" * 55)
        
        # Count total records for this analysis
        cursor.execute("SELECT COUNT(*) FROM step03_compare_monthvspreviousmonth WHERE comparison_type = 'MAY_VS_APR_2025'")
        total_records = cursor.fetchone()[0]
        print(f"📊 Total exceedance records: {total_records:,}")
        
        # Show top delivery increases using meaningful column names
        cursor.execute("""
        SELECT TOP 10 symbol, current_trade_date, current_deliv_qty, previous_deliv_qty, 
               delivery_increase_abs, delivery_increase_pct 
        FROM step03_compare_monthvspreviousmonth 
        WHERE comparison_type = 'MAY_VS_APR_2025' AND delivery_increase_pct > 0
        ORDER BY delivery_increase_pct DESC
        """)
        
        print("\n🚀 TOP 10 DELIVERY INCREASES (May vs April):")
        print(f"{'Symbol':<15} {'Date':<12} {'May Deliv':<15} {'April Peak':<15} {'Increase':<15} {'% Inc':<10}")
        print("-" * 90)
        
        for row in cursor.fetchall():
            symbol, trade_date, may_deliv, april_peak, abs_inc, pct_inc = row
            print(f"{symbol:<15} {trade_date.strftime('%Y-%m-%d'):<12} {may_deliv:>14,} {april_peak:>14,} {abs_inc:>14,} {pct_inc:>9.1f}%")
        
        # Show symbols with most frequent exceedances
        cursor.execute("""
        SELECT symbol, COUNT(*) as exceedance_days,
               MAX(delivery_increase_pct) as max_increase,
               AVG(delivery_increase_pct) as avg_increase
        FROM step03_compare_monthvspreviousmonth 
        WHERE comparison_type = 'MAY_VS_APR_2025' AND delivery_increase_pct > 0
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
        print(f"📊 Data stored with comparison_type = 'MAY_VS_APR_2025'")
        print("🎯 Column names: current_ = May data, previous_ = April baselines")
        print("🎯 Use this table for Step 3 analysis and reporting!")

    def process_may_april_comparison(self):
        """Process the complete May vs April comparison analysis"""
        # Get April baselines
        baselines = self.get_april_baselines()
        
        # Get May daily data
        may_data = self.get_may_daily_data()
        
        # Analyze exceedances
        exceedance_records = self.analyze_exceedances(baselines, may_data)
        
        return exceedance_records

    def run_complete_analysis(self):
        """Execute the complete May vs April analysis workflow"""
        print("🚀 Starting May vs April Analysis with meaningful column names...")
        
        # Process comparisons
        exceedance_records = self.process_may_april_comparison()
        
        # Insert into existing table
        self.insert_exceedance_records(exceedance_records)
        
        # Show summary
        self.show_results_summary()
        
        print(f"\n✅ STEP 03 MAY vs APRIL ANALYSIS COMPLETE!")
        print(f"📊 Table: step03_compare_monthvspreviousmonth")
        print(f"🎯 Analysis: May 2025 daily vs April 2025 peaks")
        print(f"📈 Focus: Delivery exceedances where May > April")
        print(f"💡 Meaningful columns: current_ = May, previous_ = April")
        print(f"🚀 Ready for business intelligence and decision support")
        
    def close(self):
        """Close database connection"""
        self.db.close()

def main():
    analyzer = Step03MayAprilComparison()
    try:
        analyzer.run_complete_analysis()
    finally:
        analyzer.close()

if __name__ == '__main__':
    main()