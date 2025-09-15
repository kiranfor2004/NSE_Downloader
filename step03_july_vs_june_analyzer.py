#!/usr/bin/env python3
"""
Step 03: July vs June 2025 Delivery Comparison - Using Updated Column Names

PURPOSE:
========
Update the existing step03_compare_monthvspreviousmonth table with July vs June comparison.
Use June 2025 peak delivery values as baselines and July 2025 daily data for comparison.

LOGIC:
======
1. Calculate June 2025 peak delivery values for each symbol (series='EQ')
2. Process July 2025 daily data day-by-day  
3. For each symbol, check if daily delivery > June peak delivery
4. Record complete transaction details in existing step03_compare_monthvspreviousmonth table

COLUMN MAPPING:
===============
- current_* columns = July 2025 data (current analysis month)
- previous_* columns = June 2025 baselines (previous comparison month)

TARGET TABLE: step03_compare_monthvspreviousmonth (with meaningful column names)
"""

import pandas as pd
from datetime import datetime, date
from nse_database_integration import NSEDatabaseManager

class Step03JulyJuneComparison:
    def __init__(self):
        """Initialize the Step 3 analyzer for July vs June comparison"""
        self.db = NSEDatabaseManager()
        
        print("🚀 STEP 03: July vs June 2025 Delivery Comparison")
        print("=" * 60)
        print("📋 Logic: July 2025 daily delivery vs June 2025 peak delivery")
        print("🎯 Target: Update step03_compare_monthvspreviousmonth table")
        print("📊 Focus: Delivery exceedances where July > June peak")
        print("🗓️  Baseline: June 2025 | Comparison: July 2025")
        print("💡 Column names: current_ = July data, previous_ = June baselines")
        print()
        
    def get_june_baselines(self):
        """Calculate June 2025 peak delivery and volume values for each EQ symbol"""
        print("📊 Calculating June 2025 peak baselines...")
        
        cursor = self.db.connection.cursor()
        cursor.execute("""
        SELECT symbol, 
               MAX(deliv_qty) as peak_delivery,
               MAX(ttl_trd_qnty) as peak_volume,
               MAX(trade_date) as last_june_date
        FROM step01_equity_daily 
        WHERE series = 'EQ' 
          AND YEAR(trade_date) = 2025 
          AND MONTH(trade_date) = 6
        GROUP BY symbol
        ORDER BY symbol
        """)
        
        results = cursor.fetchall()
        june_data = {}
        
        for row in results:
            symbol, peak_delivery, peak_volume, last_june_date = row
            june_data[symbol] = {
                'peak_delivery': peak_delivery,
                'peak_volume': peak_volume, 
                'last_june_date': last_june_date
            }
        
        print(f"   ✅ Calculated baselines for {len(june_data):,} symbols")
        return june_data

    def get_july_daily_data(self):
        """Get all July 2025 daily data for EQ series"""
        print("📅 Fetching July 2025 daily data...")
        
        cursor = self.db.connection.cursor()
        cursor.execute("""
        SELECT trade_date, symbol, series, prev_close, open_price, high_price, 
               low_price, last_price, close_price, avg_price, ttl_trd_qnty, 
               turnover_lacs, no_of_trades, deliv_qty, deliv_per, source_file
        FROM step01_equity_daily 
        WHERE series = 'EQ' 
          AND YEAR(trade_date) = 2025 
          AND MONTH(trade_date) = 7
        ORDER BY trade_date, symbol
        """)
        
        results = cursor.fetchall()
        print(f"   ✅ Retrieved {len(results):,} July daily records")
        return results

    def find_exceedances(self, june_baselines, july_data):
        """Find records where July delivery > June peak delivery"""
        print("🔍 Analyzing delivery exceedances...")
        
        exceedances = []
        delivery_exceedances = 0
        volume_exceedances = 0
        both_exceeded = 0
        
        for row in july_data:
            (trade_date, symbol, series, prev_close, open_price, high_price, 
             low_price, last_price, close_price, avg_price, ttl_trd_qnty, 
             turnover_lacs, no_of_trades, deliv_qty, deliv_per, source_file) = row
            
            if symbol in june_baselines:
                june_peak_delivery = june_baselines[symbol]['peak_delivery']
                june_peak_volume = june_baselines[symbol]['peak_volume']
                
                delivery_exceeded = deliv_qty > june_peak_delivery
                volume_exceeded = ttl_trd_qnty > june_peak_volume
                
                if delivery_exceeded or volume_exceeded:
                    # Calculate increases
                    del_increase_abs = deliv_qty - june_peak_delivery
                    del_increase_pct = ((deliv_qty - june_peak_delivery) / june_peak_delivery * 100) if june_peak_delivery > 0 else 0
                    
                    exceedances.append({
                        'trade_date': trade_date,
                        'symbol': symbol,
                        'series': series,
                        'july_delivery': deliv_qty,
                        'june_peak_delivery': june_peak_delivery,
                        'july_volume': ttl_trd_qnty,
                        'june_peak_volume': june_peak_volume,
                        'del_increase_abs': del_increase_abs,
                        'del_increase_pct': del_increase_pct,
                        'prev_close': prev_close,
                        'open_price': open_price,
                        'high_price': high_price,
                        'low_price': low_price,
                        'last_price': last_price,
                        'close_price': close_price,
                        'avg_price': avg_price,
                        'turnover_lacs': turnover_lacs,
                        'no_of_trades': no_of_trades,
                        'deliv_per': deliv_per,
                        'source_file': source_file,
                        'delivery_exceeded': delivery_exceeded,
                        'volume_exceeded': volume_exceeded,
                        'baseline_date': june_baselines[symbol]['last_june_date']
                    })
                    
                    if delivery_exceeded:
                        delivery_exceedances += 1
                    if volume_exceeded:
                        volume_exceedances += 1
                    if delivery_exceeded and volume_exceeded:
                        both_exceeded += 1
        
        print(f"   ✅ Found {len(exceedances):,} exceedance records")
        print(f"   📈 Delivery exceedances: {delivery_exceedances:,}")
        print(f"   📊 Volume exceedances: {volume_exceedances:,}")
        print(f"   🎯 Both exceeded: {both_exceeded:,}")
        
        return exceedances

    def insert_exceedances(self, exceedances):
        """Insert exceedance records into step03_compare_monthvspreviousmonth table"""
        print("💾 Inserting exceedance records...")
        
        cursor = self.db.connection.cursor()
        
        # Updated INSERT query with meaningful column names
        insert_sql = """
        INSERT INTO step03_compare_monthvspreviousmonth (
            current_trade_date, symbol, series,
            current_prev_close, current_open_price, current_high_price, current_low_price,
            current_last_price, current_close_price, current_avg_price,
            current_ttl_trd_qnty, current_turnover_lacs, current_no_of_trades,
            current_deliv_qty, current_deliv_per, current_source_file,
            previous_baseline_date,
            previous_prev_close, previous_open_price, previous_high_price, previous_low_price,
            previous_last_price, previous_close_price, previous_avg_price,
            previous_ttl_trd_qnty, previous_turnover_lacs, previous_no_of_trades,
            previous_deliv_qty, previous_deliv_per, previous_source_file,
            delivery_increase_abs, delivery_increase_pct, comparison_type
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        # Prepare data for insertion
        adapted_records = []
        for exc in exceedances:
            trade_date = exc['trade_date']
            symbol = exc['symbol']
            series = exc['series']
            
            # July data goes in "current" columns (current analysis month)
            prev_close = exc.get('prev_close', 0.0)
            open_price = exc.get('open_price', 0.0)
            high_price = exc.get('high_price', 0.0)
            low_price = exc.get('low_price', 0.0)
            last_price = exc.get('last_price', 0.0)
            close_price = exc.get('close_price', 0.0)
            avg_price = exc.get('avg_price', 0.0)
            ttl_trd_qnty = exc.get('july_volume', 0)
            turnover_lacs = exc.get('turnover_lacs', 0.0)
            no_of_trades = exc.get('no_of_trades', 0)
            deliv_qty = exc.get('july_delivery', 0)
            deliv_per = exc.get('deliv_per', 0.0)
            source_file = exc.get('source_file', '')
            
            # June baseline data goes in "previous" columns (baseline comparison month)
            june_peak_delivery = exc.get('june_peak_delivery', 0)
            june_peak_volume = exc.get('june_peak_volume', 0)
            del_increase_abs = exc.get('del_increase_abs', 0)
            del_increase_pct = exc.get('del_increase_pct', 0.0)
            baseline_del_date = exc.get('baseline_date')
            
            # Use baseline date if available, otherwise use trade date
            previous_baseline_date = baseline_del_date if baseline_del_date else trade_date
            
            adapted_record = (
                trade_date, symbol, series,
                # July data goes in "current" columns (current analysis month)
                prev_close or 0.0, open_price or 0.0, high_price or 0.0, low_price or 0.0, 
                last_price or 0.0, close_price or 0.0, avg_price or 0.0,
                ttl_trd_qnty or 0, turnover_lacs or 0.0, no_of_trades or 0, 
                deliv_qty or 0, deliv_per or 0.0, source_file or '',
                # June baseline data goes in "previous" columns (baseline comparison month)  
                previous_baseline_date,  # previous_baseline_date - use baseline date
                0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,  # previous price data (default to 0)
                june_peak_volume or 0, 0.0, 0,  # previous volume data
                june_peak_delivery or 0, 0.0, '',  # previous delivery data  
                del_increase_abs or 0, del_increase_pct or 0.0, 'JUL_VS_JUN_2025'
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
        """Display summary of top performers"""
        print("\n🏆 TOP DELIVERY INCREASES (July vs June 2025):")
        print("=" * 70)
        
        cursor = self.db.connection.cursor()
        cursor.execute("""
        SELECT TOP 15 symbol, 
               MAX(current_deliv_qty) as max_july_delivery,
               MAX(previous_deliv_qty) as june_peak_delivery,
               MAX(delivery_increase_pct) as max_increase_pct
        FROM step03_compare_monthvspreviousmonth 
        WHERE comparison_type = 'JUL_VS_JUN_2025'
        GROUP BY symbol
        ORDER BY max_increase_pct DESC
        """)
        
        results = cursor.fetchall()
        for i, (symbol, july_del, june_del, pct_increase) in enumerate(results, 1):
            print(f"{i:2d}. {symbol:12s} | July: {july_del:8,} | June: {june_del:8,} | +{pct_increase:8.1f}%")

    def run_analysis(self):
        """Execute the complete July vs June analysis"""
        try:
            # Step 1: Get June 2025 baselines
            june_baselines = self.get_june_baselines()
            
            # Step 2: Get July 2025 daily data
            july_data = self.get_july_daily_data()
            
            # Step 3: Find exceedances
            exceedances = self.find_exceedances(june_baselines, july_data)
            
            # Step 4: Insert results into database
            if exceedances:
                self.insert_exceedances(exceedances)
                
                # Step 5: Show summary
                self.show_results_summary()
            else:
                print("❌ No exceedances found.")
            
            print(f"\n✅ July vs June 2025 analysis completed successfully!")
            print(f"📊 Results stored in step03_compare_monthvspreviousmonth table")
            
        except Exception as e:
            print(f"❌ Error during analysis: {str(e)}")
            raise
        finally:
            self.db.close()

def main():
    """Main execution function"""
    analyzer = Step03JulyJuneComparison()
    analyzer.run_analysis()

if __name__ == "__main__":
    main()