import pyodbc
import sys
import json
from datetime import datetime, timedelta
from collections import defaultdict

class FebruaryMarchAnalyzerUpdated:
    """
    February vs March 2025 Delivery Analysis with Updated Column Names
    Uses current_ prefix for March data, previous_ prefix for February baselines
    """
    
    def __init__(self):
        self.db = self.connect_database()
        self.verify_table_structure()
    
    def connect_database(self):
        """Connect to SQL Server database"""
        try:
            connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=SRIKIRANREDDY\\SQLEX;DATABASE=master;Trusted_Connection=yes;'
            connection = pyodbc.connect(connection_string)
            print(f"✅ Connected to database: master on SRIKIRANREDDY\\SQLEX")
            return type('DB', (), {'connection': connection})()
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            sys.exit(1)

    def verify_table_structure(self):
        """Check if table has new column names or old ones"""
        cursor = self.db.connection.cursor()
        
        # Check if new column names exist
        cursor.execute("""
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'step03_compare_monthvspreviousmonth' 
        AND COLUMN_NAME LIKE 'current_%'
        """)
        current_cols_count = cursor.fetchone()[0]
        
        cursor.execute("""
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'step03_compare_monthvspreviousmonth' 
        AND COLUMN_NAME LIKE 'previous_%'
        """)
        previous_cols_count = cursor.fetchone()[0]
        
        # Check if old column names exist
        cursor.execute("""
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'step03_compare_monthvspreviousmonth' 
        AND COLUMN_NAME LIKE 'feb_%'
        """)
        feb_cols_count = cursor.fetchone()[0]
        
        print(f"🔍 Table structure analysis:")
        print(f"   current_ columns: {current_cols_count}")
        print(f"   previous_ columns: {previous_cols_count}")
        print(f"   feb_ columns: {feb_cols_count}")
        
        if current_cols_count > 0 and previous_cols_count > 0:
            self.use_new_column_names = True
            print("✅ Using NEW column names (current_/previous_)")
        elif feb_cols_count > 0:
            self.use_new_column_names = False
            print("⚠️ Using OLD column names (feb_/jan_) - will rename first")
        else:
            print("❌ Table structure unclear - stopping")
            sys.exit(1)

    def rename_columns_if_needed(self):
        """Rename columns from feb_/jan_ to current_/previous_ if needed"""
        if self.use_new_column_names:
            print("✅ Table already has new column names")
            return
            
        print("🔄 Renaming columns from feb_/jan_ to current_/previous_...")
        cursor = self.db.connection.cursor()
        
        # Define the column mappings
        feb_columns = [
            'feb_trade_date', 'feb_prev_close', 'feb_open_price', 'feb_high_price', 
            'feb_low_price', 'feb_last_price', 'feb_close_price', 'feb_avg_price',
            'feb_ttl_trd_qnty', 'feb_turnover_lacs', 'feb_no_of_trades', 
            'feb_deliv_qty', 'feb_deliv_per', 'feb_source_file'
        ]
        
        jan_columns = [
            'jan_baseline_date', 'jan_prev_close', 'jan_open_price', 'jan_high_price',
            'jan_low_price', 'jan_last_price', 'jan_close_price', 'jan_avg_price',
            'jan_ttl_trd_qnty', 'jan_turnover_lacs', 'jan_no_of_trades',
            'jan_deliv_qty', 'jan_deliv_per', 'jan_source_file'
        ]

        # Rename feb_ columns to current_
        print('   🔄 Renaming feb_ columns to current_...')
        for col in feb_columns:
            new_name = col.replace('feb_', 'current_')
            try:
                sql = f"EXEC sp_rename 'step03_compare_monthvspreviousmonth.{col}', '{new_name}', 'COLUMN'"
                cursor.execute(sql)
                print(f"      ✅ {col} → {new_name}")
            except Exception as e:
                if "does not exist" in str(e):
                    print(f"      ⚠️ {col} not found (may already be renamed)")
                else:
                    print(f"      ❌ Error renaming {col}: {e}")

        # Rename jan_ columns to previous_
        print('   🔄 Renaming jan_ columns to previous_...')
        for col in jan_columns:
            new_name = col.replace('jan_', 'previous_')
            try:
                sql = f"EXEC sp_rename 'step03_compare_monthvspreviousmonth.{col}', '{new_name}', 'COLUMN'"
                cursor.execute(sql)
                print(f"      ✅ {col} → {new_name}")
            except Exception as e:
                if "does not exist" in str(e):
                    print(f"      ⚠️ {col} not found (may already be renamed)")
                else:
                    print(f"      ❌ Error renaming {col}: {e}")

        self.use_new_column_names = True
        print("   ✅ Column renaming completed")

    def get_february_baselines(self):
        """Calculate February 2025 peak delivery and volume baselines"""
        print("📊 Calculating February 2025 peak baselines...")
        
        cursor = self.db.connection.cursor()
        cursor.execute("""
        SELECT symbol, 
               MAX(deliv_qty) as peak_delivery,
               MAX(ttl_trd_qnty) as peak_volume,
               MAX(trade_date) as last_feb_date
        FROM step01_equity_daily 
        WHERE series = 'EQ' 
          AND YEAR(trade_date) = 2025 
          AND MONTH(trade_date) = 2
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
        
        print(f"   ✅ Calculated February baselines for {len(baselines):,} symbols")
        print(f"   📊 February 2025 peak delivery and volume values established")
        
        # Show some examples
        sample_symbols = list(baselines.keys())[:5]
        for symbol in sample_symbols:
            data = baselines[symbol]
            print(f"      {symbol}: Delivery {data['peak_delivery']:,}, Volume {data['peak_volume']:,}")
        
        return baselines
    
    def get_march_daily_data(self):
        """Get March 2025 daily trading data for EQ series"""
        print("📈 Loading March 2025 daily trading data...")
        
        cursor = self.db.connection.cursor()
        cursor.execute("""
        SELECT trade_date, symbol, series, ttl_trd_qnty, deliv_qty,
               prev_close, open_price, high_price, low_price, last_price,
               close_price, avg_price, turnover_lacs, no_of_trades, deliv_per,
               source_file
        FROM step01_equity_daily 
        WHERE series = 'EQ' 
          AND YEAR(trade_date) = 2025 
          AND MONTH(trade_date) = 3
        ORDER BY trade_date, symbol
        """)
        
        march_data = cursor.fetchall()
        print(f"   ✅ Loaded {len(march_data):,} March 2025 daily records")
        return march_data

    def analyze_exceedances(self, baselines, march_data):
        """Analyze March data vs February baselines for exceedances"""
        print("📊 Processing March daily records against February baselines...")
        
        exceedance_records = []
        vol_exceeds = 0
        del_exceeds = 0
        both_exceed = 0
        processed = 0
        
        for record in march_data:
            processed += 1
            (trade_date, symbol, series, ttl_trd_qnty, deliv_qty, prev_close, 
             open_price, high_price, low_price, last_price, close_price, avg_price, 
             turnover_lacs, no_of_trades, deliv_per, source_file) = record
            
            # Check if we have February baseline for this symbol
            if symbol not in baselines:
                continue
                
            feb_data = baselines[symbol]
            feb_peak_delivery = feb_data['peak_delivery']
            feb_peak_volume = feb_data['peak_volume']
            baseline_date = feb_data['baseline_date']
            
            # Check for exceedances
            volume_exceeded = 1 if ttl_trd_qnty > feb_peak_volume else 0
            delivery_exceeded = 1 if deliv_qty > feb_peak_delivery else 0
            
            # Calculate increases
            vol_increase_abs = ttl_trd_qnty - feb_peak_volume if volume_exceeded else 0
            del_increase_abs = deliv_qty - feb_peak_delivery if delivery_exceeded else 0
            
            vol_increase_pct = (vol_increase_abs / feb_peak_volume * 100) if feb_peak_volume > 0 and volume_exceeded else 0
            del_increase_pct = (del_increase_abs / feb_peak_delivery * 100) if feb_peak_delivery > 0 and delivery_exceeded else 0
            
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
                    turnover_lacs, no_of_trades, deliv_per, feb_peak_volume, feb_peak_delivery, 
                    baseline_date, baseline_date, volume_exceeded, delivery_exceeded, 
                    vol_increase_abs, del_increase_abs, vol_increase_pct, del_increase_pct, 
                    '2025-03', source_file
                )
                exceedance_records.append(exceedance_record)
            
            if processed % 5000 == 0:
                print(f"   📊 Processed {processed:,} records, delivery exceeds: {del_exceeds:,}, volume exceeds: {vol_exceeds:,}...")
        
        print(f"\n✅ February vs March Analysis Complete!")
        print(f"   📊 Processed: {processed:,} March daily records")
        print(f"   📈 Volume exceedances: {vol_exceeds:,}")
        print(f"   📦 Delivery exceedances: {del_exceeds:,}")
        print(f"   🎯 Both exceeded: {both_exceed:,}")
        print(f"   💾 Total records to store: {len(exceedance_records):,}")
        
        return exceedance_records

    def insert_exceedance_records(self, exceedance_records):
        """Insert exceedance records into step03_compare_monthvspreviousmonth table with new column names"""
        if not exceedance_records:
            print("   ⚠️ No exceedance records to insert")
            return
            
        print(f"💾 Inserting {len(exceedance_records):,} records with updated column names...")
        
        cursor = self.db.connection.cursor()
        
        # Clear existing data
        cursor.execute("DELETE FROM step03_compare_monthvspreviousmonth WHERE comparison_type = 'MAR_VS_FEB_2025'")
        print(f"   🗑️ Cleared existing March vs February analysis data")
        
        # Use the new column names (current_ and previous_)
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
        
        # Adapt our records to fit the new table structure
        adapted_records = []
        for record in exceedance_records:
            (trade_date, symbol, series, ttl_trd_qnty, deliv_qty, prev_close, open_price, 
             high_price, low_price, last_price, close_price, avg_price, turnover_lacs, 
             no_of_trades, deliv_per, feb_peak_volume, feb_peak_delivery, baseline_vol_date, 
             baseline_del_date, volume_exceeded, delivery_exceeded, vol_increase_abs, 
             del_increase_abs, vol_increase_pct, del_increase_pct, analysis_month, source_file) = record
            
            # Use baseline dates from February peaks (not None)
            previous_baseline_date = baseline_del_date if baseline_del_date else trade_date
            
            adapted_record = (
                trade_date, symbol, series,
                # March data goes in "current" columns (current month data)
                prev_close or 0.0, open_price or 0.0, high_price or 0.0, low_price or 0.0, 
                last_price or 0.0, close_price or 0.0, avg_price or 0.0,
                ttl_trd_qnty or 0, turnover_lacs or 0.0, no_of_trades or 0, 
                deliv_qty or 0, deliv_per or 0.0, source_file or '',
                # February baseline data goes in "previous" columns (baseline data)  
                previous_baseline_date,  # previous_baseline_date - use baseline date
                0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,  # previous price data (default to 0)
                feb_peak_volume or 0, 0.0, 0,  # previous volume data
                feb_peak_delivery or 0, 0.0, '',  # previous delivery data  
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
        
        print(f"   ✅ Successfully inserted {len(adapted_records):,} records with new column names")

    def show_results_summary(self):
        """Show summary of analysis results using new column names"""
        cursor = self.db.connection.cursor()
        
        print("\n📊 STEP 03 FEBRUARY vs MARCH ANALYSIS RESULTS")
        print("=" * 55)
        
        # Count total records for this analysis
        cursor.execute("SELECT COUNT(*) FROM step03_compare_monthvspreviousmonth WHERE comparison_type = 'MAR_VS_FEB_2025'")
        total_records = cursor.fetchone()[0]
        print(f"📊 Total exceedance records: {total_records:,}")
        
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
        print(f"📊 Data stored with comparison_type = 'MAR_VS_FEB_2025'")
        print("🎯 Column names updated: current_ = March data, previous_ = February baselines")
        print("🎯 Use this table for Step 3 analysis and reporting!")

    def run_complete_analysis(self):
        """Run the complete February vs March analysis with column renaming"""
        print("\n🚀 STEP 03: February vs March 2025 Delivery Comparison")
        print("=" * 55)
        print("📋 Logic: March 2025 daily delivery vs February 2025 peak delivery")
        print("🎯 Target: Update step03_compare_monthvspreviousmonth table with new column names")
        print("📊 Focus: Delivery exceedances where March > February peak")
        print("🗓️  Baseline: February 2025 | Comparison: March 2025")
        print("")
        
        # Rename columns if needed
        self.rename_columns_if_needed()
        
        # Run analysis
        print("🔍 Processing February vs March comparison analysis...")
        print("=" * 55)
        
        # Get February baselines
        baselines = self.get_february_baselines()
        
        # Get March daily data
        march_data = self.get_march_daily_data()
        
        # Analyze exceedances
        exceedance_records = self.analyze_exceedances(baselines, march_data)
        
        # Insert results
        self.insert_exceedance_records(exceedance_records)
        
        # Show summary
        self.show_results_summary()

def main():
    """Main function"""
    try:
        analyzer = FebruaryMarchAnalyzerUpdated()
        analyzer.run_complete_analysis()
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)
    finally:
        try:
            analyzer.db.connection.close()
            print("📝 Database connection closed")
        except:
            pass

if __name__ == "__main__":
    main()