#!/usr/bin/env python3
"""
Step 03: New Logic - Daily vs Monthly Baseline Comparison

Purpose:
  Compare February 2025 daily data against February monthly baselines from Step 2.
  When daily values exceed monthly baseline, record complete transaction details.

Logic:
  1. Get February 2025 daily data from step01_equity_daily (series='EQ')
  2. Get February baselines from step02_monthly_analysis 
  3. For each daily record, check if ttl_trd_qnty OR deliv_qty exceeds baseline
  4. If exceeded, record complete line item in step03_compare_monthvspreviousmonth table

Table: step03_compare_monthvspreviousmonth
"""

import pandas as pd
from datetime import datetime
from nse_database_integration import NSEDatabaseManager

class Step03NewLogic:
    def __init__(self):
        self.db = NSEDatabaseManager()
        self.create_step03_table()
        
    def create_step03_table(self):
        """Create step03_compare_monthvspreviousmonth table"""
        print("🔧 Creating step03_compare_monthvspreviousmonth table...")
        
        cursor = self.db.connection.cursor()
        
        create_table_sql = """
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='step03_compare_monthvspreviousmonth' AND xtype='U')
        CREATE TABLE step03_compare_monthvspreviousmonth (
            id BIGINT IDENTITY(1,1) PRIMARY KEY,
            trade_date DATE NOT NULL,
            symbol NVARCHAR(50) NOT NULL,
            series NVARCHAR(10) NOT NULL,
            
            -- Daily actual values
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
            
            -- Monthly baseline values for comparison
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
        print("✅ step03_compare_monthvspreviousmonth table created/verified")
        
    def get_january_baselines(self):
        """Get January 2025 baselines from step02_monthly_analysis"""
        print("📊 Loading January 2025 baselines from step02_monthly_analysis...")
        
        cursor = self.db.connection.cursor()
        
        # Get volume baselines
        cursor.execute("""
            SELECT symbol, peak_value as volume_baseline, peak_date as volume_date
            FROM step02_monthly_analysis
            WHERE analysis_month = '2025-01' AND analysis_type = 'VOLUME'
        """)
        volume_baselines = {row[0]: {'baseline': row[1], 'date': row[2]} for row in cursor.fetchall()}
        
        # Get delivery baselines
        cursor.execute("""
            SELECT symbol, peak_value as delivery_baseline, peak_date as delivery_date
            FROM step02_monthly_analysis
            WHERE analysis_month = '2025-01' AND analysis_type = 'DELIVERY'
        """)
        delivery_baselines = {row[0]: {'baseline': row[1], 'date': row[2]} for row in cursor.fetchall()}
        
        print(f"   📈 Volume baselines (Jan): {len(volume_baselines)} symbols")
        print(f"   📦 Delivery baselines (Jan): {len(delivery_baselines)} symbols")
        
        return volume_baselines, delivery_baselines
        
    def get_february_daily_data(self):
        """Get February 2025 daily data from step01_equity_daily"""
        print("📅 Loading February 2025 daily data from step01_equity_daily...")
        
        cursor = self.db.connection.cursor()
        cursor.execute("""
            SELECT 
                trade_date, symbol, series,
                ttl_trd_qnty, deliv_qty,
                prev_close, open_price, high_price, low_price,
                last_price, close_price, avg_price,
                turnover_lacs, no_of_trades, deliv_per,
                source_file
            FROM step01_equity_daily
            WHERE series = 'EQ' 
            AND FORMAT(trade_date, 'yyyy-MM') = '2025-02'
            ORDER BY trade_date, symbol
        """)
        
        daily_data = cursor.fetchall()
        print(f"   📊 Found {len(daily_data)} daily records for February 2025")
        
        return daily_data
        
    def process_daily_vs_baseline_comparison(self):
        """Main logic: Compare daily data against baselines and store exceedances"""
        print("🔍 Processing daily vs baseline comparisons...")
        print("=" * 60)
        
        # Get baselines
        volume_baselines, delivery_baselines = self.get_january_baselines()
        
        # Get daily data
        daily_data = self.get_february_daily_data()
        
        # Process each daily record
        exceedance_records = []
        volume_exceeds = 0
        delivery_exceeds = 0
        both_exceed = 0
        
        for record in daily_data:
            (trade_date, symbol, series, ttl_trd_qnty, deliv_qty,
             prev_close, open_price, high_price, low_price,
             last_price, close_price, avg_price,
             turnover_lacs, no_of_trades, deliv_per, source_file) = record
            
            # Get baselines for this symbol
            vol_baseline = volume_baselines.get(symbol, {})
            del_baseline = delivery_baselines.get(symbol, {})
            
            vol_baseline_value = vol_baseline.get('baseline', 0)
            del_baseline_value = del_baseline.get('baseline', 0)
            vol_baseline_date = vol_baseline.get('date')
            del_baseline_date = del_baseline.get('date')
            
            # Check exceedances
            volume_exceeded = bool(ttl_trd_qnty and vol_baseline_value and ttl_trd_qnty > vol_baseline_value)
            delivery_exceeded = bool(deliv_qty and del_baseline_value and deliv_qty > del_baseline_value)
            
            # Only record if at least one exceeds baseline
            if volume_exceeded or delivery_exceeded:
                # Calculate increases
                vol_increase_abs = (ttl_trd_qnty - vol_baseline_value) if volume_exceeded else 0
                del_increase_abs = (deliv_qty - del_baseline_value) if delivery_exceeded else 0
                
                vol_increase_pct = (vol_increase_abs / vol_baseline_value * 100) if vol_baseline_value > 0 else 0
                del_increase_pct = (del_increase_abs / del_baseline_value * 100) if del_baseline_value > 0 else 0
                
                # Create record
                exceedance_record = (
                    trade_date, symbol, series,
                    ttl_trd_qnty, deliv_qty,
                    prev_close, open_price, high_price, low_price,
                    last_price, close_price, avg_price,
                    turnover_lacs, no_of_trades, deliv_per,
                    vol_baseline_value, del_baseline_value,
                    vol_baseline_date, del_baseline_date,
                    volume_exceeded, delivery_exceeded,
                    vol_increase_abs, del_increase_abs,
                    round(vol_increase_pct, 2), round(del_increase_pct, 2),
                    '2025-02', source_file
                )
                
                exceedance_records.append(exceedance_record)
                
                # Count exceedances
                if volume_exceeded:
                    volume_exceeds += 1
                if delivery_exceeded:
                    delivery_exceeds += 1
                if volume_exceeded and delivery_exceeded:
                    both_exceed += 1
        
        print(f"📊 Processing Results:")
        print(f"   📈 Volume exceedances: {volume_exceeds:,}")
        print(f"   📦 Delivery exceedances: {delivery_exceeds:,}")
        print(f"   🎯 Both exceeded: {both_exceed:,}")
        print(f"   💾 Total records to store: {len(exceedance_records):,}")
        
        return exceedance_records
        
    def insert_exceedance_records(self, exceedance_records):
        """Insert exceedance records into step03_compare_monthvspreviousmonth table"""
        if not exceedance_records:
            print("⚠️ No exceedance records to insert")
            return
            
        print(f"💾 Inserting {len(exceedance_records):,} records into step03_compare_monthvspreviousmonth...")
        
        cursor = self.db.connection.cursor()
        
        insert_sql = """
        INSERT INTO step03_compare_monthvspreviousmonth 
        (trade_date, symbol, series,
         daily_ttl_trd_qnty, daily_deliv_qty,
         prev_close, open_price, high_price, low_price,
         last_price, close_price, avg_price,
         turnover_lacs, no_of_trades, deliv_per,
         monthly_volume_baseline, monthly_delivery_baseline,
         baseline_volume_date, baseline_delivery_date,
         volume_exceeded, delivery_exceeded,
         volume_increase_abs, delivery_increase_abs,
         volume_increase_pct, delivery_increase_pct,
         analysis_month, source_file)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        # Insert in batches for better performance
        batch_size = 1000
        for i in range(0, len(exceedance_records), batch_size):
            batch = exceedance_records[i:i + batch_size]
            cursor.executemany(insert_sql, batch)
            self.db.connection.commit()
            print(f"   ✅ Inserted batch {i//batch_size + 1}: {len(batch)} records")
        
        print("✅ All exceedance records inserted successfully")
        
    def show_results_summary(self):
        """Show summary of Step 3 analysis results"""
        print("\n📊 STEP 03 ANALYSIS RESULTS SUMMARY")
        print("=" * 50)
        
        cursor = self.db.connection.cursor()
        
        # Total records
        cursor.execute("SELECT COUNT(*) FROM step03_compare_monthvspreviousmonth")
        total_records = cursor.fetchone()[0]
        
        print(f"📋 Total Exceedance Records: {total_records:,}")
        
        # Exceedance breakdown
        cursor.execute("""
            SELECT 
                SUM(CASE WHEN volume_exceeded = 1 THEN 1 ELSE 0 END) as volume_exceeds,
                SUM(CASE WHEN delivery_exceeded = 1 THEN 1 ELSE 0 END) as delivery_exceeds,
                SUM(CASE WHEN volume_exceeded = 1 AND delivery_exceeded = 1 THEN 1 ELSE 0 END) as both_exceed
            FROM step03_compare_monthvspreviousmonth
        """)
        
        result = cursor.fetchone()
        vol_ex = result[0] if result[0] is not None else 0
        del_ex = result[1] if result[1] is not None else 0
        both_ex = result[2] if result[2] is not None else 0
        print(f"📈 Volume Exceedances: {vol_ex:,}")
        print(f"📦 Delivery Exceedances: {del_ex:,}")
        print(f"🎯 Both Exceeded: {both_ex:,}")
        
        # Top performers
        print(f"\n🏆 TOP VOLUME EXCEEDANCES:")
        cursor.execute("""
            SELECT TOP 5 trade_date, symbol, daily_ttl_trd_qnty, volume_increase_pct
            FROM step03_compare_monthvspreviousmonth
            WHERE volume_exceeded = 1
            ORDER BY volume_increase_pct DESC
        """)
        
        for row in cursor.fetchall():
            print(f"   {row[0]} | {row[1]} | Volume: {row[2]:,} (+{row[3]:.1f}%)")
            
        print(f"\n📦 TOP DELIVERY EXCEEDANCES:")
        cursor.execute("""
            SELECT TOP 5 trade_date, symbol, daily_deliv_qty, delivery_increase_pct
            FROM step03_compare_monthvspreviousmonth
            WHERE delivery_exceeded = 1
            ORDER BY delivery_increase_pct DESC
        """)
        
        for row in cursor.fetchall():
            print(f"   {row[0]} | {row[1]} | Delivery: {row[2]:,} (+{row[3]:.1f}%)")
        
        # Date distribution
        print(f"\n📅 EXCEEDANCES BY DATE:")
        cursor.execute("""
            SELECT trade_date, COUNT(*) as exceedances
            FROM step03_compare_monthvspreviousmonth
            GROUP BY trade_date
            ORDER BY trade_date
        """)
        
        for row in cursor.fetchall()[:10]:  # Show first 10 dates
            print(f"   {row[0]}: {row[1]:,} exceedances")
        
    def run_complete_step03_analysis(self):
        """Run complete Step 3 analysis with new logic"""
        print("🚀 STEP 03: Daily vs Monthly Baseline Comparison")
        print("=" * 70)
        print("📋 Logic: February 2025 daily data vs January 2025 monthly baselines")
        print("🎯 Target: Record complete line items when February daily exceeds January monthly")
        print()
        
        # Process comparisons
        exceedance_records = self.process_daily_vs_baseline_comparison()
        
        # Insert into database
        self.insert_exceedance_records(exceedance_records)
        
        # Show summary
        self.show_results_summary()
        
        print(f"\n✅ STEP 03 ANALYSIS COMPLETE!")
        print(f"📊 Table: step03_compare_monthvspreviousmonth")
        print(f"🎯 Ready for querying and further analysis")
        
    def close(self):
        """Close database connection"""
        self.db.close()

def main():
    analyzer = Step03NewLogic()
    try:
        analyzer.run_complete_step03_analysis()
    finally:
        analyzer.close()

if __name__ == '__main__':
    main()
