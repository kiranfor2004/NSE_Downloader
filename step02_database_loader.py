#!/usr/bin/env python3
"""
Step 02 Database Loader: Load monthly analysis results into database

This script performs Step 02 analysis directly from database and loads results back:
1. Generate monthly baseline analysis (highest volume/delivery per symbol)
2. Perform February vs January exceedance analysis
3. Load both analyses into step02_monthly_analysis and step02_exceedance_analysis tables
"""

import pandas as pd
from datetime import datetime
from nse_database_integration import NSEDatabaseManager

class Step02DatabaseLoader:
    def __init__(self):
        self.db = NSEDatabaseManager()
        
    def generate_monthly_analysis(self, target_month='2025-01'):
        """Generate monthly analysis for highest volume/delivery per symbol"""
        print(f"🔍 Generating monthly analysis for {target_month}...")
        
        # Query for highest volume per symbol in the month
        volume_query = f"""
        SELECT 
            '{target_month}' as analysis_month,
            symbol,
            'VOLUME' as analysis_type,
            trade_date as peak_date,
            ttl_trd_qnty as peak_value,
            ttl_trd_qnty,
            deliv_qty,
            close_price,
            turnover_lacs,
            'DATABASE_GENERATED' as analysis_file
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY ttl_trd_qnty DESC) as rn
            FROM step01_equity_daily 
            WHERE series = 'EQ' 
            AND FORMAT(trade_date, 'yyyy-MM') = '{target_month}'
        ) ranked
        WHERE rn = 1
        """
        
        # Query for highest delivery per symbol in the month  
        delivery_query = f"""
        SELECT 
            '{target_month}' as analysis_month,
            symbol,
            'DELIVERY' as analysis_type,
            trade_date as peak_date,
            deliv_qty as peak_value,
            ttl_trd_qnty,
            deliv_qty,
            close_price,
            turnover_lacs,
            'DATABASE_GENERATED' as analysis_file
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY deliv_qty DESC) as rn
            FROM step01_equity_daily 
            WHERE series = 'EQ' 
            AND FORMAT(trade_date, 'yyyy-MM') = '{target_month}'
            AND deliv_qty IS NOT NULL
        ) ranked
        WHERE rn = 1
        """
        
        cursor = self.db.connection.cursor()
        
        # Load volume analysis
        print("📊 Loading volume analysis...")
        cursor.execute(volume_query)
        volume_results = cursor.fetchall()
        
        # Load delivery analysis
        print("📦 Loading delivery analysis...")
        cursor.execute(delivery_query)
        delivery_results = cursor.fetchall()
        
        # Insert into step02_monthly_analysis table
        insert_query = """
        INSERT INTO step02_monthly_analysis 
        (analysis_month, symbol, analysis_type, peak_date, peak_value, 
         ttl_trd_qnty, deliv_qty, close_price, turnover_lacs, analysis_file)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        total_inserted = 0
        for row in volume_results + delivery_results:
            cursor.execute(insert_query, row)
            total_inserted += 1
            
        self.db.connection.commit()
        print(f"✅ Inserted {total_inserted} monthly analysis records for {target_month}")
        
    def generate_exceedance_analysis(self, baseline_month='2025-01', compare_month='2025-02'):
        """Generate exceedance analysis comparing two months"""
        print(f"🔍 Generating exceedance analysis: {compare_month} vs {baseline_month}...")
        
        # Get baselines from baseline month (highest values per symbol)
        baseline_query = f"""
        WITH baselines AS (
            SELECT 
                symbol,
                MAX(ttl_trd_qnty) as jan_base_volume,
                MAX(deliv_qty) as jan_base_delivery
            FROM step01_equity_daily 
            WHERE series = 'EQ' 
            AND FORMAT(trade_date, 'yyyy-MM') = '{baseline_month}'
            GROUP BY symbol
        )
        SELECT * FROM baselines
        """
        
        cursor = self.db.connection.cursor()
        cursor.execute(baseline_query)
        baselines = {row[0]: {'volume': row[1], 'delivery': row[2]} for row in cursor.fetchall()}
        
        print(f"📊 Found baselines for {len(baselines)} symbols")
        
        # Get compare month daily data
        compare_query = f"""
        SELECT trade_date, symbol, ttl_trd_qnty, deliv_qty, close_price
        FROM step01_equity_daily 
        WHERE series = 'EQ' 
        AND FORMAT(trade_date, 'yyyy-MM') = '{compare_month}'
        ORDER BY trade_date, symbol
        """
        
        cursor.execute(compare_query)
        compare_data = cursor.fetchall()
        
        print(f"📅 Found {len(compare_data)} daily records for {compare_month}")
        
        # Find exceedances
        exceedances = []
        comparison_name = f"{compare_month.replace('-', '')}_vs_{baseline_month.replace('-', '')}"
        
        for row in compare_data:
            trade_date, symbol, volume, delivery, close_price = row
            
            if symbol in baselines:
                baseline = baselines[symbol]
                
                # Check volume exceedance
                if volume and baseline['volume'] and volume > baseline['volume']:
                    exceedances.append((
                        comparison_name,
                        trade_date,
                        symbol,
                        'VOLUME_EXCEEDED',
                        volume,
                        baseline['volume'],
                        None,  # jan_baseline_date
                        volume,
                        delivery,
                        close_price
                    ))
                
                # Check delivery exceedance
                if delivery and baseline['delivery'] and delivery > baseline['delivery']:
                    exceedances.append((
                        comparison_name,
                        trade_date,
                        symbol,
                        'DELIVERY_EXCEEDED', 
                        delivery,
                        baseline['delivery'],
                        None,  # jan_baseline_date
                        volume,
                        delivery,
                        close_price
                    ))
        
        # Insert exceedances
        if exceedances:
            insert_query = """
            INSERT INTO step02_exceedance_analysis 
            (comparison_name, feb_date, symbol, analysis_type, feb_value, jan_baseline,
             jan_baseline_date, ttl_trd_qnty, deliv_qty, close_price)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            cursor.executemany(insert_query, exceedances)
            self.db.connection.commit()
            print(f"✅ Inserted {len(exceedances)} exceedance records")
        else:
            print("⚠️ No exceedances found")
            
    def run_complete_step02_analysis(self):
        """Run complete Step 02 analysis for all available months"""
        print("🚀 Running Complete Step 02 Analysis for All Months...")
        print("=" * 50)
        
        # Get all available months from database
        cursor = self.db.connection.cursor()
        cursor.execute("""
            SELECT DISTINCT FORMAT(trade_date, 'yyyy-MM') as month
            FROM step01_equity_daily 
            WHERE series = 'EQ'
            ORDER BY month
        """)
        
        available_months = [row[0] for row in cursor.fetchall()]
        print(f"📅 Found {len(available_months)} months: {', '.join(available_months)}")
        print()
        
        # Generate monthly analysis for all months
        for month in available_months:
            print(f"📊 Processing {month}...")
            self.generate_monthly_analysis(month)
        
        print()
        print("🔍 Generating exceedance analyses...")
        
        # Generate exceedance analysis for consecutive month pairs
        for i in range(len(available_months) - 1):
            baseline_month = available_months[i]
            compare_month = available_months[i + 1]
            print(f"📈 Comparing {compare_month} vs {baseline_month}")
            self.generate_exceedance_analysis(baseline_month, compare_month)
        
        # Show summary
        self.show_results_summary()
        
    def show_results_summary(self):
        """Show summary of loaded Step 02 data"""
        print("\n📊 Step 02 Analysis Results Summary:")
        print("=" * 50)
        
        cursor = self.db.connection.cursor()
        
        # Monthly analysis summary
        cursor.execute("""
            SELECT analysis_month, analysis_type, COUNT(*) as count
            FROM step02_monthly_analysis
            GROUP BY analysis_month, analysis_type
            ORDER BY analysis_month, analysis_type
        """)
        
        print("Monthly Analysis:")
        for row in cursor.fetchall():
            print(f"  {row[0]} {row[1]}: {row[2]:,} symbols")
            
        # Exceedance analysis summary
        cursor.execute("""
            SELECT comparison_name, analysis_type, COUNT(*) as count
            FROM step02_exceedance_analysis
            GROUP BY comparison_name, analysis_type
            ORDER BY comparison_name, analysis_type
        """)
        
        print("\nExceedance Analysis:")
        for row in cursor.fetchall():
            print(f"  {row[0]} {row[1]}: {row[2]:,} records")
            
    def close(self):
        """Close database connection"""
        self.db.close()

def main():
    loader = Step02DatabaseLoader()
    try:
        loader.run_complete_step02_analysis()
    finally:
        loader.close()

if __name__ == '__main__':
    main()
