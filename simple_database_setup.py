#!/usr/bin/env python3
"""
🗄️ Simple SQLite Database Setup for NSE Data
Zero-configuration database solution
"""

import sqlite3
import pandas as pd
import glob
import os
from datetime import datetime

class SimpleNSEDatabase:
    def __init__(self, db_path="nse_data.db"):
        self.db_path = db_path
        self.connection = None
    
    def connect(self):
        """Connect to SQLite database (creates if doesn't exist)"""
        try:
            self.connection = sqlite3.connect(self.db_path)
            self.connection.execute("PRAGMA foreign_keys = ON")
            print(f"✅ Connected to database: {self.db_path}")
            return True
        except Exception as e:
            print(f"❌ Connection failed: {str(e)}")
            return False
    
    def setup_tables(self):
        """Create all necessary tables"""
        
        tables_sql = [
            """
            CREATE TABLE IF NOT EXISTS nse_raw_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                series TEXT NOT NULL,
                date DATE NOT NULL,
                prev_close REAL,
                open_price REAL,
                high_price REAL,
                low_price REAL,
                last_price REAL,
                close_price REAL,
                avg_price REAL,
                ttl_trd_qnty INTEGER,
                ttl_trd_val REAL,
                deliv_qty INTEGER,
                deliv_per REAL,
                month_year TEXT,
                data_source TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(symbol, series, date)
            )
            """,
            
            """
            CREATE TABLE IF NOT EXISTS nse_unique_analysis (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                month_year TEXT NOT NULL,
                analysis_type TEXT NOT NULL,
                max_value INTEGER NOT NULL,
                max_date DATE NOT NULL,
                series TEXT,
                prev_close REAL,
                open_price REAL,
                high_price REAL,
                low_price REAL,
                last_price REAL,
                close_price REAL,
                avg_price REAL,
                ttl_trd_qnty INTEGER,
                ttl_trd_val REAL,
                deliv_qty INTEGER,
                deliv_per REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(symbol, month_year, analysis_type)
            )
            """,
            
            """
            CREATE TABLE IF NOT EXISTS nse_comparisons (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                month1 TEXT NOT NULL,
                month2 TEXT NOT NULL,
                comparison_type TEXT NOT NULL,
                month1_value INTEGER NOT NULL,
                month2_value INTEGER NOT NULL,
                increase_amount INTEGER NOT NULL,
                increase_percentage REAL NOT NULL,
                times_higher REAL NOT NULL,
                month1_date DATE,
                month2_date DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(symbol, month1, month2, comparison_type)
            )
            """,
            
            """
            CREATE INDEX IF NOT EXISTS idx_raw_symbol_date ON nse_raw_data(symbol, date);
            """,
            
            """
            CREATE INDEX IF NOT EXISTS idx_raw_month_year ON nse_raw_data(month_year);
            """,
            
            """
            CREATE INDEX IF NOT EXISTS idx_unique_symbol_month ON nse_unique_analysis(symbol, month_year);
            """
        ]
        
        try:
            cursor = self.connection.cursor()
            for sql in tables_sql:
                cursor.execute(sql)
            
            self.connection.commit()
            print("✅ All tables and indexes created successfully!")
            return True
            
        except Exception as e:
            print(f"❌ Error creating tables: {str(e)}")
            return False
    
    def import_csv_data(self, csv_file_path, month_year):
        """Import CSV data to database"""
        try:
            # Read CSV
            df = pd.read_csv(csv_file_path)
            df.columns = df.columns.str.strip()
            
            # Add metadata
            df['month_year'] = month_year
            df['data_source'] = os.path.basename(csv_file_path)
            
            # Handle date column
            if 'DATE1' in df.columns:
                df['date'] = pd.to_datetime(df['DATE1'], format='%d-%b-%Y')
            elif 'DATE' in df.columns:
                df['date'] = pd.to_datetime(df['DATE'], format='%d-%b-%Y')
            
            # Rename columns
            column_mapping = {
                'SYMBOL': 'symbol',
                'SERIES': 'series',
                'PREV_CLOSE': 'prev_close',
                'OPEN': 'open_price',
                'HIGH': 'high_price',
                'LOW': 'low_price',
                'LAST': 'last_price',
                'CLOSE': 'close_price',
                'VWAP': 'avg_price',
                'TTL_TRD_QNTY': 'ttl_trd_qnty',
                'TTL_TRD_VAL': 'ttl_trd_val',
                'DELIV_QTY': 'deliv_qty',
                'DELIV_PER': 'deliv_per'
            }
            
            df = df.rename(columns=column_mapping)
            
            # Filter EQ only
            df = df[df['series'] == 'EQ'].copy()
            
            # Insert to database
            df.to_sql('nse_raw_data', self.connection, if_exists='append', index=False)
            
            print(f"✅ Imported {len(df)} records from {os.path.basename(csv_file_path)}")
            return len(df)
            
        except Exception as e:
            print(f"❌ Error importing {csv_file_path}: {str(e)}")
            return 0
    
    def quick_import_all_data(self):
        """Import all CSV data quickly"""
        month_folders = {
            "January_2025": "NSE_January_2025_Data",
            "February_2025": "NSE_February_2025_Data",
            "March_2025": "NSE_March_2025_Data",
            "April_2025": "NSE_April_2025_Data",
            "May_2025": "NSE_May_2025_Data",
            "June_2025": "NSE_June_2025_Data",
            "July_2025": "NSE_July_2025_Data",
            "August_2025": "NSE_August_2025_Data"
        }
        
        total_records = 0
        total_files = 0
        
        print("🚀 Quick import of all NSE data...")
        print("=" * 50)
        
        for month_year, folder in month_folders.items():
            if not os.path.exists(folder):
                continue
                
            # Different patterns for different months
            if "July" in month_year or "August" in month_year:
                pattern = f"{folder}/sec_bhavdata_full_*.csv"
            else:
                pattern = f"{folder}/cm*.csv"
            
            csv_files = glob.glob(pattern)
            
            if csv_files:
                print(f"\n📂 {month_year}: {len(csv_files)} files")
                month_records = 0
                
                for csv_file in csv_files:
                    records = self.import_csv_data(csv_file, month_year)
                    month_records += records
                    total_files += 1
                
                total_records += month_records
                print(f"   📊 {month_records:,} records imported")
        
        print(f"\n🎉 Import Complete!")
        print(f"   📁 Files processed: {total_files}")
        print(f"   📈 Total records: {total_records:,}")
        
        return total_records
    
    def get_summary(self):
        """Get data summary"""
        try:
            cursor = self.connection.cursor()
            
            # Overall summary
            cursor.execute("""
                SELECT month_year, COUNT(*) as records, 
                       COUNT(DISTINCT symbol) as symbols,
                       MIN(date) as start_date, MAX(date) as end_date
                FROM nse_raw_data 
                GROUP BY month_year 
                ORDER BY month_year
            """)
            
            results = cursor.fetchall()
            
            print("\n📊 Database Summary:")
            print("=" * 80)
            print(f"{'Month':<15} {'Records':<10} {'Symbols':<10} {'Start':<12} {'End':<12}")
            print("-" * 80)
            
            total = 0
            for month, records, symbols, start, end in results:
                total += records
                print(f"{month:<15} {records:<10,} {symbols:<10} {start:<12} {end:<12}")
            
            print("-" * 80)
            print(f"{'TOTAL':<15} {total:<10,}")
            
            return results
            
        except Exception as e:
            print(f"❌ Error getting summary: {str(e)}")
            return []
    
    def quick_query(self, sql_query):
        """Execute quick SQL query"""
        try:
            df = pd.read_sql_query(sql_query, self.connection)
            return df
        except Exception as e:
            print(f"❌ Query error: {str(e)}")
            return pd.DataFrame()
    
    def get_top_symbols(self, month_year, limit=10):
        """Get top symbols for a month"""
        sql = f"""
        SELECT symbol, MAX(ttl_trd_qnty) as max_volume, 
               MAX(deliv_qty) as max_delivery
        FROM nse_raw_data 
        WHERE month_year = '{month_year}'
        GROUP BY symbol 
        ORDER BY max_volume DESC 
        LIMIT {limit}
        """
        return self.quick_query(sql)

def main():
    """Quick setup and test"""
    print("🗄️ Simple NSE SQLite Database Setup")
    print("=" * 50)
    
    # Create database instance
    db = SimpleNSEDatabase()
    
    if not db.connect():
        return
    
    if not db.setup_tables():
        return
    
    print("\n✅ Database setup complete!")
    
    # Ask user what to do
    print("\n📋 Options:")
    print("1. Import all CSV data (recommended)")
    print("2. Show current data summary")
    print("3. Quick test query")
    print("4. Skip for now")
    
    choice = input("\nChoose option (1-4): ").strip()
    
    if choice == "1":
        records = db.quick_import_all_data()
        if records > 0:
            db.get_summary()
    
    elif choice == "2":
        db.get_summary()
    
    elif choice == "3":
        # Test query
        df = db.quick_query("SELECT COUNT(*) as total_records FROM nse_raw_data")
        print("📊 Test Query Result:")
        print(df)
    
    print(f"\n🎉 SQLite database ready: {db.db_path}")
    print("💡 You can now query your data using SQL or pandas!")
    
    return db

if __name__ == "__main__":
    database = main()
