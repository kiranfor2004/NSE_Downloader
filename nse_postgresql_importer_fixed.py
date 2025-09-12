#!/usr/bin/env python3
"""
📊 NSE PostgreSQL Data Importer (Fixed)
Import NSE CSV files with correct table structure
"""

import os
import json
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

class NSEPostgreSQLImporter:
    def __init__(self, config_file="database_config.json"):
        """Initialize PostgreSQL importer"""
        self.config_file = config_file
        self.config = self.load_config()
        self.engine = None
        self.connect()
    
    def load_config(self):
        """Load database configuration"""
        try:
            with open(self.config_file, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"❌ Config file not found: {self.config_file}")
            print("💡 Run postgresql_easy_setup.py first")
            exit(1)
    
    def connect(self):
        """Connect to PostgreSQL database"""
        try:
            # First connect to postgres database to create nse_data if needed
            postgres_url = f"postgresql://{self.config['username']}:{self.config['password']}@{self.config['host']}:{self.config['port']}/postgres"
            postgres_engine = create_engine(postgres_url)
            
            # Check if nse_data database exists, create if not
            with postgres_engine.connect() as conn:
                result = conn.execute(text("SELECT 1 FROM pg_database WHERE datname = 'nse_data'"))
                if not result.fetchone():
                    conn.execute(text("COMMIT"))
                    conn.execute(text("CREATE DATABASE nse_data"))
                    print("✅ Created nse_data database")
            
            # Connect to nse_data database
            nse_url = f"postgresql://{self.config['username']}:{self.config['password']}@{self.config['host']}:{self.config['port']}/nse_data"
            self.engine = create_engine(nse_url)
            
            print("✅ Connected to PostgreSQL nse_data database")
            return True
            
        except Exception as e:
            print(f"❌ Connection failed: {str(e)}")
            return False
    
    def create_table_if_not_exists(self):
        """Create the NSE table with correct structure"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS nse_stock_data (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(50) NOT NULL,
            series VARCHAR(10) NOT NULL,
            date1 DATE NOT NULL,
            prev_close DECIMAL(10,2),
            open_price DECIMAL(10,2),
            high_price DECIMAL(10,2),
            low_price DECIMAL(10,2),
            last_price DECIMAL(10,2),
            close_price DECIMAL(10,2),
            avg_price DECIMAL(10,2),
            ttl_trd_qnty BIGINT,
            turnover_lacs DECIMAL(15,2),
            no_of_trades INTEGER,
            deliv_qty BIGINT,
            deliv_per DECIMAL(5,2),
            import_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            source_file VARCHAR(100),
            UNIQUE(symbol, series, date1)
        );
        
        CREATE INDEX IF NOT EXISTS idx_symbol_date ON nse_stock_data(symbol, date1);
        CREATE INDEX IF NOT EXISTS idx_date ON nse_stock_data(date1);
        CREATE INDEX IF NOT EXISTS idx_symbol ON nse_stock_data(symbol);
        """
        
        try:
            with self.engine.connect() as conn:
                conn.execute(text(create_table_sql))
                conn.commit()
            print("✅ Table nse_stock_data ready")
            return True
        except Exception as e:
            print(f"❌ Error creating table: {e}")
            return False
    
    def import_csv_file(self, csv_path):
        """Import a single CSV file to PostgreSQL"""
        try:
            print(f"   📄 Reading {os.path.basename(csv_path)}...")
            
            # Read CSV with proper column names
            expected_columns = [
                'SYMBOL', 'SERIES', 'DATE1', 'PREV_CLOSE', 'OPEN_PRICE', 
                'HIGH_PRICE', 'LOW_PRICE', 'LAST_PRICE', 'CLOSE_PRICE', 
                'AVG_PRICE', 'TTL_TRD_QNTY', 'TURNOVER_LACS', 'NO_OF_TRADES', 
                'DELIV_QTY', 'DELIV_PER'
            ]
            
            df = pd.read_csv(csv_path, encoding='utf-8')
            
            # Clean column names and handle extra spaces
            df.columns = [col.strip() for col in df.columns]
            
            # Check if we have the expected columns
            if len(df.columns) < len(expected_columns):
                print(f"      ⚠️ Column mismatch: expected {len(expected_columns)}, got {len(df.columns)}")
                return False, 0
            
            # Use only the expected columns (in case there are extra)
            df = df.iloc[:, :len(expected_columns)]
            df.columns = expected_columns
            
            # Clean and convert data types
            try:
                # Convert DATE1 to proper date format
                df['DATE1'] = pd.to_datetime(df['DATE1'], format='%d-%b-%Y').dt.date
                
                # Convert numeric columns
                numeric_columns = ['PREV_CLOSE', 'OPEN_PRICE', 'HIGH_PRICE', 'LOW_PRICE', 
                                 'LAST_PRICE', 'CLOSE_PRICE', 'AVG_PRICE', 'TURNOVER_LACS', 'DELIV_PER']
                
                for col in numeric_columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # Convert integer columns
                integer_columns = ['TTL_TRD_QNTY', 'NO_OF_TRADES', 'DELIV_QTY']
                for col in integer_columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int64')
                
            except Exception as e:
                print(f"      ⚠️ Data conversion error: {str(e)[:50]}...")
            
            # Add metadata columns
            df['import_date'] = datetime.now()
            df['source_file'] = os.path.basename(csv_path)
            
            # Clean column names to lowercase for database
            df.columns = [col.lower() for col in df.columns]
            
            # Remove any rows with missing symbol or date
            initial_rows = len(df)
            df = df.dropna(subset=['symbol', 'date1'])
            final_rows = len(df)
            
            if final_rows < initial_rows:
                print(f"      ⚠️ Removed {initial_rows - final_rows} rows with missing data")
            
            if final_rows == 0:
                print(f"      ⚠️ No valid data to import")
                return False, 0
            
            # Import to PostgreSQL using upsert to handle duplicates
            try:
                # Insert data
                df.to_sql('nse_stock_data', self.engine, if_exists='append', index=False, method='multi')
                print(f"      ✅ Imported {final_rows} records")
                return True, final_rows
                
            except Exception as e:
                if "duplicate key value" in str(e):
                    print(f"      ⚠️ Skipped - data already exists")
                    return True, 0
                else:
                    print(f"      ❌ Database error: {str(e)[:50]}...")
                    return False, 0
            
        except Exception as e:
            print(f"      ❌ Error: {str(e)[:50]}...")
            return False, 0
    
    def get_current_stats(self):
        """Get current database statistics"""
        try:
            with self.engine.connect() as conn:
                # Total records
                result = conn.execute(text("SELECT COUNT(*) FROM nse_stock_data"))
                total_records = result.scalar() or 0
                
                # Unique symbols
                result = conn.execute(text("SELECT COUNT(DISTINCT symbol) FROM nse_stock_data"))
                unique_symbols = result.scalar() or 0
                
                # Date range
                result = conn.execute(text("SELECT MIN(date1), MAX(date1) FROM nse_stock_data"))
                date_range = result.fetchone()
                
                return {
                    'total_records': total_records,
                    'unique_symbols': unique_symbols,
                    'date_range': date_range
                }
        except:
            return {'total_records': 0, 'unique_symbols': 0, 'date_range': (None, None)}
    
    def import_all_data(self):
        """Import all NSE CSV files"""
        print("📊 NSE PostgreSQL Data Importer (Fixed)")
        print("=" * 50)
        
        # Create table first
        if not self.create_table_if_not_exists():
            print("❌ Could not create table")
            return
        
        # Find all NSE data directories
        data_directories = []
        for item in os.listdir('.'):
            if os.path.isdir(item) and item.startswith('NSE_') and item.endswith('_Data'):
                data_directories.append(item)
        
        if not data_directories:
            print("❌ No NSE data directories found!")
            return
        
        print(f"📁 Found {len(data_directories)} data directories:")
        total_files = 0
        for directory in sorted(data_directories):
            csv_files = [f for f in os.listdir(directory) if f.endswith('.csv')]
            total_files += len(csv_files)
            print(f"   📂 {directory}: {len(csv_files)} CSV files")
        
        print(f"\n🎯 Total CSV files to import: {total_files}")
        
        # Check current database status
        current_stats = self.get_current_stats()
        print(f"📊 Current records in database: {current_stats['total_records']:,}")
        
        if current_stats['total_records'] > 0:
            print(f"   📈 Unique symbols: {current_stats['unique_symbols']:,}")
            if current_stats['date_range'][0]:
                print(f"   📅 Date range: {current_stats['date_range'][0]} to {current_stats['date_range'][1]}")
        
        proceed = input(f"\n🚀 Proceed with import? (yes/no): ").strip().lower()
        if proceed not in ['yes', 'y']:
            print("❌ Import cancelled")
            return
        
        # Import from each directory
        total_imported = 0
        total_records = 0
        total_errors = 0
        
        for directory in sorted(data_directories):
            print(f"\n📂 Processing {directory}...")
            
            csv_files = [f for f in os.listdir(directory) if f.endswith('.csv')]
            if not csv_files:
                continue
            
            for csv_file in sorted(csv_files):
                file_path = os.path.join(directory, csv_file)
                success, record_count = self.import_csv_file(file_path)
                
                if success:
                    total_imported += 1
                    total_records += record_count
                else:
                    total_errors += 1
        
        # Final summary
        print(f"\n📊 Import Summary:")
        print("=" * 30)
        print(f"✅ Files imported: {total_imported}")
        print(f"❌ Files with errors: {total_errors}")
        print(f"📈 Total records imported: {total_records:,}")
        
        # Show final database stats
        final_stats = self.get_current_stats()
        if final_stats['total_records'] > 0:
            print(f"\n📈 Final Database Stats:")
            print(f"   📊 Total records: {final_stats['total_records']:,}")
            print(f"   🏢 Unique symbols: {final_stats['unique_symbols']:,}")
            if final_stats['date_range'][0]:
                print(f"   📅 Date range: {final_stats['date_range'][0]} to {final_stats['date_range'][1]}")
        
        if total_imported > 0:
            print(f"\n🎉 Data import completed successfully!")
            print(f"💾 Your NSE data is now in PostgreSQL")
            print(f"🔍 Table: nse_stock_data")
            print(f"📊 Ready for analysis!")

def main():
    """Main import function"""
    try:
        importer = NSEPostgreSQLImporter()
        importer.import_all_data()
    except KeyboardInterrupt:
        print("\n❌ Import cancelled by user")
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        print("💡 Make sure PostgreSQL is running and configured correctly")

if __name__ == "__main__":
    main()
