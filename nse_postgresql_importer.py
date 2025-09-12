#!/usr/bin/env python3
"""
📊 NSE PostgreSQL Data Importer
Import all NSE CSV files into PostgreSQL database
"""

import os
import json
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import glob

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
                    # Create nse_data database
                    conn.execute(text("COMMIT"))  # End any transaction
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
    
    def import_csv_file(self, csv_path):
        """Import a single CSV file to PostgreSQL"""
        try:
            print(f"   📄 Reading {os.path.basename(csv_path)}...")
            
            # Read CSV with proper encoding
            df = pd.read_csv(csv_path, encoding='utf-8')
            
            # Clean column names (remove spaces, make lowercase)
            df.columns = [col.strip().lower().replace(' ', '_').replace('-', '_') for col in df.columns]
            
            # Add metadata columns
            df['import_date'] = datetime.now()
            df['source_file'] = os.path.basename(csv_path)
            
            # Extract date from filename (e.g., sec_bhavdata_full_01072025.csv)
            try:
                filename = os.path.basename(csv_path)
                if 'bhavdata' in filename:
                    # Extract date part (e.g., 01072025)
                    date_part = filename.split('_')[-1].replace('.csv', '')
                    if len(date_part) == 8 and date_part.isdigit():
                        day = date_part[:2]
                        month = date_part[2:4]
                        year = date_part[4:]
                        trading_date = f"{year}-{month}-{day}"
                        df['trading_date'] = trading_date
                    else:
                        df['trading_date'] = None
                else:
                    df['trading_date'] = None
            except:
                df['trading_date'] = None
            
            # Import to PostgreSQL
            table_name = 'nse_raw_data'
            df.to_sql(table_name, self.engine, if_exists='append', index=False, method='multi')
            
            print(f"      ✅ Imported {len(df)} records")
            return True, len(df)
            
        except Exception as e:
            print(f"      ❌ Error: {str(e)[:50]}...")
            return False, 0
    
    def import_all_data(self):
        """Import all NSE CSV files"""
        print("📊 NSE PostgreSQL Data Importer")
        print("=" * 50)
        
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
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM nse_raw_data"))
                current_records = result.scalar()
                print(f"📊 Current records in database: {current_records:,}")
        except:
            current_records = 0
            print("📊 Table doesn't exist yet - will be created")
        
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
        try:
            with self.engine.connect() as conn:
                # Total records
                result = conn.execute(text("SELECT COUNT(*) FROM nse_raw_data"))
                final_records = result.scalar()
                
                # Unique symbols
                result = conn.execute(text("SELECT COUNT(DISTINCT symbol) FROM nse_raw_data"))
                unique_symbols = result.scalar()
                
                # Unique dates
                result = conn.execute(text("SELECT COUNT(DISTINCT trading_date) FROM nse_raw_data WHERE trading_date IS NOT NULL"))
                unique_dates = result.scalar()
                
                print(f"\n📈 Final Database Stats:")
                print(f"   📊 Total records: {final_records:,}")
                print(f"   🏢 Unique symbols: {unique_symbols:,}")
                print(f"   📅 Trading dates: {unique_dates}")
                
        except Exception as e:
            print(f"⚠️ Could not get final stats: {e}")
        
        if total_imported > 0:
            print(f"\n🎉 Data import completed successfully!")
            print(f"💾 Your NSE data is now in PostgreSQL")
            print(f"🔍 Ready for analysis and queries!")

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
