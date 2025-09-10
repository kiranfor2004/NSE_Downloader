#!/usr/bin/env python3
"""
NSE Data Supabase Uploader & Manager

This script uploads your downloaded NSE CSV data to Supabase cloud database
for better analysis, sharing, and remote access capabilities.

Requirements: 
pip install supabase python-dotenv pandas

Setup:
1. Create free account at https://supabase.com
2. Create new project
3. Get your URL and API key from Settings > API
4. Create .env file with your credentials

Author: Generated for NSE cloud data storage
Date: September 2025
"""

import os
import pandas as pd
from supabase import create_client, Client
from datetime import datetime, timedelta
import sys
import json
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv()

class NSESupabaseManager:
    def __init__(self):
        """Initialize Supabase connection"""
        self.supabase_url = os.getenv('SUPABASE_URL')
        self.supabase_key = os.getenv('SUPABASE_ANON_KEY')
        
        if not self.supabase_url or not self.supabase_key:
            print("❌ Error: Supabase credentials not found!")
            print("📝 Please create a .env file with:")
            print("   SUPABASE_URL=your_supabase_url")
            print("   SUPABASE_ANON_KEY=your_anon_key")
            sys.exit(1)
            
        try:
            self.supabase: Client = create_client(self.supabase_url, self.supabase_key)
            print("✅ Connected to Supabase successfully!")
        except Exception as e:
            print(f"❌ Failed to connect to Supabase: {e}")
            sys.exit(1)
    
    def create_tables(self):
        """Create required tables in Supabase"""
        print("\n🏗️  Creating database tables...")
        
        # NSE stock data table
        nse_data_sql = """
        CREATE TABLE IF NOT EXISTS nse_stock_data (
            id BIGSERIAL PRIMARY KEY,
            symbol VARCHAR(50) NOT NULL,
            series VARCHAR(10),
            date DATE NOT NULL,
            prev_close DECIMAL(12,2),
            open_price DECIMAL(12,2),
            high_price DECIMAL(12,2),
            low_price DECIMAL(12,2),
            last_price DECIMAL(12,2),
            close_price DECIMAL(12,2),
            avg_price DECIMAL(12,2),
            total_traded_qty BIGINT,
            turnover DECIMAL(15,2),
            no_of_trades INTEGER,
            deliverable_qty BIGINT,
            deliverable_percentage DECIMAL(8,4),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            
            -- Create unique constraint to prevent duplicates
            UNIQUE(symbol, date, series)
        );
        
        -- Create indexes for better query performance
        CREATE INDEX IF NOT EXISTS idx_nse_symbol ON nse_stock_data(symbol);
        CREATE INDEX IF NOT EXISTS idx_nse_date ON nse_stock_data(date);
        CREATE INDEX IF NOT EXISTS idx_nse_symbol_date ON nse_stock_data(symbol, date);
        CREATE INDEX IF NOT EXISTS idx_nse_deliverable_pct ON nse_stock_data(deliverable_percentage);
        CREATE INDEX IF NOT EXISTS idx_nse_turnover ON nse_stock_data(turnover);
        """
        
        # Upload status tracking table
        upload_status_sql = """
        CREATE TABLE IF NOT EXISTS upload_status (
            id BIGSERIAL PRIMARY KEY,
            filename VARCHAR(100) NOT NULL UNIQUE,
            upload_date TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            records_count INTEGER,
            file_size_kb INTEGER,
            status VARCHAR(20) DEFAULT 'completed',
            error_message TEXT
        );
        """
        
        try:
            # Execute SQL using Supabase RPC or direct SQL execution
            # Note: Supabase doesn't support direct SQL execution from Python client
            # You'll need to run these SQL commands in Supabase SQL Editor
            print("⚠️  Please run the following SQL commands in your Supabase SQL Editor:")
            print("-" * 80)
            print(nse_data_sql)
            print("\n" + "-" * 80)
            print(upload_status_sql)
            print("-" * 80)
            print("✅ Copy and paste these commands in Supabase > SQL Editor > New Query")
            
            return True
            
        except Exception as e:
            print(f"❌ Error creating tables: {e}")
            return False
    
    def upload_csv_file(self, csv_file_path):
        """Upload a single CSV file to Supabase"""
        if not os.path.exists(csv_file_path):
            print(f"❌ File not found: {csv_file_path}")
            return False
        
        filename = os.path.basename(csv_file_path)
        print(f"\n📤 Uploading {filename}...")
        
        try:
            # Read CSV file
            df = pd.read_csv(csv_file_path)
            print(f"   📊 Found {len(df)} records")
            
            # Extract date from filename (format: sec_bhavdata_full_01082025.csv)
            date_str = filename.split('_')[-1].replace('.csv', '')
            date_obj = datetime.strptime(date_str, '%d%m%Y').date()
            
            # Clean and prepare data
            df_clean = self.prepare_dataframe(df, date_obj)
            
            # Upload in batches to avoid timeout
            batch_size = 1000
            total_records = len(df_clean)
            successful_uploads = 0
            
            for i in range(0, total_records, batch_size):
                batch = df_clean.iloc[i:i+batch_size]
                batch_data = batch.to_dict('records')
                
                try:
                    # Insert batch to Supabase
                    result = self.supabase.table('nse_stock_data').insert(batch_data).execute()
                    successful_uploads += len(batch_data)
                    
                    progress = (i + len(batch_data)) / total_records * 100
                    print(f"   ⏳ Progress: {progress:.1f}% ({successful_uploads}/{total_records})", end='\r')
                    
                except Exception as e:
                    print(f"\n   ⚠️  Batch upload error: {e}")
                    # Continue with next batch
                    continue
            
            # Record upload status
            file_size_kb = os.path.getsize(csv_file_path) // 1024
            status_data = {
                'filename': filename,
                'records_count': successful_uploads,
                'file_size_kb': file_size_kb,
                'status': 'completed' if successful_uploads == total_records else 'partial'
            }
            
            try:
                self.supabase.table('upload_status').insert(status_data).execute()
            except:
                pass  # Status tracking is optional
            
            print(f"\n   ✅ Uploaded {successful_uploads}/{total_records} records")
            return successful_uploads > 0
            
        except Exception as e:
            print(f"   ❌ Upload failed: {e}")
            return False
    
    def prepare_dataframe(self, df, date_obj):
        """Clean and prepare DataFrame for Supabase upload"""
        # Create a copy to avoid modifying original
        df_clean = df.copy()
        
        # Add date column
        df_clean['date'] = date_obj
        
        # Standardize column names (convert to lowercase with underscores)
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
            'VOLUME': 'total_traded_qty',
            'TURNOVER': 'turnover',
            'TRADES': 'no_of_trades',
            'DELIVERABLE': 'deliverable_qty',
            '%DLYQT': 'deliverable_percentage'
        }
        
        # Rename columns that exist
        for old_name, new_name in column_mapping.items():
            if old_name in df_clean.columns:
                df_clean = df_clean.rename(columns={old_name: new_name})
        
        # Convert data types
        numeric_columns = [
            'prev_close', 'open_price', 'high_price', 'low_price', 
            'last_price', 'close_price', 'avg_price', 'turnover',
            'deliverable_percentage'
        ]
        
        integer_columns = [
            'total_traded_qty', 'no_of_trades', 'deliverable_qty'
        ]
        
        for col in numeric_columns:
            if col in df_clean.columns:
                df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
        
        for col in integer_columns:
            if col in df_clean.columns:
                df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce').fillna(0).astype('Int64')
        
        # Handle missing values
        df_clean = df_clean.fillna({
            'series': 'EQ',
            'deliverable_percentage': 0.0
        })
        
        # Remove rows with invalid symbols
        df_clean = df_clean.dropna(subset=['symbol'])
        df_clean = df_clean[df_clean['symbol'].str.len() > 0]
        
        # Select only columns that exist in our table
        required_columns = [
            'symbol', 'series', 'date', 'prev_close', 'open_price', 
            'high_price', 'low_price', 'last_price', 'close_price', 
            'avg_price', 'total_traded_qty', 'turnover', 'no_of_trades',
            'deliverable_qty', 'deliverable_percentage'
        ]
        
        available_columns = [col for col in required_columns if col in df_clean.columns]
        df_clean = df_clean[available_columns]
        
        return df_clean
    
    def upload_all_csv_files(self, csv_directory="NSE_August_2025_Data"):
        """Upload all CSV files from the directory"""
        if not os.path.exists(csv_directory):
            print(f"❌ Directory not found: {csv_directory}")
            return False
        
        # Get all CSV files
        csv_files = [f for f in os.listdir(csv_directory) if f.endswith('.csv')]
        csv_files.sort()  # Upload in chronological order
        
        if not csv_files:
            print(f"❌ No CSV files found in {csv_directory}")
            return False
        
        print(f"🎯 Found {len(csv_files)} CSV files to upload")
        print("-" * 60)
        
        successful_uploads = 0
        failed_uploads = []
        
        for i, csv_file in enumerate(csv_files, 1):
            file_path = os.path.join(csv_directory, csv_file)
            print(f"[{i:2d}/{len(csv_files)}] Processing {csv_file}...")
            
            if self.upload_csv_file(file_path):
                successful_uploads += 1
            else:
                failed_uploads.append(csv_file)
            
            # Small delay to avoid overwhelming the API
            time.sleep(1)
        
        # Summary
        print("\n" + "=" * 60)
        print("📊 UPLOAD SUMMARY")
        print("=" * 60)
        print(f"✅ Successful uploads: {successful_uploads}/{len(csv_files)}")
        print(f"❌ Failed uploads: {len(failed_uploads)}/{len(csv_files)}")
        
        if failed_uploads:
            print("\n⚠️  Failed files:")
            for file in failed_uploads:
                print(f"   • {file}")
        
        return successful_uploads > 0
    
    def check_data_status(self):
        """Check current data status in Supabase"""
        try:
            # Get total records
            result = self.supabase.table('nse_stock_data').select('*', count='exact').limit(1).execute()
            total_records = result.count
            
            # Get date range
            date_result = self.supabase.table('nse_stock_data')\
                .select('date')\
                .order('date')\
                .limit(1)\
                .execute()
            
            if date_result.data:
                min_date = date_result.data[0]['date']
                
                date_result_max = self.supabase.table('nse_stock_data')\
                    .select('date')\
                    .order('date', desc=True)\
                    .limit(1)\
                    .execute()
                
                max_date = date_result_max.data[0]['date']
                
                print("📊 Database Status:")
                print(f"   📈 Total records: {total_records:,}")
                print(f"   📅 Date range: {min_date} to {max_date}")
                
                # Get unique symbols count
                symbols_result = self.supabase.table('nse_stock_data')\
                    .select('symbol', count='exact')\
                    .execute()
                
                print(f"   🏢 Total data points: {total_records:,}")
                
                return True
            else:
                print("📊 Database is empty")
                return False
                
        except Exception as e:
            print(f"❌ Error checking status: {e}")
            return False

def create_env_file():
    """Create sample .env file"""
    env_content = """# Supabase Configuration
# Get these values from your Supabase project dashboard
# Dashboard > Settings > API

SUPABASE_URL=your_supabase_project_url_here
SUPABASE_ANON_KEY=your_supabase_anon_key_here

# Example:
# SUPABASE_URL=https://abcdefghijklmnopqr.supabase.co
# SUPABASE_ANON_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
"""
    
    if not os.path.exists('.env'):
        with open('.env', 'w') as f:
            f.write(env_content)
        print("✅ Created .env file template")
        return True
    else:
        print("⚠️  .env file already exists")
        return False

def install_requirements():
    """Check and install required packages"""
    required_packages = ['supabase', 'python-dotenv', 'pandas']
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
        except ImportError:
            missing_packages.append(package)
    
    if missing_packages:
        print("📦 Installing required packages...")
        import subprocess
        for package in missing_packages:
            try:
                subprocess.check_call([sys.executable, '-m', 'pip', 'install', package])
                print(f"   ✅ Installed {package}")
            except Exception as e:
                print(f"   ❌ Failed to install {package}: {e}")
                return False
    
    return True

def setup_instructions():
    """Show setup instructions"""
    print("🚀 NSE Supabase Setup Instructions")
    print("=" * 50)
    print("1. Create free account at https://supabase.com")
    print("2. Create new project (choose free tier)")
    print("3. Go to Settings > API in your project")
    print("4. Copy Project URL and anon/public key")
    print("5. Update the .env file with your credentials")
    print("6. Run this script again")
    print("\n📝 Your .env file should look like:")
    print("SUPABASE_URL=https://yourproject.supabase.co")
    print("SUPABASE_ANON_KEY=eyJhbGciOiJIUzI1NiIs...")

def main():
    print("☁️  NSE Data Supabase Manager")
    print("=" * 50)
    
    # Install requirements
    if not install_requirements():
        print("❌ Failed to install requirements")
        return
    
    # Check for .env file
    if not os.path.exists('.env'):
        print("📝 Setting up configuration...")
        create_env_file()
        setup_instructions()
        print("\n⏳ Please update .env file and run again")
        input("Press Enter to exit...")
        return
    
    # Load and check environment
    load_dotenv()
    if not os.getenv('SUPABASE_URL') or 'your_supabase' in os.getenv('SUPABASE_URL', ''):
        setup_instructions()
        print("\n⏳ Please update .env file and run again")
        input("Press Enter to exit...")
        return
    
    # Initialize manager
    try:
        manager = NSESupabaseManager()
    except SystemExit:
        print("\n⏳ Please check your .env configuration")
        input("Press Enter to exit...")
        return
    
    # Interactive menu
    while True:
        print("\n" + "=" * 50)
        print("📊 NSE Supabase Manager")
        print("=" * 50)
        print("1. 🏗️  Create database tables (run first time)")
        print("2. 📤 Upload all CSV files")
        print("3. 📊 Check database status")
        print("4. 📁 Upload single CSV file")
        print("5. ❌ Exit")
        
        choice = input("\n🎯 Choose option (1-5): ").strip()
        
        if choice == '1':
            manager.create_tables()
        
        elif choice == '2':
            print("\n🚀 Starting bulk upload...")
            manager.upload_all_csv_files()
        
        elif choice == '3':
            print("\n📊 Checking database status...")
            manager.check_data_status()
        
        elif choice == '4':
            csv_file = input("📁 Enter CSV file path: ").strip()
            if os.path.exists(csv_file):
                manager.upload_csv_file(csv_file)
            else:
                print("❌ File not found")
        
        elif choice == '5':
            print("👋 Goodbye!")
            break
        
        else:
            print("❌ Invalid choice. Please try again.")

if __name__ == "__main__":
    main()
