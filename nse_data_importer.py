#!/usr/bin/env python3
"""
📥 NSE Data Importer for PostgreSQL
Bulk import CSV files to PostgreSQL database
"""

import os
import glob
from nse_database_setup import NSEDatabaseManager
from datetime import datetime
import pandas as pd

class NSEDataImporter:
    def __init__(self):
        self.db = NSEDatabaseManager()
        self.month_folders = {
            "January": "NSE_January_2025_Data",
            "February": "NSE_February_2025_Data", 
            "March": "NSE_March_2025_Data",
            "April": "NSE_April_2025_Data",
            "May": "NSE_May_2025_Data",
            "June": "NSE_June_2025_Data",
            "July": "NSE_July_2025_Data",
            "August": "NSE_August_2025_Data"
        }
    
    def connect_database(self):
        """Connect to database"""
        if not self.db.connect():
            print("❌ Failed to connect to database")
            return False
        return True
    
    def import_month_data(self, month_name):
        """Import all CSV files for a specific month"""
        if month_name not in self.month_folders:
            print(f"❌ Unknown month: {month_name}")
            return False
        
        folder_path = self.month_folders[month_name]
        if not os.path.exists(folder_path):
            print(f"❌ Folder not found: {folder_path}")
            return False
        
        # Different file patterns for different months
        if month_name in ["July", "August"]:
            csv_pattern = f"{folder_path}/sec_bhavdata_full_*.csv"
        else:
            csv_pattern = f"{folder_path}/cm*.csv"
        
        csv_files = glob.glob(csv_pattern)
        
        if not csv_files:
            print(f"❌ No CSV files found in {folder_path}")
            return False
        
        print(f"\n📂 Importing {month_name} 2025 data...")
        print(f"📁 Found {len(csv_files)} CSV files")
        
        total_records = 0
        successful_files = 0
        
        for csv_file in sorted(csv_files):
            print(f"📄 Processing: {os.path.basename(csv_file)}")
            records = self.db.import_csv_to_database(csv_file, f"{month_name}_2025")
            
            if records > 0:
                total_records += records
                successful_files += 1
            
            # Log to metadata table
            self.log_import_operation(csv_file, f"{month_name}_2025", records)
        
        print(f"\n✅ {month_name} Import Complete:")
        print(f"   📊 Files processed: {successful_files}/{len(csv_files)}")
        print(f"   📈 Total records: {total_records:,}")
        
        return True
    
    def import_all_months(self):
        """Import data for all available months"""
        print("🚀 Starting bulk import of all NSE data...")
        
        if not self.connect_database():
            return False
        
        total_months = 0
        successful_months = 0
        
        for month_name in self.month_folders.keys():
            print(f"\n{'='*60}")
            if self.import_month_data(month_name):
                successful_months += 1
            total_months += 1
        
        print(f"\n🎉 Bulk Import Summary:")
        print(f"   📅 Months processed: {successful_months}/{total_months}")
        
        # Show final database summary
        self.db.get_data_summary()
        
        return True
    
    def log_import_operation(self, file_path, month_year, records_processed):
        """Log import operation to metadata table"""
        try:
            from sqlalchemy import text
            
            sql = text("""
                INSERT INTO nse_metadata 
                (operation_type, month_year, status, records_processed, file_path, created_at)
                VALUES (:op_type, :month, :status, :records, :filepath, :created)
            """)
            
            status = 'completed' if records_processed > 0 else 'failed'
            
            self.db.connection.execute(sql, {
                'op_type': 'import',
                'month': month_year,
                'status': status,
                'records': records_processed,
                'filepath': file_path,
                'created': datetime.now()
            })
            
            self.db.connection.commit()
            
        except Exception as e:
            print(f"⚠️  Warning: Could not log operation: {str(e)}")
    
    def check_existing_data(self):
        """Check what data already exists in database"""
        try:
            from sqlalchemy import text
            
            result = self.db.connection.execute(text("""
                SELECT month_year, COUNT(*) as record_count,
                       COUNT(DISTINCT symbol) as unique_symbols,
                       COUNT(DISTINCT date) as trading_days
                FROM nse_raw_data 
                GROUP BY month_year 
                ORDER BY month_year;
            """)).fetchall()
            
            if result:
                print("\n📊 Existing Data in Database:")
                print("-" * 70)
                print(f"{'Month':<15} {'Records':<10} {'Symbols':<10} {'Days':<8}")
                print("-" * 70)
                
                for month, records, symbols, days in result:
                    print(f"{month:<15} {records:<10,} {symbols:<10} {days:<8}")
                print("-" * 70)
            else:
                print("\n📭 No data found in database")
            
            return result
            
        except Exception as e:
            print(f"❌ Error checking existing data: {str(e)}")
            return []

def main():
    """Main import function"""
    print("📥 NSE Data Importer for PostgreSQL")
    print("=" * 50)
    
    importer = NSEDataImporter()
    
    # Connect to database
    if not importer.connect_database():
        print("❌ Cannot proceed without database connection")
        return
    
    # Check existing data
    existing = importer.check_existing_data()
    
    if existing:
        choice = input("\n❓ Data exists. Import anyway? (y/n): ").lower()
        if choice != 'y':
            print("ℹ️  Import cancelled")
            return
    
    # Import options
    print("\n📋 Import Options:")
    print("1. Import all months")
    print("2. Import specific month")
    print("3. Show data summary only")
    
    choice = input("Choose option (1-3): ").strip()
    
    if choice == "1":
        importer.import_all_months()
    elif choice == "2":
        print("\nAvailable months:")
        for i, month in enumerate(importer.month_folders.keys(), 1):
            print(f"{i}. {month}")
        
        month_choice = input("Enter month name: ").strip().title()
        if month_choice in importer.month_folders:
            importer.import_month_data(month_choice)
        else:
            print("❌ Invalid month name")
    elif choice == "3":
        importer.db.get_data_summary()
    else:
        print("❌ Invalid choice")

if __name__ == "__main__":
    main()
