#!/usr/bin/env python3
"""
📊 NSE Data Importer - Import CSV files to PostgreSQL
Import all NSE stock data into PostgreSQL database
"""

import os
import sys
import json
from datetime import datetime

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from nse_database import NSEDatabase

def import_all_nse_data():
    """Import all NSE CSV files into PostgreSQL database"""
    print("📊 NSE Data Importer - CSV to PostgreSQL")
    print("=" * 50)
    
    # Initialize database connection
    db = NSEDatabase()
    
    # Find all NSE data directories
    data_directories = []
    
    for item in os.listdir('.'):
        if os.path.isdir(item) and item.startswith('NSE_') and item.endswith('_Data'):
            data_directories.append(item)
    
    if not data_directories:
        print("❌ No NSE data directories found!")
        print("💡 Make sure you have folders like 'NSE_July_2025_Data', 'NSE_August_2025_Data'")
        return
    
    print(f"📁 Found {len(data_directories)} data directories:")
    for directory in sorted(data_directories):
        csv_files = [f for f in os.listdir(directory) if f.endswith('.csv')]
        print(f"   📂 {directory}: {len(csv_files)} CSV files")
    
    # Get current data status
    print(f"\n📊 Current database status:")
    summary = db.get_data_summary()
    
    total_imported = 0
    total_errors = 0
    
    # Import from each directory
    for directory in sorted(data_directories):
        print(f"\n📂 Processing {directory}...")
        
        csv_files = [f for f in os.listdir(directory) if f.endswith('.csv')]
        
        if not csv_files:
            print(f"   ⚠️ No CSV files found in {directory}")
            continue
        
        for csv_file in sorted(csv_files):
            file_path = os.path.join(directory, csv_file)
            
            try:
                print(f"   📄 Importing {csv_file}...")
                
                # Import the file
                success = db.import_csv_to_database(file_path)
                
                if success:
                    print(f"      ✅ Success")
                    total_imported += 1
                else:
                    print(f"      ❌ Failed")
                    total_errors += 1
                    
            except Exception as e:
                print(f"      ❌ Error: {str(e)[:50]}...")
                total_errors += 1
    
    # Final summary
    print(f"\n📊 Import Summary:")
    print("=" * 30)
    print(f"✅ Files imported successfully: {total_imported}")
    print(f"❌ Files with errors: {total_errors}")
    print(f"📈 Total files processed: {total_imported + total_errors}")
    
    # Get updated database status
    print(f"\n📊 Updated database status:")
    final_summary = db.get_data_summary()
    
    if total_imported > 0:
        print(f"\n🎉 Data import completed!")
        print(f"💾 Your NSE data is now stored in PostgreSQL")
        print(f"🔍 You can now run analysis queries on your data")
        
        # Show some basic stats
        try:
            total_records = db.get_total_records()
            unique_dates = db.get_unique_dates_count()
            unique_symbols = db.get_unique_symbols_count()
            
            print(f"\n📈 Quick Stats:")
            print(f"   📊 Total records: {total_records:,}")
            print(f"   📅 Unique trading dates: {unique_dates}")
            print(f"   🏢 Unique symbols: {unique_symbols}")
            
        except Exception as e:
            print(f"   ⚠️ Could not get stats: {e}")
    
    db.close()

def show_import_options():
    """Show available import options"""
    print("📊 NSE Data Import Options")
    print("=" * 30)
    print("1. Import ALL CSV files (Recommended)")
    print("2. Import specific month")
    print("3. Check current database status")
    print("4. Clear database and reimport")
    
    choice = input("\nChoose option (1-4): ").strip()
    
    db = NSEDatabase()
    
    if choice == "1":
        import_all_nse_data()
    
    elif choice == "2":
        print("\\nAvailable months:")
        data_dirs = [d for d in os.listdir('.') if d.startswith('NSE_') and d.endswith('_Data')]
        for i, directory in enumerate(sorted(data_dirs), 1):
            csv_count = len([f for f in os.listdir(directory) if f.endswith('.csv')])
            print(f"   {i}. {directory} ({csv_count} files)")
        
        try:
            choice_idx = int(input("\\nChoose month number: ")) - 1
            chosen_dir = sorted(data_dirs)[choice_idx]
            
            print(f"\\n📂 Importing from {chosen_dir}...")
            csv_files = [f for f in os.listdir(chosen_dir) if f.endswith('.csv')]
            
            imported = 0
            for csv_file in csv_files:
                file_path = os.path.join(chosen_dir, csv_file)
                if db.import_csv_to_database(file_path):
                    print(f"   ✅ {csv_file}")
                    imported += 1
                else:
                    print(f"   ❌ {csv_file}")
            
            print(f"\\n✅ Imported {imported} files from {chosen_dir}")
            
        except (ValueError, IndexError):
            print("❌ Invalid choice")
    
    elif choice == "3":
        print("\\n📊 Current Database Status:")
        summary = db.get_data_summary()
        
    elif choice == "4":
        confirm = input("\\n⚠️ This will delete all data and reimport. Continue? (yes/no): ")
        if confirm.lower() in ['yes', 'y']:
            print("🗑️ Clearing database...")
            db.clear_all_data()
            print("📊 Reimporting all data...")
            import_all_nse_data()
        else:
            print("❌ Operation cancelled")
    
    else:
        print("❌ Invalid choice")
    
    db.close()

if __name__ == "__main__":
    try:
        import_all_nse_data()
    except KeyboardInterrupt:
        print("\\n❌ Import cancelled by user")
    except Exception as e:
        print(f"\\n❌ Unexpected error: {e}")
        print("💡 Try running the script again or check your database connection")
