#!/usr/bin/env python3
"""
NSE Database Setup & Quick Start

Simple script to set up your NSE database and get started with analysis.
Runs the database import process and shows you what you can do next.

Requirements: None (uses built-in sqlite3)

Author: Generated for NSE data setup
Date: September 2025
"""

import os
import sys
import time
from nse_database import NSEDatabase

def main():
    """Quick setup and start"""
    print("🏦 NSE Database Quick Setup")
    print("=" * 50)
    
    # Check if CSV files exist
    csv_folder = "NSE_August_2025_Data"
    if not os.path.exists(csv_folder):
        print(f"❌ Data folder not found: {csv_folder}")
        print(f"💡 Please run your NSE downloader script first to download the CSV files")
        input("Press Enter to exit...")
        return
    
    # Count CSV files
    csv_files = [f for f in os.listdir(csv_folder) if f.startswith("sec_bhavdata_full_") and f.endswith(".csv")]
    
    if not csv_files:
        print(f"❌ No CSV files found in {csv_folder}")
        print(f"💡 Please run your NSE downloader script first")
        input("Press Enter to exit...")
        return
    
    print(f"✅ Found {len(csv_files)} CSV files to import")
    
    # Check if database already exists
    db_exists = os.path.exists("nse_data.db")
    if db_exists:
        print("📊 Database already exists")
        choice = input("Do you want to re-import all data? (y/N): ").strip().lower()
        if choice != 'y':
            print("💡 Skipping import. You can run nse_database.py or nse_query_tool.py directly")
            input("Press Enter to exit...")
            return
        else:
            # Remove existing database
            os.remove("nse_data.db")
            print("🗑️  Removed existing database")
    
    print(f"\n🚀 Starting database import...")
    print("⏳ This may take a minute...")
    
    try:
        # Create database and import
        db = NSEDatabase()
        db.import_all_csv_files(csv_folder)
        db.close()
        
        print("\n🎉 Setup Complete!")
        print("=" * 50)
        print("✅ Database created: nse_data.db")
        print("✅ All CSV data imported successfully")
        
        print("\n🔧 What you can do now:")
        print("1. Run 'python nse_query_tool.py' for quick analysis")
        print("2. Run 'python nse_database.py' for full database management")
        print("3. Use SQLite browser to explore the database directly")
        
        print("\n📊 Quick Examples:")
        print("• Find top gainers for a specific day")
        print("• Analyze volume leaders")
        print("• Track individual stock performance")
        print("• Find high delivery percentage stocks")
        print("• Export data for Excel analysis")
        
        print(f"\n💾 Database file: {os.path.abspath('nse_data.db')}")
        
        # Ask if user wants to start query tool
        start_queries = input(f"\n🔍 Start query tool now? (Y/n): ").strip().lower()
        if start_queries != 'n':
            print(f"\n🚀 Starting NSE Query Tool...")
            time.sleep(1)
            os.system('python nse_query_tool.py')
        
    except Exception as e:
        print(f"\n❌ Error during setup: {e}")
        print("💡 Try running the individual scripts manually")
    
    input(f"\nPress Enter to exit...")

if __name__ == "__main__":
    main()
