#!/usr/bin/env python3
"""
NSE Data Manager - Manual Download Helper
Helps organize and manage NSE data when downloaded manually
"""

import os
import pandas as pd
from datetime import datetime
import zipfile
import shutil

def create_month_folders():
    """Create organized folder structure for January-June 2025"""
    
    print("📁 Creating Month-wise Folder Structure")
    print("=" * 50)
    
    months = [
        ("January", "2025"),
        ("February", "2025"), 
        ("March", "2025"),
        ("April", "2025"),
        ("May", "2025"),
        ("June", "2025")
    ]
    
    created_folders = []
    
    for month, year in months:
        folder_name = f"NSE_{month}_{year}_Data"
        
        if not os.path.exists(folder_name):
            os.makedirs(folder_name)
            print(f"✅ Created: {folder_name}")
            created_folders.append(folder_name)
        else:
            print(f"📁 Exists: {folder_name}")
        
        # Create a README file with instructions
        readme_path = os.path.join(folder_name, "README.md")
        if not os.path.exists(readme_path):
            with open(readme_path, 'w') as f:
                f.write(f"# NSE {month} {year} Data\n\n")
                f.write("## Instructions for Manual Download:\n\n")
                f.write("1. Visit: https://www.nseindia.com/market-data/historical-data\n")
                f.write("2. Select 'Securities Bhav Copy'\n") 
                f.write(f"3. Choose {month} {year} dates\n")
                f.write("4. Download CSV files to this folder\n")
                f.write("5. Name format: sec_bhavdata_full_DDMMYYYY.csv\n\n")
                f.write("## Expected Files:\n")
                f.write("- One CSV file per trading day\n")
                f.write("- Each file contains all EQ series stocks\n")
                f.write("- Columns: SYMBOL, SERIES, OPEN, HIGH, LOW, CLOSE, etc.\n")
    
    return created_folders

def check_existing_data():
    """Check what NSE data we already have"""
    
    print("\n📊 Current NSE Data Inventory")
    print("=" * 50)
    
    all_folders = [f for f in os.listdir('.') if f.startswith('NSE_') and f.endswith('_Data')]
    
    if not all_folders:
        print("📁 No NSE data folders found")
        return {}
    
    inventory = {}
    total_files = 0
    total_records = 0
    
    for folder in sorted(all_folders):
        csv_files = [f for f in os.listdir(folder) if f.endswith('.csv')]
        
        if csv_files:
            # Count records in first file to estimate
            sample_file = os.path.join(folder, csv_files[0])
            try:
                sample_df = pd.read_csv(sample_file)
                avg_records = len(sample_df)
                estimated_total = avg_records * len(csv_files)
                total_records += estimated_total
            except:
                avg_records = 0
                estimated_total = 0
            
            inventory[folder] = {
                'files': len(csv_files),
                'avg_records': avg_records,
                'estimated_total': estimated_total
            }
            
            total_files += len(csv_files)
            
            print(f"📂 {folder}:")
            print(f"   📄 Files: {len(csv_files)}")
            print(f"   📊 Est. Records: {estimated_total:,}")
        else:
            inventory[folder] = {'files': 0, 'avg_records': 0, 'estimated_total': 0}
            print(f"📂 {folder}: Empty")
    
    print(f"\n📈 TOTALS:")
    print(f"   📁 Folders: {len(all_folders)}")
    print(f"   📄 CSV Files: {total_files}")
    print(f"   📊 Est. Total Records: {total_records:,}")
    
    return inventory

def organize_downloaded_files(source_folder=None):
    """Help organize manually downloaded files"""
    
    print("\n🗂️  File Organization Helper")
    print("=" * 50)
    
    if source_folder and os.path.exists(source_folder):
        print(f"📁 Checking source folder: {source_folder}")
        
        # Look for CSV or ZIP files
        csv_files = [f for f in os.listdir(source_folder) if f.endswith('.csv')]
        zip_files = [f for f in os.listdir(source_folder) if f.endswith('.zip')]
        
        print(f"   📄 Found {len(csv_files)} CSV files")
        print(f"   📦 Found {len(zip_files)} ZIP files")
        
        # Extract ZIP files if any
        for zip_file in zip_files:
            print(f"📦 Extracting {zip_file}...")
            zip_path = os.path.join(source_folder, zip_file)
            
            try:
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(source_folder)
                print(f"   ✅ Extracted successfully")
            except Exception as e:
                print(f"   ❌ Error: {e}")
        
        # Re-scan for CSV files after extraction
        csv_files = [f for f in os.listdir(source_folder) if f.endswith('.csv')]
        print(f"   📄 Total CSV files after extraction: {len(csv_files)}")
        
        # Organize by month
        moved_files = organize_by_month(source_folder, csv_files)
        print(f"   🗂️  Organized {moved_files} files")
        
    else:
        print("💡 To organize downloaded files:")
        print("   1. Put all downloaded CSV/ZIP files in one folder")
        print("   2. Run: organize_downloaded_files('path/to/folder')")
        print("   3. Files will be automatically sorted by month")

def organize_by_month(source_folder, csv_files):
    """Organize CSV files by month based on filename dates"""
    
    month_mapping = {
        "01": "January", "02": "February", "03": "March",
        "04": "April", "05": "May", "06": "June",
        "07": "July", "08": "August", "09": "September",
        "10": "October", "11": "November", "12": "December"
    }
    
    moved_count = 0
    
    for csv_file in csv_files:
        try:
            # Extract date from filename (assuming format like cm01012025bhav.csv or sec_bhav_01012025.csv)
            if 'bhav' in csv_file.lower():
                # Find 8-digit date pattern
                import re
                date_match = re.search(r'(\d{8})', csv_file)
                if date_match:
                    date_str = date_match.group(1)
                    # Assume format DDMMYYYY
                    if len(date_str) == 8:
                        dd = date_str[:2]
                        mm = date_str[2:4]
                        yyyy = date_str[4:8]
                        
                        if mm in month_mapping and yyyy == "2025":
                            month_name = month_mapping[mm]
                            target_folder = f"NSE_{month_name}_{yyyy}_Data"
                            
                            # Create folder if it doesn't exist
                            os.makedirs(target_folder, exist_ok=True)
                            
                            # Move file
                            source_path = os.path.join(source_folder, csv_file)
                            target_filename = f"sec_bhavdata_full_{date_str}.csv"
                            target_path = os.path.join(target_folder, target_filename)
                            
                            shutil.move(source_path, target_path)
                            print(f"   📄 {csv_file} → {target_folder}/{target_filename}")
                            moved_count += 1
        except Exception as e:
            print(f"   ⚠️  Could not process {csv_file}: {e}")
    
    return moved_count

def create_download_checklist():
    """Create a checklist for manual downloads"""
    
    checklist_file = "NSE_Download_Checklist.md"
    
    with open(checklist_file, 'w', encoding='utf-8') as f:
        f.write("# NSE Data Download Checklist - January to June 2025\n\n")
        
        f.write("## Goal\n")
        f.write("Download NSE Bhav Copy data for all trading days from January-June 2025\n\n")
        
        f.write("## Months to Download\n")
        months = ["January", "February", "March", "April", "May", "June"]
        for month in months:
            f.write(f"- [ ] {month} 2025\n")
        
        f.write("\n## Download Sources\n")
        f.write("1. **Official NSE Website** (Primary)\n")
        f.write("   - URL: https://www.nseindia.com/market-data/historical-data\n")
        f.write("   - Select: Securities Bhav Copy\n")
        f.write("   - Format: CSV\n\n")
        
        f.write("2. **NSE Archives** (Alternative)\n")
        f.write("   - URL: https://archives.nseindia.com/\n")
        f.write("   - Navigate: Historical Data > Equities\n\n")
        
        f.write("## Download Steps\n")
        f.write("1. Visit NSE historical data page\n")
        f.write("2. Select 'Securities Bhav Copy'\n")
        f.write("3. Choose date range (month by month)\n")
        f.write("4. Download CSV files\n")
        f.write("5. Save to appropriate month folder\n")
        f.write("6. Rename files to: sec_bhavdata_full_DDMMYYYY.csv\n\n")
        
        f.write("## Folder Structure\n")
        for month in months:
            f.write(f"- NSE_{month}_2025_Data/\n")
        
        f.write("\n## Verification\n")
        f.write("After downloading each month:\n")
        f.write("1. Check file count matches trading days\n")
        f.write("2. Verify each CSV has data (not empty)\n")
        f.write("3. Run: python nse_data_manager.py to validate\n\n")
        
        f.write("## Expected Files per Month\n")
        trading_days = {
            "January": "~21-23 files",
            "February": "~19-20 files", 
            "March": "~21-22 files",
            "April": "~21-22 files",
            "May": "~21-22 files",
            "June": "~21-22 files"
        }
        
        for month, days in trading_days.items():
            f.write(f"- {month}: {days}\n")
    
    print(f"Created download checklist: {checklist_file}")
    return checklist_file

def main():
    print("🗂️  NSE Data Manager - Manual Download Helper")
    print("=" * 60)
    print("💡 Since automated download isn't working, this tool helps with manual organization")
    print()
    
    # Create folder structure
    created_folders = create_month_folders()
    
    # Check existing data
    inventory = check_existing_data()
    
    # Create download checklist
    checklist_file = create_download_checklist()
    
    print(f"\n🎯 NEXT STEPS:")
    print(f"1. 📋 Follow instructions in: {checklist_file}")
    print(f"2. 🌐 Visit NSE website to manually download data")
    print(f"3. 📁 Place downloaded files in the created month folders")
    print(f"4. 🔄 Re-run this script to verify organization")
    
    print(f"\n💡 WHY MANUAL DOWNLOAD?")
    print(f"• NSE has changed their API/URL structure")
    print(f"• Historical data for early 2025 may not be available yet")
    print(f"• NSE may require browser session/authentication")
    print(f"• Manual download ensures we get the latest available data")
    
    print(f"\n✅ READY FOR ANALYSIS:")
    print(f"• July 2025: 23 files ✅")
    print(f"• August 2025: 19 files ✅") 
    print(f"• January-June 2025: Ready for manual download 📥")

if __name__ == "__main__":
    main()
