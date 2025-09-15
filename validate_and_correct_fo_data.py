#!/usr/bin/env python3
"""
F&O Data Validation and Correction Tool

Purpose: Compare source files with database records and correct discrepancies
Target: step04_fo_udiff_daily table validation against fo_udiff_downloads
"""

import pandas as pd
import zipfile
import os
import sys
from datetime import datetime

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from nse_database_integration import NSEDatabaseManager

class FODataValidator:
    def __init__(self):
        self.db = NSEDatabaseManager()
        self.source_folder = "fo_udiff_downloads"
        self.discrepancies = []
        
    def validate_all_february_dates(self):
        """Validate all February 2025 dates"""
        print("🔍 VALIDATING F&O DATA FOR FEBRUARY 2025")
        print("=" * 60)
        
        # Get all zip files
        zip_files = [f for f in os.listdir(self.source_folder) if f.endswith('.zip') and '202502' in f]
        zip_files.sort()
        
        for zip_file in zip_files:
            date = self.extract_date_from_filename(zip_file)
            if date:
                self.validate_single_date(date, zip_file)
                
        self.show_summary()
        
    def extract_date_from_filename(self, filename):
        """Extract date from filename like BhavCopy_NSE_FO_0_0_0_20250203_F_0000.csv.zip"""
        try:
            parts = filename.split('_')
            for part in parts:
                if len(part) == 8 and part.startswith('202502'):
                    return part
        except:
            pass
        return None
        
    def validate_single_date(self, date, zip_file):
        """Validate a single date"""
        try:
            # Get source count
            zip_path = os.path.join(self.source_folder, zip_file)
            source_count = self.get_source_file_count(zip_path)
            
            # Get database count
            db_count = self.get_database_count(date)
            
            # Check for discrepancy
            if source_count != db_count:
                missing = source_count - db_count
                self.discrepancies.append({
                    'date': date,
                    'source_count': source_count,
                    'db_count': db_count,
                    'missing': missing,
                    'zip_file': zip_file
                })
                status = f"❌ MISMATCH"
                print(f"{date}: {status} - Source: {source_count:,}, DB: {db_count:,}, Missing: {missing:,}")
            else:
                print(f"{date}: ✅ MATCH - {source_count:,} records")
                
        except Exception as e:
            print(f"{date}: ❌ ERROR - {e}")
            
    def get_source_file_count(self, zip_path):
        """Get record count from source zip file"""
        with zipfile.ZipFile(zip_path, 'r') as z:
            csv_file = z.namelist()[0]
            with z.open(csv_file) as f:
                df = pd.read_csv(f)
                return len(df)
                
    def get_database_count(self, date):
        """Get record count from database for specific date"""
        cursor = self.db.connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM step04_fo_udiff_daily WHERE trade_date = ?", date)
        return cursor.fetchone()[0]
        
    def show_summary(self):
        """Show validation summary"""
        print("\n📊 VALIDATION SUMMARY")
        print("=" * 40)
        
        if not self.discrepancies:
            print("🎉 All dates validated successfully!")
            return
            
        print(f"❌ Found {len(self.discrepancies)} dates with discrepancies:")
        total_missing = 0
        
        for disc in self.discrepancies:
            print(f"  {disc['date']}: Missing {disc['missing']:,} records")
            total_missing += disc['missing']
            
        print(f"\n🚨 TOTAL MISSING RECORDS: {total_missing:,}")
        
    def correct_discrepancies(self):
        """Correct all identified discrepancies"""
        if not self.discrepancies:
            print("✅ No discrepancies to correct!")
            return
            
        print(f"\n🔧 CORRECTING {len(self.discrepancies)} DISCREPANCIES")
        print("=" * 50)
        
        for disc in self.discrepancies:
            self.correct_single_date(disc)
            
    def correct_single_date(self, disc):
        """Correct data for a single date"""
        date = disc['date']
        zip_file = disc['zip_file']
        
        print(f"\n🔧 Correcting {date}...")
        
        try:
            # Step 1: Delete existing data for this date
            cursor = self.db.connection.cursor()
            cursor.execute("DELETE FROM step04_fo_udiff_daily WHERE trade_date = ?", date)
            deleted = cursor.rowcount
            print(f"   🗑️ Deleted {deleted:,} existing records")
            
            # Step 2: Load fresh data from source
            zip_path = os.path.join(self.source_folder, zip_file)
            loaded = self.load_data_from_zip(zip_path, date)
            print(f"   ✅ Loaded {loaded:,} fresh records")
            
            # Step 3: Commit changes
            self.db.connection.commit()
            
            # Step 4: Verify
            new_count = self.get_database_count(date)
            if new_count == disc['source_count']:
                print(f"   🎉 Verification successful: {new_count:,} records")
            else:
                print(f"   ❌ Verification failed: Expected {disc['source_count']:,}, Got {new_count:,}")
                
        except Exception as e:
            print(f"   ❌ Error correcting {date}: {e}")
            self.db.connection.rollback()
            
    def load_data_from_zip(self, zip_path, date):
        """Load data from zip file to database"""
        with zipfile.ZipFile(zip_path, 'r') as z:
            csv_file = z.namelist()[0]
            with z.open(csv_file) as f:
                df = pd.read_csv(f)
                
                # Convert date format
                df['trade_date'] = date
                
                # Use bulk insert with cursor for SQL Server
                cursor = self.db.connection.cursor()
                
                # Prepare data for bulk insert
                records = df.to_dict('records')
                
                # Build INSERT statement dynamically based on CSV columns
                columns = list(df.columns)
                placeholders = ', '.join(['?' for _ in columns])
                column_names = ', '.join(columns)
                
                insert_sql = f"INSERT INTO step04_fo_udiff_daily ({column_names}) VALUES ({placeholders})"
                
                # Insert in batches for efficiency
                batch_size = 1000
                total_inserted = 0
                
                for i in range(0, len(records), batch_size):
                    batch = records[i:i+batch_size]
                    batch_data = [[record[col] for col in columns] for record in batch]
                    cursor.executemany(insert_sql, batch_data)
                    total_inserted += len(batch)
                    
                return total_inserted
                
    def close(self):
        """Close database connection"""
        self.db.close()

def main():
    validator = FODataValidator()
    
    try:
        # Step 1: Validate all dates
        validator.validate_all_february_dates()
        
        # Step 2: Ask user if they want to correct discrepancies
        if validator.discrepancies:
            print(f"\n❓ Do you want to correct these {len(validator.discrepancies)} discrepancies? (y/n): ", end="")
            response = input().strip().lower()
            
            if response == 'y':
                validator.correct_discrepancies()
                print("\n🎉 CORRECTION COMPLETED!")
            else:
                print("\n📝 Validation completed. No corrections made.")
        
    finally:
        validator.close()

if __name__ == "__main__":
    main()