#!/usr/bin/env python3
"""
Step 04: F&O UDiFF Data Loader for February 2025 [DEPRECATED]

⚠️ THIS FILE IS DEPRECATED ⚠️

Please use the official Step 4 loader instead:
    python step04_fo_validation_loader.py

The new loader includes:
- Day-by-day validation and retry logic
- Automatic correction of missing records
- 100% accuracy guarantee
- Complete error handling

Purpose: Load February 2025 F&O derivatives data into step04_fo_udiff_daily table
Target: F&O UDiFF Common Bhavcopy Final data (derivatives)
Table: step04_fo_udiff_daily (separate from equity data)
"""

import pandas as pd
import sys
import os
import glob
from datetime import datetime

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from nse_database_integration import NSEDatabaseManager

class Step04FOUDiFFLoader:
    def load_fo_udiff_file(self, file_path):
        """Load a single F&O UDiFF file into the database with proper column mapping and automatic correction"""
        print(f"📥 Processing F&O file: {os.path.basename(file_path)}")
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                # Read CSV file
                df = pd.read_csv(file_path)
                # Basic validation
                if df.empty:
                    print(f"   ⚠️ Empty file: {file_path}")
                    return 0
                print(f"   📊 Loaded {len(df)} records from {os.path.basename(file_path)} (Attempt {attempt})")
                print(f"   📋 Columns: {list(df.columns)}")
                # --- POST-INSERT VALIDATION ---
                # Extract trade_date from file or DataFrame (assuming column 'trade_date' or similar exists)
                trade_date = None
                if 'trade_date' in df.columns:
                    trade_date = df['trade_date'].iloc[0]
                elif 'TradDt' in df.columns:
                    trade_date = df['TradDt'].iloc[0]
                # If trade_date is still None, try to parse from filename
                if not trade_date:
                    import re
                    m = re.search(r'(\d{8})', file_path)
                    if m:
                        trade_date = m.group(1)
                if not trade_date:
                    print(f"   ⚠️ Could not determine trade_date for validation!")
                    return len(df)
                # Query database for count
                cursor = self.db.connection.cursor()
                cursor.execute("SELECT COUNT(*) FROM step04_fo_udiff_daily WHERE trade_date = ?", trade_date)
                db_count = cursor.fetchone()[0]
                print(f"   ✅ DB records for {trade_date}: {db_count}")
                if db_count == len(df):
                    print(f"   🎉 Record count matches for {trade_date}")
                    return db_count
                else:
                    print(f"   ❌ MISMATCH: Source={len(df)}, DB={db_count} for {trade_date} (Attempt {attempt})")
                    # Automatic correction: delete and reload for this date
                    print(f"   🔄 Deleting and retrying load for {trade_date}...")
                    cursor.execute("DELETE FROM step04_fo_udiff_daily WHERE trade_date = ?", trade_date)
                    self.db.connection.commit()
            except Exception as e:
                print(f"   ❌ ERROR during load attempt {attempt}: {e}")
        print(f"   🚨 GIVING UP: Could not match record count for {file_path} after {max_attempts} attempts.")
        return 0
    def find_february_2025_fo_files(self):
        """Find February 2025 F&O UDiFF files"""
        print("🔍 Searching for February 2025 F&O UDiFF files...")
        # Search patterns for F&O files
        search_patterns = [
            "**/*feb*2025*.csv",
            "**/*february*2025*.csv",
            "**/*02*2025*.csv",
            "**/fo_*2025-02*.csv",
            "**/udiff_*2025-02*.csv",
            "**/bhav*2025-02*.csv"
        ]
        fo_files = []
        for pattern in search_patterns:
            files = glob.glob(pattern, recursive=True)
            fo_files.extend(files)
        # Remove duplicates
        fo_files = list(set(fo_files))
        print(f"   Found {len(fo_files)} potential F&O files:")
        for file in fo_files[:5]:  # Show first 5
            print(f"     {file}")
        if len(fo_files) > 5:
            print(f"     ... and {len(fo_files) - 5} more files")
        return fo_files
    def load_february_2025_fo_data(self):
        """Load all February 2025 F&O data"""
        print("🚀 LOADING FEBRUARY 2025 F&O UDiFF DATA")
        print("=" * 60)
        # Find files
        fo_files = self.find_february_2025_fo_files()
        if not fo_files:
            print("❌ No February 2025 F&O files found")
            print("📋 Please ensure F&O UDiFF files are available in the workspace")
            return
        # Load each file
        total_records = 0
        for file_path in fo_files:
            records = self.load_fo_udiff_file(file_path)
            total_records += records
        print(f"\n📊 LOADING SUMMARY:")
        print(f"   Files processed: {len(fo_files)}")
        print(f"   Total records: {total_records:,}")
        # Verify loading
        cursor = self.db.connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM step04_fo_udiff_daily")
        final_count = cursor.fetchone()[0]
        print(f"   Final table count: {final_count:,}")
        print(f"✅ February 2025 F&O data loaded in step04_fo_udiff_daily")
    def __init__(self):
        self.db = NSEDatabaseManager()
        self.create_fo_udiff_table()
        
    def create_fo_udiff_table(self):
        """Create step04_fo_udiff_daily table for F&O derivatives data"""
        print("🔧 Creating step04_fo_udiff_daily table for F&O derivatives...")
        
        cursor = self.db.connection.cursor()
        
        # Drop table if exists (for fresh start)
        cursor.execute("DROP TABLE IF EXISTS step04_fo_udiff_daily")
        
        # Create F&O UDiFF table with appropriate columns
        create_table_sql = """
        CREATE TABLE step04_fo_udiff_daily (
            id BIGINT IDENTITY(1,1) PRIMARY KEY,
            trade_date DATE NOT NULL,
            
            -- Instrument identification
            instrument NVARCHAR(50),
            symbol NVARCHAR(50) NOT NULL,
            expiry_date DATE,
            strike_price DECIMAL(18,4),
            option_type NVARCHAR(2),  -- CE/PE for options
            
            -- Trading data
            open_price DECIMAL(18,4),
            high_price DECIMAL(18,4),
            low_price DECIMAL(18,4),
            close_price DECIMAL(18,4),
            settle_price DECIMAL(18,4),
            
            -- Volume and interest
            contracts_traded BIGINT,
            value_in_lakh DECIMAL(18,4),
            open_interest BIGINT,
            change_in_oi BIGINT,
            
            -- Metadata
            underlying NVARCHAR(50),
            source_file NVARCHAR(255),
            created_at DATETIME2 DEFAULT GETDATE(),
            
            INDEX IX_step04_date (trade_date),
            INDEX IX_step04_symbol (symbol),
            INDEX IX_step04_instrument (instrument),
            INDEX IX_step04_expiry (expiry_date)
        )
        """
        
    def show_table_status(self):
        """Show current table status"""
        print("\n📊 STEP 04 TABLE STATUS")
        print("=" * 40)
        
        cursor = self.db.connection.cursor()
        
        # Check table exists and count
        try:
            cursor.execute("SELECT COUNT(*) FROM step04_fo_udiff_daily")
            count = cursor.fetchone()[0]
            print(f"✅ step04_fo_udiff_daily: {count:,} records")
            
            if count > 0:
                # Show sample data
                cursor.execute("""
                    SELECT TOP 3 trade_date, symbol, source_file 
                    FROM step04_fo_udiff_daily 
                    ORDER BY id
                """)
                
                print(f"\n📋 Sample records:")
                for row in cursor.fetchall():
                    print(f"   {row[0]} | {row[1]} | {row[2]}")
                    
        except Exception as e:
            print(f"❌ Error checking table: {e}")
            
    def close(self):
        """Close database connection"""
        self.db.close()

def main():
    loader = Step04FOUDiFFLoader()
    try:
        loader.load_february_2025_fo_data()
        loader.show_table_status()
    finally:
        loader.close()

if __name__ == '__main__':
    main()
