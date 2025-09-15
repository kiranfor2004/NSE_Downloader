"""
Modify table structure to allow stock symbols to appear in multiple indices
Then upload the constituent data properly
"""

import pandas as pd
import pyodbc
from datetime import datetime
import json
import sys
import os

class NSEDatabaseManager:
    def __init__(self, config_file='database_config.json'):
        """Initialize database connection using config file"""
        try:
            with open(config_file, 'r') as f:
                self.config = json.load(f)
            self.connection = None
            self.connect()
        except FileNotFoundError:
            print(f"Error: {config_file} not found")
            sys.exit(1)
        except Exception as e:
            print(f"Error loading config: {e}")
            sys.exit(1)
    
    def connect(self):
        """Establish database connection"""
        try:
            # Build connection string for Windows Authentication
            conn_str = (
                f"DRIVER={{{self.config['driver']}}};"
                f"SERVER={self.config['server']};"
                f"DATABASE={self.config['database']};"
                f"Trusted_Connection=yes;"
            )
            
            self.connection = pyodbc.connect(conn_str)
            print(f"✅ Connected to database: {self.config['database']}")
            
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            sys.exit(1)
    
    def recreate_table_structure(self):
        """Drop and recreate the table without unique constraint on symbol"""
        try:
            cursor = self.connection.cursor()
            
            # Drop the existing table
            print("🔧 Dropping existing table...")
            cursor.execute("DROP TABLE IF EXISTS index_symbol_masterdata")
            
            # Create new table structure allowing duplicate symbols
            create_table_sql = """
            CREATE TABLE index_symbol_masterdata (
                id INT IDENTITY(1,1) PRIMARY KEY,
                symbol NVARCHAR(100) NOT NULL,
                index_name NVARCHAR(200) NOT NULL,
                category NVARCHAR(50) NOT NULL,
                created_date DATETIME DEFAULT GETDATE(),
                updated_date DATETIME DEFAULT GETDATE(),
                UNIQUE(symbol, index_name)  -- Allow same symbol in different indices
            )
            """
            
            cursor.execute(create_table_sql)
            self.connection.commit()
            print("✅ Table recreated with new structure (allows symbols in multiple indices)")
            cursor.close()
            
        except Exception as e:
            print(f"❌ Error recreating table: {e}")
            return False
        
        return True
    
    def upload_constituent_data(self, csv_file):
        """Upload constituent data from CSV file to index_symbol_masterdata table"""
        try:
            # Read CSV file
            df = pd.read_csv(csv_file)
            print(f"📖 Read {len(df)} records from {csv_file}")
            
            # Display first few records
            print("\n📋 Sample data:")
            print(df.head().to_string(index=False))
            
            # Show summary by index
            print(f"\n📊 Data Summary:")
            index_counts = df['INDICES'].value_counts()
            for index_name, count in index_counts.items():
                print(f"  {index_name}: {count} symbols")
            
            # Insert data
            cursor = self.connection.cursor()
            
            insert_sql = """
            INSERT INTO index_symbol_masterdata (symbol, index_name, category)
            VALUES (?, ?, ?)
            """
            
            records_inserted = 0
            duplicate_errors = 0
            
            for index, row in df.iterrows():
                try:
                    # Map CSV columns to database columns
                    # symbol = actual stock symbol (RELIANCE, TCS, etc.)
                    # index_name = index name (NIFTY 50, NIFTY BANK, etc.)
                    # category = category from CSV
                    cursor.execute(insert_sql, 
                                 row['SYMBOL'],      # Stock symbol like RELIANCE, TCS
                                 row['INDICES'],     # Index name like NIFTY 50, NIFTY BANK
                                 row['CATEGORY'])    # Category like Broad Market, Sectoral
                    records_inserted += 1
                    
                except Exception as e:
                    if "duplicate key" in str(e).lower():
                        duplicate_errors += 1
                        print(f"⚠️ Duplicate: {row['SYMBOL']} already exists in {row['INDICES']}")
                    else:
                        print(f"⚠️ Error inserting record {index + 1}: {e}")
                    continue
            
            self.connection.commit()
            cursor.close()
            
            print(f"\n✅ Successfully inserted {records_inserted} constituent records")
            if duplicate_errors > 0:
                print(f"⚠️ Skipped {duplicate_errors} duplicate records")
            
            # Verify data
            self.verify_data()
            
            return True
            
        except Exception as e:
            print(f"❌ Error uploading data: {e}")
            return False
    
    def verify_data(self):
        """Verify the uploaded data"""
        try:
            cursor = self.connection.cursor()
            
            # Count total records
            cursor.execute("SELECT COUNT(*) FROM index_symbol_masterdata")
            total_count = cursor.fetchone()[0]
            
            # Count unique symbols
            cursor.execute("SELECT COUNT(DISTINCT symbol) FROM index_symbol_masterdata")
            unique_symbols = cursor.fetchone()[0]
            
            # Count by index
            cursor.execute("""
                SELECT index_name, COUNT(*) as count 
                FROM index_symbol_masterdata 
                GROUP BY index_name 
                ORDER BY count DESC
            """)
            
            index_counts = cursor.fetchall()
            
            # Count by category
            cursor.execute("""
                SELECT category, COUNT(*) as count 
                FROM index_symbol_masterdata 
                GROUP BY category 
                ORDER BY count DESC
            """)
            
            category_counts = cursor.fetchall()
            
            # Find symbols in multiple indices
            cursor.execute("""
                SELECT symbol, COUNT(DISTINCT index_name) as index_count,
                       STRING_AGG(index_name, ', ') as indices
                FROM index_symbol_masterdata 
                GROUP BY symbol
                HAVING COUNT(DISTINCT index_name) > 1
                ORDER BY index_count DESC
            """)
            
            multi_index_symbols = cursor.fetchall()
            
            # Sample records
            cursor.execute("""
                SELECT TOP 10 symbol, index_name, category, created_date
                FROM index_symbol_masterdata 
                ORDER BY index_name, symbol
            """)
            
            sample_records = cursor.fetchall()
            
            print(f"\n📊 Data Verification:")
            print(f"Total Records: {total_count}")
            print(f"Unique Stock Symbols: {unique_symbols}")
            
            print(f"\n📈 Index Breakdown:")
            for index_name, count in index_counts:
                print(f"  {index_name}: {count} symbols")
            
            print(f"\n📁 Category Breakdown:")
            for category, count in category_counts:
                print(f"  {category}: {count} symbols")
            
            if multi_index_symbols:
                print(f"\n🔗 Symbols in Multiple Indices (Top 10):")
                for symbol, count, indices in multi_index_symbols[:10]:
                    print(f"  {symbol}: appears in {count} indices ({indices})")
            
            print(f"\n📝 Sample Records:")
            for record in sample_records:
                print(f"  {record[0]} | {record[1]} | {record[2]} | {record[3]}")
            
            cursor.close()
            
        except Exception as e:
            print(f"❌ Error verifying data: {e}")
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            print("🔐 Database connection closed")

def main():
    """Main function to recreate table and upload constituent data"""
    
    csv_file = 'nse_index_constituents_latest.csv'
    
    print("🚀 NSE Index Constituents Upload (Fixed Structure)")
    print("=" * 60)
    
    # Check if CSV file exists
    if not os.path.exists(csv_file):
        print(f"❌ Error: {csv_file} not found")
        return
    
    # Initialize database manager
    db_manager = NSEDatabaseManager()
    
    try:
        # Recreate table structure
        if not db_manager.recreate_table_structure():
            return
        
        # Upload constituent data
        if db_manager.upload_constituent_data(csv_file):
            print("\n🎉 Constituent data upload completed successfully!")
            print("\n💡 The table now properly handles:")
            print("   - Stock symbols (RELIANCE, TCS, HDFCBANK, etc.)")
            print("   - Mapped to their indices (NIFTY 50, NIFTY BANK, etc.)")
            print("   - Same symbol can appear in multiple indices")
        else:
            print("\n❌ Upload failed!")
    
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
    
    finally:
        db_manager.close()

if __name__ == "__main__":
    main()