"""
Add Index_name column to step03_compare_monthvspreviousmonth table
by matching symbols with index_symbol_masterdata table
"""

import pyodbc
import json
import sys

class IndexNameUpdater:
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
    
    def check_table_structures(self):
        """Check the structure of both tables"""
        try:
            cursor = self.connection.cursor()
            
            # Check step03_compare_monthvspreviousmonth table structure
            print("📋 Checking step03_compare_monthvspreviousmonth table structure...")
            cursor.execute("""
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    CHARACTER_MAXIMUM_LENGTH,
                    IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'step03_compare_monthvspreviousmonth'
                ORDER BY ORDINAL_POSITION
            """)
            
            step03_columns = cursor.fetchall()
            
            if not step03_columns:
                print("❌ step03_compare_monthvspreviousmonth table not found")
                return False, False
            
            print("\nStep03 Table Columns:")
            print("-" * 60)
            print(f"{'Column Name':<30} {'Data Type':<15} {'Max Length':<12} {'Nullable'}")
            print("-" * 60)
            for col in step03_columns:
                max_len = str(col[2]) if col[2] else 'N/A'
                print(f"{col[0]:<30} {col[1]:<15} {max_len:<12} {col[3]}")
            
            # Check if index_name column already exists
            index_name_exists = any(col[0].lower() == 'index_name' for col in step03_columns)
            
            # Get sample data from step03 table
            cursor.execute("""
                SELECT TOP 5 symbol, comparison_type, current_deliv_qty, delivery_increase_pct 
                FROM step03_compare_monthvspreviousmonth 
                ORDER BY delivery_increase_pct DESC
            """)
            
            step03_sample = cursor.fetchall()
            
            print(f"\nStep03 Table Sample Data:")
            print("-" * 80)
            print(f"{'Symbol':<15} {'Comparison Type':<20} {'Current Deliv':<15} {'% Increase'}")
            print("-" * 80)
            for record in step03_sample:
                print(f"{record[0]:<15} {record[1]:<20} {record[2]:<15} {record[3]:.2f}%")
            
            # Check index_symbol_masterdata table in NSE database
            print("\n📋 Checking index_symbol_masterdata table (switching to NSE database)...")
            
            # Switch to NSE database for master data
            cursor.execute("USE NSE")
            
            cursor.execute("""
                SELECT COUNT(*) as total_records,
                       COUNT(DISTINCT symbol) as unique_symbols,
                       COUNT(DISTINCT index_name) as unique_indices
                FROM index_symbol_masterdata
            """)
            
            master_stats = cursor.fetchone()
            print(f"\nIndex Master Data Stats:")
            print(f"  Total Records: {master_stats[0]}")
            print(f"  Unique Symbols: {master_stats[1]}")
            print(f"  Unique Indices: {master_stats[2]}")
            
            # Get sample data from master table
            cursor.execute("""
                SELECT TOP 5 symbol, index_name, category 
                FROM index_symbol_masterdata 
                ORDER BY symbol
            """)
            
            master_sample = cursor.fetchall()
            
            print(f"\nIndex Master Data Sample:")
            print("-" * 60)
            print(f"{'Symbol':<15} {'Index Name':<20} {'Category'}")
            print("-" * 60)
            for record in master_sample:
                print(f"{record[0]:<15} {record[1]:<20} {record[2]}")
            
            # Switch back to master database
            cursor.execute("USE master")
            
            cursor.close()
            return True, index_name_exists
            
        except Exception as e:
            print(f"❌ Error checking table structures: {e}")
            return False, False
    
    def add_index_name_column(self):
        """Add index_name column to step03 table if it doesn't exist"""
        try:
            cursor = self.connection.cursor()
            
            print("🔧 Adding index_name column to step03_compare_monthvspreviousmonth table...")
            
            alter_sql = """
            ALTER TABLE step03_compare_monthvspreviousmonth 
            ADD index_name NVARCHAR(200) NULL
            """
            
            cursor.execute(alter_sql)
            self.connection.commit()
            print("✅ index_name column added successfully")
            cursor.close()
            return True
            
        except Exception as e:
            if "column name" in str(e).lower() and "already exists" in str(e).lower():
                print("ℹ️ index_name column already exists")
                return True
            else:
                print(f"❌ Error adding index_name column: {e}")
                return False
    
    def update_index_names(self):
        """Update index_name column by matching symbols"""
        try:
            cursor = self.connection.cursor()
            
            print("🔄 Updating index_name column...")
            
            # First, update all to "Other Index" as default
            cursor.execute("""
                UPDATE step03_compare_monthvspreviousmonth 
                SET index_name = 'Other Index' 
                WHERE index_name IS NULL
            """)
            
            print("✅ Set default 'Other Index' for all records")
            
            # Update with actual index names for matching symbols
            # Cross-database update from NSE.index_symbol_masterdata to master.step03_compare_monthvspreviousmonth
            update_sql = """
            UPDATE s3
            SET s3.index_name = ism.index_name
            FROM step03_compare_monthvspreviousmonth s3
            INNER JOIN (
                SELECT symbol, MIN(index_name) as index_name
                FROM NSE.dbo.index_symbol_masterdata
                GROUP BY symbol
            ) ism ON s3.symbol = ism.symbol
            """
            
            cursor.execute(update_sql)
            rows_updated = cursor.rowcount
            
            self.connection.commit()
            print(f"✅ Updated {rows_updated} records with matching index names")
            
            cursor.close()
            return True
            
        except Exception as e:
            print(f"❌ Error updating index names: {e}")
            return False
    
    def verify_updates(self):
        """Verify the updates and show statistics"""
        try:
            cursor = self.connection.cursor()
            
            # Get total count
            cursor.execute("SELECT COUNT(*) FROM step03_compare_monthvspreviousmonth")
            total_count = cursor.fetchone()[0]
            
            # Count by index_name
            cursor.execute("""
                SELECT index_name, COUNT(*) as count
                FROM step03_compare_monthvspreviousmonth
                GROUP BY index_name
                ORDER BY count DESC
            """)
            
            index_counts = cursor.fetchall()
            
            # Get examples of matched symbols
            cursor.execute("""
                SELECT TOP 10 
                    symbol, 
                    index_name, 
                    comparison_type, 
                    delivery_increase_pct
                FROM step03_compare_monthvspreviousmonth
                WHERE index_name != 'Other Index'
                ORDER BY delivery_increase_pct DESC
            """)
            
            matched_examples = cursor.fetchall()
            
            # Get examples of unmatched symbols  
            cursor.execute("""
                SELECT TOP 5 
                    symbol, 
                    index_name, 
                    comparison_type, 
                    delivery_increase_pct
                FROM step03_compare_monthvspreviousmonth
                WHERE index_name = 'Other Index'
                ORDER BY delivery_increase_pct DESC
            """)
            
            unmatched_examples = cursor.fetchall()
            
            print(f"\n📊 Update Verification:")
            print(f"Total Records: {total_count}")
            
            print(f"\n📈 Index Name Distribution:")
            for index_name, count in index_counts:
                percentage = (count / total_count) * 100
                print(f"  {index_name}: {count} records ({percentage:.1f}%)")
            
            if matched_examples:
                print(f"\n✅ Examples of Matched Symbols:")
                print("-" * 90)
                print(f"{'Symbol':<15} {'Index Name':<20} {'Comparison Type':<20} {'% Increase'}")
                print("-" * 90)
                for record in matched_examples:
                    print(f"{record[0]:<15} {record[1]:<20} {record[2]:<20} {record[3]:.2f}%")
            
            if unmatched_examples:
                print(f"\n❓ Examples of Unmatched Symbols (Other Index):")
                print("-" * 90)
                print(f"{'Symbol':<15} {'Index Name':<20} {'Comparison Type':<20} {'% Increase'}")
                print("-" * 90)
                for record in unmatched_examples:
                    print(f"{record[0]:<15} {record[1]:<20} {record[2]:<20} {record[3]:.2f}%")
            
            cursor.close()
            
        except Exception as e:
            print(f"❌ Error verifying updates: {e}")
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            print("\n🔐 Database connection closed")

def main():
    """Main function"""
    print("🚀 Add Index Names to Monthly Comparison Table")
    print("=" * 55)
    
    # Initialize updater
    updater = IndexNameUpdater()
    
    try:
        # Check table structures
        tables_ok, index_exists = updater.check_table_structures()
        if not tables_ok:
            return
        
        # Add index_name column if it doesn't exist
        if not index_exists:
            if not updater.add_index_name_column():
                return
        
        # Update index names
        if updater.update_index_names():
            print("\n🎉 Index name updates completed successfully!")
            
            # Verify updates
            updater.verify_updates()
        else:
            print("\n❌ Index name updates failed!")
    
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
    
    finally:
        updater.close()

if __name__ == "__main__":
    main()