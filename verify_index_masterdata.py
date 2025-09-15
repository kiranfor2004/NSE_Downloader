"""
Verify the uploaded index_symbol_masterdata table
"""

import pyodbc
import json
import sys

class DatabaseVerifier:
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
    
    def verify_table_structure(self):
        """Verify table structure"""
        try:
            cursor = self.connection.cursor()
            
            # Get table schema
            cursor.execute("""
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    IS_NULLABLE,
                    CHARACTER_MAXIMUM_LENGTH,
                    COLUMN_DEFAULT
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'index_symbol_masterdata'
                ORDER BY ORDINAL_POSITION
            """)
            
            columns = cursor.fetchall()
            
            print("\n📋 Table Structure:")
            print("-" * 80)
            print(f"{'Column Name':<20} {'Data Type':<20} {'Nullable':<10} {'Max Length':<12} {'Default'}")
            print("-" * 80)
            
            for col in columns:
                column_name = col[0]
                data_type = col[1]
                is_nullable = col[2]
                max_length = col[3] if col[3] else 'N/A'
                default_value = col[4] if col[4] else 'None'
                
                print(f"{column_name:<20} {data_type:<20} {is_nullable:<10} {str(max_length):<12} {default_value}")
            
            cursor.close()
            
        except Exception as e:
            print(f"❌ Error verifying table structure: {e}")
    
    def show_sample_data(self):
        """Show sample data from the table"""
        try:
            cursor = self.connection.cursor()
            
            # Get sample data
            cursor.execute("""
                SELECT TOP 10 
                    id,
                    symbol,
                    index_name,
                    category,
                    created_date
                FROM index_symbol_masterdata 
                ORDER BY category, symbol
            """)
            
            records = cursor.fetchall()
            
            print(f"\n📝 Sample Data (showing {len(records)} records):")
            print("-" * 100)
            print(f"{'ID':<4} {'Symbol':<25} {'Index Name':<25} {'Category':<15} {'Created Date'}")
            print("-" * 100)
            
            for record in records:
                print(f"{record[0]:<4} {record[1]:<25} {record[2]:<25} {record[3]:<15} {record[4].strftime('%Y-%m-%d %H:%M')}")
            
            cursor.close()
            
        except Exception as e:
            print(f"❌ Error showing sample data: {e}")
    
    def show_category_summary(self):
        """Show summary by category"""
        try:
            cursor = self.connection.cursor()
            
            # Get category summary
            cursor.execute("""
                SELECT 
                    category,
                    COUNT(*) as count,
                    STRING_AGG(symbol, ', ') as symbols
                FROM index_symbol_masterdata 
                GROUP BY category 
                ORDER BY count DESC
            """)
            
            summary = cursor.fetchall()
            
            print(f"\n📊 Summary by Category:")
            print("-" * 120)
            
            for cat in summary:
                category = cat[0]
                count = cat[1]
                symbols = cat[2][:80] + "..." if len(cat[2]) > 80 else cat[2]
                
                print(f"{category} ({count} indices):")
                print(f"  {symbols}")
                print()
            
            cursor.close()
            
        except Exception as e:
            print(f"❌ Error showing category summary: {e}")
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            print("🔐 Database connection closed")

def main():
    """Main function"""
    print("🔍 Index Symbol Masterdata Verification")
    print("=" * 50)
    
    # Initialize database verifier
    db_verifier = DatabaseVerifier()
    
    try:
        # Verify table structure
        db_verifier.verify_table_structure()
        
        # Show sample data
        db_verifier.show_sample_data()
        
        # Show category summary
        db_verifier.show_category_summary()
        
        print("✅ Verification completed!")
    
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
    
    finally:
        db_verifier.close()

if __name__ == "__main__":
    main()