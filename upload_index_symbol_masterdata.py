"""
Upload NSE indices simplified data to SQL database
Table: index_symbol_masterdata
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
            # Build connection string
            if self.config.get('trusted_connection', '').lower() == 'yes' or (not self.config.get('username') and not self.config.get('password')):
                # Windows Authentication
                conn_str = (
                    f"DRIVER={{{self.config['driver']}}};"
                    f"SERVER={self.config['server']};"
                    f"DATABASE={self.config['database']};"
                    f"Trusted_Connection=yes;"
                )
            else:
                # SQL Server Authentication
                conn_str = (
                    f"DRIVER={{{self.config['driver']}}};"
                    f"SERVER={self.config['server']};"
                    f"DATABASE={self.config['database']};"
                    f"UID={self.config['username']};"
                    f"PWD={self.config['password']};"
                )
            
            self.connection = pyodbc.connect(conn_str)
            print(f"✅ Connected to database: {self.config['database']}")
            
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            sys.exit(1)
    
    def create_index_symbol_masterdata_table(self):
        """Create index_symbol_masterdata table if it doesn't exist"""
        create_table_sql = """
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='index_symbol_masterdata' AND xtype='U')
        CREATE TABLE index_symbol_masterdata (
            id INT IDENTITY(1,1) PRIMARY KEY,
            symbol NVARCHAR(100) NOT NULL,
            index_name NVARCHAR(200) NOT NULL,
            category NVARCHAR(50) NOT NULL,
            created_date DATETIME DEFAULT GETDATE(),
            updated_date DATETIME DEFAULT GETDATE(),
            UNIQUE(symbol)
        )
        """
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(create_table_sql)
            self.connection.commit()
            print("✅ Table 'index_symbol_masterdata' created/verified successfully")
            cursor.close()
            
        except Exception as e:
            print(f"❌ Error creating table: {e}")
            return False
        
        return True
    
    def clear_table(self, table_name):
        """Clear all data from the specified table"""
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"DELETE FROM {table_name}")
            self.connection.commit()
            print(f"✅ Cleared all data from {table_name}")
            cursor.close()
            
        except Exception as e:
            print(f"❌ Error clearing table: {e}")
            return False
        
        return True
    
    def upload_data(self, csv_file):
        """Upload data from CSV file to index_symbol_masterdata table"""
        try:
            # Read CSV file
            df = pd.read_csv(csv_file)
            print(f"📖 Read {len(df)} records from {csv_file}")
            
            # Display first few records
            print("\n📋 Sample data:")
            print(df.head().to_string(index=False))
            
            # Clear existing data
            if not self.clear_table('index_symbol_masterdata'):
                return False
            
            # Insert data
            cursor = self.connection.cursor()
            
            insert_sql = """
            INSERT INTO index_symbol_masterdata (symbol, index_name, category)
            VALUES (?, ?, ?)
            """
            
            records_inserted = 0
            
            for index, row in df.iterrows():
                try:
                    cursor.execute(insert_sql, 
                                 row['Symbol'], 
                                 row['Index_name'], 
                                 row['Category'])
                    records_inserted += 1
                    
                except Exception as e:
                    print(f"⚠️ Error inserting record {index + 1}: {e}")
                    continue
            
            self.connection.commit()
            cursor.close()
            
            print(f"\n✅ Successfully inserted {records_inserted} records into index_symbol_masterdata table")
            
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
            
            # Count by category
            cursor.execute("""
                SELECT category, COUNT(*) as count 
                FROM index_symbol_masterdata 
                GROUP BY category 
                ORDER BY count DESC
            """)
            
            category_counts = cursor.fetchall()
            
            # Sample records
            cursor.execute("""
                SELECT TOP 5 symbol, index_name, category, created_date
                FROM index_symbol_masterdata 
                ORDER BY created_date DESC
            """)
            
            sample_records = cursor.fetchall()
            
            print(f"\n📊 Data Verification:")
            print(f"Total Records: {total_count}")
            
            print(f"\n📈 Category Breakdown:")
            for category, count in category_counts:
                print(f"  {category}: {count}")
            
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
    """Main function to upload index symbol masterdata"""
    
    csv_file = 'nse_indices_simplified.csv'
    
    print("🚀 NSE Index Symbol Masterdata Upload")
    print("=" * 50)
    
    # Check if CSV file exists
    if not os.path.exists(csv_file):
        print(f"❌ Error: {csv_file} not found")
        return
    
    # Initialize database manager
    db_manager = NSEDatabaseManager()
    
    try:
        # Create table if needed
        if not db_manager.create_index_symbol_masterdata_table():
            return
        
        # Upload data
        if db_manager.upload_data(csv_file):
            print("\n🎉 Upload completed successfully!")
        else:
            print("\n❌ Upload failed!")
    
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
    
    finally:
        db_manager.close()

if __name__ == "__main__":
    main()