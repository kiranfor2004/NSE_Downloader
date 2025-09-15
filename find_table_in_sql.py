"""
Check database tables and help locate index_symbol_masterdata table
"""

import pyodbc
import json
import sys

class DatabaseTableChecker:
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
            print(f"✅ Connected to database: {self.config['database']} on server: {self.config['server']}")
            
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            sys.exit(1)
    
    def list_all_databases(self):
        """List all databases on the server"""
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT name FROM sys.databases ORDER BY name")
            databases = cursor.fetchall()
            
            print("\n📋 All Databases on Server:")
            print("-" * 30)
            for db in databases:
                print(f"  - {db[0]}")
            
            cursor.close()
            
        except Exception as e:
            print(f"❌ Error listing databases: {e}")
    
    def list_all_tables(self):
        """List all tables in current database"""
        try:
            cursor = self.connection.cursor()
            
            # Get all user tables
            cursor.execute("""
                SELECT 
                    TABLE_SCHEMA,
                    TABLE_NAME,
                    TABLE_TYPE
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_SCHEMA, TABLE_NAME
            """)
            
            tables = cursor.fetchall()
            
            print(f"\n📋 All Tables in '{self.config['database']}' Database:")
            print("-" * 50)
            
            if not tables:
                print("  ❌ No user tables found in this database")
            else:
                print(f"{'Schema':<10} {'Table Name':<30} {'Type'}")
                print("-" * 50)
                for table in tables:
                    print(f"{table[0]:<10} {table[1]:<30} {table[2]}")
            
            cursor.close()
            return tables
            
        except Exception as e:
            print(f"❌ Error listing tables: {e}")
            return []
    
    def search_for_table(self, table_name):
        """Search for a specific table by name"""
        try:
            cursor = self.connection.cursor()
            
            # Search for table with partial match
            cursor.execute("""
                SELECT 
                    TABLE_SCHEMA,
                    TABLE_NAME,
                    TABLE_TYPE
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_NAME LIKE ?
                ORDER BY TABLE_SCHEMA, TABLE_NAME
            """, f"%{table_name}%")
            
            tables = cursor.fetchall()
            
            print(f"\n🔍 Searching for tables containing '{table_name}':")
            print("-" * 60)
            
            if not tables:
                print(f"  ❌ No tables found containing '{table_name}'")
            else:
                print(f"{'Schema':<10} {'Table Name':<35} {'Type'}")
                print("-" * 60)
                for table in tables:
                    print(f"{table[0]:<10} {table[1]:<35} {table[2]}")
            
            cursor.close()
            return tables
            
        except Exception as e:
            print(f"❌ Error searching for table: {e}")
            return []
    
    def check_table_exists(self, table_name):
        """Check if a specific table exists"""
        try:
            cursor = self.connection.cursor()
            
            cursor.execute("""
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_NAME = ?
            """, table_name)
            
            count = cursor.fetchone()[0]
            
            if count > 0:
                print(f"✅ Table '{table_name}' exists!")
                
                # Get table details
                cursor.execute("""
                    SELECT 
                        TABLE_SCHEMA,
                        TABLE_NAME,
                        TABLE_TYPE
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_NAME = ?
                """, table_name)
                
                table_info = cursor.fetchone()
                print(f"   Schema: {table_info[0]}")
                print(f"   Full Name: {table_info[0]}.{table_info[1]}")
                print(f"   Type: {table_info[2]}")
                
                # Get row count
                cursor.execute(f"SELECT COUNT(*) FROM [{table_info[0]}].[{table_info[1]}]")
                row_count = cursor.fetchone()[0]
                print(f"   Row Count: {row_count}")
                
            else:
                print(f"❌ Table '{table_name}' does not exist")
            
            cursor.close()
            return count > 0
            
        except Exception as e:
            print(f"❌ Error checking table existence: {e}")
            return False
    
    def get_connection_info(self):
        """Display current connection information"""
        try:
            cursor = self.connection.cursor()
            
            # Get current database
            cursor.execute("SELECT DB_NAME()")
            current_db = cursor.fetchone()[0]
            
            # Get server name
            cursor.execute("SELECT @@SERVERNAME")
            server_name = cursor.fetchone()[0]
            
            # Get user
            cursor.execute("SELECT SYSTEM_USER")
            current_user = cursor.fetchone()[0]
            
            print(f"\n🔗 Current Connection Info:")
            print("-" * 40)
            print(f"Server: {server_name}")
            print(f"Database: {current_db}")
            print(f"User: {current_user}")
            
            cursor.close()
            
        except Exception as e:
            print(f"❌ Error getting connection info: {e}")
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            print("\n🔐 Database connection closed")

def main():
    """Main function"""
    print("🔍 SQL Server Database and Table Checker")
    print("=" * 50)
    
    # Initialize database checker
    db_checker = DatabaseTableChecker()
    
    try:
        # Show connection info
        db_checker.get_connection_info()
        
        # List all databases
        db_checker.list_all_databases()
        
        # List all tables in current database
        tables = db_checker.list_all_tables()
        
        # Check for our specific table
        print("\n" + "="*50)
        db_checker.check_table_exists('index_symbol_masterdata')
        
        # Search for similar table names
        db_checker.search_for_table('index')
        db_checker.search_for_table('symbol')
        db_checker.search_for_table('master')
        
        print("\n✅ Database check completed!")
        
        if not tables:
            print("\n💡 Troubleshooting Tips:")
            print("1. Make sure you're connected to the correct database")
            print("2. Check if the table was created in a different schema")
            print("3. Verify the upload script ran successfully")
            print("4. Try refreshing your SQL Server Management Studio")
    
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
    
    finally:
        db_checker.close()

if __name__ == "__main__":
    main()