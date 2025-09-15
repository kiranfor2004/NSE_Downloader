"""
Check and create NSE database if needed
"""

import pyodbc
import json
import sys

class DatabaseSetup:
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
    
    def list_databases(self):
        """List all available databases"""
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT name FROM sys.databases ORDER BY name")
            databases = cursor.fetchall()
            
            print("\n📋 Available Databases:")
            for db in databases:
                print(f"  - {db[0]}")
            
            cursor.close()
            return [db[0] for db in databases]
            
        except Exception as e:
            print(f"❌ Error listing databases: {e}")
            return []
    
    def create_nse_database(self):
        """Create NSE database if it doesn't exist"""
        try:
            cursor = self.connection.cursor()
            
            # Check if NSE database exists
            cursor.execute("SELECT name FROM sys.databases WHERE name = 'NSE'")
            result = cursor.fetchone()
            
            if result:
                print("✅ NSE database already exists")
            else:
                print("🔧 Creating NSE database...")
                # Enable autocommit for CREATE DATABASE
                self.connection.autocommit = True
                cursor.execute("CREATE DATABASE NSE")
                self.connection.autocommit = False
                print("✅ NSE database created successfully")
            
            cursor.close()
            return True
            
        except Exception as e:
            print(f"❌ Error creating NSE database: {e}")
            return False
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            print("🔐 Database connection closed")

def main():
    """Main function"""
    print("🔍 Database Setup and Check")
    print("=" * 30)
    
    # Initialize database setup
    db_setup = DatabaseSetup()
    
    try:
        # List available databases
        databases = db_setup.list_databases()
        
        # Create NSE database if needed
        db_setup.create_nse_database()
        
        print("\n✅ Database setup completed!")
    
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
    
    finally:
        db_setup.close()

if __name__ == "__main__":
    main()