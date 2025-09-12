#!/usr/bin/env python3
"""
🐘 NSE Data PostgreSQL Database Setup
Complete database schema and management for NSE data storage
"""

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd
from sqlalchemy import create_engine, text
import os
import json
from datetime import datetime

class NSEDatabaseManager:
    def __init__(self, config_file="database_config.json"):
        """Initialize database manager with configuration"""
        self.config_file = config_file
        self.config = self.load_or_create_config()
        self.engine = None
        self.connection = None
    
    def load_or_create_config(self):
        """Load database configuration or create default"""
        default_config = {
            "host": "localhost",
            "port": 5432,
            "database": "nse_data",
            "username": "postgres",
            "password": "your_password_here",
            "tables": {
                "raw_data": "nse_raw_data",
                "unique_analysis": "nse_unique_analysis", 
                "comparisons": "nse_comparisons",
                "metadata": "nse_metadata"
            }
        }
        
        if os.path.exists(self.config_file):
            with open(self.config_file, 'r') as f:
                return json.load(f)
        else:
            with open(self.config_file, 'w') as f:
                json.dump(default_config, f, indent=4)
            print(f"📄 Created default config: {self.config_file}")
            print("⚠️  Please update the password in the config file!")
            return default_config
    
    def create_database(self):
        """Create NSE database if it doesn't exist"""
        try:
            # Connect to PostgreSQL server (not specific database)
            conn = psycopg2.connect(
                host=self.config['host'],
                port=self.config['port'],
                user=self.config['username'],
                password=self.config['password'],
                database='postgres'  # Connect to default database
            )
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = conn.cursor()
            
            # Check if database exists
            cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{self.config['database']}'")
            exists = cursor.fetchone()
            
            if not exists:
                cursor.execute(f"CREATE DATABASE {self.config['database']}")
                print(f"✅ Created database: {self.config['database']}")
            else:
                print(f"📊 Database already exists: {self.config['database']}")
            
            cursor.close()
            conn.close()
            return True
            
        except Exception as e:
            print(f"❌ Error creating database: {str(e)}")
            return False
    
    def connect(self):
        """Connect to NSE database"""
        try:
            connection_string = f"postgresql://{self.config['username']}:{self.config['password']}@{self.config['host']}:{self.config['port']}/{self.config['database']}"
            self.engine = create_engine(connection_string)
            self.connection = self.engine.connect()
            print(f"✅ Connected to database: {self.config['database']}")
            return True
        except Exception as e:
            print(f"❌ Connection failed: {str(e)}")
            return False
    
    def create_tables(self):
        """Create all necessary tables for NSE data"""
        
        # Raw NSE data table
        raw_data_sql = """
        CREATE TABLE IF NOT EXISTS nse_raw_data (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(50) NOT NULL,
            series VARCHAR(10) NOT NULL,
            date DATE NOT NULL,
            prev_close DECIMAL(15,4),
            open_price DECIMAL(15,4),
            high_price DECIMAL(15,4),
            low_price DECIMAL(15,4),
            last_price DECIMAL(15,4),
            close_price DECIMAL(15,4),
            avg_price DECIMAL(15,4),
            ttl_trd_qnty BIGINT,
            ttl_trd_val DECIMAL(20,4),
            deliv_qty BIGINT,
            deliv_per DECIMAL(8,4),
            month_year VARCHAR(10),
            data_source VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, series, date)
        );
        """
        
        # Unique analysis results table
        unique_analysis_sql = """
        CREATE TABLE IF NOT EXISTS nse_unique_analysis (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(50) NOT NULL,
            month_year VARCHAR(10) NOT NULL,
            analysis_type VARCHAR(20) NOT NULL, -- 'volume' or 'delivery'
            max_value BIGINT NOT NULL,
            max_date DATE NOT NULL,
            series VARCHAR(10),
            prev_close DECIMAL(15,4),
            open_price DECIMAL(15,4),
            high_price DECIMAL(15,4),
            low_price DECIMAL(15,4),
            last_price DECIMAL(15,4),
            close_price DECIMAL(15,4),
            avg_price DECIMAL(15,4),
            ttl_trd_qnty BIGINT,
            ttl_trd_val DECIMAL(20,4),
            deliv_qty BIGINT,
            deliv_per DECIMAL(8,4),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, month_year, analysis_type)
        );
        """
        
        # Month-to-month comparison results
        comparison_sql = """
        CREATE TABLE IF NOT EXISTS nse_comparisons (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(50) NOT NULL,
            month1 VARCHAR(10) NOT NULL,
            month2 VARCHAR(10) NOT NULL,
            comparison_type VARCHAR(20) NOT NULL, -- 'volume' or 'delivery'
            month1_value BIGINT NOT NULL,
            month2_value BIGINT NOT NULL,
            increase_amount BIGINT NOT NULL,
            increase_percentage DECIMAL(10,2) NOT NULL,
            times_higher DECIMAL(10,2) NOT NULL,
            month1_date DATE,
            month2_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, month1, month2, comparison_type)
        );
        """
        
        # Metadata and processing tracking
        metadata_sql = """
        CREATE TABLE IF NOT EXISTS nse_metadata (
            id SERIAL PRIMARY KEY,
            operation_type VARCHAR(50) NOT NULL, -- 'download', 'analysis', 'comparison'
            month_year VARCHAR(10),
            status VARCHAR(20) NOT NULL, -- 'completed', 'failed', 'in_progress'
            records_processed INTEGER,
            file_path VARCHAR(500),
            execution_time_seconds INTEGER,
            error_message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # Create indexes for performance
        indexes_sql = [
            "CREATE INDEX IF NOT EXISTS idx_raw_symbol_date ON nse_raw_data(symbol, date);",
            "CREATE INDEX IF NOT EXISTS idx_raw_month_year ON nse_raw_data(month_year);",
            "CREATE INDEX IF NOT EXISTS idx_unique_symbol_month ON nse_unique_analysis(symbol, month_year);",
            "CREATE INDEX IF NOT EXISTS idx_comparison_months ON nse_comparisons(month1, month2);",
            "CREATE INDEX IF NOT EXISTS idx_metadata_operation ON nse_metadata(operation_type, month_year);"
        ]
        
        try:
            # Execute table creation
            self.connection.execute(text(raw_data_sql))
            self.connection.execute(text(unique_analysis_sql))
            self.connection.execute(text(comparison_sql))
            self.connection.execute(text(metadata_sql))
            
            # Create indexes
            for index_sql in indexes_sql:
                self.connection.execute(text(index_sql))
            
            self.connection.commit()
            print("✅ All tables and indexes created successfully!")
            return True
            
        except Exception as e:
            print(f"❌ Error creating tables: {str(e)}")
            return False
    
    def test_connection(self):
        """Test database connection and show table status"""
        try:
            result = self.connection.execute(text("""
                SELECT table_name, 
                       (SELECT COUNT(*) FROM information_schema.columns 
                        WHERE table_name = t.table_name) as column_count
                FROM information_schema.tables t
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
                ORDER BY table_name;
            """))
            
            tables = result.fetchall()
            print("\n📊 Database Status:")
            print("=" * 50)
            for table_name, column_count in tables:
                print(f"🗂️  {table_name}: {column_count} columns")
            
            return True
            
        except Exception as e:
            print(f"❌ Test connection failed: {str(e)}")
            return False
    
    def import_csv_to_database(self, csv_file_path, month_year):
        """Import CSV data to raw_data table"""
        try:
            # Read CSV with pandas
            df = pd.read_csv(csv_file_path)
            df.columns = df.columns.str.strip()
            
            # Add metadata columns
            df['month_year'] = month_year
            df['data_source'] = os.path.basename(csv_file_path)
            
            # Convert date column
            if 'DATE1' in df.columns:
                df['date'] = pd.to_datetime(df['DATE1'], format='%d-%b-%Y')
            elif 'DATE' in df.columns:
                df['date'] = pd.to_datetime(df['DATE'], format='%d-%b-%Y')
            
            # Rename columns to match database schema
            column_mapping = {
                'SYMBOL': 'symbol',
                'SERIES': 'series', 
                'PREV_CLOSE': 'prev_close',
                'OPEN': 'open_price',
                'HIGH': 'high_price',
                'LOW': 'low_price',
                'LAST': 'last_price',
                'CLOSE': 'close_price',
                'VWAP': 'avg_price',
                'TTL_TRD_QNTY': 'ttl_trd_qnty',
                'TTL_TRD_VAL': 'ttl_trd_val',
                'DELIV_QTY': 'deliv_qty',
                'DELIV_PER': 'deliv_per'
            }
            
            df = df.rename(columns=column_mapping)
            
            # Filter only EQ series
            df = df[df['series'] == 'EQ'].copy()
            
            # Insert to database
            records_inserted = df.to_sql('nse_raw_data', self.engine, if_exists='append', index=False)
            
            print(f"✅ Imported {len(df)} records from {csv_file_path}")
            return len(df)
            
        except Exception as e:
            print(f"❌ Error importing {csv_file_path}: {str(e)}")
            return 0
    
    def get_data_summary(self):
        """Get summary of data in database"""
        try:
            # Raw data summary
            raw_summary = self.connection.execute(text("""
                SELECT month_year, COUNT(*) as record_count, 
                       COUNT(DISTINCT symbol) as unique_symbols,
                       MIN(date) as start_date, MAX(date) as end_date
                FROM nse_raw_data 
                GROUP BY month_year 
                ORDER BY month_year;
            """)).fetchall()
            
            print("\n📈 Data Summary:")
            print("=" * 80)
            print(f"{'Month':<12} {'Records':<10} {'Symbols':<10} {'Start Date':<12} {'End Date':<12}")
            print("-" * 80)
            
            total_records = 0
            for month, records, symbols, start_date, end_date in raw_summary:
                total_records += records
                print(f"{month:<12} {records:<10,} {symbols:<10} {start_date:<12} {end_date:<12}")
            
            print("-" * 80)
            print(f"{'TOTAL':<12} {total_records:<10,}")
            
            return raw_summary
            
        except Exception as e:
            print(f"❌ Error getting summary: {str(e)}")
            return []

def main():
    """Main setup function"""
    print("🐘 NSE PostgreSQL Database Setup")
    print("=" * 50)
    
    # Initialize database manager
    db = NSEDatabaseManager()
    
    # Check if config needs password
    if db.config['password'] == 'your_password_here':
        print("⚠️  Please set your PostgreSQL password in database_config.json")
        password = input("Enter PostgreSQL password: ")
        db.config['password'] = password
        
        # Save updated config
        with open(db.config_file, 'w') as f:
            json.dump(db.config, f, indent=4)
    
    # Setup database
    if db.create_database():
        if db.connect():
            if db.create_tables():
                if db.test_connection():
                    print("\n🎉 Database setup completed successfully!")
                    print("\n📚 Next Steps:")
                    print("1. Use db.import_csv_to_database() to import CSV files")
                    print("2. Use db.get_data_summary() to check data status")
                    print("3. Query data using SQL or pandas")
                    
                    return db
    
    print("❌ Database setup failed!")
    return None

if __name__ == "__main__":
    database = main()
