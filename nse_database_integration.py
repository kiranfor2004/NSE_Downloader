#!/usr/bin/env python3
"""
NSE Database Integration - Step-wise Data Loading

Purpose:
  Connect to local SQL Server database and load step-wise project data:
  - Step 01: Equity daily bhavcopy data
  - Step 02: Monthly analysis results and exceedance analysis
  - Step 03: Month-to-month comparison results
  - Step 04: F&O UDiFF derivatives data

Database Schema:
  Tables will be created with step prefixes for organization:
  - step01_equity_daily
  - step02_monthly_analysis
  - step02_exceedance_analysis
  - step03_monthly_comparisons
  - step04_fo_udiff_daily

Configuration:
  Uses database_config.json for connection settings or environment variables.
"""

import pandas as pd
import pyodbc
import json
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional

class NSEDatabaseManager:
    def __init__(self, config_file='database_config.json'):
        self.config = self.load_config(config_file)
        self.connection = None
        self.connect()
    
    def load_config(self, config_file: str) -> Dict:
        """Load database configuration from file or environment"""
        if os.path.exists(config_file):
            with open(config_file, 'r') as f:
                return json.load(f)
        else:
            # Fallback to environment variables or defaults
            return {
                "server": os.getenv("DB_SERVER", "localhost"),
                "database": os.getenv("DB_NAME", "NSE_Analysis"),
                "username": os.getenv("DB_USER", ""),
                "password": os.getenv("DB_PASSWORD", ""),
                "driver": os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server"),
                "trusted_connection": os.getenv("DB_TRUSTED", "yes")
            }
    
    def connect(self):
        """Establish database connection"""
        try:
            if self.config.get("trusted_connection", "").lower() == "yes":
                conn_str = f"""
                    DRIVER={{{self.config['driver']}}};
                    SERVER={self.config['server']};
                    DATABASE={self.config['database']};
                    Trusted_Connection=yes;
                """
            else:
                conn_str = f"""
                    DRIVER={{{self.config['driver']}}};
                    SERVER={self.config['server']};
                    DATABASE={self.config['database']};
                    UID={self.config['username']};
                    PWD={self.config['password']};
                """
            
            self.connection = pyodbc.connect(conn_str)
            print(f"✅ Connected to database: {self.config['database']} on {self.config['server']}")
            
            # Create NSE_Analysis database if it doesn't exist
            if self.config['database'] == 'master':
                self.create_nse_database()
            
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            print("💡 Ensure SQL Server is running and credentials are correct")
            sys.exit(1)
    
    def create_nse_database(self):
        """Create NSE_Analysis database if it doesn't exist"""
        cursor = self.connection.cursor()
        try:
            cursor.execute("SELECT name FROM sys.databases WHERE name = 'NSE_Analysis'")
            if not cursor.fetchone():
                cursor.execute("CREATE DATABASE NSE_Analysis")
                print("✅ Created NSE_Analysis database")
            else:
                print("✅ NSE_Analysis database already exists")
            
            # Switch to NSE_Analysis database
            cursor.execute("USE NSE_Analysis")
            self.connection.commit()
            
        except Exception as e:
            print(f"❌ Error creating database: {e}")
    
    def create_step01_tables(self):
        """Create tables for Step 01 - Equity Daily Data"""
        cursor = self.connection.cursor()
        
        # Step 01: Daily equity bhavcopy table
        step01_sql = """
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='step01_equity_daily' AND xtype='U')
        CREATE TABLE step01_equity_daily (
            id BIGINT IDENTITY(1,1) PRIMARY KEY,
            trade_date DATE NOT NULL,
            symbol NVARCHAR(50) NOT NULL,
            series NVARCHAR(10) NOT NULL,
            prev_close DECIMAL(18,4),
            open_price DECIMAL(18,4),
            high_price DECIMAL(18,4),
            low_price DECIMAL(18,4),
            last_price DECIMAL(18,4),
            close_price DECIMAL(18,4),
            avg_price DECIMAL(18,4),
            ttl_trd_qnty BIGINT,
            turnover_lacs DECIMAL(18,4),
            no_of_trades BIGINT,
            deliv_qty BIGINT,
            deliv_per DECIMAL(8,4),
            source_file NVARCHAR(255),
            created_at DATETIME2 DEFAULT GETDATE(),
            INDEX IX_step01_symbol_date (symbol, trade_date),
            INDEX IX_step01_date (trade_date),
            INDEX IX_step01_series (series)
        )
        """
        
        cursor.execute(step01_sql)
        self.connection.commit()
        print("✅ Step 01 tables created/verified")
    
    def create_step02_tables(self):
        """Create tables for Step 02 - Monthly Analysis"""
        cursor = self.connection.cursor()
        
        # Step 02a: Monthly unique analysis results
        step02a_sql = """
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='step02_monthly_analysis' AND xtype='U')
        CREATE TABLE step02_monthly_analysis (
            id BIGINT IDENTITY(1,1) PRIMARY KEY,
            analysis_month NVARCHAR(20) NOT NULL,
            symbol NVARCHAR(50) NOT NULL,
            analysis_type NVARCHAR(20) NOT NULL, -- 'VOLUME' or 'DELIVERY'
            peak_date DATE NOT NULL,
            peak_value BIGINT NOT NULL,
            ttl_trd_qnty BIGINT,
            deliv_qty BIGINT,
            close_price DECIMAL(18,4),
            turnover_lacs DECIMAL(18,4),
            analysis_file NVARCHAR(255),
            created_at DATETIME2 DEFAULT GETDATE(),
            INDEX IX_step02_month_symbol (analysis_month, symbol),
            INDEX IX_step02_type (analysis_type)
        )
        """
        
        # Step 02b: Exceedance analysis results
        step02b_sql = """
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='step02_exceedance_analysis' AND xtype='U')
        CREATE TABLE step02_exceedance_analysis (
            id BIGINT IDENTITY(1,1) PRIMARY KEY,
            comparison_name NVARCHAR(50) NOT NULL, -- e.g., 'Feb2025_vs_Jan2025'
            feb_date DATE NOT NULL,
            symbol NVARCHAR(50) NOT NULL,
            analysis_type NVARCHAR(20) NOT NULL, -- 'VOLUME_EXCEEDED' or 'DELIVERY_EXCEEDED'
            feb_value BIGINT NOT NULL,
            jan_baseline BIGINT NOT NULL,
            jan_baseline_date DATE,
            ttl_trd_qnty BIGINT,
            deliv_qty BIGINT,
            close_price DECIMAL(18,4),
            created_at DATETIME2 DEFAULT GETDATE(),
            INDEX IX_step02b_comparison_symbol (comparison_name, symbol),
            INDEX IX_step02b_feb_date (feb_date),
            INDEX IX_step02b_analysis_type (analysis_type)
        )
        """
        
        cursor.execute(step02a_sql)
        cursor.execute(step02b_sql)
        self.connection.commit()
        print("✅ Step 02 tables created/verified")
    
    def create_step03_tables(self):
        """Create tables for Step 03 - Month-to-Month Comparisons"""
        cursor = self.connection.cursor()
        
        step03_sql = """
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='step03_monthly_comparisons' AND xtype='U')
        CREATE TABLE step03_monthly_comparisons (
            id BIGINT IDENTITY(1,1) PRIMARY KEY,
            comparison_name NVARCHAR(50) NOT NULL, -- e.g., '2025-02_vs_2025-01'
            symbol NVARCHAR(50) NOT NULL,
            analysis_type NVARCHAR(20) NOT NULL, -- 'VOLUME' or 'DELIVERY'
            baseline_month NVARCHAR(10) NOT NULL,
            compare_month NVARCHAR(10) NOT NULL,
            baseline_value BIGINT NOT NULL,
            compare_value BIGINT NOT NULL,
            increase_absolute BIGINT NOT NULL,
            increase_percentage DECIMAL(10,2) NOT NULL,
            baseline_date DATE,
            compare_date DATE,
            created_at DATETIME2 DEFAULT GETDATE(),
            INDEX IX_step03_comparison (comparison_name),
            INDEX IX_step03_symbol (symbol),
            INDEX IX_step03_type (analysis_type),
            INDEX IX_step03_months (baseline_month, compare_month)
        )
        """
        
        cursor.execute(step03_sql)
        self.connection.commit()
        print("✅ Step 03 tables created/verified")
    
    def create_step04_tables(self):
        """Create tables for Step 04 - F&O UDiFF Data"""
        cursor = self.connection.cursor()
        
        step04_sql = """
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='step04_fo_udiff_daily' AND xtype='U')
        CREATE TABLE step04_fo_udiff_daily (
            id BIGINT IDENTITY(1,1) PRIMARY KEY,
            trade_date DATE NOT NULL,
            instrument NVARCHAR(50) NOT NULL,
            symbol NVARCHAR(50) NOT NULL,
            expiry_date DATE,
            strike_price DECIMAL(18,4),
            option_type NVARCHAR(5), -- 'CE', 'PE', NULL for futures
            open_price DECIMAL(18,4),
            high_price DECIMAL(18,4),
            low_price DECIMAL(18,4),
            close_price DECIMAL(18,4),
            settle_price DECIMAL(18,4),
            contracts BIGINT,
            value_lakhs DECIMAL(18,4),
            open_interest BIGINT,
            change_in_oi BIGINT,
            underlying_value DECIMAL(18,4),
            source_file NVARCHAR(255),
            created_at DATETIME2 DEFAULT GETDATE(),
            INDEX IX_step04_symbol_date (symbol, trade_date),
            INDEX IX_step04_instrument (instrument),
            INDEX IX_step04_expiry (expiry_date)
        )
        """
        
        cursor.execute(step04_sql)
        self.connection.commit()
        print("✅ Step 04 tables created/verified")
    
    def initialize_database(self):
        """Create all step tables"""
        print("🔧 Initializing NSE step-wise database schema...")
        self.create_step01_tables()
        self.create_step02_tables()
        self.create_step04_tables()
        print("🎉 Database initialization complete!")
    
    def get_table_count(self, table_name: str) -> int:
        """Get record count for a table"""
        cursor = self.connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        return cursor.fetchone()[0]
    
    def get_database_summary(self):
        """Display summary of all step tables"""
        tables = [
            'step01_equity_daily',
            'step02_monthly_analysis', 
            'step02_exceedance_analysis',
            'step04_fo_udiff_daily'
        ]
        
        print("\n📊 NSE Database Summary:")
        print("=" * 50)
        for table in tables:
            try:
                count = self.get_table_count(table)
                print(f"{table:<30}: {count:,} records")
            except:
                print(f"{table:<30}: Table not found")
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            print("📝 Database connection closed")

def main():
    """Initialize database and display status"""
    try:
        db = NSEDatabaseManager()
        db.initialize_database()
        db.get_database_summary()
        db.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
