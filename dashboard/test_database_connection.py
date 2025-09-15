"""
Test script to verify database connection and step03_compare_monthvspreviousmonth table access
"""

import pyodbc
import json
import sys
from pathlib import Path

def load_database_config():
    """Load database configuration"""
    try:
        config_path = Path("../database_config.json")
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        # Use master database for step03 table
        master_config = config.get('master_database', {})
        connection_string = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={master_config.get('server', 'localhost')};"
            f"DATABASE={master_config.get('database', 'master')};"
            f"Integrated Security=yes;"
        )
        return connection_string
    except Exception as e:
        print(f"❌ Failed to load database config: {e}")
        return None

def test_database_connection():
    """Test database connection and table access"""
    print("🔍 Testing NSE Dashboard Database Connection")
    print("=" * 60)
    
    # Load configuration
    connection_string = load_database_config()
    if not connection_string:
        return False
    
    try:
        # Test connection
        print("📡 Testing database connection...")
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        print("✅ Database connection successful")
        
        # Test table existence
        print("🔍 Checking step03_compare_monthvspreviousmonth table...")
        table_check_query = """
            SELECT COUNT(*) as table_exists 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = 'step03_compare_monthvspreviousmonth'
        """
        cursor.execute(table_check_query)
        table_exists = cursor.fetchone()[0]
        
        if table_exists == 0:
            print("❌ Table step03_compare_monthvspreviousmonth not found")
            return False
        
        print("✅ Table step03_compare_monthvspreviousmonth found")
        
        # Test data access and count
        print("📊 Checking table data...")
        count_query = "SELECT COUNT(*) as total_records FROM step03_compare_monthvspreviousmonth"
        cursor.execute(count_query)
        total_records = cursor.fetchone()[0]
        print(f"✅ Total records in table: {total_records:,}")
        
        if total_records == 0:
            print("⚠️ Warning: Table exists but contains no data")
            return False
        
        # Test column structure
        print("🔍 Checking table structure...")
        structure_query = """
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'step03_compare_monthvspreviousmonth'
            ORDER BY ORDINAL_POSITION
        """
        cursor.execute(structure_query)
        columns = cursor.fetchall()
        
        print("📋 Table columns:")
        required_columns = ['symbol', 'index_name', 'category', 'current_deliv_qty', 
                          'delivery_increase_pct', 'comparison_type', 'current_trade_date']
        
        found_columns = []
        for col in columns:
            col_name, data_type, nullable = col
            found_columns.append(col_name.lower())
            print(f"   • {col_name} ({data_type}) {'NULL' if nullable == 'YES' else 'NOT NULL'}")
        
        # Check required columns
        missing_columns = []
        for req_col in required_columns:
            if req_col.lower() not in found_columns:
                missing_columns.append(req_col)
        
        if missing_columns:
            print(f"❌ Missing required columns: {', '.join(missing_columns)}")
            return False
        
        print("✅ All required columns present")
        
        # Test sample data retrieval
        print("🔍 Testing sample data retrieval...")
        sample_query = """
            SELECT TOP 5 
                symbol,
                index_name,
                category,
                delivery_increase_pct,
                current_trade_date
            FROM step03_compare_monthvspreviousmonth
            ORDER BY delivery_increase_pct DESC
        """
        cursor.execute(sample_query)
        sample_data = cursor.fetchall()
        
        print("📊 Sample records (Top 5 by delivery increase):")
        for row in sample_data:
            symbol, index_name, category, increase_pct, trade_date = row
            print(f"   • {symbol} ({index_name}) - {category}: {increase_pct:.2f}% - {trade_date}")
        
        # Test category distribution
        print("📊 Testing category distribution...")
        category_query = """
            SELECT 
                category,
                COUNT(*) as count,
                AVG(delivery_increase_pct) as avg_increase
            FROM step03_compare_monthvspreviousmonth
            GROUP BY category
            ORDER BY count DESC
        """
        cursor.execute(category_query)
        categories = cursor.fetchall()
        
        print("📋 Category breakdown:")
        for cat_row in categories:
            category, count, avg_increase = cat_row
            print(f"   • {category}: {count:,} records (Avg: {avg_increase:.2f}%)")
        
        conn.close()
        print("\n" + "=" * 60)
        print("🎉 Database connection test completed successfully!")
        print("✅ The dashboard API should work correctly with this data source")
        return True
        
    except Exception as e:
        print(f"❌ Database test failed: {e}")
        return False

def test_api_requirements():
    """Test if required packages are available"""
    print("\n🔍 Testing API requirements...")
    
    required_packages = ['flask', 'flask_cors', 'pyodbc', 'pandas', 'numpy']
    missing_packages = []
    
    for package in required_packages:
        try:
            if package == 'flask_cors':
                import flask_cors
            else:
                __import__(package)
            print(f"✅ {package} is available")
        except ImportError:
            missing_packages.append(package)
            print(f"❌ {package} is missing")
    
    if missing_packages:
        print(f"\n📦 To install missing packages, run:")
        print(f"pip install {' '.join(missing_packages)}")
        return False
    
    print("✅ All required packages are available")
    return True

if __name__ == "__main__":
    print("🎯 NSE Dashboard Database Connection Test")
    print("This script verifies that the dashboard can connect to your data source")
    print()
    
    # Test requirements
    requirements_ok = test_api_requirements()
    
    # Test database
    database_ok = test_database_connection()
    
    print("\n" + "=" * 60)
    print("📋 FINAL RESULTS:")
    print(f"   Requirements: {'✅ PASS' if requirements_ok else '❌ FAIL'}")
    print(f"   Database:     {'✅ PASS' if database_ok else '❌ FAIL'}")
    
    if requirements_ok and database_ok:
        print("\n🚀 Your dashboard is ready to run!")
        print("   Next steps:")
        print("   1. cd dashboard")
        print("   2. python api.py")
        print("   3. Open index.html in your browser")
    else:
        print("\n🔧 Please resolve the issues above before running the dashboard")
        sys.exit(1)