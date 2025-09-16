import pyodbc
import json
import pandas as pd

# Load database configuration
with open('dashboard/database_config.json', 'r') as f:
    config = json.load(f)

try:
    # Connect to master database
    conn_str = f"""
    DRIVER={{{config['master_database']['driver']}}};
    SERVER={config['master_database']['server']};
    DATABASE={config['master_database']['database']};
    Trusted_Connection={config['master_database']['trusted_connection']};
    """
    
    print("🔗 Connecting to master database...")
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    # Check if table exists
    cursor.execute("""
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_NAME = 'step03_compare_monthvspreviousmonth'
    """)
    table_exists = cursor.fetchone()[0]
    
    if table_exists:
        print("✅ Table step03_compare_monthvspreviousmonth found!")
        
        # Get table schema
        cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'step03_compare_monthvspreviousmonth'
            ORDER BY ORDINAL_POSITION
        """)
        columns = cursor.fetchall()
        
        print(f"\n📊 Table Schema ({len(columns)} columns):")
        print("-" * 60)
        for col in columns:
            print(f"  {col[0]} ({col[1]})")
        
        # Get record count
        cursor.execute("SELECT COUNT(*) FROM step03_compare_monthvspreviousmonth")
        count = cursor.fetchone()[0]
        print(f"\n📈 Total Records: {count:,}")
        
        # Get sample data
        cursor.execute("SELECT TOP 3 * FROM step03_compare_monthvspreviousmonth")
        sample_data = cursor.fetchall()
        
        if sample_data:
            print(f"\n🔍 Sample Data (First 3 rows):")
            print("-" * 60)
            column_names = [desc[0] for desc in cursor.description]
            for i, row in enumerate(sample_data):
                print(f"Row {i+1}:")
                for j, value in enumerate(row):
                    if j < 10:  # Show first 10 columns only
                        print(f"  {column_names[j]}: {value}")
                print()
        
        # Test specific columns the API needs
        print("🧪 Testing API-required columns...")
        try:
            cursor.execute("""
                SELECT TOP 1 
                    current_trade_date,
                    symbol,
                    current_deliv_per,
                    delivery_increase_pct,
                    category,
                    index_name
                FROM step03_compare_monthvspreviousmonth
            """)
            test_row = cursor.fetchone()
            print("✅ All required columns accessible!")
            print(f"Sample row: {test_row}")
            
        except Exception as e:
            print(f"❌ Error accessing required columns: {e}")
        
    else:
        print("❌ Table step03_compare_monthvspreviousmonth not found!")
        
        # List all tables in master database
        cursor.execute("""
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
        """)
        tables = cursor.fetchall()
        
        print(f"\n📋 Available tables in master database ({len(tables)}):")
        for table in tables:
            print(f"  - {table[0]}")
    
    conn.close()
    
except Exception as e:
    print(f"❌ Database connection error: {e}")