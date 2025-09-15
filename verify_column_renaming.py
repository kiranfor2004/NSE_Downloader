import pyodbc
import sys

def verify_column_renaming():
    """Verify that column renaming was successful and show new structure"""
    try:
        connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=SRIKIRANREDDY\\SQLEX;DATABASE=master;Trusted_Connection=yes;'
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()

        print('🔍 VERIFYING COLUMN RENAMING RESULTS')
        print('=' * 50)

        # Check for new column names
        cursor.execute("""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'step03_compare_monthvspreviousmonth'
        AND (COLUMN_NAME LIKE 'current_%' OR COLUMN_NAME LIKE 'previous_%')
        ORDER BY COLUMN_NAME
        """)
        
        new_columns = [row[0] for row in cursor.fetchall()]
        
        # Check for old column names
        cursor.execute("""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'step03_compare_monthvspreviousmonth'
        AND (COLUMN_NAME LIKE 'feb_%' OR COLUMN_NAME LIKE 'jan_%')
        ORDER BY COLUMN_NAME
        """)
        
        old_columns = [row[0] for row in cursor.fetchall()]

        print(f"📊 NEW COLUMN NAMES ({len(new_columns)}):")
        current_cols = [col for col in new_columns if col.startswith('current_')]
        previous_cols = [col for col in new_columns if col.startswith('previous_')]
        
        print(f"\n   current_ columns ({len(current_cols)}) - March 2025 data:")
        for col in sorted(current_cols):
            print(f"      {col}")
            
        print(f"\n   previous_ columns ({len(previous_cols)}) - February 2025 baselines:")
        for col in sorted(previous_cols):
            print(f"      {col}")

        if old_columns:
            print(f"\n⚠️  OLD COLUMN NAMES STILL EXIST ({len(old_columns)}):")
            for col in sorted(old_columns):
                print(f"      {col}")
        else:
            print(f"\n✅ No old column names found - renaming was successful!")

        # Test data access with new column names
        if new_columns:
            print(f"\n🧪 TESTING DATA ACCESS WITH NEW COLUMN NAMES:")
            cursor.execute("""
            SELECT COUNT(*) 
            FROM step03_compare_monthvspreviousmonth 
            WHERE comparison_type = 'MAR_VS_FEB_2025'
            """)
            record_count = cursor.fetchone()[0]
            print(f"   📊 Found {record_count:,} analysis records")
            
            if record_count > 0:
                cursor.execute("""
                SELECT TOP 3 symbol, current_trade_date, current_deliv_qty, previous_deliv_qty, delivery_increase_pct
                FROM step03_compare_monthvspreviousmonth 
                WHERE comparison_type = 'MAR_VS_FEB_2025' AND delivery_increase_pct > 0
                ORDER BY delivery_increase_pct DESC
                """)
                
                print(f"   🚀 Top 3 delivery increases with new column names:")
                for row in cursor.fetchall():
                    symbol, date, current_del, prev_del, increase = row
                    print(f"      {symbol}: {date.strftime('%Y-%m-%d')} - {current_del:,} vs {prev_del:,} (+{increase:.1f}%)")

        connection.close()
        
        if new_columns and not old_columns:
            print(f"\n🎉 SUCCESS! Column renaming completed successfully!")
            print(f"✅ feb_ columns → current_ columns")
            print(f"✅ jan_ columns → previous_ columns")
            print(f"🎯 Table is ready for use with meaningful column names!")
        elif new_columns and old_columns:
            print(f"\n⚠️  PARTIAL SUCCESS: New columns exist but old ones still present")
            print(f"💡 May need to complete renaming process")
        else:
            print(f"\n❌ Column renaming may not have completed successfully")

    except Exception as e:
        print(f"❌ Error: {e}")

def show_column_mapping():
    """Show the mapping between old and new column names"""
    print('\n📋 COLUMN MAPPING REFERENCE:')
    print('=' * 50)
    
    mappings = [
        ('OLD COLUMN NAME', 'NEW COLUMN NAME', 'PURPOSE'),
        ('-' * 20, '-' * 20, '-' * 30),
        ('feb_trade_date', 'current_trade_date', 'March 2025 trading date'),
        ('feb_prev_close', 'current_prev_close', 'March previous close price'),
        ('feb_open_price', 'current_open_price', 'March opening price'),
        ('feb_high_price', 'current_high_price', 'March high price'),
        ('feb_low_price', 'current_low_price', 'March low price'),
        ('feb_last_price', 'current_last_price', 'March last price'),
        ('feb_close_price', 'current_close_price', 'March closing price'),
        ('feb_avg_price', 'current_avg_price', 'March average price'),
        ('feb_ttl_trd_qnty', 'current_ttl_trd_qnty', 'March total trade quantity'),
        ('feb_turnover_lacs', 'current_turnover_lacs', 'March turnover in lacs'),
        ('feb_no_of_trades', 'current_no_of_trades', 'March number of trades'),
        ('feb_deliv_qty', 'current_deliv_qty', 'March delivery quantity'),
        ('feb_deliv_per', 'current_deliv_per', 'March delivery percentage'),
        ('feb_source_file', 'current_source_file', 'March source file'),
        ('', '', ''),
        ('jan_baseline_date', 'previous_baseline_date', 'February baseline date'),
        ('jan_prev_close', 'previous_prev_close', 'February baseline price'),
        ('jan_open_price', 'previous_open_price', 'February baseline price'),
        ('jan_high_price', 'previous_high_price', 'February baseline price'),
        ('jan_low_price', 'previous_low_price', 'February baseline price'),
        ('jan_last_price', 'previous_last_price', 'February baseline price'),
        ('jan_close_price', 'previous_close_price', 'February baseline price'),
        ('jan_avg_price', 'previous_avg_price', 'February baseline price'),
        ('jan_ttl_trd_qnty', 'previous_ttl_trd_qnty', 'February peak volume'),
        ('jan_turnover_lacs', 'previous_turnover_lacs', 'February baseline turnover'),
        ('jan_no_of_trades', 'previous_no_of_trades', 'February baseline trades'),
        ('jan_deliv_qty', 'previous_deliv_qty', 'February peak delivery'),
        ('jan_deliv_per', 'previous_deliv_per', 'February baseline deliv %'),
        ('jan_source_file', 'previous_source_file', 'February source file'),
    ]
    
    for old_col, new_col, purpose in mappings:
        print(f"{old_col:<22} {new_col:<22} {purpose}")

if __name__ == "__main__":
    verify_column_renaming()
    show_column_mapping()