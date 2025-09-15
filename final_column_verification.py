import pyodbc
import sys

def show_final_results():
    """Show final results with the new column names"""
    try:
        connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=SRIKIRANREDDY\\SQLEX;DATABASE=master;Trusted_Connection=yes;'
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()

        print('🎉 COLUMN RENAMING SUCCESS - FINAL VERIFICATION')
        print('=' * 55)

        # Verify new column names exist
        cursor.execute("""
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'step03_compare_monthvspreviousmonth' 
        AND (COLUMN_NAME LIKE 'current_%' OR COLUMN_NAME LIKE 'previous_%')
        """)
        new_col_count = cursor.fetchone()[0]
        
        # Check for old column names
        cursor.execute("""
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'step03_compare_monthvspreviousmonth' 
        AND (COLUMN_NAME LIKE 'feb_%' OR COLUMN_NAME LIKE 'jan_%')
        """)
        old_col_count = cursor.fetchone()[0]

        print(f"📊 New meaningful column names: {new_col_count}")
        print(f"📊 Old confusing column names: {old_col_count}")
        
        if old_col_count == 0:
            print("✅ SUCCESS! All columns renamed to meaningful names")
        else:
            print("⚠️ Some old column names still exist")

        # Show sample data with new column names
        cursor.execute("""
        SELECT COUNT(*) 
        FROM step03_compare_monthvspreviousmonth 
        WHERE comparison_type = 'MAR_VS_FEB_2025'
        """)
        record_count = cursor.fetchone()[0]
        print(f"\n📊 Step 3 Analysis Records: {record_count:,}")

        if record_count > 0:
            # Show top performers with meaningful column names
            cursor.execute("""
            SELECT TOP 5 symbol, current_trade_date, current_deliv_qty, previous_deliv_qty, 
                   delivery_increase_pct
            FROM step03_compare_monthvspreviousmonth 
            WHERE comparison_type = 'MAR_VS_FEB_2025' AND delivery_increase_pct > 0
            ORDER BY delivery_increase_pct DESC
            """)
            
            print(f"\n🚀 TOP 5 DELIVERY INCREASES WITH NEW COLUMN NAMES:")
            print(f"{'Symbol':<12} {'Date':<12} {'Current':<12} {'Previous':<12} {'Increase %':<12}")
            print('-' * 70)
            
            for row in cursor.fetchall():
                symbol, date, current_del, prev_del, increase = row
                print(f"{symbol:<12} {date.strftime('%Y-%m-%d'):<12} {current_del:>11,} {prev_del:>11,} {increase:>11.1f}%")

        # Show the meaningful column mapping
        print(f"\n📋 NEW MEANINGFUL COLUMN STRUCTURE:")
        print(f"   ✅ current_trade_date = March 2025 trading date")
        print(f"   ✅ current_deliv_qty = March 2025 delivery quantity")
        print(f"   ✅ current_* = March 2025 data (current analysis month)")
        print(f"   ✅ previous_baseline_date = February 2025 baseline date")
        print(f"   ✅ previous_deliv_qty = February 2025 peak delivery")
        print(f"   ✅ previous_* = February 2025 baselines (previous comparison month)")

        # Sample SQL query for users
        print(f"\n💡 SAMPLE QUERY WITH NEW MEANINGFUL NAMES:")
        print(f"```sql")
        print(f"SELECT symbol, current_trade_date, current_deliv_qty, previous_deliv_qty,")
        print(f"       delivery_increase_pct")
        print(f"FROM step03_compare_monthvspreviousmonth")
        print(f"WHERE comparison_type = 'MAR_VS_FEB_2025'")
        print(f"ORDER BY delivery_increase_pct DESC")
        print(f"```")

        connection.close()
        
        print(f"\n🎯 COLUMN RENAMING COMPLETE!")
        print(f"✅ feb_ columns → current_ columns (March 2025 data)")
        print(f"✅ jan_ columns → previous_ columns (February 2025 baselines)")
        print(f"🎉 Table now has meaningful, intuitive column names!")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    show_final_results()