import pyodbc
import sys

def show_comparison_summary():
    """Show summary of both February vs March and April vs March analyses"""
    try:
        connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=SRIKIRANREDDY\\SQLEX;DATABASE=master;Trusted_Connection=yes;'
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()

        print('📊 STEP 03 COMPLETE ANALYSIS SUMMARY')
        print('=' * 50)

        # Count records by comparison type
        cursor.execute("""
        SELECT comparison_type, COUNT(*) as record_count
        FROM step03_compare_monthvspreviousmonth 
        GROUP BY comparison_type
        ORDER BY comparison_type
        """)
        
        print('📋 Analysis Types in Table:')
        total_records = 0
        for row in cursor.fetchall():
            comp_type, count = row
            total_records += count
            print(f'   {comp_type}: {count:,} records')
        
        print(f'\n📊 Total exceedance records: {total_records:,}')

        # Show comparison between Feb-Mar and Apr-Mar analyses
        print('\n🔍 COMPARISON OVERVIEW:')
        
        # February vs March summary
        cursor.execute("""
        SELECT COUNT(*) as records, 
               AVG(delivery_increase_pct) as avg_increase,
               MAX(delivery_increase_pct) as max_increase
        FROM step03_compare_monthvspreviousmonth 
        WHERE comparison_type = 'MAR_VS_FEB_2025' AND delivery_increase_pct > 0
        """)
        feb_mar = cursor.fetchone()
        
        # April vs March summary  
        cursor.execute("""
        SELECT COUNT(*) as records, 
               AVG(delivery_increase_pct) as avg_increase,
               MAX(delivery_increase_pct) as max_increase
        FROM step03_compare_monthvspreviousmonth 
        WHERE comparison_type = 'APR_VS_MAR_2025' AND delivery_increase_pct > 0
        """)
        apr_mar = cursor.fetchone()

        print(f'\n📈 FEBRUARY vs MARCH 2025:')
        if feb_mar and feb_mar[0]:
            print(f'   Records: {feb_mar[0]:,}')
            print(f'   Average increase: {feb_mar[1]:.1f}%')
            print(f'   Maximum increase: {feb_mar[2]:,.1f}%')
        else:
            print('   No records found')

        print(f'\n📈 APRIL vs MARCH 2025:')
        if apr_mar and apr_mar[0]:
            print(f'   Records: {apr_mar[0]:,}')
            print(f'   Average increase: {apr_mar[1]:.1f}%')
            print(f'   Maximum increase: {apr_mar[2]:,.1f}%')
        else:
            print('   No records found')

        # Show top performers across both analyses
        print(f'\n🚀 TOP 5 OVERALL PERFORMERS:')
        cursor.execute("""
        SELECT TOP 5 symbol, current_trade_date, current_deliv_qty, previous_deliv_qty, 
               delivery_increase_pct, comparison_type
        FROM step03_compare_monthvspreviousmonth 
        WHERE delivery_increase_pct > 0
        ORDER BY delivery_increase_pct DESC
        """)
        
        print(f"{'Symbol':<12} {'Date':<12} {'Current':<12} {'Previous':<12} {'% Inc':<10} {'Analysis':<15}")
        print('-' * 85)
        
        for row in cursor.fetchall():
            symbol, date, current_del, prev_del, increase, comp_type = row
            analysis_name = 'Mar vs Feb' if comp_type == 'MAR_VS_FEB_2025' else 'Apr vs Mar'
            print(f"{symbol:<12} {date.strftime('%Y-%m-%d'):<12} {current_del:>11,} {prev_del:>11,} {increase:>9.1f}% {analysis_name:<15}")

        # Show meaningful column usage
        print(f'\n💡 MEANINGFUL COLUMN STRUCTURE:')
        print(f'   ✅ current_trade_date = Current analysis month trading date')
        print(f'   ✅ current_deliv_qty = Current analysis month delivery quantity')
        print(f'   ✅ current_* = Current analysis month data')
        print(f'   ✅ previous_baseline_date = Previous month baseline date')
        print(f'   ✅ previous_deliv_qty = Previous month peak delivery')
        print(f'   ✅ previous_* = Previous month baseline data')
        print(f'   ✅ comparison_type = Identifies which analysis (MAR_VS_FEB_2025 or APR_VS_MAR_2025)')

        connection.close()
        
        print(f'\n🎯 SUCCESS! Both analyses completed with meaningful column names!')
        print(f'📊 Table: step03_compare_monthvspreviousmonth')
        print(f'🔍 Filter by comparison_type to access specific analysis results')

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    show_comparison_summary()