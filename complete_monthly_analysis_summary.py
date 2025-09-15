import pyodbc
import sys

def show_complete_analysis_summary():
    """Show summary of all three monthly comparison analyses"""
    try:
        connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=SRIKIRANREDDY\\SQLEX;DATABASE=master;Trusted_Connection=yes;'
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()

        print('📊 STEP 03 COMPLETE MONTHLY COMPARISON SUMMARY')
        print('=' * 60)

        # Count records by comparison type
        cursor.execute("""
        SELECT comparison_type, COUNT(*) as record_count
        FROM step03_compare_monthvspreviousmonth 
        GROUP BY comparison_type
        ORDER BY comparison_type
        """)
        
        print('📋 All Analysis Types in Table:')
        total_records = 0
        analyses = {}
        for row in cursor.fetchall():
            comp_type, count = row
            total_records += count
            analyses[comp_type] = count
            
            # Format the display name
            if comp_type == 'MAR_VS_FEB_2025':
                display_name = 'March vs February 2025'
            elif comp_type == 'APR_VS_MAR_2025':
                display_name = 'April vs March 2025'
            elif comp_type == 'MAY_VS_APR_2025':
                display_name = 'May vs April 2025'
            else:
                display_name = comp_type
                
            print(f'   {display_name}: {count:,} records')
        
        print(f'\n📊 Total exceedance records across all analyses: {total_records:,}')

        # Show detailed comparison statistics
        print('\n🔍 DETAILED COMPARISON STATISTICS:')
        
        analysis_details = [
            ('MAR_VS_FEB_2025', 'March vs February 2025'),
            ('APR_VS_MAR_2025', 'April vs March 2025'), 
            ('MAY_VS_APR_2025', 'May vs April 2025')
        ]
        
        for comp_type, display_name in analysis_details:
            if comp_type in analyses:
                cursor.execute("""
                SELECT COUNT(*) as records, 
                       AVG(delivery_increase_pct) as avg_increase,
                       MAX(delivery_increase_pct) as max_increase,
                       COUNT(DISTINCT symbol) as unique_symbols
                FROM step03_compare_monthvspreviousmonth 
                WHERE comparison_type = ? AND delivery_increase_pct > 0
                """, (comp_type,))
                
                result = cursor.fetchone()
                if result and result[0]:
                    records, avg_inc, max_inc, symbols = result
                    print(f'\n📈 {display_name}:')
                    print(f'   Records: {records:,}')
                    print(f'   Unique symbols: {symbols:,}')
                    print(f'   Average increase: {avg_inc:.1f}%')
                    print(f'   Maximum increase: {max_inc:,.1f}%')

        # Show top overall performers across all analyses
        print(f'\n🚀 TOP 10 OVERALL PERFORMERS (All Analyses):')
        cursor.execute("""
        SELECT TOP 10 symbol, current_trade_date, current_deliv_qty, previous_deliv_qty, 
               delivery_increase_pct, comparison_type
        FROM step03_compare_monthvspreviousmonth 
        WHERE delivery_increase_pct > 0
        ORDER BY delivery_increase_pct DESC
        """)
        
        print(f"{'Symbol':<12} {'Date':<12} {'Current':<12} {'Previous':<12} {'% Inc':<10} {'Analysis':<15}")
        print('-' * 90)
        
        for row in cursor.fetchall():
            symbol, date, current_del, prev_del, increase, comp_type = row
            # Format analysis name
            if comp_type == 'MAR_VS_FEB_2025':
                analysis_name = 'Mar vs Feb'
            elif comp_type == 'APR_VS_MAR_2025':
                analysis_name = 'Apr vs Mar'
            elif comp_type == 'MAY_VS_APR_2025':
                analysis_name = 'May vs Apr'
            else:
                analysis_name = comp_type[:10]
                
            print(f"{symbol:<12} {date.strftime('%Y-%m-%d'):<12} {current_del:>11,} {prev_del:>11,} {increase:>9.1f}% {analysis_name:<15}")

        # Show symbols that appear in multiple analyses (consistent performers)
        print(f'\n🎯 CONSISTENT HIGH PERFORMERS (Multiple Analyses):')
        cursor.execute("""
        SELECT symbol, COUNT(DISTINCT comparison_type) as analysis_count,
               AVG(delivery_increase_pct) as avg_increase,
               MAX(delivery_increase_pct) as max_increase
        FROM step03_compare_monthvspreviousmonth 
        WHERE delivery_increase_pct > 0
        GROUP BY symbol
        HAVING COUNT(DISTINCT comparison_type) >= 2
        ORDER BY COUNT(DISTINCT comparison_type) DESC, AVG(delivery_increase_pct) DESC
        """)
        
        print(f"{'Symbol':<12} {'Analyses':<10} {'Avg %':<10} {'Max %':<10}")
        print('-' * 45)
        
        consistent_performers = cursor.fetchall()
        if consistent_performers:
            for row in consistent_performers[:10]:  # Top 10
                symbol, count, avg_inc, max_inc = row
                print(f"{symbol:<12} {count:<10} {avg_inc:>9.1f}% {max_inc:>9.1f}%")
        else:
            print('   No symbols found in multiple analyses')

        # Show monthly trend analysis
        print(f'\n📈 MONTHLY TREND ANALYSIS:')
        print(f'   February → March: {analyses.get("MAR_VS_FEB_2025", 0):,} exceedances')
        print(f'   March → April:    {analyses.get("APR_VS_MAR_2025", 0):,} exceedances')
        print(f'   April → May:      {analyses.get("MAY_VS_APR_2025", 0):,} exceedances')
        
        # Calculate trend
        feb_mar = analyses.get("MAR_VS_FEB_2025", 0)
        apr_mar = analyses.get("APR_VS_MAR_2025", 0)
        may_apr = analyses.get("MAY_VS_APR_2025", 0)
        
        if feb_mar > 0 and apr_mar > 0:
            trend1 = ((apr_mar - feb_mar) / feb_mar) * 100
            print(f'   Trend Feb-Mar → Apr-Mar: {trend1:+.1f}%')
            
        if apr_mar > 0 and may_apr > 0:
            trend2 = ((may_apr - apr_mar) / apr_mar) * 100
            print(f'   Trend Apr-Mar → May-Apr: {trend2:+.1f}%')

        # Usage examples
        print(f'\n💡 USAGE EXAMPLES:')
        print(f'```sql')
        print(f'-- View specific analysis')
        print(f"SELECT * FROM step03_compare_monthvspreviousmonth WHERE comparison_type = 'MAY_VS_APR_2025'")
        print(f'')
        print(f'-- Compare symbol across all analyses')
        print(f"SELECT symbol, comparison_type, current_trade_date, delivery_increase_pct")
        print(f"FROM step03_compare_monthvspreviousmonth WHERE symbol = 'SPECIFIC_SYMBOL'")
        print(f"ORDER BY delivery_increase_pct DESC")
        print(f'')
        print(f'-- Find best performers in latest analysis')
        print(f"SELECT TOP 20 symbol, current_deliv_qty, previous_deliv_qty, delivery_increase_pct")
        print(f"FROM step03_compare_monthvspreviousmonth WHERE comparison_type = 'MAY_VS_APR_2025'")
        print(f"ORDER BY delivery_increase_pct DESC")
        print(f'```')

        connection.close()
        
        print(f'\n🎯 SUCCESS! All Monthly Comparisons Complete!')
        print(f'📊 Table: step03_compare_monthvspreviousmonth')
        print(f'✅ Three complete month-to-month analyses available')
        print(f'🔍 Filter by comparison_type to access specific analysis results')
        print(f'💡 Meaningful column names: current_ = analysis month, previous_ = baseline month')

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    show_complete_analysis_summary()