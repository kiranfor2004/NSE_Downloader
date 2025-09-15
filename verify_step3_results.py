import pyodbc
import sys

def verify_step3_results():
    """Verify Step 3 results in the existing table"""
    try:
        connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=SRIKIRANREDDY\\SQLEX;DATABASE=master;Trusted_Connection=yes;'
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()

        print('📊 STEP 3 FEBRUARY vs MARCH ANALYSIS RESULTS')
        print('=' * 55)

        # Check our results
        cursor.execute("SELECT COUNT(*) FROM step03_compare_monthvspreviousmonth WHERE comparison_type = 'MAR_VS_FEB_2025'")
        total = cursor.fetchone()[0]
        print(f'📋 Total exceedance records inserted: {total:,}')

        # Top performers
        cursor.execute("""
        SELECT TOP 10 symbol, feb_trade_date, feb_deliv_qty, jan_deliv_qty, 
               delivery_increase_abs, delivery_increase_pct 
        FROM step03_compare_monthvspreviousmonth 
        WHERE comparison_type = 'MAR_VS_FEB_2025' AND delivery_increase_pct > 0
        ORDER BY delivery_increase_pct DESC
        """)

        print('\n🚀 TOP 10 DELIVERY INCREASES:')
        print(f"{'Symbol':<12} {'Date':<12} {'March Deliv':<12} {'Feb Peak':<12} {'Increase %':<12}")
        print('-' * 75)
        
        for row in cursor.fetchall():
            symbol, date, march_del, feb_del, abs_inc, pct_inc = row
            print(f'{symbol:<12} {date.strftime("%Y-%m-%d"):<12} {march_del:>11,} {feb_del:>11,} {pct_inc:>11.1f}%')

        # Most frequent exceedances
        cursor.execute("""
        SELECT symbol, COUNT(*) as exceedance_days,
               MAX(delivery_increase_pct) as max_increase,
               AVG(delivery_increase_pct) as avg_increase
        FROM step03_compare_monthvspreviousmonth 
        WHERE comparison_type = 'MAR_VS_FEB_2025' AND delivery_increase_pct > 0
        GROUP BY symbol
        HAVING COUNT(*) >= 5
        ORDER BY COUNT(*) DESC, MAX(delivery_increase_pct) DESC
        """)

        print(f'\n📈 SYMBOLS WITH FREQUENT EXCEEDANCES (5+ days):')
        print(f"{'Symbol':<12} {'Days':<6} {'Max %':<10} {'Avg %':<10}")
        print('-' * 45)
        
        frequent_performers = cursor.fetchall()
        for row in frequent_performers[:10]:  # Top 10
            symbol, days, max_pct, avg_pct = row
            print(f'{symbol:<12} {days:<6} {max_pct:>9.1f}% {avg_pct:>9.1f}%')

        connection.close()
        
        print(f'\n✅ SUCCESS! Step 3 analysis complete!')
        print(f'📊 {total:,} exceedance records stored in step03_compare_monthvspreviousmonth')
        print(f'🎯 Filter by comparison_type = "MAR_VS_FEB_2025" to access the data')
        print(f'📝 Table structure: March data in "feb_*" columns, February baselines in "jan_*" columns')

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    verify_step3_results()