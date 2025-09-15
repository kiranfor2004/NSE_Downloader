"""
Analyze daily F&O record counts to understand inconsistencies
"""
import pyodbc

def analyze_daily_counts():
    conn_string = (
        'Driver={ODBC Driver 17 for SQL Server};'
        'Server=SRIKIRANREDDY\\SQLEXPRESS;'
        'Database=master;'
        'Trusted_Connection=yes;'
        'TrustServerCertificate=yes;'
    )
    
    try:
        conn = pyodbc.connect(conn_string)
        cur = conn.cursor()
        
        print("📊 DAILY F&O RECORD COUNT ANALYSIS")
        print("=" * 60)
        
        # Daily count breakdown
        print("\n📅 DAILY BREAKDOWN:")
        print("-" * 60)
        cur.execute("""
            SELECT 
                trade_date,
                COUNT(*) as daily_count,
                COUNT(DISTINCT symbol) as unique_symbols,
                SUM(CASE WHEN instrument LIKE 'FUT%' THEN 1 ELSE 0 END) as futures_count,
                SUM(CASE WHEN instrument LIKE 'OPT%' THEN 1 ELSE 0 END) as options_count
            FROM step04_fo_udiff_daily 
            WHERE trade_date LIKE '202502%'
            GROUP BY trade_date
            ORDER BY trade_date
        """)
        
        results = cur.fetchall()
        total_across_days = 0
        max_daily = 0
        min_daily = float('inf')
        
        for row in results:
            daily_count = row[1]
            print(f"{row[0]}: {daily_count:,} records ({row[3]:,} futures + {row[4]:,} options) - {row[2]} symbols")
            total_across_days += daily_count
            max_daily = max(max_daily, daily_count)
            min_daily = min(min_daily, daily_count)
        
        print(f"\n📈 CALCULATION SUMMARY:")
        print("-" * 60)
        print(f"Sum of daily counts: {total_across_days:,}")
        print(f"Trading days: {len(results)}")
        print(f"Average per day: {total_across_days // len(results):,}")
        print(f"Max daily count: {max_daily:,}")
        print(f"Min daily count: {min_daily:,}")
        
        # Total count verification
        cur.execute("SELECT COUNT(*) FROM step04_fo_udiff_daily WHERE trade_date LIKE '202502%'")
        total_records = cur.fetchone()[0]
        print(f"Total records in table: {total_records:,}")
        
        difference = abs(total_across_days - total_records)
        print(f"Difference: {difference:,}")
        
        if difference > 0:
            print("\n❌ INCONSISTENCY DETECTED!")
            print("Sum of daily counts ≠ Total table count")
        else:
            print("\n✅ CONSISTENT: Sum equals total")
        
        # Check for specific date
        print(f"\n🔍 SPECIFIC DATE ANALYSIS (20250203):")
        print("-" * 60)
        cur.execute("""
            SELECT 
                COUNT(*) as total_count,
                COUNT(DISTINCT symbol) as symbols,
                COUNT(DISTINCT instrument) as instruments,
                COUNT(DISTINCT expiry_date) as expiries,
                MIN(open_price) as min_price,
                MAX(open_price) as max_price,
                SUM(contracts_traded) as total_volume
            FROM step04_fo_udiff_daily 
            WHERE trade_date = '20250203'
        """)
        
        feb3_data = cur.fetchone()
        if feb3_data and feb3_data[0] > 0:
            print(f"Feb 3, 2025 Records: {feb3_data[0]:,}")
            print(f"Unique Symbols: {feb3_data[1]}")
            print(f"Instruments: {feb3_data[2]}")
            print(f"Expiry Dates: {feb3_data[3]}")
            print(f"Price Range: ₹{feb3_data[4]:.2f} - ₹{feb3_data[5]:,.2f}")
            print(f"Total Volume: {feb3_data[6]:,}")
            
            # Expected calculation
            print(f"\n🧮 EXPECTED RECORDS CALCULATION:")
            print("-" * 60)
            print("If Feb 3 has 34,305 records, then for 20 days:")
            expected_total = feb3_data[0] * 20
            print(f"Expected total: {feb3_data[0]:,} × 20 = {expected_total:,}")
            print(f"Actual total: {total_records:,}")
            print(f"Difference: {abs(expected_total - total_records):,}")
            
            if expected_total > total_records:
                print(f"⚠️ Missing {expected_total - total_records:,} records")
                print("Possible reasons:")
                print("  1. Some days have fewer contracts")
                print("  2. Weekend/holiday data missing")
                print("  3. Data generation inconsistency")
            else:
                print("✅ Total is reasonable")
        
        # Check if all 20 days have data
        print(f"\n📅 TRADING DAYS CHECK:")
        print("-" * 60)
        expected_dates = [
            '20250203', '20250204', '20250205', '20250206', '20250207',
            '20250210', '20250211', '20250212', '20250213', '20250214',
            '20250217', '20250218', '20250219', '20250220', '20250221',
            '20250224', '20250225', '20250226', '20250227', '20250228'
        ]
        
        cur.execute("SELECT DISTINCT trade_date FROM step04_fo_udiff_daily WHERE trade_date LIKE '202502%' ORDER BY trade_date")
        actual_dates = [row[0] for row in cur.fetchall()]
        
        print(f"Expected trading days: {len(expected_dates)}")
        print(f"Actual trading days: {len(actual_dates)}")
        
        missing_dates = set(expected_dates) - set(actual_dates)
        if missing_dates:
            print(f"❌ Missing dates: {sorted(missing_dates)}")
        else:
            print("✅ All expected dates present")
        
        extra_dates = set(actual_dates) - set(expected_dates)
        if extra_dates:
            print(f"⚠️ Extra dates: {sorted(extra_dates)}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    analyze_daily_counts()
