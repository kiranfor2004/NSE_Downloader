#!/usr/bin/env python3

import pyodbc
import json

def verify_complete_dataset_consistency():
    """Verify that all dates now have consistent UDiFF structure"""
    
    # Load database configuration
    with open('database_config.json', 'r') as f:
        config = json.load(f)

    try:
        conn_str = f"DRIVER={config['driver']};SERVER={config['server']};DATABASE={config['database']};Trusted_Connection=yes;"
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        print("🔍 COMPLETE DATASET CONSISTENCY VERIFICATION")
        print("="*60)
        
        # 1. Check all dates from Feb 3-15, 2025
        cursor.execute("""
        SELECT 
            trade_date,
            COUNT(*) as total_records,
            COUNT(CASE WHEN BizDt IS NOT NULL THEN 1 END) as bizdt_populated,
            COUNT(CASE WHEN FinInstrmId IS NOT NULL THEN 1 END) as instrmid_populated,
            COUNT(CASE WHEN ISIN IS NOT NULL THEN 1 END) as isin_populated,
            COUNT(CASE WHEN LastPric IS NOT NULL THEN 1 END) as lastpric_populated
        FROM step04_fo_udiff_daily 
        WHERE trade_date BETWEEN '20250203' AND '20250215'
        GROUP BY trade_date
        ORDER BY trade_date
        """)
        
        all_dates = cursor.fetchall()
        
        print(f"📅 Date-wise UDiFF Column Consistency:")
        print("-" * 60)
        print(f"{'Date':<12} {'Records':<8} {'BizDt':<6} {'InstrmId':<8} {'ISIN':<6} {'LastPric':<8} {'Status'}")
        print("-" * 60)
        
        all_consistent = True
        total_records_all = 0
        
        for date_data in all_dates:
            formatted_date = f"{date_data[0][:4]}-{date_data[0][4:6]}-{date_data[0][6:]}"
            total = date_data[1]
            bizdt_pct = (date_data[2] / total * 100) if total > 0 else 0
            instrmid_pct = (date_data[3] / total * 100) if total > 0 else 0
            isin_pct = (date_data[4] / total * 100) if total > 0 else 0
            lastpric_pct = (date_data[5] / total * 100) if total > 0 else 0
            
            is_consistent = (bizdt_pct == 100.0 and instrmid_pct == 100.0 and 
                           isin_pct == 100.0 and lastpric_pct == 100.0)
            
            status = "✅ Complete" if is_consistent else "❌ Missing"
            
            print(f"{formatted_date:<12} {total:<8,} {bizdt_pct:<6.1f} {instrmid_pct:<8.1f} {isin_pct:<6.1f} {lastpric_pct:<8.1f} {status}")
            
            total_records_all += total
            if not is_consistent:
                all_consistent = False
        
        print(f"\n📊 Overall Summary:")
        print(f"   Total dates checked: {len(all_dates)}")
        print(f"   Total records: {total_records_all:,}")
        print(f"   UDiFF consistency: {'✅ All dates complete' if all_consistent else '❌ Some dates incomplete'}")
        
        # 2. Check specific UDiFF columns across entire dataset
        print(f"\n🔍 UDiFF Column Population Analysis:")
        print("-" * 60)
        
        udiff_columns = [
            ('BizDt', 'Business Date'),
            ('Sgmt', 'Segment'), 
            ('Src', 'Source'),
            ('FinInstrmId', 'Financial Instrument ID'),
            ('ISIN', 'ISIN Code'),
            ('FinInstrmNm', 'Financial Instrument Name'),
            ('LastPric', 'Last Price'),
            ('UndrlygPric', 'Underlying Price'),
            ('TtlNbOfTxsExctd', 'Total Number of Transactions'),
            ('SsnId', 'Session ID')
        ]
        
        for col_name, col_desc in udiff_columns:
            cursor.execute(f"""
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN {col_name} IS NOT NULL THEN 1 END) as populated
            FROM step04_fo_udiff_daily 
            WHERE trade_date BETWEEN '20250203' AND '20250215'
            """)
            
            col_data = cursor.fetchone()
            population_pct = (col_data[1] / col_data[0] * 100) if col_data[0] > 0 else 0
            status = "✅" if population_pct == 100.0 else "❌"
            
            print(f"   {col_name:<20} {col_data[1]:>8,}/{col_data[0]:<8,} ({population_pct:>5.1f}%) {status}")
        
        # 3. Sample data verification
        print(f"\n📝 Sample Data Verification (Feb 3rd vs Feb 4th):")
        print("-" * 60)
        
        # Feb 3rd sample
        cursor.execute("""
        SELECT TOP 1 
            trade_date, symbol, BizDt, FinInstrmId, ISIN, LastPric
        FROM step04_fo_udiff_daily 
        WHERE trade_date = '20250203'
        ORDER BY id
        """)
        
        feb3_sample = cursor.fetchone()
        
        # Feb 4th sample  
        cursor.execute("""
        SELECT TOP 1 
            trade_date, symbol, BizDt, FinInstrmId, ISIN, LastPric
        FROM step04_fo_udiff_daily 
        WHERE trade_date = '20250204'
        ORDER BY id
        """)
        
        feb4_sample = cursor.fetchone()
        
        print(f"Feb 3rd Sample:")
        print(f"   Trade Date: {feb3_sample[0]}")
        print(f"   Symbol: {feb3_sample[1]}")
        print(f"   BizDt: {feb3_sample[2]}")
        print(f"   FinInstrmId: {feb3_sample[3]}")
        print(f"   ISIN: {feb3_sample[4]}")
        print(f"   LastPric: {feb3_sample[5]}")
        
        print(f"\nFeb 4th Sample:")
        print(f"   Trade Date: {feb4_sample[0]}")
        print(f"   Symbol: {feb4_sample[1]}")
        print(f"   BizDt: {feb4_sample[2]}")
        print(f"   FinInstrmId: {feb4_sample[3]}")
        print(f"   ISIN: {feb4_sample[4]}")
        print(f"   LastPric: {feb4_sample[5]}")
        
        # 4. Final database statistics
        cursor.execute("SELECT COUNT(*) FROM step04_fo_udiff_daily")
        total_db_records = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT trade_date) FROM step04_fo_udiff_daily")
        unique_dates = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT symbol) FROM step04_fo_udiff_daily")
        unique_symbols = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT instrument) FROM step04_fo_udiff_daily")
        unique_instruments = cursor.fetchone()[0]
        
        print(f"\n📊 Final Database Statistics:")
        print("-" * 60)
        print(f"   Total records: {total_db_records:,}")
        print(f"   Unique dates: {unique_dates}")
        print(f"   Unique symbols: {unique_symbols}")
        print(f"   Instrument types: {unique_instruments}")
        print(f"   Avg records/day: {total_db_records//unique_dates if unique_dates > 0 else 0:,}")
        
        conn.close()
        
        print(f"\n{'='*60}")
        if all_consistent:
            print(f"✅ VERIFICATION PASSED!")
            print(f"🎯 All dates (Feb 3-15) have complete UDiFF structure")
            print(f"📊 {total_records_all:,} records with 100% UDiFF compliance")
            print(f"🔄 Database ready for comprehensive F&O analysis")
        else:
            print(f"❌ VERIFICATION FAILED!")
            print(f"⚠️  Some dates missing UDiFF data")
            print(f"🔧 Manual correction may be required")
        print(f"{'='*60}")
        
        return all_consistent
        
    except Exception as e:
        print(f"❌ Error during verification: {e}")
        return False

if __name__ == "__main__":
    verify_complete_dataset_consistency()
