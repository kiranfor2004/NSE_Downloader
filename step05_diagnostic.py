#!/usr/bin/env python3
"""
Quick diagnostic to check the current state and optimize the process
"""

import pyodbc
import pandas as pd
import time

def check_current_status():
    """Check current processing status"""
    try:
        conn = pyodbc.connect(
            "Driver={ODBC Driver 17 for SQL Server};"
            "Server=SRIKIRANREDDY\\SQLEXPRESS;"
            "Database=master;"
            "Trusted_Connection=yes;"
        )
        
        # Check if table exists and current record count
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT COUNT(*) FROM Step05_strikepriceAnalysisderived")
            current_count = cursor.fetchone()[0]
            print(f"Current records in table: {current_count}")
            
            # Check distinct dates processed
            cursor.execute("SELECT DISTINCT Current_trade_date FROM Step05_strikepriceAnalysisderived ORDER BY Current_trade_date")
            processed_dates = [row[0] for row in cursor.fetchall()]
            print(f"Processed dates: {processed_dates}")
            
            # Check symbols per date
            cursor.execute("""
                SELECT Current_trade_date, COUNT(DISTINCT Symbol) as symbol_count, COUNT(*) as record_count
                FROM Step05_strikepriceAnalysisderived 
                GROUP BY Current_trade_date 
                ORDER BY Current_trade_date
            """)
            
            print("\nBreakdown by date:")
            for row in cursor.fetchall():
                print(f"  {row[0]}: {row[1]} symbols, {row[2]} records")
                
        except Exception as e:
            print(f"Table doesn't exist or error: {e}")
        
        # Check data volume in source table
        cursor.execute("SELECT COUNT(*) FROM step04_fo_udiff_daily WHERE Trade_date LIKE '202502%'")
        total_fo_records = cursor.fetchone()[0]
        print(f"\nTotal F&O records in Feb 2025: {total_fo_records}")
        
        # Check symbols available
        cursor.execute("SELECT COUNT(DISTINCT Symbol) FROM step04_fo_udiff_daily WHERE Trade_date LIKE '202502%'")
        total_symbols = cursor.fetchone()[0]
        print(f"Total symbols in Feb 2025: {total_symbols}")
        
        # Check trading days
        cursor.execute("SELECT COUNT(DISTINCT Trade_date) FROM step04_fo_udiff_daily WHERE Trade_date LIKE '202502%'")
        total_days = cursor.fetchone()[0]
        print(f"Total trading days in Feb 2025: {total_days}")
        
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")

def estimate_processing_time():
    """Estimate how long full processing might take"""
    try:
        conn = pyodbc.connect(
            "Driver={ODBC Driver 17 for SQL Server};"
            "Server=SRIKIRANREDDY\\SQLEXPRESS;"
            "Database=master;"
            "Trusted_Connection=yes;"
        )
        
        # Test processing time for one symbol on one day
        start_time = time.time()
        
        query = """
        SELECT TOP 100
            ID,
            Trade_date,
            Expiry_date,
            Strike_price,
            Option_type,
            Close_price
        FROM step04_fo_udiff_daily 
        WHERE Symbol = 'NIFTY' AND Trade_date = '20250203'
          AND Strike_price IS NOT NULL 
          AND Close_price IS NOT NULL
          AND Strike_price > 0
        ORDER BY Option_type, Strike_price
        """
        
        df = pd.read_sql(query, conn)
        end_time = time.time()
        
        query_time = end_time - start_time
        print(f"\nQuery time for 100 records: {query_time:.2f} seconds")
        
        # Estimate total time
        total_symbols = 233  # From previous check
        total_days = 19
        estimated_queries = total_symbols * total_days
        estimated_time_minutes = (estimated_queries * query_time) / 60
        
        print(f"Estimated total processing time: {estimated_time_minutes:.1f} minutes")
        print(f"That's about {estimated_time_minutes/60:.1f} hours")
        
        conn.close()
        
    except Exception as e:
        print(f"Error in estimation: {e}")

if __name__ == "__main__":
    print("=== CURRENT STATUS CHECK ===")
    check_current_status()
    
    print("\n=== PROCESSING TIME ESTIMATION ===")
    estimate_processing_time()
    
    print("\n=== OPTIMIZATION SUGGESTIONS ===")
    print("1. Process in smaller batches (10-20 symbols per day)")
    print("2. Add more filtering to reduce F&O records per symbol")
    print("3. Use bulk insert operations")
    print("4. Process only most active symbols first")
    print("5. Add progress checkpoints to resume from interruptions")