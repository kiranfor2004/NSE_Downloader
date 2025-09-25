#!/usr/bin/env python3
"""Quick debug to check February 2025 data availability"""

import pyodbc

try:
    conn = pyodbc.connect(
        "Driver={ODBC Driver 17 for SQL Server};"
        "Server=SRIKIRANREDDY\\SQLEXPRESS;"
        "Database=master;"
        "Trusted_Connection=yes;"
    )
    cursor = conn.cursor()
    
    # Check step04_fo_udiff_daily for February dates
    cursor.execute("SELECT COUNT(*), MIN(Trade_date), MAX(Trade_date) FROM step04_fo_udiff_daily WHERE Trade_date LIKE '202502%'")
    step04_result = cursor.fetchone()
    print(f"step04_fo_udiff_daily: {step04_result[0]} records, from {step04_result[1]} to {step04_result[2]}")
    
    # Check step03 table
    cursor.execute("SELECT COUNT(*), MIN(current_trade_date), MAX(current_trade_date) FROM step03_compare_monthvspreviousmonth WHERE current_trade_date LIKE '202502%'")
    step03_result = cursor.fetchone()
    print(f"step03_compare_monthvspreviousmonth: {step03_result[0]} records, from {step03_result[1]} to {step03_result[2]}")
    
    # Get unique trading days from step04
    cursor.execute("SELECT DISTINCT Trade_date FROM step04_fo_udiff_daily WHERE Trade_date LIKE '202502%' ORDER BY Trade_date")
    trading_days = [row[0] for row in cursor.fetchall()]
    print(f"Trading days in step04: {len(trading_days)} days")
    print(f"First few days: {trading_days[:5]}")
    print(f"Last few days: {trading_days[-5:]}")
    
    conn.close()
    
except Exception as e:
    print(f"Error: {e}")