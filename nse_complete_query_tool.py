#!/usr/bin/env python3
"""
Complete NSE Data Query Tool
View and query the entire NSE dataset loaded in Step 01
"""

import pyodbc
import json
import pandas as pd
from datetime import datetime

class NSEDataQueryTool:
    def __init__(self):
        """Initialize database connection"""
        with open('database_config.json', 'r') as f:
            config = json.load(f)
        
        self.conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={config['server']};DATABASE={config['database']};Trusted_Connection=yes;"
        
    def run_query(self, query, description=""):
        """Execute a query and return results"""
        try:
            conn = pyodbc.connect(self.conn_str)
            df = pd.read_sql(query, conn)
            conn.close()
            
            if description:
                print(f"🔍 {description}")
                print("=" * 60)
            
            return df
        except Exception as e:
            print(f"❌ Error executing query: {e}")
            return None
    
    def show_data_overview(self):
        """Show complete data overview"""
        print("📊 STEP 01: COMPLETE NSE DATA OVERVIEW")
        print("=" * 70)
        
        # Basic stats
        query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT symbol) as unique_symbols,
            COUNT(DISTINCT trade_date) as trading_days,
            MIN(trade_date) as first_date,
            MAX(trade_date) as last_date
        FROM step01_equity_daily
        """
        df = self.run_query(query, "Overall Statistics")
        if df is not None:
            row = df.iloc[0]
            print(f"📈 Total Records: {row['total_records']:,}")
            print(f"📈 Unique Symbols: {row['unique_symbols']:,}")
            print(f"📈 Trading Days: {row['trading_days']}")
            print(f"📈 Date Range: {row['first_date']} to {row['last_date']}")
        
        print("\n" + "=" * 70)
        
        # All series breakdown
        query = """
        SELECT 
            series,
            COUNT(*) as record_count,
            COUNT(DISTINCT symbol) as unique_symbols,
            ROUND(AVG(CAST(ttl_trd_qnty as FLOAT)), 0) as avg_volume,
            ROUND(AVG(CAST(turnover_lacs as FLOAT)), 2) as avg_turnover_lacs
        FROM step01_equity_daily 
        GROUP BY series 
        ORDER BY record_count DESC
        """
        df = self.run_query(query, "All Market Segments/Series")
        if df is not None:
            print(df.to_string(index=False))
        
        print("\n" + "=" * 70)
        
        # Month-wise data
        query = """
        SELECT 
            DATENAME(month, trade_date) as month_name,
            COUNT(DISTINCT trade_date) as trading_days,
            COUNT(*) as total_records,
            COUNT(DISTINCT symbol) as unique_symbols
        FROM step01_equity_daily 
        GROUP BY YEAR(trade_date), MONTH(trade_date), DATENAME(month, trade_date)
        ORDER BY YEAR(trade_date), MONTH(trade_date)
        """
        df = self.run_query(query, "Month-wise Breakdown")
        if df is not None:
            print(df.to_string(index=False))
    
    def show_sample_data(self, limit=10):
        """Show sample records"""
        query = f"""
        SELECT TOP {limit}
            symbol, series, trade_date, 
            open_price, high_price, low_price, close_price,
            ttl_trd_qnty as volume, turnover_lacs,
            deliv_qty, deliv_per
        FROM step01_equity_daily 
        ORDER BY trade_date DESC, turnover_lacs DESC
        """
        df = self.run_query(query, f"Sample Data (Top {limit} by Recent Date & Turnover)")
        if df is not None:
            print(df.to_string(index=False))
    
    def query_by_symbol(self, symbol):
        """Get all data for a specific symbol"""
        query = f"""
        SELECT 
            trade_date, series, open_price, high_price, low_price, close_price,
            ttl_trd_qnty as volume, turnover_lacs, deliv_qty, deliv_per
        FROM step01_equity_daily 
        WHERE symbol = '{symbol}'
        ORDER BY trade_date DESC
        """
        df = self.run_query(query, f"All Data for Symbol: {symbol}")
        if df is not None and len(df) > 0:
            print(f"📊 Total records for {symbol}: {len(df)}")
            print(df.to_string(index=False))
            return df
        else:
            print(f"❌ No data found for symbol: {symbol}")
            return None
    
    def query_by_date(self, date):
        """Get all data for a specific date"""
        query = f"""
        SELECT TOP 50
            symbol, series, open_price, high_price, low_price, close_price,
            ttl_trd_qnty as volume, turnover_lacs
        FROM step01_equity_daily 
        WHERE trade_date = '{date}'
        ORDER BY turnover_lacs DESC
        """
        df = self.run_query(query, f"Top 50 Stocks by Turnover on {date}")
        if df is not None and len(df) > 0:
            total_query = f"SELECT COUNT(*) as total FROM step01_equity_daily WHERE trade_date = '{date}'"
            total_df = self.run_query(total_query)
            if total_df is not None:
                print(f"📊 Total stocks traded on {date}: {total_df.iloc[0]['total']:,}")
            print(df.to_string(index=False))
            return df
        else:
            print(f"❌ No data found for date: {date}")
            return None
    
    def query_top_by_volume(self, date=None, limit=20):
        """Get top stocks by volume"""
        date_filter = f"WHERE trade_date = '{date}'" if date else ""
        order_desc = "trade_date DESC, " if not date else ""
        
        query = f"""
        SELECT TOP {limit}
            symbol, series, trade_date, ttl_trd_qnty as volume, 
            turnover_lacs, close_price
        FROM step01_equity_daily 
        {date_filter}
        ORDER BY {order_desc}ttl_trd_qnty DESC
        """
        
        title = f"Top {limit} Stocks by Volume" + (f" on {date}" if date else " (All Time)")
        df = self.run_query(query, title)
        if df is not None:
            print(df.to_string(index=False))
            return df
    
    def custom_query(self, query):
        """Execute custom SQL query"""
        df = self.run_query(query, "Custom Query Results")
        if df is not None:
            print(df.to_string(index=False))
            return df

def main():
    """Interactive query interface"""
    tool = NSEDataQueryTool()
    
    print("🚀 NSE Complete Data Query Tool")
    print("=" * 50)
    print("Available Commands:")
    print("1. overview - Complete data overview")
    print("2. sample [limit] - Show sample data")
    print("3. symbol <SYMBOL> - Query specific symbol")
    print("4. date <YYYY-MM-DD> - Query specific date")
    print("5. volume [date] [limit] - Top stocks by volume")
    print("6. query <SQL> - Custom SQL query")
    print("7. quit - Exit")
    print("=" * 50)
    
    while True:
        try:
            command = input("\n🔍 Enter command: ").strip().split()
            
            if not command:
                continue
                
            cmd = command[0].lower()
            
            if cmd == 'quit':
                break
            elif cmd == 'overview':
                tool.show_data_overview()
            elif cmd == 'sample':
                limit = int(command[1]) if len(command) > 1 else 10
                tool.show_sample_data(limit)
            elif cmd == 'symbol':
                if len(command) > 1:
                    tool.query_by_symbol(command[1])
                else:
                    print("❌ Please provide symbol name")
            elif cmd == 'date':
                if len(command) > 1:
                    tool.query_by_date(command[1])
                else:
                    print("❌ Please provide date (YYYY-MM-DD)")
            elif cmd == 'volume':
                date = command[1] if len(command) > 1 else None
                limit = int(command[2]) if len(command) > 2 else 20
                tool.query_top_by_volume(date, limit)
            elif cmd == 'query':
                if len(command) > 1:
                    sql = ' '.join(command[1:])
                    tool.custom_query(sql)
                else:
                    print("❌ Please provide SQL query")
            else:
                print("❌ Unknown command")
                
        except KeyboardInterrupt:
            print("\n👋 Goodbye!")
            break
        except Exception as e:
            print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
