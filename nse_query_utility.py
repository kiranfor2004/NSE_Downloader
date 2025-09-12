#!/usr/bin/env python3
"""
🔍 NSE Database Query Utility
Easy SQL queries and data analysis from PostgreSQL
"""

import pandas as pd
from sqlalchemy import text
from nse_database_setup import NSEDatabaseManager
import matplotlib.pyplot as plt
import seaborn as sns

class NSEQueryUtility:
    def __init__(self):
        self.db = NSEDatabaseManager()
        self.connected = False
    
    def connect(self):
        """Connect to database"""
        if self.db.connect():
            self.connected = True
            print("✅ Connected to NSE database")
            return True
        return False
    
    def get_symbol_data(self, symbol, month_year=None):
        """Get all data for a specific symbol"""
        sql = "SELECT * FROM nse_raw_data WHERE symbol = :symbol"
        params = {'symbol': symbol}
        
        if month_year:
            sql += " AND month_year = :month"
            params['month'] = month_year
        
        sql += " ORDER BY date"
        
        df = pd.read_sql(sql, self.db.engine, params=params)
        return df
    
    def get_top_volume_symbols(self, month_year, limit=10):
        """Get top symbols by trading volume for a month"""
        sql = text("""
            SELECT symbol, MAX(ttl_trd_qnty) as max_volume,
                   MAX(ttl_trd_val) as max_value,
                   COUNT(*) as trading_days
            FROM nse_raw_data 
            WHERE month_year = :month
            GROUP BY symbol 
            ORDER BY max_volume DESC 
            LIMIT :limit
        """)
        
        df = pd.read_sql(sql, self.db.engine, params={'month': month_year, 'limit': limit})
        return df
    
    def get_top_delivery_symbols(self, month_year, limit=10):
        """Get top symbols by delivery quantity for a month"""
        sql = text("""
            SELECT symbol, MAX(deliv_qty) as max_delivery,
                   AVG(deliv_per) as avg_delivery_pct,
                   COUNT(*) as trading_days
            FROM nse_raw_data 
            WHERE month_year = :month AND deliv_qty IS NOT NULL
            GROUP BY symbol 
            ORDER BY max_delivery DESC 
            LIMIT :limit
        """)
        
        df = pd.read_sql(sql, self.db.engine, params={'month': month_year, 'limit': limit})
        return df
    
    def compare_months(self, symbol, month1, month2):
        """Compare a symbol's performance between two months"""
        sql = text("""
            WITH month_stats AS (
                SELECT month_year,
                       MAX(ttl_trd_qnty) as max_volume,
                       MAX(deliv_qty) as max_delivery,
                       AVG(close_price) as avg_price,
                       MAX(high_price) as highest_price,
                       MIN(low_price) as lowest_price
                FROM nse_raw_data 
                WHERE symbol = :symbol 
                  AND month_year IN (:month1, :month2)
                GROUP BY month_year
            )
            SELECT * FROM month_stats ORDER BY month_year
        """)
        
        df = pd.read_sql(sql, self.db.engine, params={
            'symbol': symbol, 'month1': month1, 'month2': month2
        })
        return df
    
    def get_market_summary(self, month_year):
        """Get overall market summary for a month"""
        sql = text("""
            SELECT 
                COUNT(DISTINCT symbol) as total_symbols,
                COUNT(DISTINCT date) as trading_days,
                SUM(ttl_trd_qnty) as total_volume,
                SUM(ttl_trd_val) as total_value,
                SUM(deliv_qty) as total_delivery,
                AVG(deliv_per) as avg_delivery_pct,
                MIN(date) as start_date,
                MAX(date) as end_date
            FROM nse_raw_data 
            WHERE month_year = :month
        """)
        
        result = self.db.connection.execute(sql, {'month': month_year}).fetchone()
        return dict(result._mapping) if result else {}
    
    def find_growing_symbols(self, months_list, min_growth_pct=50):
        """Find symbols showing consistent growth across months"""
        if len(months_list) < 2:
            return pd.DataFrame()
        
        # Create month comparison pairs
        comparisons = []
        for i in range(len(months_list) - 1):
            month1, month2 = months_list[i], months_list[i + 1]
            
            sql = text("""
                WITH m1 AS (
                    SELECT symbol, MAX(ttl_trd_qnty) as vol1, MAX(deliv_qty) as del1
                    FROM nse_raw_data WHERE month_year = :month1
                    GROUP BY symbol
                ),
                m2 AS (
                    SELECT symbol, MAX(ttl_trd_qnty) as vol2, MAX(deliv_qty) as del2
                    FROM nse_raw_data WHERE month_year = :month2
                    GROUP BY symbol
                )
                SELECT m1.symbol,
                       m1.vol1, m2.vol2,
                       CASE WHEN m1.vol1 > 0 THEN 
                            ROUND(((m2.vol2 - m1.vol1) * 100.0 / m1.vol1), 2)
                       END as volume_growth_pct,
                       m1.del1, m2.del2,
                       CASE WHEN m1.del1 > 0 THEN 
                            ROUND(((m2.del2 - m1.del1) * 100.0 / m1.del1), 2)
                       END as delivery_growth_pct
                FROM m1 
                JOIN m2 ON m1.symbol = m2.symbol
                WHERE m2.vol2 > m1.vol1 
                  AND ((m2.vol2 - m1.vol1) * 100.0 / m1.vol1) >= :min_growth
                ORDER BY volume_growth_pct DESC
            """)
            
            df = pd.read_sql(sql, self.db.engine, params={
                'month1': month1, 'month2': month2, 'min_growth': min_growth_pct
            })
            
            df['comparison'] = f"{month1} → {month2}"
            comparisons.append(df)
        
        if comparisons:
            return pd.concat(comparisons, ignore_index=True)
        return pd.DataFrame()
    
    def export_analysis_to_excel(self, analysis_data, filename):
        """Export analysis results to Excel"""
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            for sheet_name, df in analysis_data.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        print(f"✅ Analysis exported to: {filename}")
    
    def create_sample_queries(self):
        """Show sample query examples"""
        samples = {
            "Top 10 Volume Symbols (January)": 
                "SELECT symbol, MAX(ttl_trd_qnty) as max_vol FROM nse_raw_data WHERE month_year='January_2025' GROUP BY symbol ORDER BY max_vol DESC LIMIT 10;",
            
            "Symbol Growth Between Months":
                """WITH jan AS (SELECT symbol, MAX(ttl_trd_qnty) as jan_vol FROM nse_raw_data WHERE month_year='January_2025' GROUP BY symbol),
                   feb AS (SELECT symbol, MAX(ttl_trd_qnty) as feb_vol FROM nse_raw_data WHERE month_year='February_2025' GROUP BY symbol)
                   SELECT jan.symbol, jan_vol, feb_vol, ROUND(((feb_vol-jan_vol)*100.0/jan_vol),2) as growth_pct 
                   FROM jan JOIN feb ON jan.symbol=feb.symbol WHERE feb_vol > jan_vol ORDER BY growth_pct DESC;""",
            
            "Monthly Market Summary":
                "SELECT month_year, COUNT(DISTINCT symbol) as symbols, SUM(ttl_trd_qnty) as total_volume FROM nse_raw_data GROUP BY month_year ORDER BY month_year;",
            
            "High Delivery Percentage Stocks":
                "SELECT symbol, date, deliv_per FROM nse_raw_data WHERE deliv_per > 80 ORDER BY deliv_per DESC;"
        }
        
        print("\n📚 Sample SQL Queries:")
        print("=" * 80)
        for title, query in samples.items():
            print(f"\n🔍 {title}:")
            print("-" * 50)
            print(query)
        
        return samples

def main():
    """Main query interface"""
    print("🔍 NSE Database Query Utility")
    print("=" * 50)
    
    query = NSEQueryUtility()
    
    if not query.connect():
        print("❌ Cannot connect to database")
        return
    
    while True:
        print("\n📋 Query Options:")
        print("1. Get symbol data")
        print("2. Top volume symbols (month)")
        print("3. Top delivery symbols (month)")
        print("4. Compare symbol between months") 
        print("5. Market summary (month)")
        print("6. Find growing symbols")
        print("7. Show sample queries")
        print("8. Custom SQL query")
        print("9. Exit")
        
        choice = input("\nChoose option (1-9): ").strip()
        
        try:
            if choice == "1":
                symbol = input("Enter symbol: ").upper()
                month = input("Enter month (optional): ").strip()
                month = month if month else None
                
                df = query.get_symbol_data(symbol, month)
                print(f"\n📊 Data for {symbol}:")
                print(df.to_string(index=False))
                
            elif choice == "2":
                month = input("Enter month (e.g., January_2025): ")
                limit = int(input("Enter limit (default 10): ") or "10")
                
                df = query.get_top_volume_symbols(month, limit)
                print(f"\n📈 Top {limit} Volume Symbols - {month}:")
                print(df.to_string(index=False))
                
            elif choice == "3":
                month = input("Enter month (e.g., January_2025): ")
                limit = int(input("Enter limit (default 10): ") or "10")
                
                df = query.get_top_delivery_symbols(month, limit)
                print(f"\n📦 Top {limit} Delivery Symbols - {month}:")
                print(df.to_string(index=False))
                
            elif choice == "4":
                symbol = input("Enter symbol: ").upper()
                month1 = input("Enter first month: ")
                month2 = input("Enter second month: ")
                
                df = query.compare_months(symbol, month1, month2)
                print(f"\n⚖️  {symbol} Comparison:")
                print(df.to_string(index=False))
                
            elif choice == "5":
                month = input("Enter month (e.g., January_2025): ")
                
                summary = query.get_market_summary(month)
                print(f"\n📊 Market Summary - {month}:")
                for key, value in summary.items():
                    print(f"   {key}: {value:,}" if isinstance(value, (int, float)) else f"   {key}: {value}")
                
            elif choice == "6":
                months_input = input("Enter months (comma-separated): ")
                months = [m.strip() for m in months_input.split(",")]
                min_growth = float(input("Minimum growth % (default 50): ") or "50")
                
                df = query.find_growing_symbols(months, min_growth)
                print(f"\n🚀 Growing Symbols (>{min_growth}% growth):")
                print(df.to_string(index=False))
                
            elif choice == "7":
                query.create_sample_queries()
                
            elif choice == "8":
                sql = input("Enter SQL query: ")
                try:
                    df = pd.read_sql(sql, query.db.engine)
                    print("\n📊 Query Results:")
                    print(df.to_string(index=False))
                except Exception as e:
                    print(f"❌ Query error: {str(e)}")
                
            elif choice == "9":
                print("👋 Goodbye!")
                break
                
            else:
                print("❌ Invalid choice")
                
        except Exception as e:
            print(f"❌ Error: {str(e)}")
        
        input("\nPress Enter to continue...")

if __name__ == "__main__":
    main()
