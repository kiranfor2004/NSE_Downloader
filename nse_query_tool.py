#!/usr/bin/env python3
"""
NSE Database Query Tool

Simple command-line tool for querying NSE stock market data.
Provides common queries and analysis functions.

Requirements: None (uses built-in sqlite3)

Author: Generated for NSE data analysis
Date: September 2025
"""

import sqlite3
import os
from datetime import datetime, timedelta

class NSEQueryTool:
    def __init__(self, db_path="nse_data.db"):
        """Initialize NSE Query Tool"""
        self.db_path = db_path
        if not os.path.exists(db_path):
            print(f"❌ Database not found: {db_path}")
            print("💡 Run nse_database.py first to create and populate the database")
            exit(1)
        
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
    
    def quick_stats(self):
        """Show quick database statistics"""
        cursor = self.conn.cursor()
        
        # Basic stats
        cursor.execute("SELECT COUNT(*) FROM stock_data")
        total_records = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT symbol) FROM stock_data WHERE series = 'EQ'")
        eq_stocks = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT date) FROM stock_data")
        trading_days = cursor.fetchone()[0]
        
        cursor.execute("SELECT MIN(date), MAX(date) FROM stock_data")
        date_range = cursor.fetchone()
        
        print(f"📊 Quick Stats:")
        print(f"━━━━━━━━━━━━━━━━━━━━━━")
        print(f"📈 Total Records: {total_records:,}")
        print(f"🏢 EQ Stocks: {eq_stocks:,}")
        print(f"📅 Trading Days: {trading_days}")
        print(f"📆 Date Range: {date_range[0]} → {date_range[1]}")
    
    def top_gainers(self, date=None, limit=10):
        """Find top gainers for a specific date or overall"""
        cursor = self.conn.cursor()
        
        if date:
            cursor.execute("""
                SELECT symbol, prev_close, close_price, 
                       ((close_price - prev_close) / prev_close * 100) as gain_percent,
                       total_traded_qty, delivery_percentage
                FROM stock_data 
                WHERE date = ? AND series = 'EQ' 
                  AND prev_close > 0 AND close_price > 0
                ORDER BY gain_percent DESC 
                LIMIT ?
            """, (date, limit))
            
            results = cursor.fetchall()
            print(f"\n🚀 Top {limit} Gainers on {date}:")
            print("-" * 70)
            print(f"{'Symbol':<12} {'Prev':<8} {'Close':<8} {'Gain%':<8} {'Volume':<12} {'Del%':<6}")
            print("-" * 70)
            
            for stock in results:
                print(f"{stock['symbol']:<12} {stock['prev_close']:>7.2f} {stock['close_price']:>7.2f} "
                      f"{stock['gain_percent']:>6.2f}% {stock['total_traded_qty']:>11,} {stock['delivery_percentage']:>5.1f}%")
        else:
            print("❌ Please provide a date (DD-Mon-YYYY format)")
    
    def top_losers(self, date=None, limit=10):
        """Find top losers for a specific date"""
        cursor = self.conn.cursor()
        
        if date:
            cursor.execute("""
                SELECT symbol, prev_close, close_price, 
                       ((close_price - prev_close) / prev_close * 100) as loss_percent,
                       total_traded_qty, delivery_percentage
                FROM stock_data 
                WHERE date = ? AND series = 'EQ' 
                  AND prev_close > 0 AND close_price > 0
                ORDER BY loss_percent ASC 
                LIMIT ?
            """, (date, limit))
            
            results = cursor.fetchall()
            print(f"\n📉 Top {limit} Losers on {date}:")
            print("-" * 70)
            print(f"{'Symbol':<12} {'Prev':<8} {'Close':<8} {'Loss%':<8} {'Volume':<12} {'Del%':<6}")
            print("-" * 70)
            
            for stock in results:
                print(f"{stock['symbol']:<12} {stock['prev_close']:>7.2f} {stock['close_price']:>7.2f} "
                      f"{stock['loss_percent']:>6.2f}% {stock['total_traded_qty']:>11,} {stock['delivery_percentage']:>5.1f}%")
        else:
            print("❌ Please provide a date (DD-Mon-YYYY format)")
    
    def high_delivery_stocks(self, date=None, min_delivery=80, limit=20):
        """Find stocks with high delivery percentage"""
        cursor = self.conn.cursor()
        
        if date:
            cursor.execute("""
                SELECT symbol, close_price, total_traded_qty, delivery_percentage,
                       turnover_lacs, no_of_trades
                FROM stock_data 
                WHERE date = ? AND series = 'EQ' 
                  AND delivery_percentage >= ?
                  AND total_traded_qty > 1000
                ORDER BY delivery_percentage DESC, total_traded_qty DESC
                LIMIT ?
            """, (date, min_delivery, limit))
            
            results = cursor.fetchall()
            print(f"\n📦 High Delivery Stocks on {date} (>{min_delivery}% delivery):")
            print("-" * 80)
            print(f"{'Symbol':<12} {'Price':<8} {'Volume':<12} {'Delivery%':<10} {'Turnover':<10} {'Trades':<8}")
            print("-" * 80)
            
            for stock in results:
                print(f"{stock['symbol']:<12} {stock['close_price']:>7.2f} {stock['total_traded_qty']:>11,} "
                      f"{stock['delivery_percentage']:>8.1f}% {stock['turnover_lacs']:>9.1f} {stock['no_of_trades']:>7,}")
        else:
            print("❌ Please provide a date (DD-Mon-YYYY format)")
    
    def volume_analysis(self, date=None, limit=15):
        """Analyze volume leaders"""
        cursor = self.conn.cursor()
        
        if date:
            cursor.execute("""
                SELECT symbol, close_price, total_traded_qty, turnover_lacs,
                       delivery_percentage, no_of_trades
                FROM stock_data 
                WHERE date = ? AND series = 'EQ'
                ORDER BY total_traded_qty DESC 
                LIMIT ?
            """, (date, limit))
            
            results = cursor.fetchall()
            print(f"\n📊 Volume Leaders on {date}:")
            print("-" * 85)
            print(f"{'Symbol':<12} {'Price':<8} {'Volume':<15} {'Turnover':<12} {'Trades':<10} {'Del%':<6}")
            print("-" * 85)
            
            for stock in results:
                print(f"{stock['symbol']:<12} {stock['close_price']:>7.2f} {stock['total_traded_qty']:>14,} "
                      f"{stock['turnover_lacs']:>11.1f} {stock['no_of_trades']:>9,} {stock['delivery_percentage']:>5.1f}%")
        else:
            print("❌ Please provide a date (DD-Mon-YYYY format)")
    
    def stock_performance(self, symbol):
        """Show performance of a specific stock"""
        cursor = self.conn.cursor()
        
        cursor.execute("""
            SELECT date, open_price, high_price, low_price, close_price, 
                   total_traded_qty, delivery_percentage
            FROM stock_data 
            WHERE symbol = ? AND series = 'EQ'
            ORDER BY date
        """, (symbol.upper(),))
        
        results = cursor.fetchall()
        
        if not results:
            print(f"❌ No data found for {symbol}")
            return
        
        print(f"\n📈 Performance of {symbol.upper()}:")
        print("-" * 80)
        print(f"{'Date':<12} {'Open':<8} {'High':<8} {'Low':<8} {'Close':<8} {'Volume':<12} {'Del%':<6}")
        print("-" * 80)
        
        for row in results:
            print(f"{row['date']:<12} {row['open_price']:>7.2f} {row['high_price']:>7.2f} "
                  f"{row['low_price']:>7.2f} {row['close_price']:>7.2f} {row['total_traded_qty']:>11,} {row['delivery_percentage']:>5.1f}%")
        
        # Show summary stats
        first_price = results[0]['close_price']
        last_price = results[-1]['close_price']
        total_return = ((last_price - first_price) / first_price * 100) if first_price > 0 else 0
        
        avg_volume = sum(row['total_traded_qty'] for row in results) / len(results)
        avg_delivery = sum(row['delivery_percentage'] for row in results) / len(results)
        
        print("-" * 80)
        print(f"📊 Summary: Total Return: {total_return:+.2f}% | Avg Volume: {avg_volume:,.0f} | Avg Delivery: {avg_delivery:.1f}%")
    
    def market_summary_by_date(self, date):
        """Show market summary for a specific date"""
        cursor = self.conn.cursor()
        
        cursor.execute("""
            SELECT 
                COUNT(*) as total_stocks,
                SUM(total_traded_qty) as total_volume,
                SUM(turnover_lacs) as total_turnover,
                SUM(no_of_trades) as total_trades,
                AVG(delivery_percentage) as avg_delivery,
                COUNT(CASE WHEN close_price > prev_close THEN 1 END) as gainers,
                COUNT(CASE WHEN close_price < prev_close THEN 1 END) as losers,
                COUNT(CASE WHEN close_price = prev_close THEN 1 END) as unchanged
            FROM stock_data 
            WHERE date = ? AND series = 'EQ'
        """, (date,))
        
        result = cursor.fetchone()
        
        if result['total_stocks'] == 0:
            print(f"❌ No data found for {date}")
            return
        
        print(f"\n📅 Market Summary for {date}:")
        print("=" * 50)
        print(f"🏢 Total Stocks: {result['total_stocks']:,}")
        print(f"📊 Total Volume: {result['total_volume']:,}")
        print(f"💰 Total Turnover: ₹{result['total_turnover']:,.1f} Lacs")
        print(f"🔢 Total Trades: {result['total_trades']:,}")
        print(f"📦 Avg Delivery: {result['avg_delivery']:.1f}%")
        print(f"🟢 Gainers: {result['gainers']:,}")
        print(f"🔴 Losers: {result['losers']:,}")
        print(f"🟡 Unchanged: {result['unchanged']:,}")
        
        # Calculate advance-decline ratio
        if result['losers'] > 0:
            ad_ratio = result['gainers'] / result['losers']
            print(f"📈 A/D Ratio: {ad_ratio:.2f}")
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()

def main():
    """Interactive query tool"""
    print("🔍 NSE Database Query Tool")
    print("=" * 40)
    
    query_tool = NSEQueryTool()
    query_tool.quick_stats()
    
    while True:
        print("\n🔍 Query Options:")
        print("1. Top Gainers")
        print("2. Top Losers") 
        print("3. High Delivery Stocks")
        print("4. Volume Leaders")
        print("5. Stock Performance")
        print("6. Market Summary")
        print("7. Quick Stats")
        print("8. Exit")
        
        try:
            choice = input("\n🔢 Enter choice (1-8): ").strip()
            
            if choice == '1':
                date = input("Enter date (DD-Mon-YYYY): ").strip()
                limit = input("Number of stocks (default 10): ").strip() or "10"
                query_tool.top_gainers(date, int(limit))
            
            elif choice == '2':
                date = input("Enter date (DD-Mon-YYYY): ").strip()
                limit = input("Number of stocks (default 10): ").strip() or "10"
                query_tool.top_losers(date, int(limit))
            
            elif choice == '3':
                date = input("Enter date (DD-Mon-YYYY): ").strip()
                min_del = input("Minimum delivery % (default 80): ").strip() or "80"
                limit = input("Number of stocks (default 20): ").strip() or "20"
                query_tool.high_delivery_stocks(date, float(min_del), int(limit))
            
            elif choice == '4':
                date = input("Enter date (DD-Mon-YYYY): ").strip()
                limit = input("Number of stocks (default 15): ").strip() or "15"
                query_tool.volume_analysis(date, int(limit))
            
            elif choice == '5':
                symbol = input("Enter stock symbol: ").strip()
                if symbol:
                    query_tool.stock_performance(symbol)
            
            elif choice == '6':
                date = input("Enter date (DD-Mon-YYYY): ").strip()
                if date:
                    query_tool.market_summary_by_date(date)
            
            elif choice == '7':
                query_tool.quick_stats()
            
            elif choice == '8':
                break
            
            else:
                print("❌ Invalid choice")
                
        except KeyboardInterrupt:
            print("\n\n🛑 Interrupted")
            break
        except Exception as e:
            print(f"❌ Error: {e}")
    
    query_tool.close()
    print("\n👋 Thanks for using NSE Query Tool!")

if __name__ == "__main__":
    main()
