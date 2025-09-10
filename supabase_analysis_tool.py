#!/usr/bin/env python3
"""
NSE Supabase Query & Analysis Tool

Advanced analysis tool for NSE data stored in Supabase cloud database.
Provides comprehensive stock market analysis capabilities.

Requirements: supabase python-dotenv pandas

Author: Generated for NSE cloud data analysis
Date: September 2025
"""

import os
import pandas as pd
from supabase import create_client, Client
from datetime import datetime, timedelta
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class NSESupabaseAnalyzer:
    def __init__(self):
        """Initialize Supabase connection"""
        self.supabase_url = os.getenv('SUPABASE_URL')
        self.supabase_key = os.getenv('SUPABASE_ANON_KEY')
        
        if not self.supabase_url or not self.supabase_key:
            print("❌ Error: Supabase credentials not found!")
            sys.exit(1)
            
        try:
            self.supabase: Client = create_client(self.supabase_url, self.supabase_key)
            print("✅ Connected to Supabase for analysis!")
        except Exception as e:
            print(f"❌ Failed to connect to Supabase: {e}")
            sys.exit(1)
    
    def get_top_gainers(self, date_str=None, limit=10):
        """Get top gaining stocks for a specific date"""
        try:
            if not date_str:
                # Get latest date
                result = self.supabase.table('nse_stock_data')\
                    .select('date')\
                    .order('date', desc=True)\
                    .limit(1)\
                    .execute()
                date_str = result.data[0]['date']
            
            # Calculate percentage change and get top gainers
            query = """
            SELECT 
                symbol,
                date,
                open_price,
                close_price,
                high_price,
                low_price,
                prev_close,
                total_traded_qty,
                turnover,
                ROUND(((close_price - prev_close) / prev_close * 100), 2) as percentage_change
            FROM nse_stock_data 
            WHERE date = '{}'
                AND prev_close > 0 
                AND close_price > 0
                AND total_traded_qty > 10000
            ORDER BY percentage_change DESC
            LIMIT {}
            """.format(date_str, limit)
            
            result = self.supabase.rpc('exec_sql', {'sql': query}).execute()
            
            if result.data:
                df = pd.DataFrame(result.data)
                print(f"🚀 Top {limit} Gainers for {date_str}")
                print("-" * 80)
                print(f"{'Rank':<4} {'Symbol':<15} {'Open':<8} {'Close':<8} {'High':<8} {'Change%':<8} {'Volume':<12}")
                print("-" * 80)
                
                for i, row in df.iterrows():
                    print(f"{i+1:<4} {row['symbol']:<15} {row['open_price']:<8.2f} {row['close_price']:<8.2f} "
                          f"{row['high_price']:<8.2f} {row['percentage_change']:<8.2f} {row['total_traded_qty']:<12,}")
                
                return df
            else:
                print("❌ No data found")
                return None
                
        except Exception as e:
            print(f"❌ Error getting top gainers: {e}")
            return None
    
    def get_top_losers(self, date_str=None, limit=10):
        """Get top losing stocks for a specific date"""
        try:
            if not date_str:
                # Get latest date
                result = self.supabase.table('nse_stock_data')\
                    .select('date')\
                    .order('date', desc=True)\
                    .limit(1)\
                    .execute()
                date_str = result.data[0]['date']
            
            result = self.supabase.table('nse_stock_data')\
                .select('symbol, date, open_price, close_price, high_price, low_price, prev_close, total_traded_qty, turnover')\
                .eq('date', date_str)\
                .gt('prev_close', 0)\
                .gt('close_price', 0)\
                .gt('total_traded_qty', 10000)\
                .order('close_price')\
                .limit(limit)\
                .execute()
            
            if result.data:
                df = pd.DataFrame(result.data)
                df['percentage_change'] = ((df['close_price'] - df['prev_close']) / df['prev_close'] * 100).round(2)
                df = df.sort_values('percentage_change')
                
                print(f"📉 Top {limit} Losers for {date_str}")
                print("-" * 80)
                print(f"{'Rank':<4} {'Symbol':<15} {'Open':<8} {'Close':<8} {'Low':<8} {'Change%':<8} {'Volume':<12}")
                print("-" * 80)
                
                for i, row in df.iterrows():
                    print(f"{i+1:<4} {row['symbol']:<15} {row['open_price']:<8.2f} {row['close_price']:<8.2f} "
                          f"{row['low_price']:<8.2f} {row['percentage_change']:<8.2f} {row['total_traded_qty']:<12,}")
                
                return df
            else:
                print("❌ No data found")
                return None
                
        except Exception as e:
            print(f"❌ Error getting top losers: {e}")
            return None
    
    def get_high_delivery_stocks(self, date_str=None, min_delivery_percent=80, limit=15):
        """Get stocks with high delivery percentage"""
        try:
            if not date_str:
                # Get latest date
                result = self.supabase.table('nse_stock_data')\
                    .select('date')\
                    .order('date', desc=True)\
                    .limit(1)\
                    .execute()
                date_str = result.data[0]['date']
            
            result = self.supabase.table('nse_stock_data')\
                .select('symbol, close_price, total_traded_qty, turnover, deliverable_qty, deliverable_percentage')\
                .eq('date', date_str)\
                .gte('deliverable_percentage', min_delivery_percent)\
                .gt('total_traded_qty', 50000)\
                .order('deliverable_percentage', desc=True)\
                .limit(limit)\
                .execute()
            
            if result.data:
                df = pd.DataFrame(result.data)
                
                print(f"📦 High Delivery Stocks (≥{min_delivery_percent}%) for {date_str}")
                print("-" * 90)
                print(f"{'Rank':<4} {'Symbol':<15} {'Price':<8} {'Volume':<12} {'Delivery':<12} {'Delivery%':<10}")
                print("-" * 90)
                
                for i, row in df.iterrows():
                    print(f"{i+1:<4} {row['symbol']:<15} {row['close_price']:<8.2f} {row['total_traded_qty']:<12,} "
                          f"{row['deliverable_qty']:<12,} {row['deliverable_percentage']:<10.2f}")
                
                return df
            else:
                print(f"❌ No stocks found with delivery ≥{min_delivery_percent}%")
                return None
                
        except Exception as e:
            print(f"❌ Error getting high delivery stocks: {e}")
            return None
    
    def get_volume_leaders(self, date_str=None, limit=10):
        """Get highest volume stocks"""
        try:
            if not date_str:
                # Get latest date
                result = self.supabase.table('nse_stock_data')\
                    .select('date')\
                    .order('date', desc=True)\
                    .limit(1)\
                    .execute()
                date_str = result.data[0]['date']
            
            result = self.supabase.table('nse_stock_data')\
                .select('symbol, close_price, total_traded_qty, turnover, no_of_trades')\
                .eq('date', date_str)\
                .order('total_traded_qty', desc=True)\
                .limit(limit)\
                .execute()
            
            if result.data:
                df = pd.DataFrame(result.data)
                
                print(f"📊 Volume Leaders for {date_str}")
                print("-" * 80)
                print(f"{'Rank':<4} {'Symbol':<15} {'Price':<8} {'Volume':<12} {'Turnover':<12} {'Trades':<8}")
                print("-" * 80)
                
                for i, row in df.iterrows():
                    turnover_cr = row['turnover'] / 10000000  # Convert to crores
                    print(f"{i+1:<4} {row['symbol']:<15} {row['close_price']:<8.2f} {row['total_traded_qty']:<12,} "
                          f"{turnover_cr:<12.1f} {row['no_of_trades']:<8,}")
                
                return df
            else:
                print("❌ No data found")
                return None
                
        except Exception as e:
            print(f"❌ Error getting volume leaders: {e}")
            return None
    
    def analyze_stock(self, symbol):
        """Get complete analysis for a specific stock"""
        try:
            result = self.supabase.table('nse_stock_data')\
                .select('*')\
                .eq('symbol', symbol.upper())\
                .order('date')\
                .execute()
            
            if result.data:
                df = pd.DataFrame(result.data)
                df['date'] = pd.to_datetime(df['date'])
                df = df.sort_values('date')
                
                print(f"📈 Complete Analysis for {symbol.upper()}")
                print("=" * 80)
                
                # Basic stats
                latest = df.iloc[-1]
                oldest = df.iloc[0]
                
                print(f"📅 Date Range: {oldest['date'].strftime('%Y-%m-%d')} to {latest['date'].strftime('%Y-%m-%d')}")
                print(f"📊 Total Trading Days: {len(df)}")
                print(f"💰 Latest Price: ₹{latest['close_price']:.2f}")
                print(f"📈 Highest Price: ₹{df['high_price'].max():.2f}")
                print(f"📉 Lowest Price: ₹{df['low_price'].min():.2f}")
                
                # Performance metrics
                if len(df) > 1:
                    total_return = ((latest['close_price'] - oldest['close_price']) / oldest['close_price'] * 100)
                    avg_volume = df['total_traded_qty'].mean()
                    avg_delivery = df['deliverable_percentage'].mean()
                    
                    print(f"🎯 Total Return: {total_return:.2f}%")
                    print(f"📦 Average Volume: {avg_volume:,.0f}")
                    print(f"🚚 Average Delivery%: {avg_delivery:.2f}%")
                
                print("\n📊 Recent 5 Days Performance:")
                print("-" * 80)
                print(f"{'Date':<12} {'Open':<8} {'High':<8} {'Low':<8} {'Close':<8} {'Volume':<12} {'Delivery%':<10}")
                print("-" * 80)
                
                recent_data = df.tail(5)
                for _, row in recent_data.iterrows():
                    print(f"{row['date'].strftime('%Y-%m-%d'):<12} {row['open_price']:<8.2f} "
                          f"{row['high_price']:<8.2f} {row['low_price']:<8.2f} {row['close_price']:<8.2f} "
                          f"{row['total_traded_qty']:<12,} {row['deliverable_percentage']:<10.2f}")
                
                return df
            else:
                print(f"❌ No data found for {symbol.upper()}")
                return None
                
        except Exception as e:
            print(f"❌ Error analyzing stock: {e}")
            return None
    
    def get_market_summary(self, date_str=None):
        """Get overall market summary for a date"""
        try:
            if not date_str:
                # Get latest date
                result = self.supabase.table('nse_stock_data')\
                    .select('date')\
                    .order('date', desc=True)\
                    .limit(1)\
                    .execute()
                date_str = result.data[0]['date']
            
            result = self.supabase.table('nse_stock_data')\
                .select('*')\
                .eq('date', date_str)\
                .execute()
            
            if result.data:
                df = pd.DataFrame(result.data)
                
                # Calculate market metrics
                total_stocks = len(df)
                total_turnover = df['turnover'].sum() / 10000000  # Crores
                total_volume = df['total_traded_qty'].sum()
                total_trades = df['no_of_trades'].sum()
                
                # Gainers vs Losers
                gainers = len(df[df['close_price'] > df['prev_close']])
                losers = len(df[df['close_price'] < df['prev_close']])
                unchanged = total_stocks - gainers - losers
                
                # Delivery analysis
                avg_delivery = df['deliverable_percentage'].mean()
                high_delivery = len(df[df['deliverable_percentage'] >= 80])
                
                print(f"📊 Market Summary for {date_str}")
                print("=" * 60)
                print(f"📈 Total Stocks Traded: {total_stocks:,}")
                print(f"💰 Total Turnover: ₹{total_turnover:,.1f} Crores")
                print(f"📊 Total Volume: {total_volume:,}")
                print(f"🔄 Total Trades: {total_trades:,}")
                print()
                print(f"📈 Gainers: {gainers:,} ({gainers/total_stocks*100:.1f}%)")
                print(f"📉 Losers: {losers:,} ({losers/total_stocks*100:.1f}%)")
                print(f"➡️  Unchanged: {unchanged:,} ({unchanged/total_stocks*100:.1f}%)")
                print()
                print(f"📦 Average Delivery%: {avg_delivery:.2f}%")
                print(f"🚚 High Delivery Stocks (≥80%): {high_delivery}")
                
                return df
            else:
                print("❌ No data found")
                return None
                
        except Exception as e:
            print(f"❌ Error getting market summary: {e}")
            return None
    
    def export_to_csv(self, data, filename):
        """Export analysis results to CSV"""
        if data is not None and not data.empty:
            try:
                data.to_csv(filename, index=False)
                print(f"✅ Data exported to {filename}")
            except Exception as e:
                print(f"❌ Export failed: {e}")
    
    def get_available_dates(self):
        """Get list of available dates in database"""
        try:
            result = self.supabase.table('nse_stock_data')\
                .select('date')\
                .order('date')\
                .execute()
            
            if result.data:
                dates = list(set([row['date'] for row in result.data]))
                dates.sort()
                
                print("📅 Available dates in database:")
                print("-" * 30)
                for i, date in enumerate(dates):
                    print(f"{i+1:2d}. {date}")
                
                return dates
            else:
                print("❌ No dates found")
                return []
                
        except Exception as e:
            print(f"❌ Error getting dates: {e}")
            return []

def main():
    print("☁️  NSE Supabase Analysis Tool")
    print("=" * 50)
    
    # Check for .env file
    if not os.path.exists('.env'):
        print("❌ .env file not found!")
        print("Please run the uploader script first to set up configuration")
        return
    
    # Initialize analyzer
    try:
        analyzer = NSESupabaseAnalyzer()
    except SystemExit:
        print("❌ Failed to connect. Check your .env configuration")
        return
    
    # Interactive menu
    while True:
        print("\n" + "=" * 50)
        print("📊 NSE Analysis Menu")
        print("=" * 50)
        print("1. 📈 Top Gainers")
        print("2. 📉 Top Losers") 
        print("3. 📦 High Delivery Stocks")
        print("4. 📊 Volume Leaders")
        print("5. 🔍 Analyze Specific Stock")
        print("6. 🌍 Market Summary")
        print("7. 📅 Show Available Dates")
        print("8. ❌ Exit")
        
        choice = input("\n🎯 Choose option (1-8): ").strip()
        
        if choice == '1':
            date = input("📅 Enter date (YYYY-MM-DD) or press Enter for latest: ").strip()
            limit = input("📊 Number of stocks (default 10): ").strip()
            limit = int(limit) if limit.isdigit() else 10
            
            data = analyzer.get_top_gainers(date if date else None, limit)
            if data is not None:
                export = input("💾 Export to CSV? (y/n): ").strip().lower()
                if export == 'y':
                    filename = f"top_gainers_{date or 'latest'}_{limit}.csv"
                    analyzer.export_to_csv(data, filename)
        
        elif choice == '2':
            date = input("📅 Enter date (YYYY-MM-DD) or press Enter for latest: ").strip()
            limit = input("📊 Number of stocks (default 10): ").strip()
            limit = int(limit) if limit.isdigit() else 10
            
            data = analyzer.get_top_losers(date if date else None, limit)
            if data is not None:
                export = input("💾 Export to CSV? (y/n): ").strip().lower()
                if export == 'y':
                    filename = f"top_losers_{date or 'latest'}_{limit}.csv"
                    analyzer.export_to_csv(data, filename)
        
        elif choice == '3':
            date = input("📅 Enter date (YYYY-MM-DD) or press Enter for latest: ").strip()
            min_delivery = input("📦 Minimum delivery % (default 80): ").strip()
            min_delivery = float(min_delivery) if min_delivery else 80
            
            data = analyzer.get_high_delivery_stocks(date if date else None, min_delivery)
            if data is not None:
                export = input("💾 Export to CSV? (y/n): ").strip().lower()
                if export == 'y':
                    filename = f"high_delivery_{date or 'latest'}_{min_delivery}pct.csv"
                    analyzer.export_to_csv(data, filename)
        
        elif choice == '4':
            date = input("📅 Enter date (YYYY-MM-DD) or press Enter for latest: ").strip()
            limit = input("📊 Number of stocks (default 10): ").strip()
            limit = int(limit) if limit.isdigit() else 10
            
            data = analyzer.get_volume_leaders(date if date else None, limit)
            if data is not None:
                export = input("💾 Export to CSV? (y/n): ").strip().lower()
                if export == 'y':
                    filename = f"volume_leaders_{date or 'latest'}_{limit}.csv"
                    analyzer.export_to_csv(data, filename)
        
        elif choice == '5':
            symbol = input("🔍 Enter stock symbol (e.g., RELIANCE): ").strip()
            if symbol:
                data = analyzer.analyze_stock(symbol)
                if data is not None:
                    export = input("💾 Export to CSV? (y/n): ").strip().lower()
                    if export == 'y':
                        filename = f"{symbol.upper()}_analysis.csv"
                        analyzer.export_to_csv(data, filename)
        
        elif choice == '6':
            date = input("📅 Enter date (YYYY-MM-DD) or press Enter for latest: ").strip()
            data = analyzer.get_market_summary(date if date else None)
            if data is not None:
                export = input("💾 Export to CSV? (y/n): ").strip().lower()
                if export == 'y':
                    filename = f"market_summary_{date or 'latest'}.csv"
                    analyzer.export_to_csv(data, filename)
        
        elif choice == '7':
            analyzer.get_available_dates()
        
        elif choice == '8':
            print("👋 Goodbye!")
            break
        
        else:
            print("❌ Invalid choice. Please try again.")

if __name__ == "__main__":
    main()
