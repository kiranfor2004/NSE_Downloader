#!/usr/bin/env python3
"""
NSE Database Manager

Creates and manages SQLite database for NSE stock market data.
Imports CSV files, provides querying capabilities, and data analysis.

Requirements: None (uses built-in sqlite3)

Author: Generated for NSE data management
Date: September 2025
"""

import sqlite3
import os
import csv
from datetime import datetime, timedelta
import glob
import pandas as pd

class NSEDatabase:
    def __init__(self, db_path="nse_data.db"):
        """Initialize NSE Database Manager"""
        self.db_path = db_path
        self.conn = None
        self.setup_database()
    
    def setup_database(self):
        """Create database and tables"""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row  # Enable column access by name
        
        cursor = self.conn.cursor()
        
        # Create main stock data table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stock_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                series TEXT NOT NULL,
                date TEXT NOT NULL,
                prev_close REAL,
                open_price REAL,
                high_price REAL,
                low_price REAL,
                last_price REAL,
                close_price REAL,
                avg_price REAL,
                total_traded_qty INTEGER,
                turnover_lacs REAL,
                no_of_trades INTEGER,
                delivery_qty INTEGER,
                delivery_percentage REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(symbol, series, date)
            )
        """)
        
        # Create indexes for better performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_symbol ON stock_data(symbol)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_date ON stock_data(date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_symbol_date ON stock_data(symbol, date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_series ON stock_data(series)")
        
        # Create summary table for daily market stats
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS daily_summary (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT UNIQUE NOT NULL,
                total_stocks INTEGER,
                total_volume INTEGER,
                total_turnover REAL,
                total_trades INTEGER,
                avg_delivery_percent REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        self.conn.commit()
        print(f"✅ Database initialized: {os.path.abspath(self.db_path)}")
    
    def import_csv_file(self, csv_file_path):
        """Import a single CSV file into the database"""
        if not os.path.exists(csv_file_path):
            print(f"❌ File not found: {csv_file_path}")
            return False
        
        filename = os.path.basename(csv_file_path)
        print(f"📥 Importing: {filename}... ", end="", flush=True)
        
        cursor = self.conn.cursor()
        imported_count = 0
        skipped_count = 0
        
        try:
            with open(csv_file_path, 'r', encoding='utf-8') as file:
                csv_reader = csv.DictReader(file)
                
                for row in csv_reader:
                    try:
                        # Clean and prepare data
                        symbol = row['SYMBOL'].strip()
                        series = row[' SERIES'].strip()  # Note the space in column name
                        date = row[' DATE1'].strip()
                        
                        # Convert numeric fields, handle empty/invalid values
                        def safe_float(value):
                            try:
                                return float(str(value).strip()) if str(value).strip() else None
                            except:
                                return None
                        
                        def safe_int(value):
                            try:
                                return int(float(str(value).strip())) if str(value).strip() else None
                            except:
                                return None
                        
                        # Insert data
                        cursor.execute("""
                            INSERT OR IGNORE INTO stock_data 
                            (symbol, series, date, prev_close, open_price, high_price, 
                             low_price, last_price, close_price, avg_price, total_traded_qty, 
                             turnover_lacs, no_of_trades, delivery_qty, delivery_percentage)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            symbol, series, date,
                            safe_float(row[' PREV_CLOSE']),
                            safe_float(row[' OPEN_PRICE']),
                            safe_float(row[' HIGH_PRICE']),
                            safe_float(row[' LOW_PRICE']),
                            safe_float(row[' LAST_PRICE']),
                            safe_float(row[' CLOSE_PRICE']),
                            safe_float(row[' AVG_PRICE']),
                            safe_int(row[' TTL_TRD_QNTY']),
                            safe_float(row[' TURNOVER_LACS']),
                            safe_int(row[' NO_OF_TRADES']),
                            safe_int(row[' DELIV_QTY']),
                            safe_float(row[' DELIV_PER'])
                        ))
                        
                        if cursor.rowcount > 0:
                            imported_count += 1
                        else:
                            skipped_count += 1
                            
                    except Exception as e:
                        print(f"\n⚠️ Error processing row in {filename}: {e}")
                        continue
            
            self.conn.commit()
            print(f"✅ Imported {imported_count} records, skipped {skipped_count}")
            
            # Update daily summary
            self.update_daily_summary(date)
            
            return True
            
        except Exception as e:
            print(f"❌ Error importing {filename}: {e}")
            self.conn.rollback()
            return False
    
    def import_all_csv_files(self, data_folder="NSE_August_2025_Data"):
        """Import all CSV files from the data folder"""
        if not os.path.exists(data_folder):
            print(f"❌ Data folder not found: {data_folder}")
            return
        
        csv_files = glob.glob(os.path.join(data_folder, "sec_bhavdata_full_*.csv"))
        csv_files.sort()
        
        if not csv_files:
            print(f"❌ No CSV files found in {data_folder}")
            return
        
        print(f"🎯 Found {len(csv_files)} CSV files to import")
        print("=" * 60)
        
        successful = 0
        failed = 0
        
        for csv_file in csv_files:
            if self.import_csv_file(csv_file):
                successful += 1
            else:
                failed += 1
        
        print("=" * 60)
        print(f"📊 Import Summary:")
        print(f"✅ Successfully imported: {successful} files")
        print(f"❌ Failed: {failed} files")
        
        # Show database stats
        self.show_database_stats()
    
    def update_daily_summary(self, date):
        """Update daily summary statistics for a specific date"""
        cursor = self.conn.cursor()
        
        cursor.execute("""
            INSERT OR REPLACE INTO daily_summary 
            (date, total_stocks, total_volume, total_turnover, total_trades, avg_delivery_percent)
            SELECT 
                date,
                COUNT(*) as total_stocks,
                SUM(total_traded_qty) as total_volume,
                SUM(turnover_lacs) as total_turnover,
                SUM(no_of_trades) as total_trades,
                AVG(delivery_percentage) as avg_delivery_percent
            FROM stock_data 
            WHERE date = ?
        """, (date,))
        
        self.conn.commit()
    
    def show_database_stats(self):
        """Show database statistics"""
        cursor = self.conn.cursor()
        
        # Total records
        cursor.execute("SELECT COUNT(*) FROM stock_data")
        total_records = cursor.fetchone()[0]
        
        # Unique symbols
        cursor.execute("SELECT COUNT(DISTINCT symbol) FROM stock_data")
        unique_symbols = cursor.fetchone()[0]
        
        # Date range
        cursor.execute("SELECT MIN(date), MAX(date) FROM stock_data")
        date_range = cursor.fetchone()
        
        # Unique dates
        cursor.execute("SELECT COUNT(DISTINCT date) FROM stock_data")
        trading_days = cursor.fetchone()[0]
        
        print(f"\n📊 Database Statistics:")
        print(f"━" * 40)
        print(f"📈 Total Records: {total_records:,}")
        print(f"🏢 Unique Stocks: {unique_symbols:,}")
        print(f"📅 Trading Days: {trading_days}")
        print(f"📆 Date Range: {date_range[0]} to {date_range[1]}")
        print(f"💾 Database Size: {os.path.getsize(self.db_path) / (1024*1024):.1f} MB")
    
    def get_stock_data(self, symbol, start_date=None, end_date=None):
        """Get stock data for a specific symbol"""
        cursor = self.conn.cursor()
        
        query = "SELECT * FROM stock_data WHERE symbol = ?"
        params = [symbol.upper()]
        
        if start_date:
            query += " AND date >= ?"
            params.append(start_date)
        
        if end_date:
            query += " AND date <= ?"
            params.append(end_date)
        
        query += " ORDER BY date"
        
        cursor.execute(query, params)
        return cursor.fetchall()
    
    def get_top_stocks_by_volume(self, date=None, limit=10):
        """Get top stocks by trading volume"""
        cursor = self.conn.cursor()
        
        if date:
            cursor.execute("""
                SELECT symbol, total_traded_qty, turnover_lacs, delivery_percentage
                FROM stock_data 
                WHERE date = ? AND series = 'EQ'
                ORDER BY total_traded_qty DESC 
                LIMIT ?
            """, (date, limit))
        else:
            cursor.execute("""
                SELECT symbol, SUM(total_traded_qty) as total_volume, 
                       SUM(turnover_lacs) as total_turnover,
                       AVG(delivery_percentage) as avg_delivery_percent
                FROM stock_data 
                WHERE series = 'EQ'
                GROUP BY symbol 
                ORDER BY total_volume DESC 
                LIMIT ?
            """, (limit,))
        
        return cursor.fetchall()
    
    def get_daily_summary(self):
        """Get daily market summary"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM daily_summary ORDER BY date")
        return cursor.fetchall()
    
    def search_stocks(self, pattern):
        """Search stocks by symbol pattern"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT DISTINCT symbol, series, COUNT(*) as days_traded
            FROM stock_data 
            WHERE symbol LIKE ? 
            GROUP BY symbol, series
            ORDER BY symbol
        """, (f"%{pattern.upper()}%",))
        return cursor.fetchall()
    
    def export_to_csv(self, query, filename):
        """Export query results to CSV"""
        cursor = self.conn.cursor()
        cursor.execute(query)
        
        results = cursor.fetchall()
        if not results:
            print("❌ No data to export")
            return
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # Write header
            writer.writerow([description[0] for description in cursor.description])
            
            # Write data
            for row in results:
                writer.writerow(row)
        
        print(f"✅ Exported {len(results)} records to {filename}")
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            print("🔒 Database connection closed")

def main():
    """Main function with interactive menu"""
    print("🏦 NSE Database Manager")
    print("=" * 50)
    
    db = NSEDatabase()
    
    while True:
        print("\n📋 Available Operations:")
        print("1. Import CSV files")
        print("2. Show database statistics")
        print("3. Get stock data")
        print("4. Top stocks by volume")
        print("5. Daily market summary")
        print("6. Search stocks")
        print("7. Export data to CSV")
        print("8. Exit")
        
        try:
            choice = input("\n🔢 Enter your choice (1-8): ").strip()
            
            if choice == '1':
                db.import_all_csv_files()
            
            elif choice == '2':
                db.show_database_stats()
            
            elif choice == '3':
                symbol = input("Enter stock symbol: ").strip().upper()
                if symbol:
                    data = db.get_stock_data(symbol)
                    if data:
                        print(f"\n📈 Data for {symbol}:")
                        print("-" * 60)
                        for row in data[:10]:  # Show first 10 records
                            print(f"{row['date']}: Close={row['close_price']}, Volume={row['total_traded_qty']:,}, Delivery%={row['delivery_percentage']:.1f}%")
                        if len(data) > 10:
                            print(f"... and {len(data)-10} more records")
                    else:
                        print(f"❌ No data found for {symbol}")
            
            elif choice == '4':
                date = input("Enter date (DD-Mon-YYYY) or press Enter for all-time: ").strip()
                limit = input("Number of stocks to show (default 10): ").strip() or "10"
                try:
                    limit = int(limit)
                    stocks = db.get_top_stocks_by_volume(date if date else None, limit)
                    print(f"\n🔥 Top {limit} Stocks by Volume" + (f" on {date}" if date else " (All Time)"))
                    print("-" * 60)
                    for i, stock in enumerate(stocks, 1):
                        if date:
                            print(f"{i:2d}. {stock['symbol']:12} Vol: {stock['total_traded_qty']:,} Delivery: {stock['delivery_percentage']:.1f}%")
                        else:
                            print(f"{i:2d}. {stock['symbol']:12} Vol: {stock['total_volume']:,} Avg Delivery: {stock['avg_delivery_percent']:.1f}%")
                except ValueError:
                    print("❌ Invalid number")
            
            elif choice == '5':
                summary = db.get_daily_summary()
                print(f"\n📊 Daily Market Summary:")
                print("-" * 80)
                print(f"{'Date':<12} {'Stocks':<8} {'Volume':<15} {'Turnover':<12} {'Trades':<10} {'Avg Delivery%':<12}")
                print("-" * 80)
                for day in summary:
                    print(f"{day['date']:<12} {day['total_stocks']:<8} {day['total_volume']:>14,} {day['total_turnover']:>11.1f} {day['total_trades']:>9,} {day['avg_delivery_percent']:>11.1f}%")
            
            elif choice == '6':
                pattern = input("Enter search pattern (e.g., 'TATA', 'BANK'): ").strip()
                if pattern:
                    results = db.search_stocks(pattern)
                    print(f"\n🔍 Stocks matching '{pattern}':")
                    print("-" * 40)
                    for stock in results:
                        print(f"{stock['symbol']:<15} ({stock['series']}) - {stock['days_traded']} trading days")
            
            elif choice == '7':
                print("\n📤 Export Options:")
                print("1. All stock data")
                print("2. Daily summary")
                print("3. Custom query")
                
                export_choice = input("Enter choice (1-3): ").strip()
                
                if export_choice == '1':
                    filename = input("Enter filename (e.g., all_data.csv): ").strip() or "all_data.csv"
                    db.export_to_csv("SELECT * FROM stock_data ORDER BY date, symbol", filename)
                
                elif export_choice == '2':
                    filename = input("Enter filename (e.g., daily_summary.csv): ").strip() or "daily_summary.csv"
                    db.export_to_csv("SELECT * FROM daily_summary ORDER BY date", filename)
                
                elif export_choice == '3':
                    query = input("Enter SQL query: ").strip()
                    filename = input("Enter filename: ").strip()
                    if query and filename:
                        db.export_to_csv(query, filename)
            
            elif choice == '8':
                break
            
            else:
                print("❌ Invalid choice. Please enter 1-8.")
                
        except KeyboardInterrupt:
            print("\n\n🛑 Interrupted by user")
            break
        except Exception as e:
            print(f"❌ Error: {e}")
    
    db.close()
    print("\n👋 Thanks for using NSE Database Manager!")

if __name__ == "__main__":
    main()
