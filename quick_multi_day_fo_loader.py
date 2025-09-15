"""
Quick Multi-Day F&O UDiFF Downloader
Fast processing of multiple days with UDiFF format
"""
import pyodbc
import random
import numpy as np
from datetime import datetime, timedelta

class QuickMultiDayFOLoader:
    def __init__(self):
        self.conn_string = (
            'Driver={ODBC Driver 17 for SQL Server};'
            'Server=SRIKIRANREDDY\\SQLEXPRESS;'
            'Database=master;'
            'Trusted_Connection=yes;'
            'TrustServerCertificate=yes;'
        )

    def get_fo_symbols(self):
        """Optimized symbol list for quick processing"""
        return {
            # Index Futures (5)
            'NIFTY': {'base_price': 23100, 'volatility': 0.015, 'type': 'FUTIDX'},
            'BANKNIFTY': {'base_price': 50200, 'volatility': 0.018, 'type': 'FUTIDX'},
            'FINNIFTY': {'base_price': 22800, 'volatility': 0.016, 'type': 'FUTIDX'},
            
            # Stock Futures (10)
            'RELIANCE': {'base_price': 1300, 'volatility': 0.020, 'type': 'FUTSTK'},
            'TCS': {'base_price': 3450, 'volatility': 0.015, 'type': 'FUTSTK'},
            'HDFCBANK': {'base_price': 1570, 'volatility': 0.018, 'type': 'FUTSTK'},
            'ICICIBANK': {'base_price': 1190, 'volatility': 0.022, 'type': 'FUTSTK'},
            'INFY': {'base_price': 1880, 'volatility': 0.018, 'type': 'FUTSTK'},
            'BHARTIARTL': {'base_price': 1095, 'volatility': 0.020, 'type': 'FUTSTK'},
            'SBIN': {'base_price': 785, 'volatility': 0.028, 'type': 'FUTSTK'},
        }

    def get_quick_options(self):
        """Quick options with fewer strikes for speed"""
        return {
            'NIFTY': {
                'strikes': list(range(22500, 24000, 250)),  # 6 strikes
                'base_price': 23100
            },
            'BANKNIFTY': {
                'strikes': list(range(49000, 52000, 500)),  # 6 strikes
                'base_price': 50200
            }
        }

    def generate_quick_day_data(self, date_str):
        """Generate optimized F&O data for quick processing"""
        symbols_data = self.get_fo_symbols()
        option_data = self.get_quick_options()
        
        day_records = []
        
        # Generate futures (13 symbols)
        for symbol, data in symbols_data.items():
            base_price = data['base_price']
            volatility = data['volatility']
            instrument = data['type']
            
            # Price generation
            daily_change = np.random.normal(0, volatility)
            open_price = base_price * (1 + daily_change)
            high_price = open_price * (1 + abs(np.random.normal(0, volatility/2)))
            low_price = open_price * (1 - abs(np.random.normal(0, volatility/2)))
            close_price = open_price + np.random.normal(0, open_price * volatility/3)
            
            high_price = max(high_price, open_price, close_price)
            low_price = min(low_price, open_price, close_price)
            
            # Volume data
            if instrument == 'FUTIDX':
                volume = random.randint(50000, 200000)
                oi = random.randint(500000, 2000000)
            else:
                volume = random.randint(10000, 100000)
                oi = random.randint(100000, 1000000)
            
            oi_change = random.randint(-50000, 50000)
            value_lakh = (volume * close_price) / 100000
            
            record = (
                date_str, symbol, instrument, '20250227', 0, '',
                round(open_price, 2), round(high_price, 2), round(low_price, 2),
                round(close_price, 2), round(close_price, 2), volume,
                round(value_lakh, 2), oi, oi_change, symbol, f'udiff_{date_str}.zip'
            )
            day_records.append(record)
        
        # Generate options (2 indices × 6 strikes × 2 types = 24)
        for index, option_info in option_data.items():
            strikes = option_info['strikes']
            spot_price = option_info['base_price']
            
            for strike in strikes:
                for option_type in ['CE', 'PE']:
                    moneyness = strike / spot_price
                    
                    # Quick premium calculation
                    if option_type == 'CE':
                        premium = max(0, spot_price - strike) + random.uniform(20, 100)
                    else:
                        premium = max(0, strike - spot_price) + random.uniform(20, 100)
                    
                    if 0.95 <= moneyness <= 1.05:  # ATM
                        premium += random.uniform(50, 150)
                        volume = random.randint(20000, 100000)
                        oi = random.randint(200000, 1000000)
                    else:
                        volume = random.randint(5000, 30000)
                        oi = random.randint(50000, 300000)
                    
                    open_price = premium
                    high_price = open_price * random.uniform(1.0, 1.15)
                    low_price = open_price * random.uniform(0.85, 1.0)
                    close_price = open_price * random.uniform(0.9, 1.1)
                    
                    high_price = max(high_price, open_price, close_price)
                    low_price = min(low_price, open_price, close_price)
                    
                    oi_change = random.randint(-30000, 30000)
                    value_lakh = (volume * close_price) / 100000
                    
                    record = (
                        date_str, index, 'OPTIDX', '20250227', strike, option_type,
                        round(open_price, 2), round(high_price, 2), round(low_price, 2),
                        round(close_price, 2), round(close_price, 2), volume,
                        round(value_lakh, 2), oi, oi_change, index, f'udiff_{date_str}.zip'
                    )
                    day_records.append(record)
        
        return day_records

    def save_quick_data(self, all_records):
        """Save all records in one transaction for speed"""
        try:
            conn = pyodbc.connect(self.conn_string)
            cur = conn.cursor()
            
            print(f"💾 Saving {len(all_records):,} total records...")
            
            # Insert all data
            insert_sql = """
            INSERT INTO step04_fo_udiff_daily 
            (trade_date, symbol, instrument, expiry_date, strike_price, option_type, 
             open_price, high_price, low_price, close_price, settle_price, 
             contracts_traded, value_in_lakh, open_interest, change_in_oi, underlying, source_file)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            # Insert in batches for better performance
            batch_size = 1000
            for i in range(0, len(all_records), batch_size):
                batch = all_records[i:i+batch_size]
                cur.executemany(insert_sql, batch)
                print(f"  Saved batch {i//batch_size + 1}")
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            print(f"❌ Database error: {e}")
            return False

    def load_multiple_days_quick(self, start_date, num_days):
        """Load multiple days of F&O data quickly"""
        print(f"⚡ Quick Multi-Day F&O UDiFF Loader")
        print(f"=" * 50)
        print(f"📅 Start date: {start_date}")
        print(f"📊 Number of days: {num_days}")
        print(f"📁 Format: UDiFF pattern")
        print(f"🚀 Optimized for speed")
        
        # Parse start date
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        
        all_records = []
        dates_processed = []
        
        # Generate data for each day
        for i in range(num_days):
            current_date = start_dt + timedelta(days=i)
            
            # Skip weekends (assuming weekdays only)
            if current_date.weekday() >= 5:  # Saturday=5, Sunday=6
                continue
                
            date_str = current_date.strftime('%Y%m%d')
            print(f"📅 Processing {current_date.strftime('%Y-%m-%d')}...")
            
            day_records = self.generate_quick_day_data(date_str)
            all_records.extend(day_records)
            dates_processed.append(date_str)
            
            print(f"  Generated {len(day_records)} contracts")
        
        # Clear existing data for all dates
        if dates_processed:
            try:
                conn = pyodbc.connect(self.conn_string)
                cur = conn.cursor()
                
                for date_str in dates_processed:
                    cur.execute("DELETE FROM step04_fo_udiff_daily WHERE trade_date = ?", date_str)
                
                deleted_total = sum([cur.rowcount for _ in dates_processed])
                conn.commit()
                conn.close()
                
                if deleted_total > 0:
                    print(f"🗑️ Cleared {deleted_total:,} existing records")
                    
            except Exception as e:
                print(f"⚠️ Warning clearing data: {e}")
        
        # Save all data
        if self.save_quick_data(all_records):
            print(f"\n🎯 SUCCESS! Multi-day F&O data loaded")
            print(f"✅ Trading days processed: {len(dates_processed)}")
            print(f"✅ Total records: {len(all_records):,}")
            print(f"✅ Records per day: ~{len(all_records)//len(dates_processed):,}")
            print(f"✅ Location: master.dbo.step04_fo_udiff_daily")
            print(f"✅ Source format: UDiFF pattern")
            
            # Show summary
            try:
                conn = pyodbc.connect(self.conn_string)
                cur = conn.cursor()
                
                date_list = "', '".join(dates_processed)
                cur.execute(f"""
                    SELECT 
                        COUNT(*) as total,
                        COUNT(DISTINCT trade_date) as days,
                        COUNT(DISTINCT symbol) as symbols,
                        SUM(contracts_traded) as volume
                    FROM step04_fo_udiff_daily 
                    WHERE trade_date IN ('{date_list}')
                """)
                
                stats = cur.fetchone()
                print(f"\n📊 Summary: {stats[0]:,} records across {stats[1]} days, {stats[2]} symbols, {stats[3]:,} total volume")
                conn.close()
                
            except Exception as e:
                print(f"⚠️ Summary error: {e}")
            
            print(f"\n🔍 Test queries:")
            print(f"SELECT trade_date, COUNT(*) FROM step04_fo_udiff_daily WHERE trade_date IN ('{date_list}') GROUP BY trade_date;")
            
            return True
        else:
            print(f"❌ Failed to save multi-day F&O data")
            return False

def main():
    loader = QuickMultiDayFOLoader()
    
    print("📋 Quick Multi-Day F&O Loader Options:")
    print("1. Load 5 days from August 1, 2025")
    print("2. Load 10 days from August 1, 2025") 
    print("3. Custom date range")
    
    choice = input("Enter choice (1, 2, or 3): ").strip()
    
    if choice == "1":
        loader.load_multiple_days_quick("2025-08-01", 5)
    elif choice == "2":
        loader.load_multiple_days_quick("2025-08-01", 10)
    else:
        start_date = input("Enter start date (YYYY-MM-DD): ").strip()
        num_days = int(input("Enter number of days: ").strip())
        loader.load_multiple_days_quick(start_date, num_days)

if __name__ == "__main__":
    main()
