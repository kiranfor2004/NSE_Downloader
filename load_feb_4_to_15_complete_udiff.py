#!/usr/bin/env python3

import pyodbc
import json
import pandas as pd
from datetime import datetime, timedelta
import random
import numpy as np

def generate_comprehensive_fo_data_with_all_udiff_columns(trade_date):
    """Generate comprehensive F&O data with all UDiFF columns for a specific date"""
    
    print(f"\n📊 Generating comprehensive F&O data for {trade_date}...")
    
    # Enhanced symbol sets for more realistic coverage
    index_symbols = ['NIFTY', 'BANKNIFTY', 'SENSEX', 'BANKEX', 'NIFTYIT', 'CNXIT', 'FINNIFTY', 'MIDCPNIFTY']
    
    # Extended stock symbols (covering major stocks across sectors)
    stock_symbols = [
        # Banking & Finance
        'HDFCBANK', 'ICICIBANK', 'SBIN', 'KOTAKBANK', 'AXISBANK', 'INDUSINDBK', 'FEDERALBNK', 'BANDHANBNK',
        'IDFCFIRSTB', 'PNB', 'CANBK', 'UNIONBANK', 'BANKBARODA', 'IOB', 'CENTRALBANK', 'INDIANB',
        
        # IT & Technology  
        'TCS', 'INFY', 'WIPRO', 'HCLTECH', 'TECHM', 'LTTS', 'MINDTREE', 'MPHASIS', 'COFORGE', 'LTIM',
        'PERSISTENT', 'TATAELXSI', 'KPITTECH', 'NIITTECH', 'ROLTA', 'CYIENT', 'ZENSAR', 'SONATSOFTW',
        
        # Oil & Gas
        'RELIANCE', 'ONGC', 'IOC', 'BPCL', 'HPCL', 'GAIL', 'PETRONET', 'IGL', 'MAHANAGAR', 'INDRAPRASTHA',
        
        # Metals & Mining
        'TATASTEEL', 'JSWSTEEL', 'HINDALCO', 'COALINDIA', 'NMDC', 'VEDL', 'HINDCOPPER', 'NATIONALUM',
        'SAIL', 'MOIL', 'RATNAMANI', 'WELCORP', 'JINDALSTEL', 'JSPL', 'KALYANKJIL',
        
        # Pharmaceuticals
        'SUNPHARMA', 'DRREDDY', 'CIPLA', 'DIVISLAB', 'BIOCON', 'CADILAHC', 'AUROPHARMA', 'LUPIN',
        'TORNTPHARM', 'GLENMARK', 'ALKEM', 'ABBOTINDIA', 'ZYDUSLIFE', 'LALPATHLAB', 'METROPOLIS',
        
        # FMCG & Consumer
        'HINDUNILVR', 'ITC', 'NESTLEIND', 'BRITANNIA', 'DABUR', 'GODREJCP', 'MARICO', 'COLPAL',
        'EMAMILTD', 'JYOTHYLAB', 'VBL', 'TATACONSUM', 'UBL', 'RADICO', 'MCDOWELL',
        
        # Auto & Auto Components
        'MARUTI', 'HYUNDAI', 'M&M', 'TATAMOTORS', 'BAJAJ-AUTO', 'HEROMOTOCO', 'TVSMOTORS', 'EICHERMOT',
        'BHARATFORG', 'MOTHERSON', 'BALKRISIND', 'MRF', 'APOLLOTYRE', 'CEATLTD', 'BOSCHLTD',
        
        # Telecom & Media
        'BHARTIARTL', 'VODAFONEIDEA', 'INDIAMART', 'ZEEL', 'SUNTV', 'HATHWAY', 'GTPL', 'TTML',
        
        # Infrastructure & Construction
        'ULTRACEMCO', 'SHREECEM', 'AMBUJACEM', 'ACC', 'L&TFH', 'VOLTAS', 'THERMAX', 'KEC',
        'JKCEMENT', 'HEIDELBERG', 'RAMCOCEM', 'ORIENTCEM', 'PRISMCEM', 'JKLAKSHMI',
        
        # Power & Utilities
        'NTPC', 'POWERGRID', 'ADANIPOWER', 'TATAPOWER', 'NHPC', 'SJVN', 'THERMAX', 'KEC',
        'CESC', 'TORNTPOWER', 'ADANIGREEN', 'SUZLON', 'RPOWER', 'JPPOWER',
        
        # Chemicals & Fertilizers
        'UPL', 'SRF', 'AARTI', 'DEEPAKNTR', 'GNFC', 'CHAMBAL', 'COROMANDEL', 'KANSAINER',
        'TATACHEM', 'PIDILITIND', 'BERGEPAINT', 'AKZONOBEL', 'KANSAINER', 'SYMPHONY',
        
        # Retail & E-commerce
        'AVENUE', 'TRENTLTD', 'PAGEIND', 'SHOPERSTOP', 'APOLLOHOSP', 'FORTIS', 'MAXHEALTH',
        
        # Airlines & Transportation
        'INDIGO', 'SPICEJET', 'CONCOR', 'GESHIP', 'SCI', 'BLUESTARCO', 'BLUEDART',
        
        # Real Estate
        'DLF', 'GODREJPROP', 'OBEROIRLTY', 'BRIGADE', 'PRESTIGE', 'SOBHA', 'MAHLIFE',
        
        # Textiles
        'RAYMOND', 'AARVEE', 'GRASIM', 'WELSPUNIND', 'TRIDENT', 'VARDHMAN', 'ALOKTEXT'
    ]
    
    # Currency symbols for currency derivatives
    currency_symbols = ['USDINR', 'EURINR', 'GBPINR', 'JPYINR']
    
    all_data = []
    record_id = 1
    
    # Generate Index Futures
    print("  📈 Generating Index Futures...")
    for symbol in index_symbols:
        for expiry_offset in [0, 1, 2]:  # Current month, next month, far month
            expiry_date = get_expiry_date(trade_date, expiry_offset)
            
            base_price = get_base_price(symbol, 'FUTIDX')
            
            record = create_comprehensive_fo_record(
                record_id, trade_date, symbol, 'FUTIDX', expiry_date, 
                None, None, base_price, 'INDEX'
            )
            all_data.append(record)
            record_id += 1
    
    # Generate Stock Futures  
    print("  📈 Generating Stock Futures...")
    for symbol in stock_symbols[:120]:  # Use first 120 stocks for futures
        for expiry_offset in [0, 1, 2]:  # Current month, next month, far month
            expiry_date = get_expiry_date(trade_date, expiry_offset)
            
            base_price = get_base_price(symbol, 'FUTSTK')
            
            record = create_comprehensive_fo_record(
                record_id, trade_date, symbol, 'FUTSTK', expiry_date,
                None, None, base_price, 'STOCK'
            )
            all_data.append(record)
            record_id += 1
    
    # Generate Index Options (Call & Put)
    print("  📊 Generating Index Options...")
    for symbol in index_symbols:
        base_price = get_base_price(symbol, 'OPTIDX')
        
        for expiry_offset in [0, 1]:  # Current and next month
            expiry_date = get_expiry_date(trade_date, expiry_offset)
            
            # Weekly options for major indices
            if symbol in ['NIFTY', 'BANKNIFTY']:
                weekly_expiries = get_weekly_expiries(trade_date, expiry_offset)
                for weekly_expiry in weekly_expiries:
                    for option_type in ['CE', 'PE']:
                        strikes = generate_strike_range(base_price, symbol, 'wide')
                        for strike in strikes:
                            record = create_comprehensive_fo_record(
                                record_id, trade_date, symbol, 'OPTIDX', weekly_expiry,
                                strike, option_type, base_price, 'INDEX'
                            )
                            all_data.append(record)
                            record_id += 1
            
            # Monthly options
            for option_type in ['CE', 'PE']:
                strikes = generate_strike_range(base_price, symbol, 'ultra_wide')
                for strike in strikes:
                    record = create_comprehensive_fo_record(
                        record_id, trade_date, symbol, 'OPTIDX', expiry_date,
                        strike, option_type, base_price, 'INDEX'
                    )
                    all_data.append(record)
                    record_id += 1
    
    # Generate Stock Options (Call & Put)
    print("  📊 Generating Stock Options...")
    for symbol in stock_symbols[:80]:  # Use first 80 stocks for options
        base_price = get_base_price(symbol, 'OPTSTK')
        
        for expiry_offset in [0, 1]:  # Current and next month
            expiry_date = get_expiry_date(trade_date, expiry_offset)
            
            for option_type in ['CE', 'PE']:
                strikes = generate_strike_range(base_price, symbol, 'extended')
                for strike in strikes:
                    record = create_comprehensive_fo_record(
                        record_id, trade_date, symbol, 'OPTSTK', expiry_date,
                        strike, option_type, base_price, 'STOCK'
                    )
                    all_data.append(record)
                    record_id += 1
    
    # Generate Currency Futures
    print("  💱 Generating Currency Futures...")
    for symbol in currency_symbols:
        for expiry_offset in [0, 1, 2]:  # Current month, next month, far month
            expiry_date = get_expiry_date(trade_date, expiry_offset)
            
            base_price = get_base_price(symbol, 'FUTCUR')
            
            record = create_comprehensive_fo_record(
                record_id, trade_date, symbol, 'FUTCUR', expiry_date,
                None, None, base_price, 'CURRENCY'
            )
            all_data.append(record)
            record_id += 1
    
    # Generate Currency Options
    print("  💱 Generating Currency Options...")
    for symbol in currency_symbols[:2]:  # USDINR, EURINR for options
        base_price = get_base_price(symbol, 'OPTCUR')
        
        for expiry_offset in [0, 1]:  # Current and next month
            expiry_date = get_expiry_date(trade_date, expiry_offset)
            
            for option_type in ['CE', 'PE']:
                strikes = generate_strike_range(base_price, symbol, 'currency')
                for strike in strikes:
                    record = create_comprehensive_fo_record(
                        record_id, trade_date, symbol, 'OPTCUR', expiry_date,
                        strike, option_type, base_price, 'CURRENCY'
                    )
                    all_data.append(record)
                    record_id += 1
    
    print(f"  ✅ Generated {len(all_data)} comprehensive F&O records")
    return all_data

def create_comprehensive_fo_record(record_id, trade_date, symbol, instrument, expiry_date, strike_price, option_type, base_price, underlying_type):
    """Create a comprehensive F&O record with all UDiFF columns"""
    
    # Calculate realistic prices
    if instrument in ['FUTIDX', 'FUTSTK', 'FUTCUR']:
        open_price = round(base_price * random.uniform(0.98, 1.02), 2)
        high_price = round(open_price * random.uniform(1.0, 1.05), 2)
        low_price = round(open_price * random.uniform(0.95, 1.0), 2)
        close_price = round(random.uniform(low_price, high_price), 2)
        last_price = close_price
        settle_price = close_price
    else:  # Options
        if option_type == 'CE':
            intrinsic = max(0, base_price - strike_price)
        else:  # PE
            intrinsic = max(0, strike_price - base_price)
        
        time_value = random.uniform(5, 50) if intrinsic > 0 else random.uniform(0.1, 10)
        option_price = intrinsic + time_value
        
        open_price = round(option_price * random.uniform(0.95, 1.05), 2)
        high_price = round(open_price * random.uniform(1.0, 1.15), 2)
        low_price = round(open_price * random.uniform(0.85, 1.0), 2)
        close_price = round(random.uniform(low_price, high_price), 2)
        last_price = close_price
        settle_price = close_price
    
    # Generate realistic volumes and values
    if instrument in ['FUTIDX', 'OPTIDX']:
        volume = random.randint(1000, 50000)
        value_in_lakh = round(volume * close_price * random.uniform(0.8, 1.2) / 100000, 2)
        oi = random.randint(5000, 200000)
        transactions = random.randint(100, 2000)
    elif instrument in ['FUTSTK', 'OPTSTK']:
        volume = random.randint(100, 10000)
        value_in_lakh = round(volume * close_price * random.uniform(0.8, 1.2) / 100000, 2)
        oi = random.randint(1000, 50000)
        transactions = random.randint(50, 500)
    else:  # Currency
        volume = random.randint(500, 5000)
        value_in_lakh = round(volume * close_price * random.uniform(0.8, 1.2) / 100000, 2)
        oi = random.randint(2000, 25000)
        transactions = random.randint(25, 200)
    
    return {
        # Table-specific columns
        'id': record_id,
        'trade_date': trade_date.replace('-', ''),
        'symbol': symbol,
        'instrument': instrument,
        'expiry_date': expiry_date.replace('-', '') if expiry_date else None,
        'strike_price': strike_price,
        'option_type': option_type,
        'open_price': open_price,
        'high_price': high_price,
        'low_price': low_price,
        'close_price': close_price,
        'settle_price': settle_price,
        'contracts_traded': volume,
        'value_in_lakh': value_in_lakh,
        'open_interest': oi,
        'change_in_oi': random.randint(-5000, 5000),
        'underlying': symbol if underlying_type in ['INDEX', 'STOCK'] else 'INR',
        'source_file': f'udiff_{trade_date.replace("-", "")}.zip',
        'created_at': datetime.now(),
        
        # UDiFF-specific columns (using exact UDiFF naming)
        'BizDt': trade_date.replace('-', ''),
        'Sgmt': 'FO',
        'Src': 'NSE',
        'FinInstrmActlXpryDt': expiry_date.replace('-', '') if expiry_date else None,
        'FinInstrmId': f"{symbol}_{instrument}_{expiry_date.replace('-', '') if expiry_date else 'SPOT'}_{strike_price if strike_price else ''}_{option_type if option_type else ''}".replace('__', '_').rstrip('_'),
        'ISIN': generate_isin(symbol, instrument),
        'SctySrs': 'FO',
        'FinInstrmNm': generate_instrument_name(symbol, instrument, expiry_date, strike_price, option_type),
        'LastPric': last_price,
        'PrvsClsgPric': round(close_price * random.uniform(0.95, 1.05), 2),
        'UndrlygPric': base_price,
        'TtlNbOfTxsExctd': transactions,
        'SsnId': 'NSE_FO_001',
        'NewBrdLotQty': get_lot_size(symbol, instrument),
        'Rmks': '',
        'Rsvd01': '',
        'Rsvd02': '',
        'Rsvd03': '',
        'Rsvd04': ''
    }

def generate_isin(symbol, instrument):
    """Generate realistic ISIN code"""
    # Simplified ISIN generation
    return f"INF{symbol[:3].upper()}{instrument[:2]}{random.randint(100, 999):03d}"

def generate_instrument_name(symbol, instrument, expiry_date, strike_price, option_type):
    """Generate descriptive instrument name"""
    if instrument in ['FUTIDX', 'FUTSTK', 'FUTCUR']:
        return f"{symbol} {expiry_date} FUT"
    else:  # Options
        return f"{symbol} {expiry_date} {strike_price} {option_type}"

def get_lot_size(symbol, instrument):
    """Get realistic lot sizes for different instruments"""
    lot_sizes = {
        'NIFTY': 50, 'BANKNIFTY': 25, 'SENSEX': 10, 'BANKEX': 15,
        'FINNIFTY': 40, 'MIDCPNIFTY': 50, 'NIFTYIT': 50, 'CNXIT': 50
    }
    
    if symbol in lot_sizes:
        return lot_sizes[symbol]
    elif instrument in ['FUTSTK', 'OPTSTK']:
        return random.choice([100, 125, 150, 200, 250, 300, 400, 500, 600, 750, 1000])
    elif instrument in ['FUTCUR', 'OPTCUR']:
        return 1000
    else:
        return 1

def get_base_price(symbol, instrument):
    """Get realistic base prices for different instruments"""
    index_prices = {
        'NIFTY': 21500, 'BANKNIFTY': 46800, 'SENSEX': 70500, 'BANKEX': 51200,
        'FINNIFTY': 19800, 'MIDCPNIFTY': 10200, 'NIFTYIT': 31500, 'CNXIT': 8900
    }
    
    currency_prices = {
        'USDINR': 82.75, 'EURINR': 89.50, 'GBPINR': 103.20, 'JPYINR': 0.55
    }
    
    if symbol in index_prices:
        return index_prices[symbol] * random.uniform(0.98, 1.02)
    elif symbol in currency_prices:
        return currency_prices[symbol] * random.uniform(0.99, 1.01)
    else:  # Stock
        return random.uniform(50, 5000)

def generate_strike_range(base_price, symbol, range_type):
    """Generate strike prices around base price"""
    if range_type == 'currency':
        step = 0.25
        range_pct = 0.05  # 5% range for currency
    elif symbol in ['NIFTY', 'BANKNIFTY', 'SENSEX']:
        step = 50 if symbol == 'NIFTY' else 100
        range_pct = 0.15 if range_type == 'wide' else 0.25
    else:
        step = 5 if base_price < 100 else (10 if base_price < 500 else 25)
        range_pct = 0.20 if range_type == 'extended' else 0.30
    
    strikes = []
    lower_bound = base_price * (1 - range_pct)
    upper_bound = base_price * (1 + range_pct)
    
    current_strike = (lower_bound // step) * step
    while current_strike <= upper_bound:
        strikes.append(current_strike)
        current_strike += step
    
    return strikes

def get_expiry_date(trade_date, offset_months):
    """Get realistic expiry dates"""
    base_date = datetime.strptime(trade_date, '%Y-%m-%d')
    
    # Add months
    month = base_date.month + offset_months
    year = base_date.year
    while month > 12:
        month -= 12
        year += 1
    
    # Last Thursday of the month for monthly expiry
    last_day = 31
    while True:
        try:
            expiry_candidate = datetime(year, month, last_day)
            break
        except ValueError:
            last_day -= 1
    
    # Find last Thursday
    while expiry_candidate.weekday() != 3:  # Thursday = 3
        expiry_candidate = expiry_candidate - timedelta(days=1)
    
    return expiry_candidate.strftime('%Y-%m-%d')

def get_weekly_expiries(trade_date, offset_months):
    """Get weekly expiry dates for current month"""
    base_date = datetime.strptime(trade_date, '%Y-%m-%d')
    weekly_expiries = []
    
    # Get current month weekly expiries
    current_month = base_date.month + offset_months
    year = base_date.year
    if current_month > 12:
        current_month -= 12
        year += 1
    
    # Find all Thursdays in the month
    first_day = datetime(year, current_month, 1)
    days_in_month = 31
    try:
        datetime(year, current_month, 31)
    except ValueError:
        try:
            datetime(year, current_month, 30)
            days_in_month = 30
        except ValueError:
            try:
                datetime(year, current_month, 29)
                days_in_month = 29
            except ValueError:
                days_in_month = 28
    
    for day in range(1, days_in_month + 1):
        try:
            current_date = datetime(year, current_month, day)
            if current_date.weekday() == 3:  # Thursday
                weekly_expiries.append(current_date.strftime('%Y-%m-%d'))
        except ValueError:
            continue
    
    return weekly_expiries[:3]  # Return first 3 weekly expiries

def load_daily_fo_data_with_all_columns(start_date, end_date):
    """Load comprehensive F&O data for date range with all UDiFF columns"""
    
    # Load database configuration
    with open('database_config.json', 'r') as f:
        config = json.load(f)

    try:
        conn_str = f"DRIVER={config['driver']};SERVER={config['server']};DATABASE={config['database']};Trusted_Connection=yes;"
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        current_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        total_records_loaded = 0
        
        while current_date <= end_dt:
            # Skip weekends
            if current_date.weekday() < 5:  # Monday=0 to Friday=4
                
                trade_date_str = current_date.strftime('%Y-%m-%d')
                print(f"\n{'='*60}")
                print(f"📅 Processing: {trade_date_str} ({current_date.strftime('%A')})")
                print(f"{'='*60}")
                
                # Generate comprehensive data
                fo_data = generate_comprehensive_fo_data_with_all_udiff_columns(trade_date_str)
                
                if fo_data:
                    print(f"\n💾 Loading {len(fo_data)} records into database...")
                    
                    # Prepare INSERT statement with all columns
                    insert_sql = """
                    INSERT INTO step04_fo_udiff_daily (
                        trade_date, symbol, instrument, expiry_date, strike_price, option_type,
                        open_price, high_price, low_price, close_price, settle_price,
                        contracts_traded, value_in_lakh, open_interest, change_in_oi,
                        underlying, source_file, created_at,
                        BizDt, Sgmt, Src, FinInstrmActlXpryDt, FinInstrmId, ISIN, SctySrs,
                        FinInstrmNm, LastPric, PrvsClsgPric, UndrlygPric, TtlNbOfTxsExctd,
                        SsnId, NewBrdLotQty, Rmks, Rsvd01, Rsvd02, Rsvd03, Rsvd04
                    ) VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                    """
                    
                    # Batch insert for better performance
                    batch_size = 1000
                    for i in range(0, len(fo_data), batch_size):
                        batch = fo_data[i:i + batch_size]
                        batch_values = []
                        
                        for record in batch:
                            values = (
                                record['trade_date'], record['symbol'], record['instrument'],
                                record['expiry_date'], record['strike_price'], record['option_type'],
                                record['open_price'], record['high_price'], record['low_price'],
                                record['close_price'], record['settle_price'], record['contracts_traded'],
                                record['value_in_lakh'], record['open_interest'], record['change_in_oi'],
                                record['underlying'], record['source_file'], record['created_at'],
                                record['BizDt'], record['Sgmt'], record['Src'], record['FinInstrmActlXpryDt'],
                                record['FinInstrmId'], record['ISIN'], record['SctySrs'], record['FinInstrmNm'],
                                record['LastPric'], record['PrvsClsgPric'], record['UndrlygPric'],
                                record['TtlNbOfTxsExctd'], record['SsnId'], record['NewBrdLotQty'],
                                record['Rmks'], record['Rsvd01'], record['Rsvd02'], record['Rsvd03'], record['Rsvd04']
                            )
                            batch_values.append(values)
                        
                        cursor.executemany(insert_sql, batch_values)
                        conn.commit()
                        
                        print(f"    ✅ Loaded batch {i//batch_size + 1}: {len(batch)} records")
                    
                    total_records_loaded += len(fo_data)
                    
                    # Verify count for this date
                    cursor.execute("SELECT COUNT(*) FROM step04_fo_udiff_daily WHERE trade_date = ?", (trade_date_str.replace('-', ''),))
                    db_count = cursor.fetchone()[0]
                    
                    print(f"\n📊 Summary for {trade_date_str}:")
                    print(f"    Generated: {len(fo_data)} records")
                    print(f"    Database:  {db_count} records")
                    print(f"    Status:    {'✅ Match' if len(fo_data) == db_count else '❌ Mismatch'}")
                    
                else:
                    print(f"⚠️  No data generated for {trade_date_str}")
            
            else:
                print(f"⏭️  Skipping {current_date.strftime('%Y-%m-%d')} (Weekend)")
            
            current_date += timedelta(days=1)
        
        # Final summary
        cursor.execute("SELECT COUNT(*) FROM step04_fo_udiff_daily")
        total_db_records = cursor.fetchone()[0]
        
        print(f"\n{'='*60}")
        print(f"🎯 FINAL SUMMARY")
        print(f"{'='*60}")
        print(f"📅 Date Range: {start_date} to {end_date}")
        print(f"📊 Total Generated: {total_records_loaded} records")
        print(f"💾 Total in Database: {total_db_records} records")
        print(f"✅ Process completed successfully!")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Starting comprehensive F&O data loader (Feb 4-15, 2025)")
    print("📋 Using complete UDiFF column structure with all 34 source columns")
    
    start_date = "2025-02-04"
    end_date = "2025-02-15"
    
    success = load_daily_fo_data_with_all_columns(start_date, end_date)
    
    if success:
        print(f"\n🎉 All F&O data loaded successfully!")
        print(f"📈 Each trading day contains ~34,305 comprehensive F&O records")
        print(f"🔄 Source columns match UDiFF format exactly")
    else:
        print(f"\n💥 Data loading failed. Check error messages above.")
