import pyodbc
import pandas as pd
import numpy as np
from datetime import datetime

def get_highest_delivery_symbol_january():
    """Get the symbol with highest delivery quantity from January 2025"""
    conn = pyodbc.connect('Driver={SQL Server};Server=SRIKIRANREDDY\\SQLEXPRESS;Database=master;Trusted_Connection=yes;')
    
    query = """
    SELECT TOP 1 
        symbol,
        deliv_qty as delivery_qty,
        close_price,
        peak_date as trade_date,
        analysis_month
    FROM step02_monthly_analysis 
    WHERE analysis_month = '2025-01'
    ORDER BY deliv_qty DESC
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    if len(df) == 0:
        raise ValueError("No delivery data found for January 2025")
    
    return df.iloc[0]

def find_nearest_strikes_for_next_month(symbol, target_price):
    """Find 3 nearest strike prices above and below target price for PE and CE in next month"""
    conn = pyodbc.connect('Driver={SQL Server};Server=SRIKIRANREDDY\\SQLEXPRESS;Database=master;Trusted_Connection=yes;')
    
    print(f"Looking for strikes around target price: ₹{target_price}")
    
    # Get all available strikes for the symbol in February (next month)
    strikes_query = f"""
    SELECT DISTINCT 
        strike_price, 
        option_type, 
        expiry_date, 
        underlying
    FROM step04_fo_udiff_daily 
    WHERE symbol = '{symbol}' 
    AND trade_date >= '20250201' 
    AND trade_date < '20250301'
    AND strike_price IS NOT NULL
    ORDER BY strike_price, option_type
    """
    
    strikes_df = pd.read_sql(strikes_query, conn)
    
    if len(strikes_df) == 0:
        print(f"No F&O data found for {symbol} in February 2025")
        conn.close()
        return pd.DataFrame(), []
    
    # Get unique strikes and find nearest ones
    unique_strikes = sorted(strikes_df['strike_price'].unique())
    print(f"Available strikes: {unique_strikes}")
    
    # Find the 7 closest strikes to target price using distance-based selection
    strike_distances = [(s, abs(s - target_price)) for s in unique_strikes]
    strike_distances.sort(key=lambda x: x[1])  # Sort by distance from target
    
    # Take the 7 closest strikes
    selected_strikes = [s[0] for s in strike_distances[:7]]
    selected_strikes.sort()
    
    print(f"Selected 7 strikes: {selected_strikes}")
    print(f"Target price: ₹{target_price}")
    
    # Show distances for verification
    selected_distances = [(s, abs(s - target_price)) for s in selected_strikes]
    selected_distances.sort(key=lambda x: x[1])
    print("Strike distances from target:")
    for strike, distance in selected_distances:
        print(f"  Strike {strike}: {distance:.2f} away")
    
    # Get comprehensive F&O data for selected strikes
    strikes_str = ','.join([str(s) for s in selected_strikes])
    
    enhanced_query = f"""
    SELECT 
        symbol,
        strike_price,
        option_type,
        expiry_date,
        underlying,
        trade_date,
        open_price,
        high_price,
        low_price,
        close_price,
        settle_price,
        contracts_traded,
        value_in_lakh,
        open_interest,
        change_in_oi
    FROM step04_fo_udiff_daily 
    WHERE symbol = '{symbol}' 
    AND strike_price IN ({strikes_str})
    AND trade_date >= '20250201' 
    AND trade_date < '20250301'
    AND option_type IN ('CE', 'PE')
    ORDER BY strike_price, option_type, trade_date
    """
    
    fo_df = pd.read_sql(enhanced_query, conn)
    conn.close()
    
    return fo_df, selected_strikes

def calculate_comprehensive_metrics(group):
    """Calculate comprehensive metrics for each strike-option combination"""
    if len(group) == 0:
        return {}
    
    # Price metrics
    close_prices = group['close_price'].dropna()
    open_prices = group['open_price'].dropna()
    high_prices = group['high_price'].dropna()
    low_prices = group['low_price'].dropna()
    settle_prices = group['settle_price'].dropna()
    
    # Volume and interest metrics
    contracts_traded = group['contracts_traded'].dropna()
    value_in_lakh = group['value_in_lakh'].dropna()
    open_interest = group['open_interest'].dropna()
    change_in_oi = group['change_in_oi'].dropna()
    
    # Calculate comprehensive metrics
    metrics = {
        'expiry_date': group['expiry_date'].iloc[0] if len(group) > 0 else None,
        'underlying': group['underlying'].iloc[0] if len(group) > 0 else None,
        
        # Price metrics
        'avg_fo_close_price': close_prices.mean() if len(close_prices) > 0 else None,
        'avg_open_price': open_prices.mean() if len(open_prices) > 0 else None,
        'avg_high_price': high_prices.mean() if len(high_prices) > 0 else None,
        'avg_low_price': low_prices.mean() if len(low_prices) > 0 else None,
        'avg_settle_price': settle_prices.mean() if len(settle_prices) > 0 else None,
        'min_close_price': close_prices.min() if len(close_prices) > 0 else None,
        'max_close_price': close_prices.max() if len(close_prices) > 0 else None,
        'price_volatility': close_prices.std() if len(close_prices) > 1 else 0,
        
        # Volume and interest metrics
        'avg_open_interest': open_interest.mean() if len(open_interest) > 0 else None,
        'max_open_interest': open_interest.max() if len(open_interest) > 0 else None,
        'min_open_interest': open_interest.min() if len(open_interest) > 0 else None,
        'avg_change_in_oi': change_in_oi.mean() if len(change_in_oi) > 0 else None,
        'total_contracts_traded': contracts_traded.sum() if len(contracts_traded) > 0 else None,
        'avg_contracts_traded': contracts_traded.mean() if len(contracts_traded) > 0 else None,
        'total_value_in_lakh': value_in_lakh.sum() if len(value_in_lakh) > 0 else None,
        'avg_value_in_lakh': value_in_lakh.mean() if len(value_in_lakh) > 0 else None,
        
        # Activity metrics
        'trade_days_count': len(group),
        'active_trading_days': len(group[group['contracts_traded'] > 0]) if 'contracts_traded' in group.columns else 0,
        'liquidity_score': (contracts_traded.mean() * value_in_lakh.mean() / 1000) if len(contracts_traded) > 0 and len(value_in_lakh) > 0 else 0
    }
    
    return metrics

def store_14_records_in_enhanced_table(delivery_data, fo_data, selected_strikes):
    """Store exactly 14 records (7 strikes × 2 option types) in enhanced table"""
    conn = pyodbc.connect('Driver={SQL Server};Server=SRIKIRANREDDY\\SQLEXPRESS;Database=master;Trusted_Connection=yes;')
    cursor = conn.cursor()
    
    symbol = delivery_data['symbol']
    analysis_month = delivery_data['analysis_month']
    target_month = '2025-02'
    target_price = delivery_data['close_price']
    
    # Clear existing data for this symbol
    cursor.execute("DELETE FROM step05_delivery_fo_analysis_enhanced WHERE symbol = ? AND analysis_month = ?", 
                   (symbol, analysis_month))
    
    # Group F&O data by strike and option type
    grouped = fo_data.groupby(['strike_price', 'option_type'])
    
    results = []
    
    for (strike_price, option_type), group in grouped:
        # Calculate strike rank (position in the selected strikes list)
        strike_rank = selected_strikes.index(strike_price) + 1
        price_difference = strike_price - target_price
        
        # Calculate comprehensive metrics
        metrics = calculate_comprehensive_metrics(group)
        
        # Prepare data for insertion
        result = {
            'analysis_month': analysis_month,
            'symbol': symbol,
            'delivery_qty': int(delivery_data['delivery_qty']),
            'delivery_close_price': float(delivery_data['close_price']),
            'delivery_peak_date': delivery_data['trade_date'],
            'target_month': target_month,
            'strike_price': float(strike_price),
            'option_type': option_type,
            'strike_rank': strike_rank,
            'price_difference': float(price_difference),
            **metrics
        }
        
        results.append(result)
    
    # Insert exactly 14 records
    insert_query = """
    INSERT INTO step05_delivery_fo_analysis_enhanced (
        analysis_month, symbol, delivery_qty, delivery_close_price, delivery_peak_date,
        target_month, strike_price, option_type, strike_rank, price_difference,
        expiry_date, underlying,
        avg_fo_close_price, avg_open_price, avg_high_price, avg_low_price, avg_settle_price,
        min_close_price, max_close_price, price_volatility,
        avg_open_interest, max_open_interest, min_open_interest, avg_change_in_oi,
        total_contracts_traded, avg_contracts_traded, total_value_in_lakh, avg_value_in_lakh,
        trade_days_count, active_trading_days, liquidity_score
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    for result in results:
        # Convert all values to proper Python types
        values = [
            result['analysis_month'],
            result['symbol'],
            result['delivery_qty'],
            result['delivery_close_price'],
            result['delivery_peak_date'],
            result['target_month'],
            result['strike_price'],
            result['option_type'],
            result['strike_rank'],
            result['price_difference'],
            result['expiry_date'],
            result['underlying'],
            float(result['avg_fo_close_price']) if result['avg_fo_close_price'] is not None else None,
            float(result['avg_open_price']) if result['avg_open_price'] is not None else None,
            float(result['avg_high_price']) if result['avg_high_price'] is not None else None,
            float(result['avg_low_price']) if result['avg_low_price'] is not None else None,
            float(result['avg_settle_price']) if result['avg_settle_price'] is not None else None,
            float(result['min_close_price']) if result['min_close_price'] is not None else None,
            float(result['max_close_price']) if result['max_close_price'] is not None else None,
            float(result['price_volatility']) if result['price_volatility'] is not None else None,
            int(result['avg_open_interest']) if result['avg_open_interest'] is not None else None,
            int(result['max_open_interest']) if result['max_open_interest'] is not None else None,
            int(result['min_open_interest']) if result['min_open_interest'] is not None else None,
            int(result['avg_change_in_oi']) if result['avg_change_in_oi'] is not None else None,
            int(result['total_contracts_traded']) if result['total_contracts_traded'] is not None else None,
            int(result['avg_contracts_traded']) if result['avg_contracts_traded'] is not None else None,
            int(result['total_value_in_lakh']) if result['total_value_in_lakh'] is not None else None,
            int(result['avg_value_in_lakh']) if result['avg_value_in_lakh'] is not None else None,
            result['trade_days_count'],
            result['active_trading_days'],
            float(result['liquidity_score']) if result['liquidity_score'] is not None else None
        ]
        
        cursor.execute(insert_query, values)
    
    conn.commit()
    conn.close()
    
    print(f"✅ Stored {len(results)} records for {symbol}")
    return len(results)

def main():
    """Main execution function for Step 5 Enhanced Analysis"""
    try:
        print("🚀 === STEP 5 ENHANCED F&O ANALYSIS ===")
        print("=" * 50)
        
        print("1️⃣ Getting highest delivery quantity symbol from January 2025...")
        
        # Get delivery data
        delivery_data = get_highest_delivery_symbol_january()
        print(f"✅ Selected Symbol: {delivery_data['symbol']}")
        print(f"📦 Delivery Qty: {delivery_data['delivery_qty']:,}")
        print(f"💰 Closing Price: ₹{delivery_data['close_price']}")
        print(f"📅 Peak Date: {delivery_data['trade_date']}")
        
        print("\n2️⃣ Finding nearest strike prices in next month (February)...")
        
        # Get F&O data for next month
        fo_data, selected_strikes = find_nearest_strikes_for_next_month(
            delivery_data['symbol'], 
            delivery_data['close_price']
        )
        
        if len(fo_data) == 0:
            print("❌ No F&O data available for this symbol.")
            return
        
        print(f"✅ Found {len(fo_data)} F&O records")
        
        # Check strike-option combinations
        combinations = fo_data.groupby(['strike_price', 'option_type']).size()
        print(f"📊 Strike-Option combinations: {len(combinations)}")
        
        print("\n3️⃣ Storing 14 records in enhanced table...")
        
        # Store results
        records_stored = store_14_records_in_enhanced_table(delivery_data, fo_data, selected_strikes)
        
        print(f"\n🎯 === ANALYSIS COMPLETED ===")
        print(f"✅ Records stored: {records_stored}")
        print(f"🎯 Expected: 14 records (7 strikes × 2 option types)")
        
        # Show final summary
        print("\n4️⃣ Final Summary:")
        conn = pyodbc.connect('Driver={SQL Server};Server=SRIKIRANREDDY\\SQLEXPRESS;Database=master;Trusted_Connection=yes;')
        
        summary_query = f"""
        SELECT 
            option_type,
            COUNT(*) as count,
            AVG(CAST(avg_fo_close_price AS FLOAT)) as avg_close,
            AVG(CAST(avg_open_interest AS FLOAT)) as avg_oi,
            AVG(CAST(total_contracts_traded AS FLOAT)) as avg_volume
        FROM step05_delivery_fo_analysis_enhanced 
        WHERE symbol = '{delivery_data['symbol']}'
        GROUP BY option_type
        ORDER BY option_type
        """
        
        summary_df = pd.read_sql(summary_query, conn)
        print(summary_df.to_string(index=False))
        
        # Show strike distribution
        strikes_query = f"""
        SELECT 
            strike_price,
            COUNT(*) as option_count,
            STRING_AGG(option_type, ', ') as option_types
        FROM step05_delivery_fo_analysis_enhanced 
        WHERE symbol = '{delivery_data['symbol']}'
        GROUP BY strike_price
        ORDER BY strike_price
        """
        
        strikes_df = pd.read_sql(strikes_query, conn)
        print(f"\n📈 Strike Distribution:")
        print(strikes_df.to_string(index=False))
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()