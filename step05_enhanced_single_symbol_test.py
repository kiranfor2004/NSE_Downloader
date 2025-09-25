import pyodbc
import pandas as pd
import numpy as np
from datetime import datetime

def get_january_delivery_data():
    """Get the highest delivery quantity symbol from January 2025"""
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

def get_february_fo_strikes_enhanced(symbol, target_price):
    """Get F&O data for February 2025 with enhanced metrics"""
    conn = pyodbc.connect('Driver={SQL Server};Server=SRIKIRANREDDY\\SQLEXPRESS;Database=master;Trusted_Connection=yes;')
    
    # Get all available strikes for the symbol in February
    strikes_query = f"""
    SELECT DISTINCT strike_price, option_type, expiry_date, underlying
    FROM step04_fo_udiff_daily 
    WHERE symbol = '{symbol}' 
    AND trade_date >= '20250201' 
    AND trade_date < '20250301'
    ORDER BY strike_price, option_type
    """
    
    strikes_df = pd.read_sql(strikes_query, conn)
    
    if len(strikes_df) == 0:
        print(f"No F&O data found for {symbol} in February 2025")
        conn.close()
        return pd.DataFrame(), [], target_price
    
    # Find the 7 nearest strikes (3 below, 1 closest, 3 above)
    unique_strikes = sorted(strikes_df['strike_price'].unique())
    strike_distances = [(strike, abs(strike - target_price)) for strike in unique_strikes]
    strike_distances.sort(key=lambda x: x[1])  # Sort by distance
    
    # Get the closest strike and its neighbors
    closest_strikes = [s[0] for s in strike_distances[:7]]
    closest_strikes.sort()
    
    print(f"Target price: {target_price}")
    print(f"Selected strikes: {closest_strikes}")
    
    # Get enhanced F&O data for selected strikes
    strikes_str = ','.join([str(s) for s in closest_strikes])
    
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
    ORDER BY strike_price, option_type, trade_date
    """
    
    fo_df = pd.read_sql(enhanced_query, conn)
    conn.close()
    
    return fo_df, closest_strikes, target_price

def calculate_enhanced_metrics(group):
    """Calculate enhanced metrics for each strike-option combination"""
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
    
    # Calculate metrics
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

def store_enhanced_results(delivery_data, fo_data, selected_strikes, target_price):
    """Store enhanced results in the database"""
    conn = pyodbc.connect('Driver={SQL Server};Server=SRIKIRANREDDY\\SQLEXPRESS;Database=master;Trusted_Connection=yes;')
    cursor = conn.cursor()
    
    symbol = delivery_data['symbol']
    analysis_month = delivery_data['analysis_month']
    target_month = '2025-02'
    
    # Group F&O data by strike and option type
    grouped = fo_data.groupby(['strike_price', 'option_type'])
    
    results = []
    
    for (strike_price, option_type), group in grouped:
        # Calculate strike rank (position in the selected strikes list)
        strike_rank = selected_strikes.index(strike_price) + 1
        price_difference = strike_price - target_price
        
        # Calculate enhanced metrics
        metrics = calculate_enhanced_metrics(group)
        
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
    
    # Insert all results
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
        # Convert numpy types to Python native types
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
        
        # Insert all results
        cursor.execute(insert_query, values)
    
    conn.commit()
    conn.close()
    
    print(f"Stored {len(results)} enhanced records for {symbol}")
    return len(results)

def main():
    """Main execution function"""
    try:
        print("=== Step 5 Enhanced F&O Analysis ===")
        print("1. Getting January delivery data...")
        
        # Get delivery data
        delivery_data = get_january_delivery_data()
        print(f"Symbol: {delivery_data['symbol']}")
        print(f"Delivery Qty: {delivery_data['delivery_qty']:,}")
        print(f"Closing Price: ₹{delivery_data['close_price']}")
        print(f"Date: {delivery_data['trade_date']}")
        
        print("\n2. Getting February F&O data with enhanced metrics...")
        
        # Get F&O data
        fo_data, selected_strikes, target_price = get_february_fo_strikes_enhanced(
            delivery_data['symbol'], 
            delivery_data['close_price']
        )
        
        if len(fo_data) == 0:
            print("No F&O data available for this symbol.")
            return
        
        print(f"Found {len(fo_data)} F&O records")
        print(f"Strike-Option combinations: {len(fo_data.groupby(['strike_price', 'option_type']))}")
        
        print("\n3. Storing enhanced results in database...")
        
        # Store results
        records_stored = store_enhanced_results(delivery_data, fo_data, selected_strikes, target_price)
        
        print(f"\n✅ Enhanced analysis completed!")
        print(f"Records stored: {records_stored}")
        
        # Show summary
        print("\n4. Summary of stored data:")
        conn = pyodbc.connect('Driver={SQL Server};Server=SRIKIRANREDDY\\SQLEXPRESS;Database=master;Trusted_Connection=yes;')
        
        summary_query = f"""
        SELECT 
            option_type,
            COUNT(*) as count,
            AVG(avg_fo_close_price) as avg_close,
            AVG(avg_open_interest) as avg_oi,
            AVG(total_contracts_traded) as avg_volume,
            AVG(liquidity_score) as avg_liquidity
        FROM step05_delivery_fo_analysis_enhanced 
        WHERE symbol = '{delivery_data['symbol']}'
        GROUP BY option_type
        ORDER BY option_type
        """
        
        summary_df = pd.read_sql(summary_query, conn)
        print(summary_df.to_string(index=False))
        
        conn.close()
        
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()