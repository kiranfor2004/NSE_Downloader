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

def find_nearest_strikes_comprehensive_fo_data(symbol, target_price):
    """Get comprehensive F&O data including ALL columns from step04_fo_udiff_daily"""
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
    
    # Find the 7 closest strikes to target price
    unique_strikes = sorted(strikes_df['strike_price'].unique())
    print(f"Available strikes: {unique_strikes}")
    
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
    
    # Get ALL F&O data columns for selected strikes
    strikes_str = ','.join([str(s) for s in selected_strikes])
    
    comprehensive_query = f"""
    SELECT 
        symbol,
        strike_price,
        option_type,
        expiry_date,
        underlying,
        instrument,
        trade_date,
        open_price,
        high_price,
        low_price,
        close_price,
        settle_price,
        contracts_traded,
        value_in_lakh,
        open_interest,
        change_in_oi,
        source_file,
        BizDt,
        Sgmt,
        Src,
        FininstrmActlXpryDt,
        FinInstrmId,
        ISIN,
        SctySrs,
        FinInstrmNm,
        LastPric,
        PrvsClsgPric,
        UndrlygPric,
        TtlNbOfTxsExctd,
        SsnId,
        NewBrdLotQty,
        Rmks,
        Rsvd1,
        Rsvd2,
        Rsvd3,
        Rsvd4
    FROM step04_fo_udiff_daily 
    WHERE symbol = '{symbol}' 
    AND strike_price IN ({strikes_str})
    AND trade_date >= '20250201' 
    AND trade_date < '20250301'
    AND option_type IN ('CE', 'PE')
    ORDER BY strike_price, option_type, trade_date
    """
    
    fo_df = pd.read_sql(comprehensive_query, conn)
    conn.close()
    
    return fo_df, selected_strikes

def calculate_all_comprehensive_metrics(group):
    """Calculate comprehensive metrics including ALL F&O columns and trade_date info"""
    if len(group) == 0:
        return {}
    
    # Basic metrics
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
    
    # Additional price metrics
    last_prices = group['LastPric'].dropna()
    prev_close_prices = group['PrvsClsgPric'].dropna()
    underlying_prices = group['UndrlygPric'].dropna()
    
    # Transaction metrics
    total_txns = group['TtlNbOfTxsExctd'].dropna()
    new_board_lots = group['NewBrdLotQty'].dropna()
    
    # Trade date metrics
    trade_dates = group['trade_date'].dropna().sort_values()
    first_trade_date = trade_dates.iloc[0] if len(trade_dates) > 0 else None
    last_trade_date = trade_dates.iloc[-1] if len(trade_dates) > 0 else None
    total_trading_days = len(trade_dates.unique()) if len(trade_dates) > 0 else 0
    
    # For trade_date, use the most recent trade date
    latest_trade_date = last_trade_date
    # Create comma-separated list of all unique trade dates
    all_trade_dates_list = ','.join(sorted(trade_dates.unique().astype(str))) if len(trade_dates) > 0 else None
    
    # Get latest values for categorical/text fields
    latest_record = group.iloc[-1] if len(group) > 0 else None
    
    # Calculate comprehensive metrics
    metrics = {
        # Core F&O info
        'expiry_date': latest_record['expiry_date'] if latest_record is not None else None,
        'underlying': latest_record['underlying'] if latest_record is not None else None,
        'instrument': latest_record['instrument'] if latest_record is not None else None,
        
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
        
        # Additional price metrics
        'avg_last_price': last_prices.mean() if len(last_prices) > 0 else None,
        'avg_previous_close_price': prev_close_prices.mean() if len(prev_close_prices) > 0 else None,
        'avg_underlying_price': underlying_prices.mean() if len(underlying_prices) > 0 else None,
        'total_transactions_executed': total_txns.sum() if len(total_txns) > 0 else None,
        'avg_transactions_executed': total_txns.mean() if len(total_txns) > 0 else None,
        'session_id': latest_record['SsnId'] if latest_record is not None else None,
        'new_board_lot_qty': new_board_lots.iloc[0] if len(new_board_lots) > 0 else None,
        
        # Financial instrument details
        'fin_instrm_actual_expiry_dt': latest_record['FininstrmActlXpryDt'] if latest_record is not None else None,
        'fin_instrm_id': latest_record['FinInstrmId'] if latest_record is not None else None,
        'isin': latest_record['ISIN'] if latest_record is not None else None,
        'security_series': latest_record['SctySrs'] if latest_record is not None else None,
        'fin_instrm_name': latest_record['FinInstrmNm'] if latest_record is not None else None,
        
        # Market segment details
        'business_date': latest_record['BizDt'] if latest_record is not None else None,
        'segment': latest_record['Sgmt'] if latest_record is not None else None,
        'source': latest_record['Src'] if latest_record is not None else None,
        
        # Additional fields
        'remarks': latest_record['Rmks'] if latest_record is not None else None,
        'reserved1': latest_record['Rsvd1'] if latest_record is not None else None,
        'reserved2': latest_record['Rsvd2'] if latest_record is not None else None,
        'reserved3': latest_record['Rsvd3'] if latest_record is not None else None,
        'reserved4': latest_record['Rsvd4'] if latest_record is not None else None,
        'source_file': latest_record['source_file'] if latest_record is not None else None,
        
        # Trade date metrics (NEW)
        'first_trade_date': first_trade_date,
        'last_trade_date': last_trade_date,
        'total_trading_days': total_trading_days,
        'trade_date': latest_trade_date,
        'all_trade_dates': all_trade_dates_list,
        
        # Activity metrics
        'trade_days_count': len(group),
        'active_trading_days': len(group[group['contracts_traded'] > 0]) if 'contracts_traded' in group.columns else 0,
        'liquidity_score': (contracts_traded.mean() * value_in_lakh.mean() / 1000) if len(contracts_traded) > 0 and len(value_in_lakh) > 0 else 0
    }
    
    return metrics

def store_comprehensive_results(delivery_data, fo_data, selected_strikes):
    """Store comprehensive results with ALL F&O columns"""
    conn = pyodbc.connect('Driver={SQL Server};Server=SRIKIRANREDDY\\SQLEXPRESS;Database=master;Trusted_Connection=yes;')
    cursor = conn.cursor()
    
    symbol = delivery_data['symbol']
    analysis_month = delivery_data['analysis_month']
    target_month = '2025-02'
    target_price = delivery_data['close_price']
    
    # Clear existing data for this symbol
    cursor.execute("DELETE FROM step05_delivery_fo_analysis_comprehensive WHERE symbol = ? AND analysis_month = ?", 
                   (symbol, analysis_month))
    
    # Group F&O data by strike and option type
    grouped = fo_data.groupby(['strike_price', 'option_type'])
    
    results = []
    
    for (strike_price, option_type), group in grouped:
        # Calculate strike rank
        strike_rank = selected_strikes.index(strike_price) + 1
        price_difference = strike_price - target_price
        
        # Calculate comprehensive metrics
        metrics = calculate_all_comprehensive_metrics(group)
        
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
    
    # Insert comprehensive records
    insert_query = """
    INSERT INTO step05_delivery_fo_analysis_comprehensive (
        analysis_month, symbol, delivery_qty, delivery_close_price, delivery_peak_date,
        target_month, strike_price, option_type, strike_rank, price_difference,
        expiry_date, underlying, instrument,
        avg_fo_close_price, avg_open_price, avg_high_price, avg_low_price, avg_settle_price,
        min_close_price, max_close_price, price_volatility,
        avg_open_interest, max_open_interest, min_open_interest, avg_change_in_oi,
        total_contracts_traded, avg_contracts_traded, total_value_in_lakh, avg_value_in_lakh,
        avg_last_price, avg_previous_close_price, avg_underlying_price,
        total_transactions_executed, avg_transactions_executed, session_id, new_board_lot_qty,
        fin_instrm_actual_expiry_dt, fin_instrm_id, isin, security_series, fin_instrm_name,
        business_date, segment, source,
        remarks, reserved1, reserved2, reserved3, reserved4, source_file,
        first_trade_date, last_trade_date, total_trading_days, trade_date, all_trade_dates,
        trade_days_count, active_trading_days, liquidity_score
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    for result in results:
        # Convert all values to proper Python types for database insertion
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
            result['instrument'],
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
            float(result['avg_last_price']) if result['avg_last_price'] is not None else None,
            float(result['avg_previous_close_price']) if result['avg_previous_close_price'] is not None else None,
            float(result['avg_underlying_price']) if result['avg_underlying_price'] is not None else None,
            int(result['total_transactions_executed']) if result['total_transactions_executed'] is not None else None,
            int(result['avg_transactions_executed']) if result['avg_transactions_executed'] is not None else None,
            result['session_id'],
            int(result['new_board_lot_qty']) if result['new_board_lot_qty'] is not None else None,
            result['fin_instrm_actual_expiry_dt'],
            result['fin_instrm_id'],
            result['isin'],
            result['security_series'],
            result['fin_instrm_name'],
            result['business_date'],
            result['segment'],
            result['source'],
            result['remarks'],
            result['reserved1'],
            result['reserved2'],
            result['reserved3'],
            result['reserved4'],
            result['source_file'],
            result['first_trade_date'],
            result['last_trade_date'],
            result['total_trading_days'],
            result['trade_date'],
            result['all_trade_dates'],
            result['trade_days_count'],
            result['active_trading_days'],
            float(result['liquidity_score']) if result['liquidity_score'] is not None else None
        ]
        
        cursor.execute(insert_query, values)
    
    conn.commit()
    conn.close()
    
    print(f"✅ Stored {len(results)} comprehensive records for {symbol}")
    return len(results)

def main():
    """Main execution function for Comprehensive Step 5 Analysis"""
    try:
        print("🚀 === STEP 5 COMPREHENSIVE F&O ANALYSIS ===")
        print("=" * 60)
        
        print("1️⃣ Getting highest delivery quantity symbol from January 2025...")
        
        # Get delivery data
        delivery_data = get_highest_delivery_symbol_january()
        print(f"✅ Selected Symbol: {delivery_data['symbol']}")
        print(f"📦 Delivery Qty: {delivery_data['delivery_qty']:,}")
        print(f"💰 Closing Price: ₹{delivery_data['close_price']}")
        print(f"📅 Peak Date: {delivery_data['trade_date']}")
        
        print("\n2️⃣ Getting comprehensive F&O data with ALL columns...")
        
        # Get comprehensive F&O data
        fo_data, selected_strikes = find_nearest_strikes_comprehensive_fo_data(
            delivery_data['symbol'], 
            delivery_data['close_price']
        )
        
        if len(fo_data) == 0:
            print("❌ No F&O data available for this symbol.")
            return
        
        print(f"✅ Found {len(fo_data)} F&O records")
        print(f"📊 F&O data columns: {len(fo_data.columns)}")
        
        # Check strike-option combinations
        combinations = fo_data.groupby(['strike_price', 'option_type']).size()
        print(f"📈 Strike-Option combinations: {len(combinations)}")
        
        print("\n3️⃣ Storing comprehensive analysis with ALL F&O columns...")
        
        # Store comprehensive results
        records_stored = store_comprehensive_results(delivery_data, fo_data, selected_strikes)
        
        print(f"\n🎯 === COMPREHENSIVE ANALYSIS COMPLETED ===")
        print(f"✅ Records stored: {records_stored}")
        print(f"🎯 Expected: 14 records (7 strikes × 2 option types)")
        print(f"📊 Total columns stored: 55 (including ALL F&O columns)")
        
        # Show comprehensive summary
        print("\n4️⃣ Comprehensive Summary:")
        conn = pyodbc.connect('Driver={SQL Server};Server=SRIKIRANREDDY\\SQLEXPRESS;Database=master;Trusted_Connection=yes;')
        
        summary_query = f"""
        SELECT 
            option_type,
            COUNT(*) as count,
            AVG(CAST(avg_fo_close_price AS FLOAT)) as avg_close,
            AVG(CAST(avg_open_interest AS FLOAT)) as avg_oi,
            AVG(CAST(total_contracts_traded AS FLOAT)) as avg_volume,
            AVG(CAST(avg_underlying_price AS FLOAT)) as avg_underlying
        FROM step05_delivery_fo_analysis_comprehensive 
        WHERE symbol = '{delivery_data['symbol']}'
        GROUP BY option_type
        ORDER BY option_type
        """
        
        summary_df = pd.read_sql(summary_query, conn)
        print(summary_df.to_string(index=False))
        
        # Show additional comprehensive data
        details_query = f"""
        SELECT TOP 2
            strike_price,
            option_type,
            isin,
            fin_instrm_name,
            segment,
            avg_last_price,
            new_board_lot_qty
        FROM step05_delivery_fo_analysis_comprehensive 
        WHERE symbol = '{delivery_data['symbol']}'
        ORDER BY strike_price, option_type
        """
        
        details_df = pd.read_sql(details_query, conn)
        print(f"\n📋 Sample Comprehensive Data:")
        print(details_df.to_string(index=False))
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()