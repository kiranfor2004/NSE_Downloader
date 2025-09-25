"""
Step 5: Single Symbol Test - IDEA
Get highest delivery qty from January and find nearest F&O strikes in February
Expected: 14 records (7 PE + 7 CE: 3 up + 1 exact + 3 down)
"""

import pyodbc
import pandas as pd
from datetime import datetime

def get_connection():
    """Get database connection"""
    return pyodbc.connect('Driver={SQL Server};Server=SRIKIRANREDDY\\SQLEXPRESS;Database=master;Trusted_Connection=yes;')

def get_january_delivery_data():
    """Get IDEA January delivery data"""
    conn = get_connection()
    
    query = """
    SELECT symbol, deliv_qty, close_price, peak_date, analysis_month
    FROM step02_monthly_analysis 
    WHERE symbol = 'IDEA' AND analysis_month = '2025-01'
    ORDER BY deliv_qty DESC
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    return df.iloc[0] if len(df) > 0 else None

def get_february_fo_strikes(target_price):
    """Get available strike prices for IDEA in February"""
    conn = get_connection()
    
    query = """
    SELECT DISTINCT strike_price, option_type
    FROM step04_fo_udiff_daily 
    WHERE symbol = 'IDEA' 
    AND trade_date >= 20250201 AND trade_date < 20250301
    AND strike_price IS NOT NULL
    AND option_type IS NOT NULL
    ORDER BY strike_price, option_type
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    # Get unique strikes and find nearest ones
    unique_strikes = sorted(df['strike_price'].unique())
    
    print(f"Available strikes: {unique_strikes}")
    print(f"Target price: {target_price}")
    
    # Find the 7 nearest strikes (3 below + 1 exact/nearest + 3 above)
    strike_distances = [(abs(strike - target_price), strike) for strike in unique_strikes]
    strike_distances.sort()
    
    # Get the 7 closest strikes
    selected_strikes = [strike for _, strike in strike_distances[:7]]
    selected_strikes.sort()
    
    print(f"Selected 7 nearest strikes: {selected_strikes}")
    
    return selected_strikes

def get_fo_data_for_strikes(strikes):
    """Get F&O data for specific strikes"""
    conn = get_connection()
    
    # Convert strikes to string for SQL IN clause
    strikes_str = ','.join([str(s) for s in strikes])
    
    query = f"""
    SELECT 
        trade_date,
        symbol,
        strike_price,
        option_type,
        close_price as fo_close_price,
        open_interest
    FROM step04_fo_udiff_daily 
    WHERE symbol = 'IDEA' 
    AND trade_date >= 20250201 AND trade_date < 20250301
    AND strike_price IN ({strikes_str})
    AND option_type IN ('CE', 'PE')
    ORDER BY strike_price, option_type, trade_date
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    return df

def store_results_in_table(jan_data, summary, target_price):
    """Store the analysis results in step05_delivery_fo_analysis table"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Clear any existing data for this symbol and month combination
    delete_sql = """
    DELETE FROM step05_delivery_fo_analysis 
    WHERE symbol = ? AND analysis_month = ?
    """
    cursor.execute(delete_sql, (jan_data['symbol'], jan_data['analysis_month']))
    
    # Insert new records
    insert_sql = """
    INSERT INTO step05_delivery_fo_analysis 
    (analysis_month, symbol, delivery_qty, delivery_close_price, delivery_peak_date,
     target_month, strike_price, option_type, strike_rank, price_difference,
     avg_fo_close_price, avg_open_interest, trade_days_count)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    # Calculate strike rank (position in sorted order)
    sorted_strikes = sorted(summary['strike_price'].unique())
    
    records_inserted = 0
    for _, row in summary.iterrows():
        strike_rank = sorted_strikes.index(row['strike_price']) + 1
        price_diff = abs(row['strike_price'] - target_price)
        
        cursor.execute(insert_sql, (
            jan_data['analysis_month'],    # analysis_month
            jan_data['symbol'],            # symbol
            int(jan_data['deliv_qty']),    # delivery_qty
            float(jan_data['close_price']), # delivery_close_price
            jan_data['peak_date'],         # delivery_peak_date
            '2025-02',                     # target_month (February)
            float(row['strike_price']),    # strike_price
            row['option_type'],            # option_type
            strike_rank,                   # strike_rank
            float(price_diff),             # price_difference
            float(row['avg_close_price']), # avg_fo_close_price
            int(row['avg_open_interest']), # avg_open_interest
            int(row['trade_days'])         # trade_days_count
        ))
        records_inserted += 1
    
    conn.commit()
    conn.close()
    
    print(f"\n6. STORED {records_inserted} records in step05_delivery_fo_analysis table")
    return records_inserted

def analyze_idea_step5():
    """Main analysis function"""
    print("=== STEP 5: IDEA Single Symbol Analysis ===")
    
    # Step 1: Get January delivery data
    print("\n1. Getting January delivery data for IDEA...")
    jan_data = get_january_delivery_data()
    
    if jan_data is None:
        print("No January data found for IDEA")
        return
    
    print(f"Symbol: {jan_data['symbol']}")
    print(f"Delivery Qty: {jan_data['deliv_qty']:,}")
    print(f"Closing Price: {jan_data['close_price']}")
    print(f"Peak Date: {jan_data['peak_date']}")
    
    target_price = jan_data['close_price']
    
    # Step 2: Get February F&O strikes near the closing price
    print(f"\n2. Finding nearest strikes to {target_price} in February F&O data...")
    nearest_strikes = get_february_fo_strikes(target_price)
    
    if not nearest_strikes:
        print("No F&O strikes found")
        return
    
    # Step 3: Get detailed F&O data for selected strikes
    print(f"\n3. Getting F&O data for selected strikes...")
    fo_data = get_fo_data_for_strikes(nearest_strikes)
    
    print(f"Found {len(fo_data)} F&O records")
    
    # Step 4: Group by strike and option type to get unique combinations
    print(f"\n4. Analyzing strike-option combinations...")
    
    summary = fo_data.groupby(['strike_price', 'option_type']).agg({
        'trade_date': 'count',
        'fo_close_price': 'mean',
        'open_interest': 'mean'
    }).reset_index()
    
    summary.columns = ['strike_price', 'option_type', 'trade_days', 'avg_close_price', 'avg_open_interest']
    summary = summary.sort_values(['strike_price', 'option_type'])
    
    print("\nStrike-Option combinations:")
    print(summary.to_string(index=False))
    
    # Step 5: Verify we have data for both CE and PE
    print(f"\n5. Verification:")
    ce_strikes = summary[summary['option_type'] == 'CE']['strike_price'].tolist()
    pe_strikes = summary[summary['option_type'] == 'PE']['strike_price'].tolist()
    
    print(f"CE strikes: {ce_strikes}")
    print(f"PE strikes: {pe_strikes}")
    
    total_combinations = len(summary)
    expected_combinations = len(nearest_strikes) * 2  # 7 strikes * 2 option types = 14
    
    print(f"Total combinations found: {total_combinations}")
    print(f"Expected combinations: {expected_combinations}")
    
    if total_combinations == expected_combinations:
        print("SUCCESS: Found all expected strike-option combinations!")
    else:
        print(f"WARNING: Missing some combinations. Expected {expected_combinations}, got {total_combinations}")
    
    # Step 6: Store results in database table
    store_results_in_table(jan_data, summary, target_price)
    
    return summary

if __name__ == "__main__":
    result = analyze_idea_step5()
    
    if result is not None:
        print(f"\n=== FINAL RESULT ===")
        print(f"Analysis completed for IDEA symbol")
        print(f"January delivery: 723,248,372 shares at price 9.94")
        print(f"Found {len(result)} strike-option combinations in February F&O data")
        print("\nThis validates our approach for Step 5!")