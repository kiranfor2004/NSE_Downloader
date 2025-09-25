#!/usr/bin/env python3
"""
Step 6: Fixed Price Reduction Analyzer
======================================

Fixed version that handles boolean parameters correctly and
provides detailed analysis results even if database insertion fails.

Author: NSE Data Analysis Team
Date: September 2025
"""

import pyodbc
import pandas as pd
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_database_connection():
    connection_string = (
        'Driver={ODBC Driver 17 for SQL Server};'
        'Server=SRIKIRANREDDY\\SQLEXPRESS;'
        'Database=master;'
        'Trusted_Connection=yes;'
    )
    return pyodbc.connect(connection_string)

def get_selected_strikes():
    """Get our selected strikes from step05_strike_analysis."""
    conn = get_database_connection()
    
    query = """
    SELECT DISTINCT
        equity_symbol,
        fo_strike_price,
        fo_option_type,
        equity_closing_price,
        strike_rank,
        moneyness
    FROM step05_strike_analysis
    ORDER BY equity_symbol, strike_rank, fo_option_type
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def get_historical_option_prices(conn, symbol, strike_price, option_type):
    """Get historical prices for a specific strike-option combination."""
    query = """
    SELECT 
        trade_date,
        close_price,
        open_price,
        high_price,
        low_price,
        contracts_traded,
        open_interest,
        expiry_date
    FROM step04_fo_udiff_daily
    WHERE symbol = ? 
    AND strike_price = ? 
    AND option_type = ?
    AND trade_date LIKE '202502%'
    AND instrument = 'STO'
    AND close_price IS NOT NULL
    AND close_price > 0
    ORDER BY trade_date ASC
    """
    
    return pd.read_sql(query, conn, params=[symbol, strike_price, option_type])

def analyze_price_reduction(price_data):
    """Analyze price reduction patterns in the data."""
    if price_data.empty or len(price_data) < 2:
        return None
    
    price_data = price_data.copy()
    price_data['trade_date'] = pd.to_datetime(price_data['trade_date'], format='%Y%m%d')
    price_data = price_data.sort_values('trade_date')
    
    # Calculate daily percentage changes
    price_data['price_change_pct'] = price_data['close_price'].pct_change() * 100
    
    # Find maximum and minimum prices
    max_price = price_data['close_price'].max()
    min_price = price_data['close_price'].min()
    max_date = price_data[price_data['close_price'] == max_price]['trade_date'].iloc[0]
    min_date = price_data[price_data['close_price'] == min_price]['trade_date'].iloc[0]
    
    # Calculate overall reduction from max to min
    if max_price > 0:
        total_reduction_pct = ((max_price - min_price) / max_price) * 100
    else:
        total_reduction_pct = 0
    
    # Find largest single-day drop
    max_single_day_drop = price_data['price_change_pct'].min() if not price_data['price_change_pct'].isna().all() else 0
    max_drop_date = None
    if not price_data['price_change_pct'].isna().all() and pd.notna(max_single_day_drop):
        max_drop_idx = price_data['price_change_pct'].idxmin()
        max_drop_date = price_data.loc[max_drop_idx, 'trade_date']
    
    # Find consecutive drops (multiple days of decline)
    consecutive_drops = 0
    max_consecutive_drops = 0
    consecutive_drop_pct = 0
    max_consecutive_drop_pct = 0
    
    for pct_change in price_data['price_change_pct']:
        if pd.notna(pct_change) and pct_change < 0:
            consecutive_drops += 1
            consecutive_drop_pct += abs(pct_change)
        else:
            if consecutive_drops > max_consecutive_drops:
                max_consecutive_drops = consecutive_drops
                max_consecutive_drop_pct = consecutive_drop_pct
            consecutive_drops = 0
            consecutive_drop_pct = 0
    
    # Check final consecutive drops
    if consecutive_drops > max_consecutive_drops:
        max_consecutive_drops = consecutive_drops
        max_consecutive_drop_pct = consecutive_drop_pct
    
    return {
        'total_trading_days': len(price_data),
        'max_price': max_price,
        'min_price': min_price,
        'max_date': max_date,
        'min_date': min_date,
        'total_reduction_pct': total_reduction_pct,
        'max_single_day_drop_pct': abs(max_single_day_drop) if pd.notna(max_single_day_drop) else 0,
        'max_drop_date': max_drop_date,
        'max_consecutive_drops': max_consecutive_drops,
        'max_consecutive_drop_pct': max_consecutive_drop_pct,
        'has_50_pct_reduction': total_reduction_pct >= 50,
        'price_volatility': price_data['close_price'].std(),
        'avg_daily_volume': price_data['contracts_traded'].mean()
    }

def categorize_reduction(analysis_result):
    """Categorize the severity of price reduction."""
    if not analysis_result:
        return 'No Data', 'Unknown'
    
    total_reduction = analysis_result['total_reduction_pct']
    max_single_drop = analysis_result['max_single_day_drop_pct']
    consecutive_drops = analysis_result['max_consecutive_drops']
    
    # Reduction severity
    if total_reduction >= 75:
        severity = 'Extreme'
    elif total_reduction >= 50:
        severity = 'High'
    elif total_reduction >= 25:
        severity = 'Moderate'
    elif total_reduction >= 10:
        severity = 'Low'
    else:
        severity = 'Minimal'
    
    # Risk category
    if total_reduction >= 50 and max_single_drop >= 20:
        risk = 'Critical'
    elif total_reduction >= 50 or max_single_drop >= 15:
        risk = 'High'
    elif total_reduction >= 25 or consecutive_drops >= 3:
        risk = 'Medium'
    elif total_reduction >= 10:
        risk = 'Low'
    else:
        risk = 'Minimal'
    
    return severity, risk

def display_analysis_result(strike_info, analysis_result):
    """Display analysis result."""
    symbol = strike_info['equity_symbol']
    strike = strike_info['fo_strike_price']
    option_type = strike_info['fo_option_type']
    
    if not analysis_result:
        print(f"❌ {symbol} {strike} {option_type}: No analysis data")
        return False
    
    reduction_pct = analysis_result['total_reduction_pct']
    max_price = analysis_result['max_price']
    min_price = analysis_result['min_price']
    max_drop = analysis_result['max_single_day_drop_pct']
    trading_days = analysis_result['total_trading_days']
    
    severity, risk = categorize_reduction(analysis_result)
    
    if reduction_pct >= 75:
        status = "🚨 SEVERE 75%+ REDUCTION"
        color = "🔴"
    elif reduction_pct >= 50:
        status = "🔴 50%+ REDUCTION"
        color = "🔴"
    elif reduction_pct >= 25:
        status = "🟡 25%+ REDUCTION"
        color = "🟡"
    else:
        status = f"📊 {reduction_pct:.1f}% reduction"
        color = "🟢"
    
    print(f"{color} {symbol} {strike} {option_type}: {status}")
    print(f"   Max: ₹{max_price:.2f} → Min: ₹{min_price:.2f} | Days: {trading_days} | Max Drop: {max_drop:.1f}% | Risk: {risk}")
    
    return reduction_pct >= 50

def main():
    print("🔍 STEP 6: FIXED 50% PRICE REDUCTION ANALYZER")
    print("=" * 60)
    print("Analyzing 28 selected strikes for price reductions")
    print("Comparing step05_strike_analysis vs step04_fo_udiff_daily")
    
    try:
        # Get selected strikes from step05
        selected_strikes = get_selected_strikes()
        print(f"\n📊 Found {len(selected_strikes)} selected strikes to analyze")
        
        conn = get_database_connection()
        
        total_analyzed = 0
        reductions_found = 0
        severe_reductions = 0
        all_results = []
        
        print(f"\n🔍 Analyzing each strike for price reductions...")
        print("-" * 80)
        
        for idx, strike_info in selected_strikes.iterrows():
            symbol = strike_info['equity_symbol']
            strike = strike_info['fo_strike_price']
            option_type = strike_info['fo_option_type']
            
            try:
                # Get historical price data
                price_data = get_historical_option_prices(conn, symbol, strike, option_type)
                
                if price_data.empty:
                    print(f"⚠️  {symbol} {strike} {option_type}: No historical data")
                    continue
                
                # Analyze price reduction
                analysis = analyze_price_reduction(price_data)
                
                if analysis:
                    total_analyzed += 1
                    
                    # Display result
                    has_50_reduction = display_analysis_result(strike_info, analysis)
                    
                    if has_50_reduction:
                        reductions_found += 1
                        if analysis['total_reduction_pct'] >= 75:
                            severe_reductions += 1
                    
                    # Store for summary
                    analysis['symbol'] = symbol
                    analysis['strike'] = strike
                    analysis['option_type'] = option_type
                    analysis['moneyness'] = strike_info['moneyness']
                    all_results.append(analysis)
                    
                else:
                    print(f"❌ {symbol} {strike} {option_type}: Analysis failed")
                    
            except Exception as e:
                print(f"❌ {symbol} {strike} {option_type}: Error - {e}")
                logging.error(f"Error analyzing {symbol} {strike} {option_type}: {e}")
        
        # Summary results
        print(f"\n🎯 REDUCTION ANALYSIS SUMMARY")
        print("=" * 60)
        print(f"Total strikes analyzed: {total_analyzed}/{len(selected_strikes)}")
        print(f"50%+ reductions found: {reductions_found}")
        print(f"75%+ severe reductions: {severe_reductions}")
        print(f"Reduction rate: {(reductions_found/total_analyzed*100):.1f}%" if total_analyzed > 0 else "N/A")
        
        # Detailed breakdown by symbol
        if all_results:
            print(f"\n📈 DETAILED BREAKDOWN BY SYMBOL:")
            print("-" * 60)
            
            # Group by symbol
            symbol_summary = {}
            for result in all_results:
                symbol = result['symbol']
                if symbol not in symbol_summary:
                    symbol_summary[symbol] = {'total': 0, 'reductions': 0, 'avg_reduction': 0}
                
                symbol_summary[symbol]['total'] += 1
                if result['has_50_pct_reduction']:
                    symbol_summary[symbol]['reductions'] += 1
                symbol_summary[symbol]['avg_reduction'] += result['total_reduction_pct']
            
            for symbol, data in symbol_summary.items():
                avg_reduction = data['avg_reduction'] / data['total']
                reduction_rate = (data['reductions'] / data['total']) * 100
                print(f"{symbol}: {data['reductions']}/{data['total']} strikes with 50%+ reduction ({reduction_rate:.1f}%) | Avg reduction: {avg_reduction:.1f}%")
            
            # Show top reductions
            top_reductions = sorted([r for r in all_results if r['has_50_pct_reduction']], 
                                  key=lambda x: x['total_reduction_pct'], reverse=True)
            
            if top_reductions:
                print(f"\n🔴 TOP 50%+ REDUCTIONS:")
                print("-" * 80)
                for i, result in enumerate(top_reductions[:10], 1):
                    severity, risk = categorize_reduction(result)
                    print(f"{i:2d}. {result['symbol']} {result['strike']} {result['option_type']}: "
                          f"{result['total_reduction_pct']:.1f}% reduction | "
                          f"₹{result['max_price']:.2f} → ₹{result['min_price']:.2f} | "
                          f"Risk: {risk} | {result['moneyness']}")
            else:
                print(f"\n✅ No 50%+ reductions found in the analyzed strikes")
            
            # Option type analysis
            ce_reductions = [r for r in all_results if r['option_type'] == 'CE' and r['has_50_pct_reduction']]
            pe_reductions = [r for r in all_results if r['option_type'] == 'PE' and r['has_50_pct_reduction']]
            
            print(f"\n📊 OPTION TYPE BREAKDOWN:")
            print(f"Call Options (CE): {len(ce_reductions)} with 50%+ reduction")
            print(f"Put Options (PE): {len(pe_reductions)} with 50%+ reduction")
        
    except Exception as e:
        logging.error(f"Main error: {e}")
        print(f"❌ Error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()