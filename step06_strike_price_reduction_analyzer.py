#!/usr/bin/env python3
"""
Step 6: Strike Price 50% Reduction Analyzer
===========================================

This script analyzes strike price reductions by comparing:
1. Selected strikes from step05_strike_analysis table (our 28 test records)
2. All F&O data from step04_fo_udiff_daily table

Goal: Find instances where strike prices show 50% or more reduction
in option prices over time periods.

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
    if not price_data['price_change_pct'].isna().all():
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
        'max_single_day_drop_pct': abs(max_single_day_drop) if max_single_day_drop else 0,
        'max_drop_date': max_drop_date,
        'max_consecutive_drops': max_consecutive_drops,
        'max_consecutive_drop_pct': max_consecutive_drop_pct,
        'has_50_pct_reduction': total_reduction_pct >= 50,
        'price_volatility': price_data['close_price'].std(),
        'avg_daily_volume': price_data['contracts_traded'].mean()
    }

def create_reduction_analysis_table(conn):
    """Create table to store reduction analysis results."""
    drop_sql = "IF OBJECT_ID('step06_price_reduction_analysis', 'U') IS NOT NULL DROP TABLE step06_price_reduction_analysis"
    
    create_sql = """
    CREATE TABLE step06_price_reduction_analysis (
        id BIGINT IDENTITY(1,1) PRIMARY KEY,
        analysis_date DATETIME2 DEFAULT GETDATE(),
        
        -- Strike identification
        equity_symbol NVARCHAR(50) NOT NULL,
        fo_strike_price DECIMAL(18,4) NOT NULL,
        fo_option_type VARCHAR(5) NOT NULL,
        equity_closing_price DECIMAL(18,4),
        strike_rank INT,
        moneyness NVARCHAR(20),
        
        -- Price reduction analysis
        total_trading_days INT,
        max_price DECIMAL(18,4),
        min_price DECIMAL(18,4),
        max_date DATE,
        min_date DATE,
        total_reduction_pct DECIMAL(10,4),
        max_single_day_drop_pct DECIMAL(10,4),
        max_drop_date DATE,
        max_consecutive_drops INT,
        max_consecutive_drop_pct DECIMAL(10,4),
        has_50_pct_reduction BIT,
        price_volatility DECIMAL(18,4),
        avg_daily_volume DECIMAL(18,4),
        
        -- Analysis flags
        reduction_severity NVARCHAR(20),
        risk_category NVARCHAR(20),
        
        INDEX IX_step06_symbol (equity_symbol),
        INDEX IX_step06_reduction (has_50_pct_reduction),
        INDEX IX_step06_severity (reduction_severity)
    )
    """
    
    cursor = conn.cursor()
    try:
        cursor.execute(drop_sql)
        cursor.execute(create_sql)
        conn.commit()
        logging.info("✅ step06_price_reduction_analysis table created")
    except Exception as e:
        logging.error(f"Error creating table: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()

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

def insert_analysis_result(conn, strike_info, analysis_result):
    """Insert analysis result into database."""
    if not analysis_result:
        return False
    
    severity, risk = categorize_reduction(analysis_result)
    
    insert_sql = """
    INSERT INTO step06_price_reduction_analysis (
        equity_symbol, fo_strike_price, fo_option_type, equity_closing_price,
        strike_rank, moneyness,
        total_trading_days, max_price, min_price, max_date, min_date,
        total_reduction_pct, max_single_day_drop_pct, max_drop_date,
        max_consecutive_drops, max_consecutive_drop_pct, has_50_pct_reduction,
        price_volatility, avg_daily_volume,
        reduction_severity, risk_category
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    cursor = conn.cursor()
    try:
        values = (
            strike_info['equity_symbol'],
            float(strike_info['fo_strike_price']),
            strike_info['fo_option_type'],
            float(strike_info['equity_closing_price']),
            int(strike_info['strike_rank']),
            strike_info['moneyness'],
            analysis_result['total_trading_days'],
            float(analysis_result['max_price']),
            float(analysis_result['min_price']),
            analysis_result['max_date'],
            analysis_result['min_date'],
            float(analysis_result['total_reduction_pct']),
            float(analysis_result['max_single_day_drop_pct']),
            analysis_result['max_drop_date'],
            analysis_result['max_consecutive_drops'],
            float(analysis_result['max_consecutive_drop_pct']),
            analysis_result['has_50_pct_reduction'],
            float(analysis_result['price_volatility']) if analysis_result['price_volatility'] else None,
            float(analysis_result['avg_daily_volume']) if analysis_result['avg_daily_volume'] else None,
            severity,
            risk
        )
        
        cursor.execute(insert_sql, values)
        conn.commit()
        return True
        
    except Exception as e:
        logging.error(f"Error inserting analysis result: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()

def main():
    print("🔍 STEP 6: 50% STRIKE PRICE REDUCTION ANALYZER")
    print("=" * 60)
    print("Analyzing 28 selected strikes for 50% price reductions")
    print("Comparing step05_strike_analysis vs step04_fo_udiff_daily")
    
    try:
        # Get selected strikes from step05
        selected_strikes = get_selected_strikes()
        print(f"\n📊 Found {len(selected_strikes)} selected strikes to analyze")
        
        # Setup database
        conn = get_database_connection()
        create_reduction_analysis_table(conn)
        
        total_analyzed = 0
        reductions_found = 0
        severe_reductions = 0
        
        print(f"\n🔍 Analyzing each strike for price reductions...")
        
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
                    # Insert into database
                    inserted = insert_analysis_result(conn, strike_info, analysis)
                    
                    if inserted:
                        total_analyzed += 1
                        
                        reduction_pct = analysis['total_reduction_pct']
                        max_drop = analysis['max_single_day_drop_pct']
                        
                        if reduction_pct >= 50:
                            reductions_found += 1
                            status = "🔴 50%+ REDUCTION"
                            if reduction_pct >= 75:
                                severe_reductions += 1
                                status = "🚨 SEVERE 75%+ REDUCTION"
                        else:
                            status = f"📊 {reduction_pct:.1f}% reduction"
                        
                        print(f"✅ {symbol} {strike} {option_type}: {status} (Max: ₹{analysis['max_price']:.2f} → Min: ₹{analysis['min_price']:.2f})")
                    
                else:
                    print(f"❌ {symbol} {strike} {option_type}: Analysis failed")
                    
            except Exception as e:
                print(f"❌ {symbol} {strike} {option_type}: Error - {e}")
                logging.error(f"Error analyzing {symbol} {strike} {option_type}: {e}")
        
        # Summary results
        print(f"\n🎯 REDUCTION ANALYSIS SUMMARY")
        print("=" * 50)
        print(f"Total strikes analyzed: {total_analyzed}/{len(selected_strikes)}")
        print(f"50%+ reductions found: {reductions_found}")
        print(f"75%+ severe reductions: {severe_reductions}")
        print(f"Reduction rate: {(reductions_found/total_analyzed*100):.1f}%" if total_analyzed > 0 else "N/A")
        
        # Detailed breakdown
        if total_analyzed > 0:
            summary_query = """
            SELECT 
                reduction_severity,
                risk_category,
                COUNT(*) as count,
                AVG(total_reduction_pct) as avg_reduction,
                MAX(total_reduction_pct) as max_reduction
            FROM step06_price_reduction_analysis
            GROUP BY reduction_severity, risk_category
            ORDER BY avg_reduction DESC
            """
            
            summary_df = pd.read_sql(summary_query, conn)
            print(f"\n📈 DETAILED BREAKDOWN:")
            print(summary_df.to_string(index=False))
            
            # Show top reductions
            top_reductions_query = """
            SELECT TOP 10
                equity_symbol,
                fo_strike_price,
                fo_option_type,
                total_reduction_pct,
                max_single_day_drop_pct,
                reduction_severity,
                risk_category
            FROM step06_price_reduction_analysis
            WHERE has_50_pct_reduction = 1
            ORDER BY total_reduction_pct DESC
            """
            
            top_df = pd.read_sql(top_reductions_query, conn)
            if not top_df.empty:
                print(f"\n🔴 TOP 50%+ REDUCTIONS:")
                print(top_df.to_string(index=False))
            else:
                print(f"\n✅ No 50%+ reductions found in the analyzed data")
        
    except Exception as e:
        logging.error(f"Main error: {e}")
        print(f"❌ Error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()