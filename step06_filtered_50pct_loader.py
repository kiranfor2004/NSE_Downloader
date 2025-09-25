#!/usr/bin/env python3
"""
Step 6: Filtered 50%+ Reduction Database Loader
===============================================

This script creates a refined database table that ONLY stores strike records
with 50% or more price reduction, with a specific column to mark them.

Features:
- Only loads records with 50%+ reduction
- Adds specific reduction_flag column
- Cleaner, focused dataset for analysis
- Enhanced filtering and validation

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

def create_filtered_reduction_table(conn):
    """Create table specifically for 50%+ reduction records only."""
    drop_sql = "IF OBJECT_ID('step06_filtered_50pct_reductions', 'U') IS NOT NULL DROP TABLE step06_filtered_50pct_reductions"
    
    create_sql = """
    CREATE TABLE step06_filtered_50pct_reductions (
        id BIGINT IDENTITY(1,1) PRIMARY KEY,
        analysis_date DATETIME2 DEFAULT GETDATE(),
        
        -- Strike identification
        equity_symbol NVARCHAR(50) NOT NULL,
        fo_strike_price DECIMAL(18,4) NOT NULL,
        fo_option_type VARCHAR(5) NOT NULL,
        equity_closing_price DECIMAL(18,4),
        fo_close_price DECIMAL(18,4),
        fo_trade_date VARCHAR(10),
        strike_rank INT,
        moneyness NVARCHAR(20),
        
        -- Reduction specifics (only 50%+ records)
        reduction_percentage DECIMAL(10,4) NOT NULL,
        reduction_flag VARCHAR(20) DEFAULT '50_PERCENT_PLUS' NOT NULL,
        reduction_category VARCHAR(20) NOT NULL,
        
        -- Price details
        max_price DECIMAL(18,4) NOT NULL,
        min_price DECIMAL(18,4) NOT NULL,
        price_drop_amount DECIMAL(18,4) NOT NULL,
        max_date DATE,
        min_date DATE,
        
        -- Risk metrics
        max_single_day_drop_pct DECIMAL(10,4),
        max_drop_date DATE,
        max_consecutive_drops INT,
        risk_level VARCHAR(20),
        
        -- Trading details
        total_trading_days INT,
        avg_daily_volume DECIMAL(18,4),
        price_volatility DECIMAL(18,4),
        
        -- Time decay analysis
        expiry_impact VARCHAR(50),
        theta_decay_severity VARCHAR(20),
        
        -- Validation
        qualified_for_50pct BIT DEFAULT 1,
        
        INDEX IX_reduction_symbol (equity_symbol),
        INDEX IX_reduction_percentage (reduction_percentage),
        INDEX IX_reduction_flag (reduction_flag),
        INDEX IX_reduction_category (reduction_category),
        INDEX IX_risk_level (risk_level)
    )
    """
    
    cursor = conn.cursor()
    try:
        cursor.execute(drop_sql)
        cursor.execute(create_sql)
        conn.commit()
        logging.info("✅ step06_filtered_50pct_reductions table created")
        print("✅ Filtered 50%+ reduction table created")
    except Exception as e:
        logging.error(f"Error creating table: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()

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

def analyze_for_50pct_reduction(price_data):
    """Analyze and return data ONLY if 50%+ reduction is found."""
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
        price_drop_amount = max_price - min_price
    else:
        return None
    
    # ✅ FILTER: Only proceed if 50%+ reduction
    if total_reduction_pct < 50:
        return None
    
    # Find largest single-day drop
    max_single_day_drop = price_data['price_change_pct'].min() if not price_data['price_change_pct'].isna().all() else 0
    max_drop_date = None
    if not price_data['price_change_pct'].isna().all() and pd.notna(max_single_day_drop):
        max_drop_idx = price_data['price_change_pct'].idxmin()
        max_drop_date = price_data.loc[max_drop_idx, 'trade_date']
    
    # Find consecutive drops
    consecutive_drops = 0
    max_consecutive_drops = 0
    
    for pct_change in price_data['price_change_pct']:
        if pd.notna(pct_change) and pct_change < 0:
            consecutive_drops += 1
        else:
            if consecutive_drops > max_consecutive_drops:
                max_consecutive_drops = consecutive_drops
            consecutive_drops = 0
    
    # Check final consecutive drops
    if consecutive_drops > max_consecutive_drops:
        max_consecutive_drops = consecutive_drops
    
    # Categorize reduction (only 50%+ get categorized)
    if total_reduction_pct >= 90:
        reduction_category = 'EXTREME_90_PLUS'
        theta_decay_severity = 'CRITICAL'
    elif total_reduction_pct >= 75:
        reduction_category = 'SEVERE_75_PLUS'
        theta_decay_severity = 'HIGH'
    elif total_reduction_pct >= 60:
        reduction_category = 'HIGH_60_PLUS'
        theta_decay_severity = 'MODERATE'
    else:  # 50-59%
        reduction_category = 'MODERATE_50_PLUS'
        theta_decay_severity = 'LOW'
    
    # Risk level
    if total_reduction_pct >= 75 and abs(max_single_day_drop) >= 20:
        risk_level = 'CRITICAL'
    elif total_reduction_pct >= 75 or abs(max_single_day_drop) >= 15:
        risk_level = 'HIGH'
    elif total_reduction_pct >= 60:
        risk_level = 'MEDIUM'
    else:
        risk_level = 'MODERATE'
    
    # Expiry impact
    days_to_analyze = len(price_data)
    if total_reduction_pct > 90 and days_to_analyze < 40:
        expiry_impact = 'SEVERE_THETA_DECAY_SHORT_TERM'
    elif total_reduction_pct > 75:
        expiry_impact = 'HIGH_TIME_DECAY'
    elif total_reduction_pct > 60:
        expiry_impact = 'MODERATE_TIME_DECAY'
    else:
        expiry_impact = 'STANDARD_TIME_DECAY'
    
    # Volume analysis
    avg_volume = price_data['contracts_traded'].mean()
    volatility = price_data['close_price'].std()
    
    return {
        'reduction_percentage': total_reduction_pct,
        'reduction_category': reduction_category,
        'max_price': max_price,
        'min_price': min_price,
        'price_drop_amount': price_drop_amount,
        'max_date': max_date,
        'min_date': min_date,
        'max_single_day_drop_pct': abs(max_single_day_drop) if pd.notna(max_single_day_drop) else 0,
        'max_drop_date': max_drop_date,
        'max_consecutive_drops': max_consecutive_drops,
        'risk_level': risk_level,
        'total_trading_days': len(price_data),
        'avg_daily_volume': avg_volume,
        'price_volatility': volatility,
        'expiry_impact': expiry_impact,
        'theta_decay_severity': theta_decay_severity,
        'qualified_for_50pct': True,
        'fo_close_price': price_data['close_price'].iloc[-1],  # Latest close price
        'fo_trade_date': price_data['trade_date'].iloc[-1].strftime('%Y%m%d') if pd.notna(price_data['trade_date'].iloc[-1]) else None
    }

def insert_50pct_reduction_record(conn, strike_info, analysis_result):
    """Insert ONLY qualified 50%+ reduction records."""
    if not analysis_result:
        return False
    
    insert_sql = """
    INSERT INTO step06_filtered_50pct_reductions (
        equity_symbol, fo_strike_price, fo_option_type, equity_closing_price,
        fo_close_price, fo_trade_date,
        strike_rank, moneyness,
        reduction_percentage, reduction_category,
        max_price, min_price, price_drop_amount, max_date, min_date,
        max_single_day_drop_pct, max_drop_date, max_consecutive_drops, risk_level,
        total_trading_days, avg_daily_volume, price_volatility,
        expiry_impact, theta_decay_severity, qualified_for_50pct
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    cursor = conn.cursor()
    try:
        values = (
            strike_info['equity_symbol'],
            float(strike_info['fo_strike_price']),
            strike_info['fo_option_type'],
            float(strike_info['equity_closing_price']),
            float(analysis_result['fo_close_price']) if pd.notna(analysis_result['fo_close_price']) else None,
            analysis_result['fo_trade_date'],
            int(strike_info['strike_rank']),
            strike_info['moneyness'],
            float(analysis_result['reduction_percentage']),
            analysis_result['reduction_category'],
            float(analysis_result['max_price']),
            float(analysis_result['min_price']),
            float(analysis_result['price_drop_amount']),
            analysis_result['max_date'].date() if pd.notna(analysis_result['max_date']) else None,
            analysis_result['min_date'].date() if pd.notna(analysis_result['min_date']) else None,
            float(analysis_result['max_single_day_drop_pct']),
            analysis_result['max_drop_date'].date() if pd.notna(analysis_result['max_drop_date']) else None,
            analysis_result['max_consecutive_drops'],
            analysis_result['risk_level'],
            analysis_result['total_trading_days'],
            float(analysis_result['avg_daily_volume']) if pd.notna(analysis_result['avg_daily_volume']) else None,
            float(analysis_result['price_volatility']) if pd.notna(analysis_result['price_volatility']) else None,
            analysis_result['expiry_impact'],
            analysis_result['theta_decay_severity'],
            1  # qualified_for_50pct = True
        )
        
        cursor.execute(insert_sql, values)
        conn.commit()
        return True
        
    except Exception as e:
        logging.error(f"Error inserting 50%+ reduction record: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()

def main():
    print("🔍 STEP 6: FILTERED 50%+ REDUCTION DATABASE LOADER")
    print("=" * 65)
    print("Creating refined table with ONLY 50%+ reduction records")
    print("Adding specific reduction_flag column for identification")
    
    try:
        # Setup database
        conn = get_database_connection()
        create_filtered_reduction_table(conn)
        
        # Get selected strikes
        selected_strikes = get_selected_strikes()
        print(f"\n📊 Analyzing {len(selected_strikes)} strikes for 50%+ reductions...")
        
        total_analyzed = 0
        qualified_records = 0
        rejected_records = 0
        
        print(f"\n🔍 Processing (filtering for 50%+ reductions only)...")
        print("-" * 65)
        
        for idx, strike_info in selected_strikes.iterrows():
            symbol = strike_info['equity_symbol']
            strike = strike_info['fo_strike_price']
            option_type = strike_info['fo_option_type']
            
            try:
                # Get historical price data
                price_data = get_historical_option_prices(conn, symbol, strike, option_type)
                
                if price_data.empty:
                    print(f"⚠️  {symbol} {strike} {option_type}: No data - skipping")
                    rejected_records += 1
                    continue
                
                # Analyze for 50%+ reduction (returns None if < 50%)
                analysis = analyze_for_50pct_reduction(price_data)
                
                if analysis:  # Only if 50%+ reduction found
                    # Insert into database
                    inserted = insert_50pct_reduction_record(conn, strike_info, analysis)
                    
                    if inserted:
                        qualified_records += 1
                        reduction_pct = analysis['reduction_percentage']
                        category = analysis['reduction_category']
                        risk = analysis['risk_level']
                        
                        # Status based on reduction level
                        if reduction_pct >= 90:
                            status = "🚨 EXTREME"
                        elif reduction_pct >= 75:
                            status = "🔴 SEVERE"
                        elif reduction_pct >= 60:
                            status = "🟡 HIGH"
                        else:
                            status = "🟢 QUALIFIED"
                        
                        print(f"✅ {symbol} {strike} {option_type}: {status} ({reduction_pct:.1f}%) - {category} - {risk} RISK")
                    else:
                        rejected_records += 1
                        print(f"❌ {symbol} {strike} {option_type}: Database insert failed")
                else:
                    # Less than 50% reduction - filtered out
                    rejected_records += 1
                    print(f"🚫 {symbol} {strike} {option_type}: < 50% reduction - filtered out")
                
                total_analyzed += 1
                
            except Exception as e:
                rejected_records += 1
                print(f"❌ {symbol} {strike} {option_type}: Error - {e}")
                logging.error(f"Error processing {symbol} {strike} {option_type}: {e}")
        
        # Final summary
        print(f"\n📊 FILTERED DATABASE LOADING SUMMARY")
        print("=" * 55)
        print(f"Total strikes analyzed: {total_analyzed}")
        print(f"✅ Qualified 50%+ reductions: {qualified_records}")
        print(f"🚫 Rejected (< 50% or errors): {rejected_records}")
        print(f"Qualification rate: {(qualified_records/total_analyzed*100):.1f}%" if total_analyzed > 0 else "N/A")
        
        # Verify database records
        if qualified_records > 0:
            verify_query = """
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT equity_symbol) as unique_symbols,
                AVG(reduction_percentage) as avg_reduction,
                MIN(reduction_percentage) as min_reduction,
                MAX(reduction_percentage) as max_reduction,
                COUNT(CASE WHEN reduction_category = 'EXTREME_90_PLUS' THEN 1 END) as extreme_count,
                COUNT(CASE WHEN reduction_category = 'SEVERE_75_PLUS' THEN 1 END) as severe_count,
                COUNT(CASE WHEN reduction_category = 'HIGH_60_PLUS' THEN 1 END) as high_count,
                COUNT(CASE WHEN reduction_category = 'MODERATE_50_PLUS' THEN 1 END) as moderate_count
            FROM step06_filtered_50pct_reductions
            """
            
            verify_df = pd.read_sql(verify_query, conn)
            record_count = verify_df.iloc[0]['total_records']
            avg_reduction = verify_df.iloc[0]['avg_reduction']
            min_reduction = verify_df.iloc[0]['min_reduction']
            max_reduction = verify_df.iloc[0]['max_reduction']
            extreme_count = verify_df.iloc[0]['extreme_count']
            severe_count = verify_df.iloc[0]['severe_count']
            high_count = verify_df.iloc[0]['high_count']
            moderate_count = verify_df.iloc[0]['moderate_count']
            
            print(f"\n✅ DATABASE VERIFICATION:")
            print(f"Records stored: {record_count} (only 50%+ reductions)")
            print(f"Average reduction: {avg_reduction:.1f}%")
            print(f"Range: {min_reduction:.1f}% to {max_reduction:.1f}%")
            
            print(f"\n📈 CATEGORY BREAKDOWN:")
            print(f"🚨 EXTREME (90%+): {extreme_count}")
            print(f"🔴 SEVERE (75-89%): {severe_count}")
            print(f"🟡 HIGH (60-74%): {high_count}")
            print(f"🟢 MODERATE (50-59%): {moderate_count}")
            
            # Show reduction flag verification
            flag_query = """
            SELECT 
                reduction_flag,
                COUNT(*) as count
            FROM step06_filtered_50pct_reductions
            GROUP BY reduction_flag
            """
            
            flag_df = pd.read_sql(flag_query, conn)
            print(f"\n🏷️  REDUCTION FLAG VERIFICATION:")
            print(flag_df.to_string(index=False))
            
            print(f"\n🎯 Filtered table 'step06_filtered_50pct_reductions' ready!")
            print(f"📌 Contains ONLY records with 50%+ price reductions")
            print(f"🏷️  All records have reduction_flag = '50_PERCENT_PLUS'")
        
    except Exception as e:
        logging.error(f"Main error: {e}")
        print(f"❌ Error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()