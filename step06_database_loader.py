#!/usr/bin/env python3
"""
Step 6: Database Storage for Price Reduction Analysis
====================================================

This script creates a database table and loads all the 50% price reduction
analysis results from our 28 selected strikes.

Features:
- Creates step06_price_reduction_analysis table
- Loads all analysis results with proper data types
- Provides summary statistics and verification

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

def create_price_reduction_table(conn):
    """Create comprehensive price reduction analysis table."""
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
        
        -- Analysis categorization
        reduction_severity NVARCHAR(20),
        risk_category NVARCHAR(20),
        
        -- Additional metrics
        price_range_ratio DECIMAL(10,4),  -- (max-min)/max
        volume_stability NVARCHAR(20),
        expiry_impact NVARCHAR(50),
        
        INDEX IX_step06_symbol (equity_symbol),
        INDEX IX_step06_reduction (has_50_pct_reduction),
        INDEX IX_step06_severity (reduction_severity),
        INDEX IX_step06_risk (risk_category)
    )
    """
    
    cursor = conn.cursor()
    try:
        cursor.execute(drop_sql)
        cursor.execute(create_sql)
        conn.commit()
        logging.info("✅ step06_price_reduction_analysis table created")
        print("✅ Database table created successfully")
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
        price_range_ratio = (max_price - min_price) / max_price
    else:
        total_reduction_pct = 0
        price_range_ratio = 0
    
    # Find largest single-day drop
    max_single_day_drop = price_data['price_change_pct'].min() if not price_data['price_change_pct'].isna().all() else 0
    max_drop_date = None
    if not price_data['price_change_pct'].isna().all() and pd.notna(max_single_day_drop):
        max_drop_idx = price_data['price_change_pct'].idxmin()
        max_drop_date = price_data.loc[max_drop_idx, 'trade_date']
    
    # Find consecutive drops
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
    
    # Volume analysis
    avg_volume = price_data['contracts_traded'].mean()
    volume_std = price_data['contracts_traded'].std()
    volume_cv = (volume_std / avg_volume) if avg_volume > 0 else 0
    
    if volume_cv < 0.3:
        volume_stability = 'Stable'
    elif volume_cv < 0.7:
        volume_stability = 'Moderate'
    else:
        volume_stability = 'Volatile'
    
    # Expiry impact analysis
    days_to_analyze = len(price_data)
    if total_reduction_pct > 90 and days_to_analyze < 40:
        expiry_impact = 'Severe Theta Decay'
    elif total_reduction_pct > 70:
        expiry_impact = 'High Time Decay'
    elif total_reduction_pct > 50:
        expiry_impact = 'Moderate Time Decay'
    else:
        expiry_impact = 'Minimal Time Impact'
    
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
        'avg_daily_volume': avg_volume,
        'price_range_ratio': price_range_ratio,
        'volume_stability': volume_stability,
        'expiry_impact': expiry_impact
    }

def categorize_reduction(analysis_result):
    """Categorize the severity of price reduction."""
    if not analysis_result:
        return 'No Data', 'Unknown'
    
    total_reduction = analysis_result['total_reduction_pct']
    max_single_drop = analysis_result['max_single_day_drop_pct']
    consecutive_drops = analysis_result['max_consecutive_drops']
    
    # Reduction severity
    if total_reduction >= 90:
        severity = 'Extreme'
    elif total_reduction >= 75:
        severity = 'Severe'
    elif total_reduction >= 50:
        severity = 'High'
    elif total_reduction >= 25:
        severity = 'Moderate'
    elif total_reduction >= 10:
        severity = 'Low'
    else:
        severity = 'Minimal'
    
    # Risk category
    if total_reduction >= 75 and max_single_drop >= 20:
        risk = 'Critical'
    elif total_reduction >= 50 and max_single_drop >= 15:
        risk = 'High'
    elif total_reduction >= 50 or consecutive_drops >= 3:
        risk = 'Medium'
    elif total_reduction >= 25:
        risk = 'Low'
    else:
        risk = 'Minimal'
    
    return severity, risk

def insert_analysis_result(conn, strike_info, analysis_result):
    """Insert analysis result into database with proper data type handling."""
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
        reduction_severity, risk_category,
        price_range_ratio, volume_stability, expiry_impact
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    cursor = conn.cursor()
    try:
        # Convert boolean to integer for SQL Server BIT type
        has_50_reduction = 1 if analysis_result['has_50_pct_reduction'] else 0
        
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
            analysis_result['max_date'].date() if pd.notna(analysis_result['max_date']) else None,
            analysis_result['min_date'].date() if pd.notna(analysis_result['min_date']) else None,
            float(analysis_result['total_reduction_pct']),
            float(analysis_result['max_single_day_drop_pct']),
            analysis_result['max_drop_date'].date() if pd.notna(analysis_result['max_drop_date']) else None,
            analysis_result['max_consecutive_drops'],
            float(analysis_result['max_consecutive_drop_pct']),
            has_50_reduction,  # Convert to integer
            float(analysis_result['price_volatility']) if pd.notna(analysis_result['price_volatility']) else None,
            float(analysis_result['avg_daily_volume']) if pd.notna(analysis_result['avg_daily_volume']) else None,
            severity,
            risk,
            float(analysis_result['price_range_ratio']),
            analysis_result['volume_stability'],
            analysis_result['expiry_impact']
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
    print("💾 STEP 6: DATABASE STORAGE FOR PRICE REDUCTION ANALYSIS")
    print("=" * 70)
    print("Creating table and loading 50% reduction analysis results")
    
    try:
        # Setup database
        conn = get_database_connection()
        create_price_reduction_table(conn)
        
        # Get selected strikes
        selected_strikes = get_selected_strikes()
        print(f"\n📊 Loading {len(selected_strikes)} strike analysis results...")
        
        total_processed = 0
        successful_inserts = 0
        failed_inserts = 0
        
        print(f"\n🔄 Processing each strike...")
        
        for idx, strike_info in selected_strikes.iterrows():
            symbol = strike_info['equity_symbol']
            strike = strike_info['fo_strike_price']
            option_type = strike_info['fo_option_type']
            
            try:
                # Get historical price data
                price_data = get_historical_option_prices(conn, symbol, strike, option_type)
                
                if price_data.empty:
                    print(f"⚠️  {symbol} {strike} {option_type}: No data - skipping")
                    failed_inserts += 1
                    continue
                
                # Analyze price reduction
                analysis = analyze_price_reduction(price_data)
                
                if analysis:
                    # Insert into database
                    inserted = insert_analysis_result(conn, strike_info, analysis)
                    
                    if inserted:
                        successful_inserts += 1
                        reduction_pct = analysis['total_reduction_pct']
                        
                        if reduction_pct >= 90:
                            status = "🚨 EXTREME"
                        elif reduction_pct >= 75:
                            status = "🔴 SEVERE"
                        elif reduction_pct >= 50:
                            status = "🟡 HIGH"
                        else:
                            status = "🟢 MODERATE"
                        
                        print(f"✅ {symbol} {strike} {option_type}: {status} ({reduction_pct:.1f}% reduction)")
                    else:
                        failed_inserts += 1
                        print(f"❌ {symbol} {strike} {option_type}: Database insert failed")
                else:
                    failed_inserts += 1
                    print(f"❌ {symbol} {strike} {option_type}: Analysis failed")
                
                total_processed += 1
                
            except Exception as e:
                failed_inserts += 1
                print(f"❌ {symbol} {strike} {option_type}: Error - {e}")
                logging.error(f"Error processing {symbol} {strike} {option_type}: {e}")
        
        # Final summary
        print(f"\n📊 DATABASE LOADING SUMMARY")
        print("=" * 50)
        print(f"Total strikes processed: {total_processed}")
        print(f"Successful database inserts: {successful_inserts}")
        print(f"Failed inserts: {failed_inserts}")
        print(f"Success rate: {(successful_inserts/total_processed*100):.1f}%" if total_processed > 0 else "N/A")
        
        # Verify database records
        if successful_inserts > 0:
            verify_query = """
            SELECT 
                COUNT(*) as total_records,
                COUNT(CASE WHEN has_50_pct_reduction = 1 THEN 1 END) as reduction_count,
                AVG(total_reduction_pct) as avg_reduction,
                MAX(total_reduction_pct) as max_reduction,
                MIN(total_reduction_pct) as min_reduction
            FROM step06_price_reduction_analysis
            """
            
            verify_df = pd.read_sql(verify_query, conn)
            record_count = verify_df.iloc[0]['total_records']
            reduction_count = verify_df.iloc[0]['reduction_count']
            avg_reduction = verify_df.iloc[0]['avg_reduction']
            max_reduction = verify_df.iloc[0]['max_reduction']
            min_reduction = verify_df.iloc[0]['min_reduction']
            
            print(f"\n✅ DATABASE VERIFICATION:")
            print(f"Records in database: {record_count}")
            print(f"50%+ reductions: {reduction_count}")
            print(f"Average reduction: {avg_reduction:.1f}%")
            print(f"Range: {min_reduction:.1f}% to {max_reduction:.1f}%")
            
            # Show breakdown by symbol and severity
            breakdown_query = """
            SELECT 
                equity_symbol,
                reduction_severity,
                COUNT(*) as count,
                AVG(total_reduction_pct) as avg_reduction
            FROM step06_price_reduction_analysis
            GROUP BY equity_symbol, reduction_severity
            ORDER BY equity_symbol, avg_reduction DESC
            """
            
            breakdown_df = pd.read_sql(breakdown_query, conn)
            print(f"\n📈 BREAKDOWN BY SYMBOL & SEVERITY:")
            print(breakdown_df.to_string(index=False))
            
            print(f"\n🎯 Database table 'step06_price_reduction_analysis' ready for analysis!")
        
    except Exception as e:
        logging.error(f"Main error: {e}")
        print(f"❌ Error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()