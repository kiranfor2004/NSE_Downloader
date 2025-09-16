import pyodbc

def analyze_filter_options():
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=SRIKIRANREDDY\\SQLEXPRESS;DATABASE=master;Trusted_Connection=yes;')
    cursor = conn.cursor()

    print('=== COMPREHENSIVE FILTER ANALYSIS ===')

    print('\n1. DELIVERY INCREASE PATTERNS:')
    cursor.execute('''SELECT 
        CASE 
            WHEN delivery_increase_pct < 0 THEN 'Decreased'
            WHEN delivery_increase_pct = 0 THEN 'No Change'
            WHEN delivery_increase_pct BETWEEN 0 AND 25 THEN 'Low Increase (0-25%)'
            WHEN delivery_increase_pct BETWEEN 25 AND 50 THEN 'Moderate Increase (25-50%)'
            WHEN delivery_increase_pct BETWEEN 50 AND 100 THEN 'High Increase (50-100%)'
            WHEN delivery_increase_pct > 100 THEN 'Very High Increase (>100%)'
        END as delivery_trend,
        COUNT(*) as count
    FROM step03_compare_monthvspreviousmonth 
    WHERE delivery_increase_pct IS NOT NULL 
    GROUP BY 
        CASE 
            WHEN delivery_increase_pct < 0 THEN 'Decreased'
            WHEN delivery_increase_pct = 0 THEN 'No Change'
            WHEN delivery_increase_pct BETWEEN 0 AND 25 THEN 'Low Increase (0-25%)'
            WHEN delivery_increase_pct BETWEEN 25 AND 50 THEN 'Moderate Increase (25-50%)'
            WHEN delivery_increase_pct BETWEEN 50 AND 100 THEN 'High Increase (50-100%)'
            WHEN delivery_increase_pct > 100 THEN 'Very High Increase (>100%)'
        END
    ORDER BY count DESC''')
    delivery_trends = cursor.fetchall()
    for trend, count in delivery_trends:
        print(f'  • {trend}: {count:,} symbols')

    print('\n2. PRICE RANGES:')
    cursor.execute('''SELECT 
        CASE 
            WHEN current_close_price < 50 THEN 'Penny Stocks (<₹50)'
            WHEN current_close_price BETWEEN 50 AND 200 THEN 'Low Price (₹50-200)'
            WHEN current_close_price BETWEEN 200 AND 500 THEN 'Mid Price (₹200-500)'
            WHEN current_close_price BETWEEN 500 AND 1000 THEN 'High Price (₹500-1000)'
            WHEN current_close_price > 1000 THEN 'Premium Stocks (>₹1000)'
        END as price_range,
        COUNT(*) as count
    FROM step03_compare_monthvspreviousmonth 
    WHERE current_close_price IS NOT NULL AND current_close_price > 0
    GROUP BY 
        CASE 
            WHEN current_close_price < 50 THEN 'Penny Stocks (<₹50)'
            WHEN current_close_price BETWEEN 50 AND 200 THEN 'Low Price (₹50-200)'
            WHEN current_close_price BETWEEN 200 AND 500 THEN 'Mid Price (₹200-500)'
            WHEN current_close_price BETWEEN 500 AND 1000 THEN 'High Price (₹500-1000)'
            WHEN current_close_price > 1000 THEN 'Premium Stocks (>₹1000)'
        END
    ORDER BY count DESC''')
    price_ranges = cursor.fetchall()
    for price_range, count in price_ranges:
        print(f'  • {price_range}: {count:,} symbols')

    print('\n3. VOLUME ANALYSIS:')
    cursor.execute('''SELECT 
        CASE 
            WHEN current_ttl_trd_qnty < 10000 THEN 'Low Volume (<10K)'
            WHEN current_ttl_trd_qnty BETWEEN 10000 AND 100000 THEN 'Medium Volume (10K-100K)'
            WHEN current_ttl_trd_qnty BETWEEN 100000 AND 1000000 THEN 'High Volume (100K-1M)'
            WHEN current_ttl_trd_qnty > 1000000 THEN 'Very High Volume (>1M)'
        END as volume_range,
        COUNT(*) as count
    FROM step03_compare_monthvspreviousmonth 
    WHERE current_ttl_trd_qnty IS NOT NULL 
    GROUP BY 
        CASE 
            WHEN current_ttl_trd_qnty < 10000 THEN 'Low Volume (<10K)'
            WHEN current_ttl_trd_qnty BETWEEN 10000 AND 100000 THEN 'Medium Volume (10K-100K)'
            WHEN current_ttl_trd_qnty BETWEEN 100000 AND 1000000 THEN 'High Volume (100K-1M)'
            WHEN current_ttl_trd_qnty > 1000000 THEN 'Very High Volume (>1M)'
        END
    ORDER BY count DESC''')
    volume_ranges = cursor.fetchall()
    for volume_range, count in volume_ranges:
        print(f'  • {volume_range}: {count:,} symbols')

    print('\n4. TURNOVER ANALYSIS:')
    cursor.execute('''SELECT 
        CASE 
            WHEN current_turnover_lacs < 10 THEN 'Low Turnover (<₹10L)'
            WHEN current_turnover_lacs BETWEEN 10 AND 100 THEN 'Medium Turnover (₹10L-100L)'
            WHEN current_turnover_lacs BETWEEN 100 AND 1000 THEN 'High Turnover (₹100L-1000L)'
            WHEN current_turnover_lacs > 1000 THEN 'Very High Turnover (>₹1000L)'
        END as turnover_range,
        COUNT(*) as count
    FROM step03_compare_monthvspreviousmonth 
    WHERE current_turnover_lacs IS NOT NULL 
    GROUP BY 
        CASE 
            WHEN current_turnover_lacs < 10 THEN 'Low Turnover (<₹10L)'
            WHEN current_turnover_lacs BETWEEN 10 AND 100 THEN 'Medium Turnover (₹10L-100L)'
            WHEN current_turnover_lacs BETWEEN 100 AND 1000 THEN 'High Turnover (₹100L-1000L)'
            WHEN current_turnover_lacs > 1000 THEN 'Very High Turnover (>₹1000L)'
        END
    ORDER BY count DESC''')
    turnover_ranges = cursor.fetchall()
    for turnover_range, count in turnover_ranges:
        print(f'  • {turnover_range}: {count:,} symbols')

    print('\n5. INDEX NAMES:')
    cursor.execute('SELECT DISTINCT index_name, COUNT(*) as count FROM step03_compare_monthvspreviousmonth WHERE index_name IS NOT NULL GROUP BY index_name ORDER BY count DESC')
    indexes = cursor.fetchall()
    for index_name, count in indexes:
        print(f'  • {index_name}: {count:,} symbols')

    print('\n6. SERIES TYPES:')
    cursor.execute('SELECT DISTINCT series, COUNT(*) as count FROM step03_compare_monthvspreviousmonth WHERE series IS NOT NULL GROUP BY series ORDER BY count DESC')
    series_types = cursor.fetchall()
    for series, count in series_types:
        print(f'  • {series}: {count:,} symbols')

    print('\n7. DELIVERY PERCENTAGE RANGES:')
    cursor.execute('''SELECT 
        CASE 
            WHEN current_deliv_per < 20 THEN 'Low Delivery (<20%)'
            WHEN current_deliv_per BETWEEN 20 AND 40 THEN 'Medium Delivery (20-40%)'
            WHEN current_deliv_per BETWEEN 40 AND 60 THEN 'High Delivery (40-60%)'
            WHEN current_deliv_per BETWEEN 60 AND 80 THEN 'Very High Delivery (60-80%)'
            WHEN current_deliv_per > 80 THEN 'Extreme Delivery (>80%)'
        END as delivery_range,
        COUNT(*) as count
    FROM step03_compare_monthvspreviousmonth 
    WHERE current_deliv_per IS NOT NULL 
    GROUP BY 
        CASE 
            WHEN current_deliv_per < 20 THEN 'Low Delivery (<20%)'
            WHEN current_deliv_per BETWEEN 20 AND 40 THEN 'Medium Delivery (20-40%)'
            WHEN current_deliv_per BETWEEN 40 AND 60 THEN 'High Delivery (40-60%)'
            WHEN current_deliv_per BETWEEN 60 AND 80 THEN 'Very High Delivery (60-80%)'
            WHEN current_deliv_per > 80 THEN 'Extreme Delivery (>80%)'
        END
    ORDER BY count DESC''')
    delivery_ranges = cursor.fetchall()
    for delivery_range, count in delivery_ranges:
        print(f'  • {delivery_range}: {count:,} symbols')

    print('\n8. MONTHLY TRENDS:')
    cursor.execute('''SELECT 
        YEAR(current_trade_date) as year,
        MONTH(current_trade_date) as month,
        DATENAME(MONTH, current_trade_date) as month_name,
        COUNT(*) as count,
        COUNT(DISTINCT symbol) as unique_symbols
    FROM step03_compare_monthvspreviousmonth 
    WHERE current_trade_date IS NOT NULL 
    GROUP BY YEAR(current_trade_date), MONTH(current_trade_date), DATENAME(MONTH, current_trade_date)
    ORDER BY year, month''')
    monthly_trends = cursor.fetchall()
    for year, month, month_name, count, unique_symbols in monthly_trends:
        print(f'  • {month_name} {year}: {count:,} records ({unique_symbols:,} symbols)')

    conn.close()

if __name__ == "__main__":
    analyze_filter_options()