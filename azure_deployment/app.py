"""
Azure Web App Flask Application for NSE Dashboard
"""
import os
import sys
import json
import pyodbc
import pandas as pd
from flask import Flask, render_template_string, jsonify, request
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

def get_database_config():
    """Get database configuration from environment variables or local config"""
    try:
        # Try Azure environment variables first
        if os.environ.get('DATABASE_SERVER'):
            return {
                'server': os.environ.get('DATABASE_SERVER'),
                'database': os.environ.get('DATABASE_NAME', 'master'),
                'username': os.environ.get('DATABASE_USERNAME', ''),
                'password': os.environ.get('DATABASE_PASSWORD', ''),
                'driver': 'ODBC Driver 17 for SQL Server',
                'trusted_connection': os.environ.get('TRUSTED_CONNECTION', 'no')
            }
        else:
            # Fall back to local config file
            config_path = os.path.join(os.path.dirname(__file__), '..', 'database_config.json')
            if os.path.exists(config_path):
                with open(config_path, 'r') as f:
                    return json.load(f)
            else:
                # Default local configuration
                return {
                    'server': 'SRIKIRANREDDY\\SQLEXPRESS',
                    'database': 'master',
                    'username': '',
                    'password': '',
                    'driver': 'ODBC Driver 17 for SQL Server',
                    'trusted_connection': 'yes'
                }
    except Exception as e:
        logger.error(f"Error loading database config: {e}")
        raise

def get_database_connection():
    """Create database connection"""
    config = get_database_config()
    
    try:
        if config.get('trusted_connection', '').lower() == 'yes':
            # Local development with Windows Authentication
            conn_str = (
                f"DRIVER={{{config['driver']}}};"
                f"SERVER={config['server']};"
                f"DATABASE={config['database']};"
                f"Trusted_Connection=yes"
            )
        else:
            # Azure or SQL Authentication
            conn_str = (
                f"DRIVER={{{config['driver']}}};"
                f"SERVER={config['server']};"
                f"DATABASE={config['database']};"
                f"UID={config['username']};"
                f"PWD={config['password']}"
            )
        
        conn = pyodbc.connect(conn_str)
        logger.info("Database connection successful")
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise

def get_dashboard_data():
    """Fetch data for dashboard"""
    query = """
    SELECT TOP 1000
        symbol,
        ISNULL(current_deliv_per, 0) as delivery_percentage,
        ISNULL(((current_close_price - current_prev_close) / NULLIF(current_prev_close, 0)) * 100, 0) as price_change_pct,
        ISNULL(current_ttl_trd_qnty, 0) as volume,
        ISNULL(current_turnover_lacs, 0) as turnover,
        ISNULL(current_close_price, 0) as close_price,
        ISNULL(current_high_price, 0) as high_price,
        ISNULL(current_low_price, 0) as low_price,
        ISNULL(current_open_price, 0) as open_price,
        ISNULL(current_deliv_qty, 0) as delivery_qty,
        ISNULL(current_no_of_trades, 0) as no_of_trades,
        ISNULL(index_name, 'Others') as index_name,
        ISNULL(category, 'Others') as category,
        ISNULL(previous_deliv_qty, 0) as prev_delivery_qty,
        ISNULL(delivery_increase_abs, 0) as delivery_increase_abs,
        ISNULL(delivery_increase_pct, 0) as delivery_increase_pct,
        CASE
            WHEN symbol LIKE '%BANK%' OR symbol IN ('HDFCBANK', 'ICICIBANK', 'SBIN', 'AXISBANK', 'KOTAKBANK') THEN 'Banking'
            WHEN symbol IN ('TCS', 'INFY', 'WIPRO', 'HCLTECH', 'TECHM') THEN 'IT'
            WHEN symbol IN ('RELIANCE', 'ONGC', 'BPCL', 'IOC') THEN 'Energy'
            WHEN symbol IN ('ITC', 'HINDUNILVR', 'NESTLEIND', 'BRITANNIA') THEN 'FMCG'
            WHEN symbol IN ('MARUTI', 'M&M', 'TATAMOTORS', 'BAJAJ-AUTO') THEN 'Auto'
            WHEN symbol IN ('SUNPHARMA', 'DRREDDY', 'CIPLA', 'DIVISLAB') THEN 'Pharma'
            ELSE 'Others'
        END as sector
    FROM step03_compare_monthvspreviousmonth
    WHERE current_turnover_lacs > 50
        AND symbol IS NOT NULL
    ORDER BY current_turnover_lacs DESC
    """
    
    try:
        conn = get_database_connection()
        df = pd.read_sql(query, conn)
        conn.close()
        
        logger.info(f"Loaded {len(df)} records for dashboard")
        
        # Process data for different sections
        market_overview = process_market_overview(df)
        symbol_analysis = process_symbol_analysis(df)
        category_index = process_category_index(df)
        delivery_flow = process_delivery_flow(df)
        
        return {
            'market_overview': market_overview,
            'symbol_analysis': symbol_analysis,
            'category_index': category_index,
            'delivery_flow': delivery_flow,
            'search_symbols': {symbol: symbol for symbol in df['symbol'].tolist()},
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
    except Exception as e:
        logger.error(f"Error fetching dashboard data: {e}")
        raise

def process_market_overview(df):
    """Process data for market overview tab"""
    total_turnover = df['turnover'].sum() / 100000  # Convert to Crores
    total_volume = df['volume'].sum() / 10000000    # Convert to Crores
    avg_delivery = df['delivery_percentage'].mean()
    gainers = len(df[df['price_change_pct'] > 0])
    losers = len(df[df['price_change_pct'] < 0])
    high_delivery = len(df[df['delivery_percentage'] > 50])
    
    # Sector performance
    sector_performance = df.groupby('sector').agg({
        'turnover': 'sum',
        'volume': 'sum',
        'delivery_percentage': 'mean',
        'price_change_pct': 'mean'
    }).reset_index()
    
    # Top performers
    top_turnover = df.nlargest(10, 'turnover')
    top_volume = df.nlargest(10, 'volume')
    top_delivery = df.nlargest(10, 'delivery_percentage')
    
    return {
        'kpis': {
            'total_turnover': round(total_turnover, 2),
            'total_volume': round(total_volume, 2),
            'avg_delivery': round(avg_delivery, 2),
            'gainers': gainers,
            'losers': losers,
            'high_delivery': high_delivery
        },
        'sector_performance': {
            'sectors': sector_performance['sector'].tolist(),
            'turnover': sector_performance['turnover'].tolist(),
            'volume': sector_performance['volume'].tolist(),
            'delivery_pct': sector_performance['delivery_percentage'].tolist(),
            'price_change': sector_performance['price_change_pct'].tolist()
        },
        'top_turnover': {
            'symbols': top_turnover['symbol'].tolist(),
            'turnover': top_turnover['turnover'].tolist()
        },
        'top_volume': {
            'symbols': top_volume['symbol'].tolist(),
            'volume': top_volume['volume'].tolist()
        },
        'top_delivery': {
            'symbols': top_delivery['symbol'].tolist(),
            'delivery_pct': top_delivery['delivery_percentage'].tolist()
        }
    }

def process_symbol_analysis(df):
    """Process data for symbol analysis tab"""
    return {
        'all_symbols': df['symbol'].tolist(),
        'symbol_data': df.to_dict('records')
    }

def process_category_index(df):
    """Process data for category and index performance"""
    # Category performance
    category_data = df.groupby('category').agg({
        'turnover': 'sum',
        'volume': 'sum',
        'delivery_qty': 'sum',
        'delivery_percentage': 'mean',
        'price_change_pct': 'mean',
        'symbol': 'count'
    }).reset_index()
    
    # Index performance
    index_data = df.groupby('index_name').agg({
        'turnover': 'sum',
        'volume': 'sum',
        'delivery_qty': 'sum',
        'delivery_percentage': 'mean',
        'price_change_pct': 'mean',
        'symbol': 'count'
    }).reset_index()
    
    return {
        'kpis': {
            'total_categories': len(category_data),
            'total_indices': len(index_data),
            'avg_category_delivery': round(category_data['delivery_percentage'].mean(), 2),
            'avg_index_delivery': round(index_data['delivery_percentage'].mean(), 2),
            'top_category_turnover': round(category_data['turnover'].max() / 100000, 2),
            'top_index_turnover': round(index_data['turnover'].max() / 100000, 2)
        },
        'category_performance': {
            'categories': category_data['category'].tolist(),
            'turnover': category_data['turnover'].tolist(),
            'volume': category_data['volume'].tolist(),
            'delivery_qty': category_data['delivery_qty'].tolist(),
            'avg_delivery': category_data['delivery_percentage'].tolist(),
            'avg_price_change': category_data['price_change_pct'].tolist(),
            'symbol_count': category_data['symbol'].tolist()
        },
        'index_performance': {
            'indices': index_data['index_name'].tolist(),
            'turnover': index_data['turnover'].tolist(),
            'volume': index_data['volume'].tolist(),
            'delivery_qty': index_data['delivery_qty'].tolist(),
            'avg_delivery': index_data['delivery_percentage'].tolist(),
            'avg_price_change': index_data['price_change_pct'].tolist(),
            'symbol_count': index_data['symbol'].tolist()
        }
    }

def process_delivery_flow(df):
    """Process data for delivery flow analysis"""
    # Calculate flow statistics
    total_prev_delivery = df['prev_delivery_qty'].sum()
    total_current_delivery = df['delivery_qty'].sum()
    net_delivery_change = ((total_current_delivery - total_prev_delivery) / total_prev_delivery * 100) if total_prev_delivery > 0 else 0
    
    positive_flow_stocks = len(df[df['delivery_increase_pct'] > 0])
    negative_flow_stocks = len(df[df['delivery_increase_pct'] < 0])
    high_growth_stocks = len(df[df['delivery_increase_pct'] > 50])
    
    # Add flow categories
    df_flow = df.copy()
    df_flow['flow_category'] = df_flow['delivery_increase_pct'].apply(lambda x:
        'High Growth (>50%)' if x > 50 else
        'Strong Growth (20-50%)' if x > 20 else
        'Moderate Growth (5-20%)' if x > 5 else
        'Stable (-5% to 5%)' if x >= -5 else
        'Decline (<-5%)'
    )
    
    # Aggregate by different dimensions
    sankey_by_sector = df_flow.groupby(['sector', 'flow_category']).agg({
        'prev_delivery_qty': 'sum',
        'delivery_qty': 'sum',
        'symbol': 'count'
    }).reset_index()
    
    sankey_by_category = df_flow.groupby(['category', 'flow_category']).agg({
        'prev_delivery_qty': 'sum',
        'delivery_qty': 'sum',
        'symbol': 'count'
    }).reset_index()
    
    sankey_by_index = df_flow.groupby(['index_name', 'flow_category']).agg({
        'prev_delivery_qty': 'sum',
        'delivery_qty': 'sum',
        'symbol': 'count'
    }).reset_index()
    
    # Top symbols by delivery flow
    top_sankey_symbols = df_flow.nlargest(20, 'delivery_increase_pct')
    
    return {
        'kpis': {
            'total_prev_delivery': round(total_prev_delivery / 100000, 2),
            'total_current_delivery': round(total_current_delivery / 100000, 2),
            'net_delivery_change': round(net_delivery_change, 2),
            'positive_flow_stocks': positive_flow_stocks,
            'negative_flow_stocks': negative_flow_stocks,
            'high_growth_stocks': high_growth_stocks
        },
        'sankey_by_sector': {
            'sectors': sankey_by_sector['sector'].tolist(),
            'flow_categories': sankey_by_sector['flow_category'].tolist(),
            'prev_delivery': sankey_by_sector['prev_delivery_qty'].tolist(),
            'current_delivery': sankey_by_sector['delivery_qty'].tolist(),
            'stock_count': sankey_by_sector['symbol'].tolist()
        },
        'sankey_by_category': {
            'categories': sankey_by_category['category'].tolist(),
            'flow_categories': sankey_by_category['flow_category'].tolist(),
            'prev_delivery': sankey_by_category['prev_delivery_qty'].tolist(),
            'current_delivery': sankey_by_category['delivery_qty'].tolist(),
            'stock_count': sankey_by_category['symbol'].tolist()
        },
        'sankey_by_index': {
            'indices': sankey_by_index['index_name'].tolist(),
            'flow_categories': sankey_by_index['flow_category'].tolist(),
            'prev_delivery': sankey_by_index['prev_delivery_qty'].tolist(),
            'current_delivery': sankey_by_index['delivery_qty'].tolist(),
            'stock_count': sankey_by_index['symbol'].tolist()
        },
        'top_symbols_flow': {
            'symbols': top_sankey_symbols['symbol'].tolist(),
            'prev_delivery': top_sankey_symbols['prev_delivery_qty'].tolist(),
            'current_delivery': top_sankey_symbols['delivery_qty'].tolist(),
            'delivery_increase_pct': top_sankey_symbols['delivery_increase_pct'].tolist(),
            'sectors': top_sankey_symbols['sector'].tolist(),
            'categories': top_sankey_symbols['category'].tolist()
        }
    }

@app.route('/')
def dashboard():
    """Main dashboard route"""
    try:
        # Get dashboard data
        dashboard_data = get_dashboard_data()
        
        # Read the HTML template
        template_path = os.path.join(os.path.dirname(__file__), 'templates', 'dashboard.html')
        with open(template_path, 'r', encoding='utf-8') as f:
            html_template = f.read()
        
        # Render template with data
        return render_template_string(html_template, dashboard_data=dashboard_data)
        
    except Exception as e:
        logger.error(f"Error rendering dashboard: {e}")
        return f"Error loading dashboard: {str(e)}", 500

@app.route('/api/data')
def api_data():
    """API endpoint for dashboard data"""
    try:
        dashboard_data = get_dashboard_data()
        return jsonify(dashboard_data)
    except Exception as e:
        logger.error(f"Error in API endpoint: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/health')
def health_check():
    """Health check endpoint"""
    try:
        # Test database connection
        conn = get_database_connection()
        conn.close()
        return jsonify({
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'database': 'connected'
        })
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'timestamp': datetime.now().isoformat(),
            'database': 'disconnected',
            'error': str(e)
        }), 500

if __name__ == '__main__':
    # For local development
    app.run(debug=True, host='0.0.0.0', port=5000)