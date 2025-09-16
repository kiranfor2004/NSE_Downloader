"""
NSE Delivery Analysis Dashboard API - Enhanced for Three-Tab Dashboard
Provides comprehensive REST endpoints for the specialized dashboard frontend
"""

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import pyodbc
import json
import logging
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Dict, List, Optional, Tuple
import os
import numpy as np
from collections import defaultdict

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseConnection:
    def __init__(self):
        self.connection_string = None
        self.load_config()

    def load_config(self):
        """Load database configuration from JSON file"""
        try:
            with open('database_config.json', 'r') as f:
                config = json.load(f)
            
            # Try to get from master_database first, fallback to root level
            master_config = config.get('master_database', config)
            
            # Get server name and escape backslashes properly
            server_name = master_config.get('server', 'SRIKIRANREDDY\\SQLEXPRESS')
            database_name = master_config.get('database', 'master')
            driver_name = master_config.get('driver', 'ODBC Driver 17 for SQL Server')
            
            # Use the same connection pattern as working scripts
            self.connection_string = (
                f"DRIVER={{{driver_name}}};"
                f"SERVER={server_name};"
                f"DATABASE={database_name};"
                f"Trusted_Connection=yes;"
            )
            logger.info(f"Database configuration loaded successfully")
        except Exception as e:
            logger.error(f"Failed to load database config: {e}")
            # Fallback configuration
            self.connection_string = (
                "DRIVER={ODBC Driver 17 for SQL Server};"
                "SERVER=SRIKIRANREDDY\\SQLEXPRESS;"
                "DATABASE=master;"
                "Trusted_Connection=yes;"
            )

    def get_connection(self):
        """Get database connection"""
        try:
            return pyodbc.connect(self.connection_string)
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise

    def execute_query(self, query: str, params: tuple = ()) -> List[Dict]:
        """Execute query and return results as list of dictionaries"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                
                # Get column names
                columns = [desc[0] for desc in cursor.description]
                
                # Fetch all rows and convert to dictionaries
                rows = cursor.fetchall()
                result = []
                for row in rows:
                    row_dict = {}
                    for i, value in enumerate(row):
                        # Handle datetime objects
                        if isinstance(value, datetime):
                            row_dict[columns[i]] = value.isoformat()
                        elif isinstance(value, (int, float)) and value is not None:
                            # Handle potential division by zero or None values in percentages
                            row_dict[columns[i]] = value
                        else:
                            row_dict[columns[i]] = value
                    result.append(row_dict)
                
                return result
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise

# Initialize database connection
db = DatabaseConnection()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ===================== UTILITY FUNCTIONS =====================

def normalize_trading_date(date_str: Optional[str]) -> Optional[str]:
    """Normalize incoming trading_date strings to 'YYYY-MM-DD'"""
    if not date_str:
        return None
    try:
        # Handle various date formats
        if 'T' in date_str:
            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            return dt.strftime('%Y-%m-%d')
        
        try:
            dt = parsedate_to_datetime(date_str)
            return dt.strftime('%Y-%m-%d')
        except Exception:
            pass
        
        # Try common date formats
        for fmt in ('%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%d-%m-%Y'):
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.strftime('%Y-%m-%d')
            except Exception:
                continue
        
        try:
            dt = datetime.fromisoformat(date_str)
            return dt.strftime('%Y-%m-%d')
        except Exception:
            return None
    except Exception:
        return None

def calculate_delivery_percentage(delivery_qty: float, total_traded_qty: float) -> float:
    """Calculate delivery percentage safely"""
    if total_traded_qty and total_traded_qty > 0:
        return (delivery_qty / total_traded_qty) * 100
    return 0.0

def safe_percentage_change(current: float, previous: float) -> float:
    """Calculate percentage change safely"""
    if previous and previous > 0:
        return ((current - previous) / previous) * 100
    return 0.0

# ===================== BASIC ENDPOINTS =====================

@app.route('/')
def serve_dashboard():
    """Serve the main dashboard HTML"""
    return send_from_directory(BASE_DIR, 'index_enhanced.html')

@app.route('/dashboard')
def serve_dashboard_alias():
    """Serve the dashboard when user navigates to /dashboard"""
    return send_from_directory(BASE_DIR, 'index_enhanced.html')

@app.route('/<path:filename>')
def serve_static_files(filename):
    """Serve static files like CSS, JS, etc."""
    return send_from_directory(BASE_DIR, filename)

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    try:
        # Test database connection
        db.execute_query("SELECT 1 as test")
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

# ===================== TAB 1: CURRENT MONTH DAILY PERFORMANCE =====================

@app.route('/api/tab1/daily-performance', methods=['GET'])
def get_daily_performance():
    """Get daily performance data for Tab 1"""
    try:
        trading_date = request.args.get('trading_date')
        
        # If no date specified, get the latest date
        if not trading_date:
            latest_date_query = """
                SELECT TOP 1 current_trade_date 
                FROM step03_compare_monthvspreviousmonth 
                ORDER BY current_trade_date DESC
            """
            latest_result = db.execute_query(latest_date_query)
            if latest_result:
                trading_date = latest_result[0]['current_trade_date']
                if isinstance(trading_date, str) and 'T' in trading_date:
                    trading_date = trading_date.split('T')[0]
        
        # Daily performance query with all required KPIs
        daily_query = """
            SELECT 
                symbol,
                current_trade_date,
                current_deliv_qty,
                previous_deliv_qty,
                current_ttl_trd_qnty,
                current_open_price,
                current_high_price,
                current_low_price,
                current_close_price,
                current_turnover_lacs,
                delivery_increase_pct,
                category,
                index_name,
                CASE 
                    WHEN current_ttl_trd_qnty > 0 
                    THEN (current_deliv_qty * 100.0 / current_ttl_trd_qnty)
                    ELSE 0 
                END as delivery_percentage,
                CASE 
                    WHEN current_deliv_qty > previous_deliv_qty THEN 1 
                    ELSE 0 
                END as higher_than_previous
            FROM step03_compare_monthvspreviousmonth
            WHERE CAST(current_trade_date AS DATE) = ?
            ORDER BY delivery_increase_pct DESC
        """
        
        daily_data = db.execute_query(daily_query, (trading_date,))
        
        # Calculate aggregate KPIs
        total_symbols = len(daily_data)
        symbols_higher_delivery = sum(1 for item in daily_data if item['higher_than_previous'] == 1)
        
        # Summary statistics
        if daily_data:
            avg_delivery_pct = sum(item.get('delivery_percentage', 0) for item in daily_data) / len(daily_data)
            total_delivery_qty = sum(item.get('current_deliv_qty', 0) for item in daily_data)
            total_traded_qty = sum(item.get('current_ttl_trd_qnty', 0) for item in daily_data)
        else:
            avg_delivery_pct = 0
            total_delivery_qty = 0
            total_traded_qty = 0
        
        return jsonify({
            'trading_date': trading_date,
            'daily_data': daily_data,
            'kpis': {
                'total_symbols': total_symbols,
                'symbols_with_higher_delivery': symbols_higher_delivery,
                'percentage_symbols_higher': (symbols_higher_delivery / total_symbols * 100) if total_symbols > 0 else 0,
                'avg_delivery_percentage': round(avg_delivery_pct, 2),
                'total_delivery_quantity': total_delivery_qty,
                'total_traded_quantity': total_traded_qty,
                'overall_delivery_percentage': (total_delivery_qty / total_traded_qty * 100) if total_traded_qty > 0 else 0
            },
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        logger.error(f"Error fetching daily performance: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/tab1/heatmap-data', methods=['GET'])
def get_heatmap_data():
    """Get heatmap data for delivery percentage visualization"""
    try:
        # Get last 30 trading days for heatmap
        heatmap_query = """
            SELECT TOP 1000
                symbol,
                current_trade_date,
                CASE 
                    WHEN current_ttl_trd_qnty > 0 
                    THEN (current_deliv_qty * 100.0 / current_ttl_trd_qnty)
                    ELSE 0 
                END as delivery_percentage,
                category,
                index_name
            FROM step03_compare_monthvspreviousmonth
            ORDER BY current_trade_date DESC, delivery_percentage DESC
        """
        
        heatmap_raw = db.execute_query(heatmap_query)
        
        # Structure data for heatmap: {symbol: {date: delivery_percentage}}
        heatmap_data = defaultdict(dict)
        dates = set()
        symbols = set()
        
        for row in heatmap_raw:
            symbol = row['symbol']
            trade_date = row['current_trade_date']
            if isinstance(trade_date, str) and 'T' in trade_date:
                trade_date = trade_date.split('T')[0]
            
            delivery_pct = row['delivery_percentage']
            
            heatmap_data[symbol][trade_date] = delivery_pct
            dates.add(trade_date)
            symbols.add(symbol)
        
        # Convert to list format for frontend
        heatmap_list = []
        for symbol in symbols:
            for date in dates:
                value = heatmap_data[symbol].get(date, 0)
                heatmap_list.append({
                    'symbol': symbol,
                    'date': date,
                    'value': value
                })
        
        return jsonify({
            'heatmap_data': heatmap_list,
            'dates': sorted(list(dates)),
            'symbols': sorted(list(symbols)),
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        logger.error(f"Error fetching heatmap data: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/tab1/candlestick-data/<symbol>', methods=['GET'])
def get_candlestick_data(symbol):
    """Get candlestick/OHLC data for a specific symbol"""
    try:
        candlestick_query = """
            SELECT 
                current_trade_date,
                current_open_price,
                current_high_price,
                current_low_price,
                current_close_price,
                current_ttl_trd_qnty,
                current_deliv_qty,
                current_turnover_lacs
            FROM step03_compare_monthvspreviousmonth
            WHERE symbol = ?
            ORDER BY current_trade_date ASC
        """
        
        candlestick_data = db.execute_query(candlestick_query, (symbol,))
        
        # Format data for candlestick chart
        formatted_data = []
        for row in candlestick_data:
            date = row['current_trade_date']
            if isinstance(date, str) and 'T' in date:
                date = date.split('T')[0]
            
            formatted_data.append({
                'date': date,
                'open': row['current_open_price'],
                'high': row['current_high_price'],
                'low': row['current_low_price'],
                'close': row['current_close_price'],
                'volume': row['current_ttl_trd_qnty'],
                'delivery_qty': row['current_deliv_qty'],
                'turnover': row['current_turnover_lacs']
            })
        
        return jsonify({
            'symbol': symbol,
            'candlestick_data': formatted_data,
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        logger.error(f"Error fetching candlestick data: {e}")
        return jsonify({'error': str(e)}), 500

# ===================== TAB 2: MONTHLY TRENDS AND COMPARISON =====================

@app.route('/api/tab2/monthly-trends', methods=['GET'])
def get_monthly_trends():
    """Get monthly trends and comparison data for Tab 2"""
    try:
        # Get current and previous month data
        monthly_query = """
            SELECT 
                YEAR(current_trade_date) as year,
                MONTH(current_trade_date) as month,
                COUNT(DISTINCT current_trade_date) as trading_days,
                COUNT(*) as total_records,
                SUM(current_deliv_qty) as total_delivery_qty,
                SUM(current_ttl_trd_qnty) as total_traded_qty,
                SUM(current_turnover_lacs) as total_turnover,
                AVG(current_deliv_qty) as avg_daily_delivery,
                AVG(delivery_increase_pct) as avg_delivery_increase_pct
            FROM step03_compare_monthvspreviousmonth
            GROUP BY YEAR(current_trade_date), MONTH(current_trade_date)
            ORDER BY year DESC, month DESC
        """
        
        monthly_data = db.execute_query(monthly_query)
        
        # Calculate ratios and additional metrics
        for month_data in monthly_data:
            total_delivery = month_data['total_delivery_qty']
            total_traded = month_data['total_traded_qty']
            trading_days = month_data['trading_days']
            
            # Delivery ratio
            month_data['delivery_volume_ratio'] = (total_delivery / total_traded * 100) if total_traded > 0 else 0
            
            # Average per trading day
            month_data['avg_daily_delivery'] = total_delivery / trading_days if trading_days > 0 else 0
            month_data['avg_daily_traded'] = total_traded / trading_days if trading_days > 0 else 0
        
        # Get symbol contribution data for treemap
        symbol_contribution_query = """
            SELECT 
                symbol,
                category,
                index_name,
                SUM(current_deliv_qty) as total_delivery_qty,
                COUNT(*) as trading_days,
                AVG(delivery_increase_pct) as avg_increase_pct
            FROM step03_compare_monthvspreviousmonth
            WHERE YEAR(current_trade_date) = YEAR(GETDATE()) 
                AND MONTH(current_trade_date) = MONTH(GETDATE())
            GROUP BY symbol, category, index_name
            ORDER BY total_delivery_qty DESC
        """
        
        symbol_contributions = db.execute_query(symbol_contribution_query)
        
        # Calculate percentage contribution for treemap
        total_market_delivery = sum(item['total_delivery_qty'] for item in symbol_contributions)
        for item in symbol_contributions:
            item['contribution_percentage'] = (item['total_delivery_qty'] / total_market_delivery * 100) if total_market_delivery > 0 else 0
        
        return jsonify({
            'monthly_data': monthly_data,
            'symbol_contributions': symbol_contributions,
            'total_market_delivery': total_market_delivery,
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        logger.error(f"Error fetching monthly trends: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/tab2/category-comparison', methods=['GET'])
def get_category_comparison():
    """Get category-wise comparison data"""
    try:
        category_query = """
            SELECT 
                category,
                COUNT(DISTINCT symbol) as symbol_count,
                SUM(current_deliv_qty) as total_delivery,
                SUM(previous_deliv_qty) as total_previous_delivery,
                AVG(delivery_increase_pct) as avg_increase_pct,
                SUM(current_turnover_lacs) as total_turnover
            FROM step03_compare_monthvspreviousmonth
            GROUP BY category
            ORDER BY total_delivery DESC
        """
        
        category_data = db.execute_query(category_query)
        
        # Calculate additional metrics
        for cat_data in category_data:
            current_delivery = cat_data['total_delivery']
            previous_delivery = cat_data['total_previous_delivery']
            
            # Month-over-month change
            cat_data['mom_change_pct'] = safe_percentage_change(current_delivery, previous_delivery)
            
            # Average delivery per symbol
            cat_data['avg_delivery_per_symbol'] = current_delivery / cat_data['symbol_count'] if cat_data['symbol_count'] > 0 else 0
        
        return jsonify({
            'category_data': category_data,
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        logger.error(f"Error fetching category comparison: {e}")
        return jsonify({'error': str(e)}), 500

# ===================== TAB 3: SYMBOL-SPECIFIC DEEP DIVE =====================

@app.route('/api/tab3/symbol-detail/<symbol>', methods=['GET'])
def get_symbol_detail(symbol):
    """Get detailed analysis for a specific symbol"""
    try:
        # Get all data for the symbol
        symbol_query = """
            SELECT *
            FROM step03_compare_monthvspreviousmonth
            WHERE symbol = ?
            ORDER BY current_trade_date ASC
        """
        
        symbol_data = db.execute_query(symbol_query, (symbol,))
        
        if not symbol_data:
            return jsonify({'error': 'Symbol not found'}), 404
        
        # Calculate summary statistics
        current_month_delivery = sum(item['current_deliv_qty'] for item in symbol_data)
        previous_month_delivery = sum(item['previous_deliv_qty'] for item in symbol_data)
        
        # Prepare data for correlation analysis
        prices = [item['current_close_price'] for item in symbol_data if item['current_close_price']]
        deliveries = [item['current_deliv_qty'] for item in symbol_data if item['current_deliv_qty']]
        
        # Calculate correlation (simple implementation)
        correlation = 0
        if len(prices) == len(deliveries) and len(prices) > 1:
            # Simple correlation calculation
            mean_price = sum(prices) / len(prices)
            mean_delivery = sum(deliveries) / len(deliveries)
            
            numerator = sum((prices[i] - mean_price) * (deliveries[i] - mean_delivery) for i in range(len(prices)))
            
            price_variance = sum((p - mean_price) ** 2 for p in prices)
            delivery_variance = sum((d - mean_delivery) ** 2 for d in deliveries)
            
            if price_variance > 0 and delivery_variance > 0:
                correlation = numerator / (price_variance * delivery_variance) ** 0.5
        
        # Get latest data for current metrics
        latest_data = symbol_data[-1] if symbol_data else {}
        
        # Calculate delivery percentage for donut chart
        current_deliv_qty = latest_data.get('current_deliv_qty', 0)
        current_ttl_trd_qnty = latest_data.get('current_ttl_trd_qnty', 0)
        delivery_percentage = calculate_delivery_percentage(current_deliv_qty, current_ttl_trd_qnty)
        
        return jsonify({
            'symbol': symbol,
            'symbol_data': symbol_data,
            'summary': {
                'current_month_total_delivery': current_month_delivery,
                'previous_month_total_delivery': previous_month_delivery,
                'month_over_month_change': safe_percentage_change(current_month_delivery, previous_month_delivery),
                'correlation_price_delivery': round(correlation, 4),
                'latest_delivery_percentage': round(delivery_percentage, 2),
                'trading_days': len(symbol_data),
                'category': latest_data.get('category', ''),
                'index_name': latest_data.get('index_name', '')
            },
            'latest_data': latest_data,
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        logger.error(f"Error fetching symbol detail: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/tab3/symbol-correlation/<symbol>', methods=['GET'])
def get_symbol_correlation(symbol):
    """Get correlation data for scatter plot"""
    try:
        correlation_query = """
            SELECT 
                current_trade_date,
                current_close_price,
                current_deliv_qty,
                current_turnover_lacs,
                current_ttl_trd_qnty
            FROM step03_compare_monthvspreviousmonth
            WHERE symbol = ?
            ORDER BY current_trade_date ASC
        """
        
        correlation_data = db.execute_query(correlation_query, (symbol,))
        
        # Format data for scatter plot
        scatter_data = []
        for row in correlation_data:
            if row['current_close_price'] and row['current_deliv_qty']:
                scatter_data.append({
                    'date': row['current_trade_date'],
                    'price': row['current_close_price'],
                    'delivery_qty': row['current_deliv_qty'],
                    'turnover': row['current_turnover_lacs'],
                    'total_traded': row['current_ttl_trd_qnty']
                })
        
        return jsonify({
            'symbol': symbol,
            'scatter_data': scatter_data,
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        logger.error(f"Error fetching symbol correlation: {e}")
        return jsonify({'error': str(e)}), 500

# ===================== UTILITY ENDPOINTS =====================

@app.route('/api/available-symbols', methods=['GET'])
def get_available_symbols():
    """Get list of all available symbols"""
    try:
        symbols_query = """
            SELECT DISTINCT 
                symbol,
                category,
                index_name,
                COUNT(*) as data_points
            FROM step03_compare_monthvspreviousmonth
            GROUP BY symbol, category, index_name
            ORDER BY symbol
        """
        
        symbols = db.execute_query(symbols_query)
        
        return jsonify({
            'symbols': symbols,
            'count': len(symbols),
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        logger.error(f"Error fetching symbols: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/available-dates', methods=['GET'])
def get_available_dates():
    """Get all available trading dates"""
    try:
        dates_query = """
            SELECT DISTINCT 
                CAST(current_trade_date AS DATE) as trading_date,
                COUNT(*) as symbol_count
            FROM step03_compare_monthvspreviousmonth
            GROUP BY CAST(current_trade_date AS DATE)
            ORDER BY trading_date DESC
        """
        
        dates_data = db.execute_query(dates_query)
        
        return jsonify({
            'dates': dates_data,
            'count': len(dates_data),
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        logger.error(f"Error fetching dates: {e}")
        return jsonify({'error': str(e)}), 500

# ===================== ERROR HANDLERS =====================

@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Endpoint not found'}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500

if __name__ == '__main__':
    logger.info("Starting NSE Delivery Analysis Dashboard API - Enhanced Version")
    logger.info("Available endpoints:")
    logger.info("=== TAB 1: Current Month Daily Performance ===")
    logger.info("  GET /api/tab1/daily-performance - Daily performance KPIs")
    logger.info("  GET /api/tab1/heatmap-data - Heatmap data for delivery percentage")
    logger.info("  GET /api/tab1/candlestick-data/<symbol> - OHLC data for specific symbol")
    logger.info("=== TAB 2: Monthly Trends and Comparison ===")
    logger.info("  GET /api/tab2/monthly-trends - Monthly trends and comparison")
    logger.info("  GET /api/tab2/category-comparison - Category-wise comparison")
    logger.info("=== TAB 3: Symbol-Specific Deep Dive ===")
    logger.info("  GET /api/tab3/symbol-detail/<symbol> - Detailed symbol analysis")
    logger.info("  GET /api/tab3/symbol-correlation/<symbol> - Correlation data for scatter plot")
    logger.info("=== UTILITY ENDPOINTS ===")
    logger.info("  GET /api/available-symbols - Get all available symbols")
    logger.info("  GET /api/available-dates - Get all available trading dates")
    logger.info("  GET /api/health - Health check")
    
    app.run(debug=True, host='0.0.0.0', port=5001)