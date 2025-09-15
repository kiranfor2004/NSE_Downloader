"""
NSE Delivery Analysis Dashboard API
Provides REST endpoints for the dashboard frontend
"""

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import pyodbc
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional
import os

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
            logger.info(f"Connection string: {self.connection_string}")
        except Exception as e:
            logger.error(f"Failed to load database config: {e}")
            # Fallback configuration using same pattern as working test
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
                        else:
                            row_dict[columns[i]] = value
                    result.append(row_dict)
                
                return result
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise

# Initialize database connection
db = DatabaseConnection()

@app.route('/')
def serve_dashboard():
    """Serve the main dashboard HTML"""
    return send_from_directory('.', 'index.html')

@app.route('/<path:filename>')
def serve_static_files(filename):
    """Serve static files like CSS, JS, etc."""
    return send_from_directory('.', filename)

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

@app.route('/api/delivery-data', methods=['GET'])
def get_delivery_data():
    """Get all delivery analysis data"""
    try:
        # Get query parameters for filtering
        category = request.args.get('category')
        trading_date = request.args.get('trading_date')  # New date filter
        limit = request.args.get('limit', type=int)
        offset = request.args.get('offset', type=int, default=0)
        sort_by = request.args.get('sort_by', 'delivery_increase_pct')
        sort_order = request.args.get('sort_order', 'DESC')
        search = request.args.get('search')

        # Build base query
        base_query = """
            SELECT 
                symbol,
                index_name,
                category,
                current_deliv_qty,
                delivery_increase_pct,
                comparison_type,
                current_trade_date,
                current_close_price,
                previous_deliv_qty,
                previous_baseline_date,
                previous_close_price
            FROM step03_compare_monthvspreviousmonth
            WHERE 1=1
        """
        
        params = []
        
        # Add filters
        if category and category != 'all':
            base_query += " AND category = ?"
            params.append(category)
        
        if trading_date:
            base_query += " AND current_trade_date = ?"
            params.append(trading_date)
        
        if search:
            base_query += " AND symbol LIKE ?"
            params.append(f"%{search}%")
        
        # Add sorting
        valid_sort_columns = ['delivery_increase_pct', 'current_trade_date', 'symbol', 'category']
        if sort_by in valid_sort_columns:
            base_query += f" ORDER BY {sort_by} {sort_order}"
        else:
            base_query += " ORDER BY delivery_increase_pct DESC"
        
        # Add pagination
        if limit:
            base_query += f" OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY"
        
        data = db.execute_query(base_query, tuple(params))
        
        # Get total count for pagination
        count_query = """
            SELECT COUNT(*) as total_count
            FROM step03_compare_monthvspreviousmonth
            WHERE 1=1
        """
        
        count_params = []
        if category and category != 'all':
            count_query += " AND category = ?"
            count_params.append(category)
        
        if search:
            count_query += " AND symbol LIKE ?"
            count_params.append(f"%{search}%")
        
        count_result = db.execute_query(count_query, tuple(count_params))
        total_count = count_result[0]['total_count'] if count_result else 0
        
        return jsonify({
            'data': data,
            'total_count': total_count,
            'page_size': limit,
            'offset': offset
        })

    except Exception as e:
        logger.error(f"Error fetching delivery data: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/summary-stats', methods=['GET'])
def get_summary_stats():
    """Get summary statistics for the dashboard"""
    try:
        # Overall statistics
        stats_query = """
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT symbol) as unique_symbols,
                AVG(delivery_increase_pct) as avg_increase,
                MAX(delivery_increase_pct) as max_increase,
                MIN(delivery_increase_pct) as min_increase
            FROM step03_compare_monthvspreviousmonth
        """
        
        stats = db.execute_query(stats_query)[0]
        
        # Category distribution
        category_query = """
            SELECT 
                category,
                COUNT(*) as count,
                AVG(delivery_increase_pct) as avg_increase
            FROM step03_compare_monthvspreviousmonth
            GROUP BY category
            ORDER BY count DESC
        """
        
        category_stats = db.execute_query(category_query)
        
        # Index distribution
        index_query = """
            SELECT 
                index_name,
                category,
                COUNT(*) as count,
                AVG(delivery_increase_pct) as avg_increase
            FROM step03_compare_monthvspreviousmonth
            GROUP BY index_name, category
            ORDER BY count DESC
        """
        
        index_stats = db.execute_query(index_query)
        
        # Top performers
        top_performers_query = """
            SELECT TOP 10
                symbol,
                index_name,
                category,
                delivery_increase_pct,
                current_deliv_qty,
                current_trade_date
            FROM step03_compare_monthvspreviousmonth
            ORDER BY delivery_increase_pct DESC
        """
        
        top_performers = db.execute_query(top_performers_query)
        
        # Monthly trends
        trend_query = """
            SELECT 
                comparison_type,
                COUNT(*) as count,
                AVG(delivery_increase_pct) as avg_increase
            FROM step03_compare_monthvspreviousmonth
            GROUP BY comparison_type
            ORDER BY comparison_type
        """
        
        trends = db.execute_query(trend_query)
        
        return jsonify({
            'overall_stats': stats,
            'category_distribution': category_stats,
            'index_distribution': index_stats,
            'top_performers': top_performers,
            'monthly_trends': trends,
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        logger.error(f"Error fetching summary stats: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/performance-analysis', methods=['GET'])
def get_performance_analysis():
    """Get performance analysis data"""
    try:
        # Performance distribution by ranges
        distribution_query = """
            SELECT 
                CASE 
                    WHEN delivery_increase_pct <= 50 THEN '0-50%'
                    WHEN delivery_increase_pct <= 100 THEN '50-100%'
                    WHEN delivery_increase_pct <= 500 THEN '100-500%'
                    WHEN delivery_increase_pct <= 1000 THEN '500-1000%'
                    ELSE '1000%+'
                END as performance_range,
                COUNT(*) as count
            FROM step03_compare_monthvspreviousmonth
            GROUP BY 
                CASE 
                    WHEN delivery_increase_pct <= 50 THEN '0-50%'
                    WHEN delivery_increase_pct <= 100 THEN '50-100%'
                    WHEN delivery_increase_pct <= 500 THEN '100-500%'
                    WHEN delivery_increase_pct <= 1000 THEN '500-1000%'
                    ELSE '1000%+'
                END
            ORDER BY 
                CASE 
                    WHEN delivery_increase_pct <= 50 THEN 1
                    WHEN delivery_increase_pct <= 100 THEN 2
                    WHEN delivery_increase_pct <= 500 THEN 3
                    WHEN delivery_increase_pct <= 1000 THEN 4
                    ELSE 5
                END
        """
        
        distribution = db.execute_query(distribution_query)
        
        # Sector-wise performance (for sectoral indices only)
        sector_query = """
            SELECT 
                index_name,
                COUNT(*) as count,
                AVG(delivery_increase_pct) as avg_increase,
                MAX(delivery_increase_pct) as max_increase,
                MIN(delivery_increase_pct) as min_increase
            FROM step03_compare_monthvspreviousmonth
            WHERE category = 'Sectoral'
            GROUP BY index_name
            ORDER BY avg_increase DESC
        """
        
        sector_performance = db.execute_query(sector_query)
        
        return jsonify({
            'performance_distribution': distribution,
            'sector_performance': sector_performance,
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        logger.error(f"Error fetching performance analysis: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/symbol/<symbol>', methods=['GET'])
def get_symbol_details(symbol):
    """Get detailed information for a specific symbol"""
    try:
        query = """
            SELECT *
            FROM step03_compare_monthvspreviousmonth
            WHERE symbol = ?
        """
        
        data = db.execute_query(query, (symbol,))
        
        if not data:
            return jsonify({'error': 'Symbol not found'}), 404
        
        return jsonify({
            'symbol_data': data[0],
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        logger.error(f"Error fetching symbol details: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/categories', methods=['GET'])
def get_categories():
    """Get list of available categories"""
    try:
        query = """
            SELECT DISTINCT category
            FROM step03_compare_monthvspreviousmonth
            ORDER BY category
        """
        
        categories = db.execute_query(query)
        category_list = [cat['category'] for cat in categories]
        
        return jsonify({
            'categories': category_list,
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        logger.error(f"Error fetching categories: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/indices', methods=['GET'])
def get_indices():
    """Get list of available indices with their categories"""
    try:
        query = """
            SELECT DISTINCT 
                index_name,
                category,
                COUNT(*) as symbol_count
            FROM step03_compare_monthvspreviousmonth
            GROUP BY index_name, category
            ORDER BY symbol_count DESC
        """
        
        indices = db.execute_query(query)
        
        return jsonify({
            'indices': indices,
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        logger.error(f"Error fetching indices: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/trading-dates', methods=['GET'])
def get_trading_dates():
    """Get all available trading dates"""
    try:
        query = """
            SELECT DISTINCT current_trade_date 
            FROM step03_compare_monthvspreviousmonth 
            ORDER BY current_trade_date DESC
        """
        
        dates = db.execute_query(query)
        
        # Convert to simple list of date strings
        date_list = [row['current_trade_date'] for row in dates]
        
        return jsonify({
            'trading_dates': date_list,
            'count': len(date_list),
            'latest_date': date_list[0] if date_list else None,
            'earliest_date': date_list[-1] if date_list else None,
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        logger.error(f"Error fetching trading dates: {e}")
        return jsonify({'error': str(e)}), 500

@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Endpoint not found'}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500

if __name__ == '__main__':
    logger.info("Starting NSE Delivery Analysis Dashboard API")
    logger.info(f"Available endpoints:")
    logger.info(f"  GET /api/health - Health check")
    logger.info(f"  GET /api/delivery-data - Get delivery data (with filters)")
    logger.info(f"  GET /api/summary-stats - Get summary statistics")
    logger.info(f"  GET /api/performance-analysis - Get performance analysis")
    logger.info(f"  GET /api/symbol/<symbol> - Get symbol details")
    logger.info(f"  GET /api/categories - Get available categories")
    logger.info(f"  GET /api/indices - Get available indices")
    logger.info(f"  GET /api/trading-dates - Get available trading dates")
    logger.info(f"  GET /api/categories - Get available categories")
    logger.info(f"  GET /api/indices - Get available indices")
    
    app.run(debug=True, host='0.0.0.0', port=5000)