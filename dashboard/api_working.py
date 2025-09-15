"""
NSE Delivery Analysis Dashboard API - Working Version
Provides REST endpoints for the dashboard frontend
"""

from flask import Flask, jsonify, request, send_file, send_from_directory
from flask_cors import CORS
import pyodbc
import json
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional

app = Flask(__name__)
# Enhanced CORS configuration for local development - allow all origins
CORS(app, origins="*", 
     allow_headers=["Content-Type", "Authorization"], 
     methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"])

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database connection
def get_db_connection():
    """Get database connection using working configuration"""
    try:
        connection_string = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=SRIKIRANREDDY\\SQLEXPRESS;"
            "DATABASE=master;"
            "Trusted_Connection=yes;"
        )
        return pyodbc.connect(connection_string)
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM step03_compare_monthvspreviousmonth")
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        
        return jsonify({
            'status': 'healthy',
            'database': 'connected',
            'records': count,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@app.route('/', methods=['GET'])
def serve_dashboard():
    """Serve the HTML dashboard"""
    try:
        return send_file('index.html')
    except Exception as e:
        logger.error(f"Failed to serve dashboard: {e}")
        return f"Dashboard not found: {e}", 404

@app.route('/<path:filename>', methods=['GET'])
def serve_static_files(filename):
    """Serve static files (CSS, JS, etc.)"""
    try:
        return send_from_directory('.', filename)
    except Exception as e:
        logger.error(f"Failed to serve static file {filename}: {e}")
        return f"File not found: {filename}", 404

@app.route('/api/delivery-data', methods=['GET'])
def get_delivery_data():
    """Get delivery comparison data with optional filters"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get query parameters
        symbol = request.args.get('symbol')
        category = request.args.get('category')
        index_name = request.args.get('index')
        limit = int(request.args.get('limit', 1000))
        
        # Build query with correct column names
        query = "SELECT TOP (?) * FROM step03_compare_monthvspreviousmonth WHERE 1=1"
        params = [limit]
        
        if symbol:
            query += " AND symbol = ?"
            params.append(symbol)
        
        if category:
            query += " AND category = ?"
            params.append(category)
            
        if index_name:
            query += " AND index_name = ?"
            params.append(index_name)
        
        query += " ORDER BY delivery_increase_pct DESC"
        
        cursor.execute(query, params)
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        
        delivery_data = []
        for row in rows:
            row_dict = dict(zip(columns, row))
            # Map to expected API format
            mapped_row = {
                'symbol': row_dict.get('symbol'),
                'index_name': row_dict.get('index_name'),
                'category': row_dict.get('category'),
                'date': row_dict.get('current_trade_date'),
                'current_month_delivery_percentage': row_dict.get('current_deliv_per'),
                'current_month_delivery_qty': row_dict.get('current_deliv_qty'),
                'current_month_delivery_value': row_dict.get('current_turnover_lacs'),
                'current_month_traded_qty': row_dict.get('current_ttl_trd_qnty'),
                'current_month_traded_value': row_dict.get('current_turnover_lacs'),
                'previous_month_delivery_percentage': row_dict.get('previous_deliv_per'),
                'previous_month_delivery_qty': row_dict.get('previous_deliv_qty'),
                'previous_month_delivery_value': row_dict.get('previous_turnover_lacs'),
                'previous_month_traded_qty': row_dict.get('previous_ttl_trd_qnty'),
                'previous_month_traded_value': row_dict.get('previous_turnover_lacs'),
                'delivery_percentage_change': row_dict.get('delivery_increase_pct'),
                'delivery_qty_change': row_dict.get('delivery_increase_abs'),
                'delivery_value_change': row_dict.get('delivery_increase_pct'),
                'traded_qty_change': 0,  # Calculate if needed
                'traded_value_change': 0,  # Calculate if needed
                # Add raw field names for dashboard calculations
                'delivery_increase_abs': row_dict.get('delivery_increase_abs'),
                'delivery_increase_pct': row_dict.get('delivery_increase_pct'),
                'current_deliv_qty': row_dict.get('current_deliv_qty'),
                'current_turnover_lacs': row_dict.get('current_turnover_lacs')
            }
            delivery_data.append(mapped_row)
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'delivery_data': delivery_data,
            'total_count': len(delivery_data),
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error fetching delivery data: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/summary-stats', methods=['GET'])
def get_summary_stats():
    """Get summary statistics"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Basic stats with correct column names
        cursor.execute("""
            SELECT 
                COUNT(*) as total_symbols,
                AVG(CAST(current_deliv_per AS FLOAT)) as avg_delivery_percentage,
                SUM(CAST(current_turnover_lacs AS FLOAT)) as total_delivery_value,
                SUM(CAST(current_turnover_lacs AS FLOAT)) as total_traded_value,
                SUM(CASE WHEN delivery_increase_pct > 0 THEN 1 ELSE 0 END) as top_gainers,
                SUM(CASE WHEN delivery_increase_pct < 0 THEN 1 ELSE 0 END) as top_losers
            FROM step03_compare_monthvspreviousmonth
        """)
        
        row = cursor.fetchone()
        summary_stats = {
            'total_symbols': row[0] or 0,
            'avg_delivery_percentage': float(row[1]) if row[1] else 0.0,
            'total_delivery_value': float(row[2]) if row[2] else 0.0,
            'total_traded_value': float(row[3]) if row[3] else 0.0,
            'top_gainers': row[4] or 0,
            'top_losers': row[5] or 0
        }
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'summary_stats': summary_stats,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error fetching summary stats: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/categories', methods=['GET'])
def get_categories():
    """Get available categories"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT DISTINCT category FROM step03_compare_monthvspreviousmonth WHERE category IS NOT NULL ORDER BY category")
        categories = [row[0] for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'categories': categories,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error fetching categories: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/indices', methods=['GET'])
def get_indices():
    """Get available indices"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT DISTINCT index_name FROM step03_compare_monthvspreviousmonth WHERE index_name IS NOT NULL ORDER BY index_name")
        indices = [row[0] for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'indices': indices,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error fetching indices: {e}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    logger.info("Starting NSE Delivery Analysis Dashboard API")
    logger.info("Available endpoints:")
    logger.info("  GET /api/health - Health check")
    logger.info("  GET /api/delivery-data - Get delivery data (with filters)")
    logger.info("  GET /api/summary-stats - Get summary statistics")
    logger.info("  GET /api/categories - Get available categories")
    logger.info("  GET /api/indices - Get available indices")
    
    app.run(debug=True, host='0.0.0.0', port=5000)