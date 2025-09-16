#!/usr/bin/env python3
"""
Working NSE Dashboard API - Final Version
"""
import os
import sys
import json
import logging
from datetime import datetime
import pyodbc
from flask import Flask, jsonify, render_template_string
from flask_cors import CORS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    "server": "SRIKIRANREDDY\\SQLEXPRESS",
    "database": "master",
    "driver": "ODBC Driver 17 for SQL Server",
    "trusted_connection": "yes"
}

def get_db_connection():
    """Get database connection"""
    connection_string = (
        f"DRIVER={{{DB_CONFIG['driver']}}};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        f"Trusted_Connection={DB_CONFIG['trusted_connection']};"
    )
    return pyodbc.connect(connection_string)

# Flask app
app = Flask(__name__)
CORS(app)

# Simple HTML dashboard
DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>NSE Delivery Analysis Dashboard</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        body { font-family: Arial, sans-serif; margin: 0; padding: 20px; background: #f0f0f0; }
        .header { background: #2c3e50; color: white; padding: 20px; text-align: center; margin-bottom: 20px; }
        .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-bottom: 20px; }
        .stat-card { background: white; padding: 20px; border-radius: 5px; text-align: center; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }
        .stat-value { font-size: 2em; color: #3498db; font-weight: bold; }
        .stat-label { color: #666; margin-top: 10px; }
        .chart-container { background: white; padding: 20px; border-radius: 5px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }
        .status { text-align: center; padding: 20px; }
        .loading { color: #f39c12; }
        .success { color: #27ae60; }
        .error { color: #e74c3c; }
    </style>
</head>
<body>
    <div class="header">
        <h1>NSE Delivery Analysis Dashboard</h1>
        <p>Professional Market Intelligence Platform</p>
    </div>
    
    <div id="status" class="status loading">Loading dashboard data...</div>
    
    <div id="dashboard" style="display: none;">
        <div class="stats">
            <div class="stat-card">
                <div class="stat-value" id="total-symbols">--</div>
                <div class="stat-label">Total Symbols</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="avg-delivery">--</div>
                <div class="stat-label">Average Delivery %</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="max-delivery">--</div>
                <div class="stat-label">Maximum Delivery %</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="total-records">--</div>
                <div class="stat-label">Total Records</div>
            </div>
        </div>
        
        <div class="chart-container">
            <h3>Top Symbols by Delivery Percentage</h3>
            <div id="main-chart" style="height: 400px;"></div>
        </div>
    </div>
    
    <script>
        async function loadDashboard() {
            try {
                const response = await fetch('/api/dashboard-data');
                if (!response.ok) throw new Error('Failed to load data');
                
                const data = await response.json();
                
                // Update stats
                document.getElementById('total-symbols').textContent = data.total_symbols || '--';
                document.getElementById('avg-delivery').textContent = data.avg_delivery ? data.avg_delivery.toFixed(2) + '%' : '--';
                document.getElementById('max-delivery').textContent = data.max_delivery ? data.max_delivery.toFixed(2) + '%' : '--';
                document.getElementById('total-records').textContent = data.total_records || '--';
                
                // Create chart
                if (data.chart_data) {
                    createChart(data.chart_data);
                }
                
                document.getElementById('status').style.display = 'none';
                document.getElementById('dashboard').style.display = 'block';
                
            } catch (error) {
                document.getElementById('status').innerHTML = 
                    '<div class="error">Failed to load data: ' + error.message + '</div>';
            }
        }
        
        function createChart(data) {
            const trace = {
                x: data.map(item => item.symbol),
                y: data.map(item => item.delivery_pct),
                type: 'bar',
                marker: { color: '#3498db' }
            };
            
            const layout = {
                title: 'Top 20 Symbols by Delivery Percentage',
                xaxis: { title: 'Symbol', tickangle: -45 },
                yaxis: { title: 'Delivery %' }
            };
            
            Plotly.newPlot('main-chart', [trace], layout, {responsive: true});
        }
        
        document.addEventListener('DOMContentLoaded', loadDashboard);
    </script>
</body>
</html>
"""

@app.route('/')
def index():
    """Serve dashboard"""
    return render_template_string(DASHBOARD_HTML)

@app.route('/api/health')
def health():
    """Health check"""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/dashboard-data')
def dashboard_data():
    """Get dashboard data"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get statistics
        cursor.execute("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT symbol) as total_symbols,
                AVG(CAST(current_deliv_pct AS FLOAT)) as avg_delivery,
                MAX(CAST(current_deliv_pct AS FLOAT)) as max_delivery
            FROM step03_compare_monthvspreviousmonth
            WHERE current_deliv_pct IS NOT NULL
        """)
        
        stats = cursor.fetchone()
        
        # Get top symbols for chart
        cursor.execute("""
            SELECT TOP 20 
                symbol,
                CAST(current_deliv_pct AS FLOAT) as delivery_pct
            FROM step03_compare_monthvspreviousmonth
            WHERE current_deliv_pct IS NOT NULL
            ORDER BY CAST(current_deliv_pct AS FLOAT) DESC
        """)
        
        chart_data = []
        for row in cursor.fetchall():
            chart_data.append({
                'symbol': row.symbol,
                'delivery_pct': float(row.delivery_pct)
            })
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'total_records': stats.total_records,
            'total_symbols': stats.total_symbols,
            'avg_delivery': float(stats.avg_delivery) if stats.avg_delivery else 0,
            'max_delivery': float(stats.max_delivery) if stats.max_delivery else 0,
            'chart_data': chart_data
        })
        
    except Exception as e:
        logger.error(f"Error in dashboard_data: {e}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    logger.info("Starting NSE Dashboard API - Working Version")
    logger.info("Testing database connection first...")
    
    try:
        # Test database connection
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM step03_compare_monthvspreviousmonth")
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        logger.info(f"Database OK: {count} records found")
        
        # Start Flask app
        logger.info("Starting Flask server on http://localhost:5000")
        app.run(host='127.0.0.1', port=5000, debug=False, use_reloader=False)
        
    except Exception as e:
        logger.error(f"Failed to start: {e}")
        import traceback
        traceback.print_exc()