#!/usr/bin/env python3
"""
Working API Dashboard - Fixed Version
"""
import os
import sys
import json
import logging
from datetime import datetime, timedelta
import pyodbc
import numpy as np
from flask import Flask, jsonify, render_template_string, send_from_directory
from flask_cors import CORS

# Set up logging without Unicode characters
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load database configuration
def load_db_config():
    try:
        config_path = os.path.join(os.path.dirname(__file__), 'database_config.json')
        with open(config_path, 'r') as f:
            config = json.load(f)
        logger.info("Database configuration loaded successfully")
        return config
    except Exception as e:
        logger.error(f"Failed to load database config: {e}")
        raise

# Database connection function
def get_db_connection():
    config = load_db_config()
    connection_string = (
        f"DRIVER={{{config['driver']}}};"
        f"SERVER={config['server']};"
        f"DATABASE={config['database']};"
        f"Trusted_Connection={config['trusted_connection']};"
    )
    return pyodbc.connect(connection_string)

# Create Flask app
app = Flask(__name__)
CORS(app)

# Enhanced HTML template
ENHANCED_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>NSE Delivery Analysis Dashboard - Enhanced</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: #0f1419; color: #e6e6e6; }
        .header { background: linear-gradient(135deg, #1e3a8a 0%, #3b82f6 100%); padding: 20px; text-align: center; }
        .header h1 { color: white; margin-bottom: 10px; }
        .header p { color: #e2e8f0; }
        .dashboard-container { max-width: 1400px; margin: 0 auto; padding: 20px; }
        .kpi-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 20px; margin-bottom: 30px; }
        .kpi-card { background: #1a1a1a; border: 1px solid #333; border-radius: 10px; padding: 20px; text-align: center; transition: transform 0.3s; }
        .kpi-card:hover { transform: translateY(-5px); }
        .kpi-value { font-size: 2em; font-weight: bold; color: #3b82f6; margin-bottom: 10px; }
        .kpi-label { color: #9ca3af; font-size: 0.9em; }
        .chart-container { background: #1a1a1a; border: 1px solid #333; border-radius: 10px; padding: 20px; margin-bottom: 20px; }
        .chart-container h3 { color: #e6e6e6; margin-bottom: 15px; text-align: center; }
        .status { padding: 20px; text-align: center; }
        .error { color: #ef4444; }
        .success { color: #10b981; }
        .loading { color: #f59e0b; }
    </style>
</head>
<body>
    <div class="header">
        <h1>NSE Delivery Analysis Dashboard</h1>
        <p>Professional Market Intelligence Platform - Enhanced Version</p>
    </div>
    
    <div class="dashboard-container">
        <div id="status" class="status loading">Loading dashboard data...</div>
        
        <div id="kpi-section" style="display: none;">
            <div class="kpi-grid">
                <div class="kpi-card">
                    <div class="kpi-value" id="total-symbols">--</div>
                    <div class="kpi-label">Total Symbols</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value" id="avg-delivery">--</div>
                    <div class="kpi-label">Average Delivery %</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value" id="max-delivery">--</div>
                    <div class="kpi-label">Maximum Delivery %</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-value" id="active-markets">--</div>
                    <div class="kpi-label">Active Markets</div>
                </div>
            </div>
        </div>
        
        <div id="chart-section" style="display: none;">
            <div class="chart-container">
                <h3>Market Performance Overview</h3>
                <div id="main-chart" style="height: 400px;"></div>
            </div>
        </div>
    </div>
    
    <script>
        // Dashboard functionality
        async function loadDashboard() {
            try {
                // Test API health
                const healthResponse = await fetch('/api/health');
                if (!healthResponse.ok) throw new Error('API health check failed');
                
                // Load basic data
                const dataResponse = await fetch('/api/basic-data');
                if (!dataResponse.ok) throw new Error('Failed to load data');
                
                const data = await dataResponse.json();
                
                // Update KPIs
                document.getElementById('total-symbols').textContent = data.total_symbols || '--';
                document.getElementById('avg-delivery').textContent = data.avg_delivery ? data.avg_delivery.toFixed(2) + '%' : '--';
                document.getElementById('max-delivery').textContent = data.max_delivery ? data.max_delivery.toFixed(2) + '%' : '--';
                document.getElementById('active-markets').textContent = data.active_markets || '--';
                
                // Create chart
                if (data.chart_data && data.chart_data.length > 0) {
                    createChart(data.chart_data);
                }
                
                // Show sections
                document.getElementById('status').style.display = 'none';
                document.getElementById('kpi-section').style.display = 'block';
                document.getElementById('chart-section').style.display = 'block';
                
            } catch (error) {
                console.error('Dashboard loading error:', error);
                document.getElementById('status').innerHTML = 
                    '<div class="error">Failed to load dashboard data. Error: ' + error.message + '</div>';
            }
        }
        
        function createChart(data) {
            const trace = {
                x: data.map(item => item.symbol),
                y: data.map(item => item.delivery_percentage),
                type: 'bar',
                marker: {
                    color: data.map(item => item.delivery_percentage > 50 ? '#10b981' : '#3b82f6'),
                    opacity: 0.8
                },
                name: 'Delivery %'
            };
            
            const layout = {
                title: {
                    text: 'Top Symbols by Delivery Percentage',
                    font: { color: '#e6e6e6' }
                },
                xaxis: { 
                    title: 'Symbol',
                    color: '#9ca3af',
                    tickangle: -45
                },
                yaxis: { 
                    title: 'Delivery %',
                    color: '#9ca3af'
                },
                plot_bgcolor: '#1a1a1a',
                paper_bgcolor: '#1a1a1a',
                font: { color: '#e6e6e6' }
            };
            
            Plotly.newPlot('main-chart', [trace], layout, {responsive: true});
        }
        
        // Load dashboard when page loads
        document.addEventListener('DOMContentLoaded', loadDashboard);
    </script>
</body>
</html>
"""

@app.route('/')
def index():
    """Serve the enhanced dashboard"""
    return render_template_string(ENHANCED_HTML)

@app.route('/api/health')
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "message": "Enhanced NSE Dashboard API is running",
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/basic-data')
def basic_data():
    """Get basic dashboard data"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get basic statistics
        cursor.execute("""
            SELECT 
                COUNT(DISTINCT symbol) as total_symbols,
                AVG(CAST(current_deliv_pct AS FLOAT)) as avg_delivery,
                MAX(CAST(current_deliv_pct AS FLOAT)) as max_delivery,
                COUNT(DISTINCT category) as active_markets
            FROM step03_compare_monthvspreviousmonth
            WHERE current_deliv_pct IS NOT NULL
        """)
        
        stats = cursor.fetchone()
        
        # Get top symbols for chart
        cursor.execute("""
            SELECT TOP 20 
                symbol,
                CAST(current_deliv_pct AS FLOAT) as delivery_percentage
            FROM step03_compare_monthvspreviousmonth
            WHERE current_deliv_pct IS NOT NULL
            ORDER BY CAST(current_deliv_pct AS FLOAT) DESC
        """)
        
        chart_data = []
        for row in cursor.fetchall():
            chart_data.append({
                'symbol': row.symbol,
                'delivery_percentage': float(row.delivery_percentage)
            })
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'total_symbols': stats.total_symbols,
            'avg_delivery': float(stats.avg_delivery) if stats.avg_delivery else 0,
            'max_delivery': float(stats.max_delivery) if stats.max_delivery else 0,
            'active_markets': stats.active_markets,
            'chart_data': chart_data
        })
        
    except Exception as e:
        logger.error(f"Error in basic_data endpoint: {e}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    logger.info("Starting Enhanced NSE Dashboard API - FIXED VERSION")
    logger.info("Available endpoints:")
    logger.info("  GET / - Enhanced Dashboard")
    logger.info("  GET /api/health - Health check")
    logger.info("  GET /api/basic-data - Basic dashboard data")
    
    try:
        app.run(host='127.0.0.1', port=5555, debug=False, use_reloader=False)
    except Exception as e:
        logger.error(f"Failed to start Flask app: {e}")
        import traceback
        traceback.print_exc()