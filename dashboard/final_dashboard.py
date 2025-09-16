#!/usr/bin/env python3
"""
Final Working NSE Dashboard - Guaranteed to Work
"""
import os
import json
import pyodbc
from flask import Flask, jsonify, render_template_string
from flask_cors import CORS

# Create Flask app
app = Flask(__name__)
CORS(app)

# Database connection
def get_db_connection():
    connection_string = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=SRIKIRANREDDY\\SQLEXPRESS;"
        "DATABASE=master;"
        "Trusted_Connection=yes;"
    )
    return pyodbc.connect(connection_string)

# Simple HTML dashboard
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>NSE Dashboard</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
        .header { background: #2c3e50; color: white; padding: 20px; text-align: center; margin-bottom: 20px; }
        .stats { display: flex; justify-content: space-around; margin-bottom: 20px; }
        .stat { background: white; padding: 20px; border-radius: 5px; text-align: center; min-width: 150px; }
        .stat-value { font-size: 24px; color: #3498db; font-weight: bold; }
        .chart { background: white; padding: 20px; border-radius: 5px; }
        .loading { text-align: center; padding: 50px; color: #666; }
        .error { text-align: center; padding: 50px; color: #e74c3c; }
    </style>
</head>
<body>
    <div class="header">
        <h1>NSE Delivery Analysis Dashboard</h1>
        <p>Real-time Market Data</p>
    </div>
    
    <div id="loading" class="loading">Loading data...</div>
    <div id="error" class="error" style="display:none;">Failed to load data</div>
    
    <div id="content" style="display:none;">
        <div class="stats">
            <div class="stat">
                <div class="stat-value" id="total-records">--</div>
                <div>Total Records</div>
            </div>
            <div class="stat">
                <div class="stat-value" id="total-symbols">--</div>
                <div>Total Symbols</div>
            </div>
            <div class="stat">
                <div class="stat-value" id="avg-delivery">--</div>
                <div>Avg Delivery %</div>
            </div>
            <div class="stat">
                <div class="stat-value" id="max-delivery">--</div>
                <div>Max Delivery %</div>
            </div>
        </div>
        
        <div class="chart">
            <h3>Top Symbols by Delivery Percentage</h3>
            <div id="chart" style="height: 400px;"></div>
        </div>
    </div>
    
    <script>
        async function loadData() {
            try {
                const response = await fetch('/api/data');
                const data = await response.json();
                
                document.getElementById('total-records').textContent = data.total_records;
                document.getElementById('total-symbols').textContent = data.total_symbols;
                document.getElementById('avg-delivery').textContent = data.avg_delivery.toFixed(2) + '%';
                document.getElementById('max-delivery').textContent = data.max_delivery.toFixed(2) + '%';
                
                // Create chart
                const trace = {
                    x: data.chart_data.map(d => d.symbol),
                    y: data.chart_data.map(d => d.delivery_pct),
                    type: 'bar',
                    marker: { color: '#3498db' }
                };
                
                Plotly.newPlot('chart', [trace], {
                    title: 'Top 15 Symbols by Delivery %',
                    xaxis: { title: 'Symbol' },
                    yaxis: { title: 'Delivery %' }
                });
                
                document.getElementById('loading').style.display = 'none';
                document.getElementById('content').style.display = 'block';
                
            } catch (error) {
                document.getElementById('loading').style.display = 'none';
                document.getElementById('error').style.display = 'block';
                document.getElementById('error').textContent = 'Error: ' + error.message;
            }
        }
        
        // Load data when page loads
        document.addEventListener('DOMContentLoaded', loadData);
    </script>
</body>
</html>
"""

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/api/data')
def get_data():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get basic stats
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
        
        # Get top symbols
        cursor.execute("""
            SELECT TOP 15 
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
            'avg_delivery': float(stats.avg_delivery or 0),
            'max_delivery': float(stats.max_delivery or 0),
            'chart_data': chart_data
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/health')
def health():
    return jsonify({'status': 'healthy'})

if __name__ == '__main__':
    print("=" * 50)
    print("🚀 Starting NSE Dashboard Server")
    print("=" * 50)
    
    # Test database connection first
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM step03_compare_monthvspreviousmonth")
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        print(f"✅ Database connection successful: {count} records found")
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        exit(1)
    
    print(f"🌐 Dashboard will be available at: http://localhost:5000")
    print(f"📊 Data source: step03_compare_monthvspreviousmonth table")
    print("=" * 50)
    
    # Start Flask server
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)