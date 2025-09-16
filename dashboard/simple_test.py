#!/usr/bin/env python3
"""
Ultra Simple Working Dashboard - No Crash Version
"""
from flask import Flask, jsonify, render_template_string
import pyodbc

app = Flask(__name__)

# Simple HTML
HTML = """
<!DOCTYPE html>
<html>
<head><title>NSE Dashboard</title></head>
<body>
    <h1>NSE Delivery Analysis Dashboard</h1>
    <div id="status">Loading...</div>
    <div id="data"></div>
    
    <script>
        fetch('/api/test')
        .then(r => r.json())
        .then(data => {
            document.getElementById('status').innerHTML = 'Connected!';
            document.getElementById('data').innerHTML = 
                '<p>Records: ' + data.count + '</p>' +
                '<p>Status: ' + data.status + '</p>';
        })
        .catch(e => {
            document.getElementById('status').innerHTML = 'Error: ' + e.message;
        });
    </script>
</body>
</html>
"""

@app.route('/')
def home():
    return render_template_string(HTML)

@app.route('/api/test')
def test():
    try:
        conn = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=SRIKIRANREDDY\\SQLEXPRESS;"
            "DATABASE=master;"
            "Trusted_Connection=yes;"
        )
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM step03_compare_monthvspreviousmonth")
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        
        return jsonify({
            'status': 'success', 
            'count': count,
            'message': 'Database connected successfully'
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'count': 0,
            'message': f'Database error: {str(e)}'
        })

if __name__ == '__main__':
    print("Starting simple test server on http://localhost:5000")
    app.run(host='127.0.0.1', port=5000, debug=False, use_reloader=False)