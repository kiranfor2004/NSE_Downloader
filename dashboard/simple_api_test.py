"""
Simple Flask API Test
"""
from flask import Flask, jsonify
from flask_cors import CORS
import pyodbc
import json

app = Flask(__name__)
CORS(app)

@app.route('/api/health', methods=['GET'])
def health_check():
    try:
        # Load config and test connection
        with open('database_config.json', 'r') as f:
            config = json.load(f)
        
        master_config = config.get('master_database', {})
        server_name = master_config.get('server', 'SRIKIRANREDDY\\SQLEXPRESS')
        database_name = master_config.get('database', 'master')
        driver_name = master_config.get('driver', 'ODBC Driver 17 for SQL Server')
        
        connection_string = (
            f"DRIVER={{{driver_name}}};"
            f"SERVER={server_name};"
            f"DATABASE={database_name};"
            f"Trusted_Connection=yes;"
        )
        
        # Test database connection
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM step03_compare_monthvspreviousmonth")
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        
        return jsonify({
            'status': 'healthy',
            'database': 'connected',
            'records': count,
            'timestamp': '2024-12-28'
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

if __name__ == '__main__':
    print("Starting simple Flask API test...")
    app.run(debug=True, host='127.0.0.1', port=5001)