"""
Simple API server to test the dashboard
"""
from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS
import json

app = Flask(__name__)
CORS(app)

@app.route('/')
def serve_dashboard():
    return send_from_directory('.', 'index.html')

@app.route('/<path:filename>')
def serve_static_files(filename):
    return send_from_directory('.', filename)

@app.route('/api/health')
def health():
    return jsonify({
        'status': 'healthy',
        'timestamp': '2025-09-15T20:00:00'
    })

@app.route('/api/delivery-data')
def get_delivery_data():
    # Return sample data for testing
    sample_data = [
        {
            'symbol': 'RELIANCE',
            'delivery_increase_pct': 150.5,
            'category': 'Large Cap',
            'current_deliv_qty': 1000000,
            'current_trade_date': '2025-09-15',
            'current_close_price': 2500.0
        },
        {
            'symbol': 'TCS',
            'delivery_increase_pct': 75.2,
            'category': 'Large Cap',
            'current_deliv_qty': 500000,
            'current_trade_date': '2025-09-15',
            'current_close_price': 3500.0
        },
        {
            'symbol': 'INFY',
            'delivery_increase_pct': 45.8,
            'category': 'Large Cap',
            'current_deliv_qty': 750000,
            'current_trade_date': '2025-09-15',
            'current_close_price': 1800.0
        }
    ]
    
    return jsonify({
        'data': sample_data,
        'total_count': len(sample_data),
        'page_size': len(sample_data),
        'offset': 0
    })

if __name__ == '__main__':
    print("Starting simple test API server...")
    app.run(debug=True, host='0.0.0.0', port=5000)