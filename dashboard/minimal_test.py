"""
Minimal Flask test to diagnose connection issues
"""

from flask import Flask, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

@app.route('/test')
def test():
    return jsonify({'message': 'Test successful'})

if __name__ == '__main__':
    print("Starting minimal Flask test server...")
    try:
        app.run(debug=True, host='0.0.0.0', port=5002)
    except Exception as e:
        print(f"Error starting server: {e}")