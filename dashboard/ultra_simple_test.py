#!/usr/bin/env python3
"""
Ultra Simple API to test database connection
"""
import os
import sys
import json
import logging
import traceback
import pyodbc

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_database():
    """Test database connection"""
    try:
        logger.info("Testing database connection...")
        
        config_path = os.path.join(os.path.dirname(__file__), 'database_config.json')
        logger.info(f"Config path: {config_path}")
        
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        logger.info(f"Config loaded: {config}")
        
        connection_string = (
            f"DRIVER={{{config['driver']}}};"
            f"SERVER={config['server']};"
            f"DATABASE={config['database']};"
            f"Trusted_Connection={config['trusted_connection']};"
        )
        
        logger.info(f"Connection string: {connection_string}")
        
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM step03_compare_monthvspreviousmonth")
        count = cursor.fetchone()[0]
        
        logger.info(f"SUCCESS: Database has {count} records")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        traceback.print_exc()
        return False

def test_flask():
    """Test minimal Flask app"""
    try:
        logger.info("Testing Flask...")
        
        from flask import Flask, jsonify
        from flask_cors import CORS
        
        app = Flask(__name__)
        CORS(app)
        
        @app.route('/test')
        def test():
            return jsonify({"status": "working", "message": "Flask is running"})
        
        logger.info("Flask app created successfully")
        logger.info("Starting server on port 5556...")
        
        app.run(host='127.0.0.1', port=5556, debug=False, use_reloader=False)
        
    except Exception as e:
        logger.error(f"Flask test failed: {e}")
        traceback.print_exc()
        return False

if __name__ == '__main__':
    logger.info("=== ULTRA SIMPLE TEST ===")
    
    # Test database first
    if test_database():
        logger.info("Database test PASSED")
        
        # Test Flask
        test_flask()
    else:
        logger.error("Database test FAILED")