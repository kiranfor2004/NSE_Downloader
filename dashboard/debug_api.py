#!/usr/bin/env python3
"""
Debug version of API dashboard to identify startup issues
"""
import os
import sys
import traceback
import logging

# Set up comprehensive logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('debug.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def check_environment():
    """Check all environment requirements"""
    logger.info("=== ENVIRONMENT CHECK ===")
    logger.info(f"Python version: {sys.version}")
    logger.info(f"Current working directory: {os.getcwd()}")
    logger.info(f"Script directory: {os.path.dirname(os.path.abspath(__file__))}")
    
    # Check required imports
    required_modules = ['flask', 'flask_cors', 'pyodbc', 'json', 'datetime', 'numpy']
    for module in required_modules:
        try:
            if module == 'flask_cors':
                import flask_cors
                logger.info(f"✓ {module} imported successfully")
            else:
                __import__(module)
                logger.info(f"✓ {module} imported successfully")
        except ImportError as e:
            logger.error(f"✗ Failed to import {module}: {e}")
            return False
    return True

def check_database_config():
    """Check database configuration"""
    logger.info("=== DATABASE CONFIG CHECK ===")
    try:
        config_path = os.path.join(os.path.dirname(__file__), 'database_config.json')
        logger.info(f"Looking for config at: {config_path}")
        
        if not os.path.exists(config_path):
            logger.error(f"Database config file not found at: {config_path}")
            return False
            
        with open(config_path, 'r') as f:
            import json
            config = json.load(f)
            logger.info(f"✓ Database config loaded: {config}")
            return True
    except Exception as e:
        logger.error(f"✗ Database config error: {e}")
        traceback.print_exc()
        return False

def test_database_connection():
    """Test actual database connection"""
    logger.info("=== DATABASE CONNECTION TEST ===")
    try:
        import pyodbc
        import json
        
        config_path = os.path.join(os.path.dirname(__file__), 'database_config.json')
        with open(config_path, 'r') as f:
            db_config = json.load(f)
        
        connection_string = (
            f"DRIVER={{{db_config['driver']}}};"
            f"SERVER={db_config['server']};"
            f"DATABASE={db_config['database']};"
            f"UID={db_config['username']};"
            f"PWD={db_config['password']}"
        )
        
        logger.info("Attempting database connection...")
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        
        # Test query
        cursor.execute("SELECT COUNT(*) FROM step03_compare_monthvspreviousmonth")
        count = cursor.fetchone()[0]
        logger.info(f"✓ Database connection successful. Table has {count} records")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"✗ Database connection failed: {e}")
        traceback.print_exc()
        return False

def create_minimal_flask_app():
    """Create minimal Flask app with debugging"""
    logger.info("=== CREATING FLASK APP ===")
    try:
        from flask import Flask, jsonify
        from flask_cors import CORS
        
        app = Flask(__name__)
        CORS(app)
        
        @app.route('/health')
        def health():
            logger.info("Health endpoint called")
            return jsonify({"status": "healthy", "message": "Debug API is running"})
        
        @app.route('/')
        def index():
            logger.info("Index endpoint called")
            return "<h1>Debug API Server</h1><p>Server is running successfully!</p>"
        
        logger.info("✓ Flask app created successfully")
        return app
        
    except Exception as e:
        logger.error(f"✗ Flask app creation failed: {e}")
        traceback.print_exc()
        return None

def main():
    """Main execution function"""
    logger.info("=== STARTING DEBUG INVESTIGATION ===")
    
    # Step 1: Check environment
    if not check_environment():
        logger.error("Environment check failed. Exiting.")
        return False
    
    # Step 2: Check database config
    if not check_database_config():
        logger.error("Database config check failed. Exiting.")
        return False
    
    # Step 3: Test database connection
    if not test_database_connection():
        logger.error("Database connection test failed. Exiting.")
        return False
    
    # Step 4: Create Flask app
    app = create_minimal_flask_app()
    if not app:
        logger.error("Flask app creation failed. Exiting.")
        return False
    
    # Step 5: Try to run the app
    logger.info("=== STARTING FLASK SERVER ===")
    try:
        logger.info("About to call app.run()...")
        app.run(host='0.0.0.0', port=5003, debug=True)
        logger.info("Flask app.run() completed")
        
    except Exception as e:
        logger.error(f"✗ Flask server failed to start: {e}")
        traceback.print_exc()
        return False
    
    return True

if __name__ == '__main__':
    try:
        success = main()
        if success:
            logger.info("=== DEBUG COMPLETED SUCCESSFULLY ===")
        else:
            logger.error("=== DEBUG FAILED ===")
    except Exception as e:
        logger.error(f"Unexpected error in main: {e}")
        traceback.print_exc()