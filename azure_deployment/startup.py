import os
import sys

# Add the application directory to Python path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# Set Azure database environment variables
os.environ['AZURE_DB_SERVER'] = 'nse-dashboard-kiran-001-sql-server.database.windows.net'
os.environ['AZURE_DB_NAME'] = 'nse-dashboard-kiran-001-db'
os.environ['AZURE_DB_USERNAME'] = 'nseadmin'
os.environ['AZURE_DB_PASSWORD'] = 'NSEData@2024#Secure'

# Import the Flask application
from app import app

if __name__ == '__main__':
    # Azure Web Apps will set the PORT environment variable
    port = int(os.environ.get('PORT', 8000))
    app.run(host='0.0.0.0', port=port, debug=False)