# NSE Dashboard - Azure Deployment Package

## 📁 Package Structure

```
azure_deployment/
├── app.py                 # Main Flask application
├── requirements.txt       # Python dependencies
├── startup.py            # Azure Web App startup script
├── web.config            # IIS configuration for Azure
├── templates/
│   └── dashboard.html    # Dashboard template
├── DEPLOYMENT_GUIDE.md   # Detailed deployment instructions
└── README.md            # This file
```

## 🚀 Quick Start

### 1. Local Testing

```powershell
cd azure_deployment
pip install -r requirements.txt

# Set environment variables for local testing
$env:LOCAL_DB_SERVER="SRIKIRANREDDY\SQLEXPRESS"
$env:LOCAL_DB_NAME="your_database_name"

python app.py
```

Visit: http://localhost:5000

### 2. Azure Deployment

#### Option A: ZIP Deployment
1. Compress the entire `azure_deployment` folder
2. Upload to Azure Web App via Portal or Azure CLI
3. Configure environment variables in Azure Portal

#### Option B: GitHub Deployment
1. Push this folder to GitHub repository
2. Configure GitHub Actions for automatic deployment
3. Set up publish profile in repository secrets

## 🔧 Configuration Required

### Environment Variables (Set in Azure Portal)

**For Azure SQL Database:**
```
AZURE_DB_SERVER=your-server.database.windows.net
AZURE_DB_NAME=your-database-name
AZURE_DB_USERNAME=your-username
AZURE_DB_PASSWORD=your-password
```

**For Local Database (if keeping local):**
```
LOCAL_DB_SERVER=your-local-server
LOCAL_DB_NAME=your-database-name
LOCAL_DB_USERNAME=your-username (optional)
LOCAL_DB_PASSWORD=your-password (optional)
```

## 📊 Dashboard Features

- **4 Professional Tabs:**
  - 📈 Market Overview
  - 🔍 Symbol Analysis
  - 📊 Category & Index Performance
  - 🔀 Delivery Flow Analysis

- **Interactive Charts:**
  - Sector performance scatter plots
  - Delivery flow Sankey diagrams
  - Volume and turnover analysis
  - Category performance radials
  - Index heatmaps and treemaps

- **Real-time Search:**
  - Live symbol search with autocomplete
  - Detailed symbol information display

## 🛠 Technology Stack

- **Backend:** Flask 2.3.3
- **Database:** SQL Server (ODBC Driver 18)
- **Frontend:** HTML5, CSS3, JavaScript
- **Charts:** Plotly.js
- **Deployment:** Azure Web App (Python 3.11)

## 📋 Dependencies

All dependencies are listed in `requirements.txt`:
- Flask 2.3.3
- pandas 2.0.3
- pyodbc 4.0.39
- gunicorn 21.2.0

## 🔍 Health Checks

The application includes built-in health check endpoints:
- `/health` - Basic application health
- `/health/db` - Database connectivity check

## 📚 Documentation

See `DEPLOYMENT_GUIDE.md` for:
- Detailed deployment steps
- Database migration strategies
- Troubleshooting guide
- Security configurations
- Performance optimization

## 🆘 Support

For deployment issues:
1. Check application logs in Azure Portal
2. Verify environment variables
3. Test database connectivity
4. Review deployment guide

## 📄 License

This dashboard is part of the NSE Downloader project.

---

**Ready for Azure deployment! 🚀**