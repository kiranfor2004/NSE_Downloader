# Environment Configuration for Azure Deployment

## Required Environment Variables

### Database Configuration (Choose one option)

#### Option 1: Azure SQL Database (Recommended for Production)
```
AZURE_DB_SERVER=your-server.database.windows.net
AZURE_DB_NAME=your-database-name
AZURE_DB_USERNAME=your-username
AZURE_DB_PASSWORD=your-password
```

#### Option 2: Azure SQL Managed Instance
```
AZURE_DB_SERVER=your-instance.public.database.windows.net,3342
AZURE_DB_NAME=your-database-name
AZURE_DB_USERNAME=your-username
AZURE_DB_PASSWORD=your-password
```

## Azure Web App Configuration Steps

### 1. Database Setup Options

#### Option A: Migrate Local Database to Azure SQL Database

1. **Export Local Database:**
   ```powershell
   # Using SQL Server Management Studio (SSMS)
   # Right-click database → Tasks → Export Data-tier Application
   # Or use SqlCmd:
   sqlcmd -S SRIKIRANREDDY\SQLEXPRESS -d your_database -Q "SELECT * FROM step03_compare_monthvspreviousmonth" -o data_export.csv -h-1 -s"," -W
   ```

2. **Create Azure SQL Database:**
   - Go to Azure Portal → Create Resource → SQL Database
   - Choose appropriate pricing tier (Basic/Standard for development)
   - Configure firewall rules to allow Azure services

3. **Import Data:**
   ```sql
   -- Create table structure in Azure SQL Database
   CREATE TABLE step03_compare_monthvspreviousmonth (
       [symbol] NVARCHAR(50),
       [category] NVARCHAR(100),
       [index_name] NVARCHAR(100),
       [sector] NVARCHAR(100),
       [previous_month_delivery] DECIMAL(18,2),
       [current_month_delivery] DECIMAL(18,2),
       [delivery_increase_pct] DECIMAL(18,2),
       [volume] BIGINT,
       [turnover_lacs] DECIMAL(18,2),
       [close] DECIMAL(18,4),
       [pct_change] DECIMAL(18,4),
       [delivery_pct] DECIMAL(18,4)
   );
   ```

#### Option B: Keep Local Database (Development/Testing)

Set these environment variables in Azure Web App:
```
LOCAL_DB_SERVER=your-local-server-ip
LOCAL_DB_NAME=your-database-name
LOCAL_DB_USERNAME=your-username
LOCAL_DB_PASSWORD=your-password
```

**Note:** Your local server must be accessible from internet (configure firewall, VPN, or Azure Hybrid Connection)

### 2. Azure Web App Deployment

#### Method 1: Manual Deployment via FTP/ZIP

1. **Create Azure Web App:**
   - Portal → Create Resource → Web App
   - Runtime Stack: Python 3.11
   - Operating System: Linux (recommended)

2. **Deploy Files:**
   ```powershell
   # Zip the deployment folder
   Compress-Archive -Path "azure_deployment\*" -DestinationPath "nse_dashboard.zip"
   
   # Upload via Azure Portal → Development Tools → App Service Editor
   # Or use Azure CLI:
   az webapp deployment source config-zip --resource-group your-rg --name your-app --src nse_dashboard.zip
   ```

#### Method 2: GitHub Actions Deployment

1. **Push to GitHub Repository**
2. **Configure GitHub Actions** (create `.github/workflows/azure-deploy.yml`):

```yaml
name: Deploy to Azure Web App

on:
  push:
    branches: [ main ]

jobs:
  deploy:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v2
    
    - name: Set up Python
      uses: actions/setup-python@v2
      with:
        python-version: '3.11'
    
    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -r azure_deployment/requirements.txt
    
    - name: Deploy to Azure Web App
      uses: azure/webapps-deploy@v2
      with:
        app-name: 'your-app-name'
        publish-profile: ${{ secrets.AZURE_WEBAPP_PUBLISH_PROFILE }}
        package: azure_deployment
```

### 3. Environment Variables Setup in Azure

Go to Azure Portal → Your Web App → Configuration → Application settings:

```
Name: AZURE_DB_SERVER          Value: your-server.database.windows.net
Name: AZURE_DB_NAME            Value: your-database-name  
Name: AZURE_DB_USERNAME        Value: your-username
Name: AZURE_DB_PASSWORD        Value: your-password
Name: FLASK_ENV                Value: production
Name: PYTHONPATH               Value: /home/site/wwwroot
```

### 4. Database Connection String Format

The application will automatically detect Azure environment and use:

```python
# For Azure SQL Database
connection_string = (
    f"Driver={{ODBC Driver 18 for SQL Server}};"
    f"Server=tcp:{server},1433;"
    f"Database={database};"
    f"Uid={username};"
    f"Pwd={password};"
    f"Encrypt=yes;"
    f"TrustServerCertificate=no;"
    f"Connection Timeout=30;"
)
```

### 5. Testing the Deployment

1. **Local Testing:**
   ```powershell
   cd azure_deployment
   pip install -r requirements.txt
   python app.py
   # Visit http://localhost:5000
   ```

2. **Azure Testing:**
   - Visit: https://your-app-name.azurewebsites.net
   - Check logs: Portal → Your Web App → Log Stream

### 6. Troubleshooting

#### Common Issues:

1. **Database Connection Errors:**
   - Verify firewall rules allow Azure IP ranges
   - Check environment variables are set correctly
   - Test connection string manually

2. **Module Import Errors:**
   - Ensure all dependencies are in requirements.txt
   - Check Python version compatibility

3. **Template Rendering Errors:**
   - Verify templates folder is included in deployment
   - Check file paths are correct

#### Debug Commands:

```powershell
# Check application logs
az webapp log tail --name your-app-name --resource-group your-rg

# SSH into container (Linux App Service)
az webapp ssh --name your-app-name --resource-group your-rg

# Test database connection
python -c "import pyodbc; print('pyodbc available')"
```

### 7. Performance Optimization

1. **Enable Application Insights** for monitoring
2. **Configure Auto-scaling** based on CPU/memory
3. **Use Azure CDN** for static assets
4. **Enable Gzip Compression** in web.config

### 8. Security Considerations

1. **Use Azure Key Vault** for sensitive configuration
2. **Enable HTTPS only**
3. **Configure Custom Domain** if needed
4. **Implement Authentication** if required

### 9. Cost Optimization

1. **Choose appropriate App Service Plan**:
   - Free Tier: For development/testing
   - Basic B1: For small production workloads
   - Standard S1+: For production with scaling needs

2. **Database Pricing**:
   - Basic: For development ($5/month)
   - Standard S0: For small production ($15/month)
   - Consider serverless for variable workloads

### 10. Monitoring and Maintenance

1. **Set up Application Insights** for performance monitoring
2. **Configure Alerts** for errors and performance issues
3. **Regular Database Maintenance** (index optimization, statistics update)
4. **Backup Strategy** for both application and database

## Support Contacts

- Azure Support: https://azure.microsoft.com/support/
- Documentation: https://docs.microsoft.com/azure/app-service/
- Community: https://stackoverflow.com/questions/tagged/azure-web-app-service