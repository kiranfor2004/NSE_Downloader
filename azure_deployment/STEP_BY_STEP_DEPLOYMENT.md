# 🚀 Complete Azure Deployment Guide with Database Migration

## 📋 Prerequisites

Before starting, ensure you have:
- **Azure Account** with active subscription
- **Azure CLI** installed and logged in
- **SQL Server Management Studio (SSMS)** or **Azure Data Studio**
- **PowerShell 5.1** or higher
- **Local NSE database** running and accessible

## 🗂 Project Structure

```
azure_deployment/
├── app.py                          # Flask application
├── requirements.txt                # Python dependencies
├── startup.py                      # Azure startup script
├── web.config                      # IIS configuration
├── templates/
│   └── dashboard.html              # Dashboard template
├── database_scripts/
│   ├── 01_create_table.sql         # Table creation script
│   ├── export_database.ps1         # Export from local DB
│   └── import_to_azure.ps1         # Import to Azure SQL
├── DEPLOYMENT_GUIDE.md             # This guide
└── README.md                       # Quick reference
```

---

## 📊 Phase 1: Database Migration

### Step 1.1: Export Local Database

1. **Open PowerShell as Administrator**
2. **Navigate to deployment directory**:
   ```powershell
   cd C:\Users\kiran\NSE_Downloader\azure_deployment
   ```

3. **Run database export script**:
   ```powershell
   .\database_scripts\export_database.ps1 -LocalServer "SRIKIRANREDDY\SQLEXPRESS" -Database "NSE_DATA"
   ```

   **Expected Output**:
   ```
   🚀 Starting NSE Database Export for Azure Migration
   ============================================================
   📁 Created export directory: .\database_export
   📊 Exporting step03_compare_monthvspreviousmonth...
      ✅ Exported 15,247 records to .\database_export\step03_data.csv
   📊 Exporting database_statistics...
      ✅ Exported 1 records to .\database_export\database_stats.csv
   📊 Exporting symbol_metadata...
      ✅ Exported 1,023 records to .\database_export\symbol_metadata.csv
   🎉 Database Export Completed Successfully!
   ```

4. **Verify export files**:
   ```powershell
   dir .\database_export
   ```

### Step 1.2: Create Azure SQL Database

1. **Login to Azure Portal**: https://portal.azure.com

2. **Create Resource Group**:
   - Click **"Create a resource"**
   - Search **"Resource group"**
   - Name: `nse-dashboard-rg`
   - Region: `East US` (or your preferred region)

3. **Create Azure SQL Database**:
   - Click **"Create a resource"**
   - Search **"SQL Database"**
   - Fill details:
     - **Database name**: `nse-dashboard-db`
     - **Server**: Create new server
       - **Server name**: `nse-dashboard-server-[unique-id]`
       - **Admin login**: `nsedashboardadmin`
       - **Password**: `YourSecurePassword123!`
     - **Pricing tier**: Basic (for testing) or Standard S0 (for production)

4. **Configure Firewall**:
   - Go to your SQL Server → **Firewalls and virtual networks**
   - Add your current IP address
   - Check **"Allow Azure services"**

### Step 1.3: Import Data to Azure SQL

1. **Run import script**:
   ```powershell
   .\database_scripts\import_to_azure.ps1 `
     -AzureServer "nse-dashboard-server-[unique-id].database.windows.net" `
     -Database "nse-dashboard-db" `
     -Username "nsedashboardadmin" `
     -Password "YourSecurePassword123!"
   ```

2. **Verify import**:
   - Use Azure Data Studio or SSMS
   - Connect to Azure SQL Database
   - Run: `SELECT COUNT(*) FROM step03_compare_monthvspreviousmonth`

---

## 🌐 Phase 2: Azure Web App Deployment

### Step 2.1: Create Azure Web App

1. **Via Azure Portal**:
   - **Create a resource** → **Web App**
   - **App name**: `nse-dashboard-app-[unique-id]`
   - **Runtime stack**: Python 3.11
   - **Operating System**: Linux
   - **Region**: Same as database
   - **App Service Plan**: Basic B1 (or Free F1 for testing)

2. **Via Azure CLI** (Alternative):
   ```bash
   # Create App Service Plan
   az appservice plan create --name nse-dashboard-plan --resource-group nse-dashboard-rg --sku B1 --is-linux

   # Create Web App
   az webapp create --resource-group nse-dashboard-rg --plan nse-dashboard-plan --name nse-dashboard-app-[unique-id] --runtime "PYTHON|3.11"
   ```

### Step 2.2: Configure Environment Variables

1. **Go to Azure Portal** → Your Web App → **Configuration**

2. **Add Application Settings**:
   ```
   Name: AZURE_DB_SERVER          Value: nse-dashboard-server-[unique-id].database.windows.net
   Name: AZURE_DB_NAME            Value: nse-dashboard-db
   Name: AZURE_DB_USERNAME        Value: nsedashboardadmin
   Name: AZURE_DB_PASSWORD        Value: YourSecurePassword123!
   Name: FLASK_ENV                Value: production
   Name: PYTHONPATH               Value: /home/site/wwwroot
   ```

3. **Click "Save"**

### Step 2.3: Deploy Application Files

#### Method A: ZIP Deployment (Recommended)

1. **Create deployment package**:
   ```powershell
   # Create ZIP file
   Compress-Archive -Path "C:\Users\kiran\NSE_Downloader\azure_deployment\*" -DestinationPath "nse-dashboard.zip" -Force
   ```

2. **Deploy via Azure CLI**:
   ```bash
   az webapp deployment source config-zip --resource-group nse-dashboard-rg --name nse-dashboard-app-[unique-id] --src nse-dashboard.zip
   ```

#### Method B: FTP Deployment

1. **Get FTP credentials**:
   - Azure Portal → Your Web App → **Deployment Center**
   - Note FTP endpoint and credentials

2. **Upload files**:
   - Use FileZilla or similar FTP client
   - Upload all files from `azure_deployment` folder

#### Method C: GitHub Deployment

1. **Push to GitHub**:
   ```bash
   git add azure_deployment/
   git commit -m "Add Azure deployment files"
   git push origin main
   ```

2. **Configure GitHub Actions**:
   - Azure Portal → Your Web App → **Deployment Center**
   - Source: GitHub
   - Select repository and branch
   - Build provider: GitHub Actions

### Step 2.4: Configure Web App Settings

1. **Set Startup Command**:
   - Azure Portal → Your Web App → **Configuration** → **General settings**
   - **Startup Command**: `python startup.py`

2. **Enable Logs**:
   - **Monitoring** → **App Service logs**
   - Enable **Application Logging (Filesystem)**

---

## ✅ Phase 3: Testing and Validation

### Step 3.1: Test Application

1. **Access your dashboard**:
   ```
   https://nse-dashboard-app-[unique-id].azurewebsites.net
   ```

2. **Check health endpoints**:
   ```
   https://nse-dashboard-app-[unique-id].azurewebsites.net/health
   https://nse-dashboard-app-[unique-id].azurewebsites.net/health/db
   ```

### Step 3.2: Monitor Logs

1. **Real-time logs**:
   ```bash
   az webapp log tail --name nse-dashboard-app-[unique-id] --resource-group nse-dashboard-rg
   ```

2. **Log Stream in Portal**:
   - Azure Portal → Your Web App → **Log stream**

### Step 3.3: Verify Dashboard Features

- ✅ **Market Overview** tab loads with data
- ✅ **Symbol Analysis** search functionality works
- ✅ **Category & Index Performance** charts display
- ✅ **Delivery Flow Analysis** Sankey diagrams render
- ✅ All KPI cards show accurate data

---

## 🔧 Troubleshooting

### Common Issues and Solutions

#### 1. Database Connection Errors

**Error**: `Cannot connect to database`

**Solutions**:
- Verify firewall rules allow Azure IP ranges
- Check environment variables are correctly set
- Test connection string manually

```sql
-- Test connection in Azure Data Studio
SELECT GETDATE() as current_time;
```

#### 2. Module Import Errors

**Error**: `ModuleNotFoundError: No module named 'pandas'`

**Solutions**:
- Verify `requirements.txt` includes all dependencies
- Check Python version compatibility
- Restart Web App after deployment

#### 3. Template Not Found

**Error**: `TemplateNotFound: dashboard.html`

**Solutions**:
- Ensure `templates/` folder is included in deployment
- Check file paths in deployment package
- Verify Flask template directory configuration

#### 4. Data Loading Issues

**Error**: Dashboard shows no data

**Solutions**:
- Verify database contains data: `SELECT COUNT(*) FROM step03_compare_monthvspreviousmonth`
- Check database connection environment variables
- Review application logs for database errors

### Debug Commands

```powershell
# Check Web App logs
az webapp log tail --name your-app-name --resource-group nse-dashboard-rg

# SSH into Web App container (Linux)
az webapp ssh --name your-app-name --resource-group nse-dashboard-rg

# Test database connection
python -c "import pyodbc; print('pyodbc available')"

# Verify environment variables
python -c "import os; print(os.environ.get('AZURE_DB_SERVER'))"
```

---

## 🔐 Security Configuration

### 1. Database Security

- ✅ Use **SQL Authentication** with strong passwords
- ✅ Configure **firewall rules** to restrict access
- ✅ Enable **Azure Defender** for SQL
- ✅ Use **Azure Key Vault** for sensitive data (optional)

### 2. Web App Security

- ✅ Enable **HTTPS only**
- ✅ Configure **Authentication** if needed
- ✅ Set **Custom Domain** with SSL certificate
- ✅ Enable **Application Insights** for monitoring

### 3. Environment Variables

```bash
# Use Azure Key Vault (Advanced)
az keyvault create --name nse-dashboard-vault --resource-group nse-dashboard-rg --location eastus

# Store secrets
az keyvault secret set --vault-name nse-dashboard-vault --name "db-password" --value "YourSecurePassword123!"

# Reference in Web App
az webapp config appsettings set --resource-group nse-dashboard-rg --name nse-dashboard-app-[unique-id] --settings AZURE_DB_PASSWORD="@Microsoft.KeyVault(VaultName=nse-dashboard-vault;SecretName=db-password)"
```

---

## 📈 Performance Optimization

### 1. Database Optimization

```sql
-- Create additional indexes for better performance
CREATE INDEX IX_step03_symbol_category ON step03_compare_monthvspreviousmonth (symbol, category);
CREATE INDEX IX_step03_delivery_performance ON step03_compare_monthvspreviousmonth (delivery_increase_pct DESC);

-- Update statistics
UPDATE STATISTICS step03_compare_monthvspreviousmonth;
```

### 2. Application Optimization

- ✅ Enable **Gzip compression**
- ✅ Use **Azure CDN** for static assets
- ✅ Implement **caching** for database queries
- ✅ Configure **Auto-scaling** rules

### 3. Monitoring Setup

```bash
# Enable Application Insights
az monitor app-insights component create --app nse-dashboard-insights --location eastus --resource-group nse-dashboard-rg --application-type web

# Link to Web App
az webapp config appsettings set --resource-group nse-dashboard-rg --name nse-dashboard-app-[unique-id] --settings APPINSIGHTS_INSTRUMENTATIONKEY="your-instrumentation-key"
```

---

## 💰 Cost Optimization

### Pricing Estimates (Monthly)

| Component | Tier | Cost |
|-----------|------|------|
| SQL Database | Basic (2GB) | ~$5 |
| Web App | Basic B1 | ~$13 |
| **Total** | | **~$18/month** |

### Cost-Saving Tips

1. **Use appropriate pricing tiers**:
   - Basic/Free for development
   - Standard for production

2. **Monitor usage**:
   - Set up billing alerts
   - Review cost analysis monthly

3. **Optimize resources**:
   - Scale down during off-hours
   - Use serverless options for variable workloads

---

## 🔄 Maintenance and Updates

### Regular Tasks

1. **Database Maintenance**:
   ```sql
   -- Monthly maintenance
   REBUILD INDEX ALL ON step03_compare_monthvspreviousmonth;
   UPDATE STATISTICS step03_compare_monthvspreviousmonth;
   ```

2. **Application Updates**:
   - Update Python dependencies regularly
   - Monitor security advisories
   - Test new Azure features

3. **Backup Strategy**:
   - Azure SQL Database has automatic backups
   - Export important data monthly
   - Test restore procedures

### Update Process

1. **Test locally** with new data/features
2. **Deploy to staging** environment (optional)
3. **Deploy to production** during maintenance window
4. **Verify functionality** after deployment

---

## 📞 Support and Resources

### Azure Resources

- **Azure Documentation**: https://docs.microsoft.com/azure/
- **Azure Status**: https://status.azure.com/
- **Azure Support**: https://azure.microsoft.com/support/

### Community Support

- **Stack Overflow**: Tag with `azure-web-app-service`
- **GitHub Issues**: Flask/Plotly specific issues
- **Azure Community**: https://techcommunity.microsoft.com/t5/azure/ct-p/Azure

### Emergency Contacts

- **Azure Support**: Available 24/7 for production issues
- **Database Issues**: Check Azure SQL Database metrics
- **Application Issues**: Review Application Insights logs

---

## 🎉 Deployment Complete!

Your NSE Professional Dashboard is now live on Azure with:

✅ **Secure cloud database** with all your NSE data  
✅ **Scalable web application** handling multiple users  
✅ **Professional dashboard** with 4 interactive tabs  
✅ **Real-time data access** with search functionality  
✅ **Monitoring and logging** for production support  

### Quick Access Links

- **Dashboard URL**: `https://nse-dashboard-app-[unique-id].azurewebsites.net`
- **Health Check**: `https://nse-dashboard-app-[unique-id].azurewebsites.net/health`
- **Azure Portal**: `https://portal.azure.com`

**🚀 Your professional NSE dashboard is ready for production use!**