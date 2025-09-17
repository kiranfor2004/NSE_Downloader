# 🎯 NSE Dashboard - Azure Deployment Quick Start

## 🚀 Three Deployment Methods

### Method 1: Fully Automated (Recommended)
**One-click deployment script that handles everything**

```powershell
# Run this single command for complete deployment
.\deploy_to_azure.ps1 `
  -SubscriptionId "your-subscription-id" `
  -AppName "nse-dashboard-[unique-id]" `
  -SqlAdminUsername "nsedashboardadmin" `
  -SqlAdminPassword "YourSecurePassword123!"
```

**What it does:**
- ✅ Exports local database automatically
- ✅ Creates all Azure resources (SQL Database, Web App)
- ✅ Migrates data to Azure SQL
- ✅ Deploys Flask application
- ✅ Configures environment variables
- ✅ Tests deployment
- ⏱️ **Total time: 15-20 minutes**

### Method 2: Step-by-Step Manual
**Follow detailed guide for learning/customization**

See: `STEP_BY_STEP_DEPLOYMENT.md`

**Steps:**
1. Export database: `.\database_scripts\export_database.ps1`
2. Create Azure resources via Portal
3. Import data: `.\database_scripts\import_to_azure.ps1`
4. Deploy application files
5. Configure environment variables
- ⏱️ **Total time: 45-60 minutes**

### Method 3: Hybrid Approach
**Automated database + manual web app deployment**

```powershell
# 1. Export and migrate database
.\database_scripts\export_database.ps1
.\database_scripts\import_to_azure.ps1 -AzureServer "your-server" -Database "your-db" -Username "user" -Password "pass"

# 2. Use Azure Portal for Web App creation
# 3. ZIP deploy application files
```

---

## 📋 Prerequisites Checklist

Before starting any method:

- [ ] **Azure Account** with active subscription
- [ ] **Azure CLI** installed and logged in (`az login`)
- [ ] **Local NSE database** accessible (SRIKIRANREDDY\SQLEXPRESS)
- [ ] **PowerShell 5.1+** 
- [ ] **Unique app name** chosen (will be your URL)

---

## 🎯 Quickest Path to Success

### For First-Time Deployment:

1. **Choose a unique app name**: `nse-dashboard-[your-initials]-[number]`
2. **Get your Azure subscription ID**: 
   ```powershell
   az account show --query id -o tsv
   ```
3. **Run automated deployment**:
   ```powershell
   .\deploy_to_azure.ps1 `
     -SubscriptionId "your-subscription-id" `
     -AppName "nse-dashboard-kr-001" `
     -SqlAdminUsername "nsedashboardadmin" `
     -SqlAdminPassword "SecurePass123!"
   ```

### Result:
- 🌐 **Live Dashboard**: `https://nse-dashboard-kr-001-webapp.azurewebsites.net`
- 📊 **All 4 tabs working** with your data
- 🔒 **Secure Azure SQL Database**
- 📈 **Ready for production use**

---

## 🛠 Troubleshooting Quick Fixes

### Common Issues:

**1. App name already taken**
```
Error: The name 'nse-dashboard-001' is already taken
```
**Fix**: Add your initials or different number:
```powershell
-AppName "nse-dashboard-kr-002"
```

**2. Database connection fails**
```
Error: Cannot connect to database
```
**Fix**: Check local SQL Server is running:
```powershell
Get-Service MSSQL*
```

**3. Deployment script fails**
```
Error: Azure CLI not found
```
**Fix**: Install Azure CLI and login:
```powershell
az login
```

**4. Permission errors**
```
Error: Insufficient privileges
```
**Fix**: Run PowerShell as Administrator

---

## 💰 Cost Estimate

**Monthly Azure Costs (Basic Tier):**
- SQL Database (Basic): ~$5
- Web App (Basic B1): ~$13
- **Total: ~$18/month**

**Free Trial Options:**
- Azure Free Account: $200 credit
- Free tier Web App: Available for 12 months
- Your dashboard can run FREE for several months!

---

## ✅ Success Indicators

After deployment, you should see:

1. **Web App Running**: 
   - Visit your dashboard URL
   - All 4 tabs load with data

2. **Health Checks Pass**:
   - `/health` returns "Application is healthy"
   - `/health/db` returns database connection info

3. **Interactive Features Work**:
   - Symbol search with autocomplete
   - Chart interactions and filters
   - Sankey diagrams display correctly

4. **Data Accuracy**:
   - KPI cards show correct numbers
   - Charts match your local dashboard

---

## 🔄 Next Steps After Deployment

### Immediate:
- [ ] Bookmark your dashboard URL
- [ ] Test all functionality thoroughly
- [ ] Share URL with stakeholders

### Within 24 hours:
- [ ] Set up monitoring alerts
- [ ] Configure custom domain (optional)
- [ ] Review security settings

### Ongoing:
- [ ] Monitor costs weekly
- [ ] Update data monthly
- [ ] Scale resources as needed

---

## 📞 Support

### Quick Help:
1. **Check Azure Portal** → Your Resource Group
2. **Review deployment logs** in Web App
3. **Test database connection** manually

### Documentation:
- **Complete Guide**: `STEP_BY_STEP_DEPLOYMENT.md`
- **Database Scripts**: `database_scripts/` folder
- **Azure Docs**: https://docs.microsoft.com/azure/

### Emergency:
If dashboard is down:
1. Check Web App status in Azure Portal
2. Review Application Logs
3. Restart Web App if needed

---

## 🎉 Ready to Deploy!

**Choose your method and start deploying your professional NSE dashboard to Azure!**

For most users, we recommend **Method 1 (Fully Automated)** for the quickest and most reliable deployment.

---

*Last updated: September 2025*
*Version: 1.0*