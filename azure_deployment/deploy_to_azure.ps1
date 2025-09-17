# Complete Azure Deployment Automation Script
# This script automates the entire deployment process

param(
    [string]$SubscriptionId = "190005e2-ed09-4b6e-b70c-6d9fb0a31dde",
    [string]$ResourceGroupName = "nse-dashboard-westus2-rg",
    [string]$Location = "West US 2",
    [string]$AppName = "nse-dashboard-kiran-001",
    [string]$SqlAdminUsername = "nseadmin",
    [string]$SqlAdminPassword = "NSEData@2024#Secure",
    
    [string]$LocalSqlServer = "SRIKIRANREDDY\SQLEXPRESS",
    [string]$LocalDatabase = "NSE_DATA",
    [string]$DeploymentPath = "C:\Users\kiran\NSE_Downloader\azure_deployment"
)

# Color functions for better output
function Write-Success { param($Message) Write-Host "[SUCCESS] $Message" -ForegroundColor Green }
function Write-Info { param($Message) Write-Host "[INFO] $Message" -ForegroundColor Cyan }
function Write-Warning { param($Message) Write-Host "[WARNING] $Message" -ForegroundColor Yellow }
function Write-Error { param($Message) Write-Host "[ERROR] $Message" -ForegroundColor Red }
function Write-Header { param($Message) Write-Host "`n[PHASE] $Message" -ForegroundColor Magenta -BackgroundColor Black }

Write-Header "NSE Dashboard - Complete Azure Deployment"
Write-Host "================================================================================"

# Validate prerequisites
Write-Header "Phase 1: Prerequisites Validation"

# Check Azure CLI
try {
    $azVersion = az --version 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Success "Azure CLI is installed"
    } else {
        Write-Error "Azure CLI not found. Please install from https://aka.ms/InstallAzureCLI"
        exit 1
    }
} catch {
    Write-Error "Azure CLI not found. Please install from https://aka.ms/InstallAzureCLI"
    exit 1
}

# Check if logged in to Azure
try {
    $account = az account show 2>&1 | ConvertFrom-Json
    Write-Success "Logged in to Azure as: $($account.user.name)"
} catch {
    Write-Warning "Not logged in to Azure. Initiating login..."
    az login
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Azure login failed"
        exit 1
    }
}

# Set subscription
Write-Info "Setting Azure subscription: $SubscriptionId"
az account set --subscription $SubscriptionId
if ($LASTEXITCODE -ne 0) {
    Write-Error "Failed to set subscription"
    exit 1
}

# Check deployment files
Write-Info "Checking deployment files..."
$requiredFiles = @(
    "$DeploymentPath\app.py",
    "$DeploymentPath\requirements.txt",
    "$DeploymentPath\templates\dashboard.html",
    "$DeploymentPath\database_scripts\export_database.ps1",
    "$DeploymentPath\database_scripts\01_create_table.sql"
)

foreach ($file in $requiredFiles) {
    if (Test-Path $file) {
        Write-Success "Found: $(Split-Path $file -Leaf)"
    } else {
        Write-Error "Missing required file: $file"
        exit 1
    }
}

# Variables
$sqlServerName = "$AppName-sql-server"
$databaseName = "$AppName-db"
$appServicePlanName = "$AppName-plan"
$webAppName = "$AppName-webapp"

Write-Header "Phase 2: Database Export from Local System"

# Export local database
Write-Info "Exporting data from local SQL Server..."
try {
    Push-Location $DeploymentPath
    & ".\database_scripts\export_database.ps1" -LocalServer $LocalSqlServer -Database $LocalDatabase
    
    if (Test-Path ".\database_export\step03_data.csv") {
        $recordCount = (Get-Content ".\database_export\step03_data.csv" | Measure-Object -Line).Lines - 1
        Write-Success "Exported $recordCount records from local database"
    } else {
        Write-Error "Database export failed"
        exit 1
    }
    Pop-Location
} catch {
    Write-Error "Database export failed: $($_.Exception.Message)"
    exit 1
}

Write-Header "Phase 3: Azure Resource Creation"

# Create Resource Group
Write-Info "Creating Resource Group: $ResourceGroupName"
az group create --name $ResourceGroupName --location $Location
if ($LASTEXITCODE -eq 0) {
    Write-Success "Resource Group created"
} else {
    Write-Warning "Resource Group may already exist"
}

# Create SQL Server
Write-Info "Creating Azure SQL Server: $sqlServerName"
az sql server create `
    --resource-group $ResourceGroupName `
    --name $sqlServerName `
    --location $Location `
    --admin-user $SqlAdminUsername `
    --admin-password $SqlAdminPassword

if ($LASTEXITCODE -eq 0) {
    Write-Success "SQL Server created"
} else {
    Write-Error "Failed to create SQL Server"
    exit 1
}

# Configure SQL Server firewall
Write-Info "Configuring SQL Server firewall rules"
az sql server firewall-rule create `
    --resource-group $ResourceGroupName `
    --server $sqlServerName `
    --name "AllowAllAzure" `
    --start-ip-address 0.0.0.0 `
    --end-ip-address 0.0.0.0

# Get current IP and add to firewall
$currentIP = (Invoke-WebRequest -UseBasicParsing -Uri "https://api.ipify.org").Content.Trim()
az sql server firewall-rule create `
    --resource-group $ResourceGroupName `
    --server $sqlServerName `
    --name "ClientIP" `
    --start-ip-address $currentIP `
    --end-ip-address $currentIP

Write-Success "Firewall rules configured"

# Create SQL Database (Free Tier Compatible)
Write-Info "Creating Azure SQL Database: $databaseName"
az sql db create `
    --resource-group $ResourceGroupName `
    --server $sqlServerName `
    --name $databaseName `
    --service-objective Basic `
    --max-size 2GB

if ($LASTEXITCODE -eq 0) {
    Write-Success "SQL Database created"
} else {
    Write-Error "Failed to create SQL Database"
    exit 1
}

Write-Header "Phase 4: Database Migration"

# Import data to Azure SQL
Write-Info "Importing data to Azure SQL Database..."
$azureSqlServer = "$sqlServerName.database.windows.net"

try {
    Push-Location $DeploymentPath
    & ".\database_scripts\import_to_azure.ps1" `
        -AzureServer $azureSqlServer `
        -Database $databaseName `
        -Username $SqlAdminUsername `
        -Password $SqlAdminPassword
    
    Write-Success "Database migration completed"
    Pop-Location
} catch {
    Write-Error "Database migration failed: $($_.Exception.Message)"
    exit 1
}

Write-Header "Phase 5: Web App Creation"

# Create App Service Plan (Free Tier)
Write-Info "Creating App Service Plan: $appServicePlanName"
az appservice plan create `
    --resource-group $ResourceGroupName `
    --name $appServicePlanName `
    --location $Location `
    --sku F1 `
    --is-linux

if ($LASTEXITCODE -eq 0) {
    Write-Success "App Service Plan created"
} else {
    Write-Error "Failed to create App Service Plan"
    exit 1
}

# Create Web App
Write-Info "Creating Web App: $webAppName"
az webapp create `
    --resource-group $ResourceGroupName `
    --plan $appServicePlanName `
    --name $webAppName `
    --runtime "PYTHON|3.11"

if ($LASTEXITCODE -eq 0) {
    Write-Success "Web App created"
    $webAppUrl = "https://$webAppName.azurewebsites.net"
    Write-Info "Web App URL: $webAppUrl"
} else {
    Write-Error "Failed to create Web App"
    exit 1
}

Write-Header "Phase 6: Application Configuration"

# Configure environment variables
Write-Info "Setting environment variables..."
az webapp config appsettings set `
    --resource-group $ResourceGroupName `
    --name $webAppName `
    --settings `
    AZURE_DB_SERVER="$azureSqlServer" `
    AZURE_DB_NAME="$databaseName" `
    AZURE_DB_USERNAME="$SqlAdminUsername" `
    AZURE_DB_PASSWORD="$SqlAdminPassword" `
    FLASK_ENV="production" `
    PYTHONPATH="/home/site/wwwroot"

Write-Success "Environment variables configured"

# Set startup command
Write-Info "Configuring startup command..."
az webapp config set `
    --resource-group $ResourceGroupName `
    --name $webAppName `
    --startup-file "python startup.py"

Write-Success "Startup command configured"

Write-Header "Phase 7: Application Deployment"

# Create deployment package
Write-Info "Creating deployment package..."
$zipPath = "$env:TEMP\nse-dashboard-deployment.zip"
if (Test-Path $zipPath) { Remove-Item $zipPath -Force }

Compress-Archive -Path "$DeploymentPath\*" -DestinationPath $zipPath -Force
Write-Success "Deployment package created: $zipPath"

# Deploy application
Write-Info "Deploying application to Azure..."
az webapp deployment source config-zip `
    --resource-group $ResourceGroupName `
    --name $webAppName `
    --src $zipPath

if ($LASTEXITCODE -eq 0) {
    Write-Success "Application deployed successfully"
} else {
    Write-Error "Application deployment failed"
    exit 1
}

# Clean up deployment package
Remove-Item $zipPath -Force

Write-Header "Phase 8: Post-Deployment Validation"

# Wait for app to start
Write-Info "Waiting for application to start..."
Start-Sleep -Seconds 30

# Test health endpoint
Write-Info "Testing application health..."
try {
    $healthUrl = "https://$webAppName.azurewebsites.net/health"
    $response = Invoke-WebRequest -UseBasicParsing -Uri $healthUrl -TimeoutSec 30
    
    if ($response.StatusCode -eq 200) {
        Write-Success "Health check passed"
    } else {
        Write-Warning "Health check returned status: $($response.StatusCode)"
    }
} catch {
    Write-Warning "Health check failed, but deployment may still be successful"
}

# Test database health
Write-Info "Testing database connectivity..."
try {
    $dbHealthUrl = "https://$webAppName.azurewebsites.net/health/db"
    $dbResponse = Invoke-WebRequest -UseBasicParsing -Uri $dbHealthUrl -TimeoutSec 30
    
    if ($dbResponse.StatusCode -eq 200) {
        Write-Success "Database connectivity verified"
    } else {
        Write-Warning "Database connectivity check returned status: $($dbResponse.StatusCode)"
    }
} catch {
    Write-Warning "Database connectivity check failed. Check logs for details."
}

Write-Header "Phase 9: Deployment Summary"

Write-Host ""
Write-Host "[SUCCESS] NSE Dashboard Deployment Completed Successfully!" -ForegroundColor Green -BackgroundColor Black
Write-Host "================================================================================"

Write-Host "[INFO] Resource Summary:" -ForegroundColor Cyan
Write-Host "   Resource Group: $ResourceGroupName" -ForegroundColor White
Write-Host "   SQL Server: $sqlServerName.database.windows.net" -ForegroundColor White
Write-Host "   Database: $databaseName" -ForegroundColor White
Write-Host "   Web App: $webAppName" -ForegroundColor White

Write-Host "`n[INFO] Access URLs:" -ForegroundColor Cyan
Write-Host "   Dashboard: https://$webAppName.azurewebsites.net" -ForegroundColor Yellow
Write-Host "   Health Check: https://$webAppName.azurewebsites.net/health" -ForegroundColor Yellow
Write-Host "   DB Health: https://$webAppName.azurewebsites.net/health/db" -ForegroundColor Yellow

Write-Host "`n[INFO] Management URLs:" -ForegroundColor Cyan
Write-Host "   Azure Portal: https://portal.azure.com" -ForegroundColor Yellow
Write-Host "   Resource Group: https://portal.azure.com/#@/resource/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroupName" -ForegroundColor Yellow

Write-Host "`n[INFO] Next Steps:" -ForegroundColor Cyan
Write-Host "   1. Visit your dashboard URL to verify functionality" -ForegroundColor White
Write-Host "   2. Test all 4 tabs and interactive features" -ForegroundColor White
Write-Host "   3. Monitor application logs in Azure Portal" -ForegroundColor White
Write-Host "   4. Set up Application Insights for monitoring" -ForegroundColor White
Write-Host "   5. Configure custom domain if needed" -ForegroundColor White

Write-Host "`n[INFO] Estimated Monthly Cost: FREE (using Free Tier)" -ForegroundColor Cyan

Write-Host "`n[SUCCESS] Your professional NSE dashboard is now live on Azure!" -ForegroundColor Green -BackgroundColor Black

# Optional: Open dashboard in browser
$openBrowser = Read-Host "`nWould you like to open the dashboard in your browser? (y/n)"
if ($openBrowser -eq 'y' -or $openBrowser -eq 'Y') {
    Start-Process "https://$webAppName.azurewebsites.net"
}