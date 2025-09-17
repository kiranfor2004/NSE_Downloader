# Azure Database Import Script - Clean Version
# This script imports the exported CSV data into Azure SQL Database

param(
    [Parameter(Mandatory=$true)]
    [string]$AzureServer,  # e.g., "your-server.database.windows.net"
    
    [Parameter(Mandatory=$true)]
    [string]$Database,     # e.g., "nse_dashboard_db"
    
    [Parameter(Mandatory=$true)]
    [string]$Username,     # Azure SQL username
    
    [Parameter(Mandatory=$true)]
    [string]$Password,     # Azure SQL password
    
    [string]$DataDirectory = ".\database_export"
)

Write-Host "[START] Starting Azure SQL Database Import" -ForegroundColor Green
Write-Host "=" * 60

# Connection string
$connectionString = "Server=tcp:$AzureServer,1433;Initial Catalog=$Database;Persist Security Info=False;User ID=$Username;Password=$Password;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;"

Write-Host "[SERVER] Server: $AzureServer" -ForegroundColor Cyan
Write-Host "[DATABASE] Database: $Database" -ForegroundColor Cyan
Write-Host "[USER] Username: $Username" -ForegroundColor Cyan

# Function to execute SQL command
function Invoke-AzureSqlCommand {
    param(
        [string]$Query,
        [string]$Description
    )
    
    try {
        Write-Host "[PROCESS] $Description..." -ForegroundColor Yellow
        
        # Use sqlcmd for Azure SQL Database
        $command = "sqlcmd -S `"$AzureServer`" -d `"$Database`" -U `"$Username`" -P `"$Password`" -Q `"$Query`" -I"
        
        $result = Invoke-Expression $command 2>&1
        
        if ($LASTEXITCODE -eq 0) {
            Write-Host "   [SUCCESS] $Description completed successfully" -ForegroundColor Green
            return $true
        } else {
            Write-Host "   [ERROR] $Description failed: $result" -ForegroundColor Red
            return $false
        }
        
    } catch {
        Write-Host "   [ERROR] Error in $Description : $($_.Exception.Message)" -ForegroundColor Red
        return $false
    }
}

# Step 1: Test connection
Write-Host "`n1. Testing Azure SQL Database connection" -ForegroundColor Yellow

$testQuery = "SELECT 1 as test"
$connectionTest = Invoke-AzureSqlCommand -Query $testQuery -Description "Connection test"

if (-not $connectionTest) {
    Write-Host "[ERROR] Cannot connect to Azure SQL Database. Please check your credentials." -ForegroundColor Red
    exit 1
}

# Step 2: Create table structure
Write-Host "`n2. Creating table structure" -ForegroundColor Yellow

$createTableScript = "$DataDirectory\..\database_scripts\01_create_table.sql"
if (Test-Path $createTableScript) {
    $createTableSQL = Get-Content $createTableScript -Raw
    $tableCreated = Invoke-AzureSqlCommand -Query $createTableSQL -Description "Create table structure"
    
    if (-not $tableCreated) {
        Write-Host "[ERROR] Failed to create table structure" -ForegroundColor Red
        exit 1
    }
} else {
    Write-Host "[ERROR] Table creation script not found at $createTableScript" -ForegroundColor Red
    exit 1
}

# Step 3: Check data files
Write-Host "`n3. Checking data files" -ForegroundColor Yellow

$dataFile = "$DataDirectory\step03_data.csv"
if (-not (Test-Path $dataFile)) {
    Write-Host "[ERROR] Data file not found: $dataFile" -ForegroundColor Red
    exit 1
}

$csvContent = Get-Content $dataFile
$recordCount = ($csvContent | Measure-Object).Count - 1  # Subtract header row

Write-Host "[DATA] Found data file with $recordCount records" -ForegroundColor Cyan

# Step 4: Import data using CSV bulk insert
Write-Host "`n4. Importing data using CSV method" -ForegroundColor Yellow

try {
    # Read CSV data
    $csvData = Import-Csv $dataFile
    
    if ($csvData.Count -eq 0) {
        Write-Host "[WARNING] No data records found in CSV file" -ForegroundColor Yellow
        exit 0
    }
    
    Write-Host "[INFO] Processing $($csvData.Count) records..." -ForegroundColor Cyan
    
    # Process in batches of 100 records
    $batchSize = 100
    $insertCount = 0
    $batch = @()
    
    foreach ($row in $csvData) {
        # Build INSERT statement for this row
        $insertSQL = @"
INSERT INTO step03_compare_monthvspreviousmonth 
(current_trade_date, symbol, series, current_prev_close, current_open_price, 
current_high_price, current_low_price, current_last_price, current_close_price, 
current_avg_price, current_ttl_trd_qnty, current_turnover_lacs, current_no_of_trades, 
current_deliv_qty, current_deliv_per, current_source_file, previous_trade_date, 
previous_prev_close, previous_open_price, previous_high_price, previous_low_price, 
previous_last_price, previous_close_price, previous_avg_price, previous_ttl_trd_qnty, 
previous_turnover_lacs, previous_no_of_trades, previous_deliv_qty, previous_deliv_per, 
previous_source_file, price_change, price_change_percent, comparison_type, 
category, index_name) 
VALUES 
('$($row.current_trade_date)', '$($row.symbol)', '$($row.series)', 
$($row.current_prev_close), $($row.current_open_price), $($row.current_high_price), 
$($row.current_low_price), $($row.current_last_price), $($row.current_close_price), 
$($row.current_avg_price), $($row.current_ttl_trd_qnty), $($row.current_turnover_lacs), 
$($row.current_no_of_trades), $($row.current_deliv_qty), $($row.current_deliv_per), 
'$($row.current_source_file)', '$($row.previous_trade_date)', $($row.previous_prev_close), 
$($row.previous_open_price), $($row.previous_high_price), $($row.previous_low_price), 
$($row.previous_last_price), $($row.previous_close_price), $($row.previous_avg_price), 
$($row.previous_ttl_trd_qnty), $($row.previous_turnover_lacs), $($row.previous_no_of_trades), 
$($row.previous_deliv_qty), $($row.previous_deliv_per), '$($row.previous_source_file)', 
$($row.price_change), $($row.price_change_percent), '$($row.comparison_type)', 
'$($row.category)', '$($row.index_name)')
"@
        
        $batch += $insertSQL
        
        # Execute batch when it reaches batch size
        if ($batch.Count -ge $batchSize) {
            $batchSQL = $batch -join "; "
            $insertResult = Invoke-AzureSqlCommand -Query $batchSQL -Description "Batch insert ($($batch.Count) records)"
            if ($insertResult) {
                $insertCount += $batch.Count
                Write-Host "   [PROGRESS] Inserted batch: $insertCount / $($csvData.Count) records" -ForegroundColor Cyan
            }
            $batch = @()
        }
    }
    
    # Execute remaining records in final batch
    if ($batch.Count -gt 0) {
        $batchSQL = $batch -join "; "
        $insertResult = Invoke-AzureSqlCommand -Query $batchSQL -Description "Final batch insert ($($batch.Count) records)"
        if ($insertResult) {
            $insertCount += $batch.Count
        }
    }
    
} catch {
    Write-Host "[ERROR] Error during data import: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

# Step 5: Validate import
Write-Host "`n5. Validating import" -ForegroundColor Yellow

$validateQuery = "SELECT COUNT(*) as record_count FROM step03_compare_monthvspreviousmonth"
$validationResult = Invoke-AzureSqlCommand -Query $validateQuery -Description "Count imported records"

if ($validationResult) {
    # Get the actual count
    $countResult = sqlcmd -S "$AzureServer" -d "$Database" -U "$Username" -P "$Password" -Q "$validateQuery" -h -1
    $actualCount = ($countResult | Where-Object { $_ -match '^\d+$' })[0]
    
    Write-Host "[RESULT] Successfully imported $actualCount records" -ForegroundColor Green
    Write-Host "[RESULT] Expected: $($csvData.Count), Actual: $actualCount" -ForegroundColor Cyan
    
    if ($actualCount -eq $csvData.Count) {
        Write-Host "[SUCCESS] Import validation passed!" -ForegroundColor Green
    } else {
        Write-Host "[WARNING] Record count mismatch - please verify data" -ForegroundColor Yellow
    }
}

# Step 6: Create sample queries for verification
Write-Host "`n6. Creating verification queries" -ForegroundColor Yellow

$verificationQueries = @"
-- Sample verification queries
-- 1. Check total record count
SELECT COUNT(*) as total_records FROM step03_compare_monthvspreviousmonth;

-- 2. Check date range
SELECT 
    MIN(current_trade_date) as earliest_date,
    MAX(current_trade_date) as latest_date
FROM step03_compare_monthvspreviousmonth;

-- 3. Check symbols
SELECT COUNT(DISTINCT symbol) as unique_symbols FROM step03_compare_monthvspreviousmonth;

-- 4. Sample data preview
SELECT TOP 5 * FROM step03_compare_monthvspreviousmonth ORDER BY current_trade_date DESC;
"@

$verificationFile = "$DataDirectory\azure_verification_queries.sql"
$verificationQueries | Out-File -FilePath $verificationFile -Encoding UTF8

Write-Host "[SUCCESS] Created verification queries at: $verificationFile" -ForegroundColor Green

Write-Host "`n[COMPLETE] Azure SQL Database import completed!" -ForegroundColor Green
Write-Host "=" * 60
Write-Host "[SUMMARY] Import Summary:" -ForegroundColor Cyan
Write-Host "- Records processed: $($csvData.Count)" -ForegroundColor White
Write-Host "- Records inserted: $insertCount" -ForegroundColor White  
Write-Host "- Server: $AzureServer" -ForegroundColor White
Write-Host "- Database: $Database" -ForegroundColor White
Write-Host "`n[NEXT] Next Steps:" -ForegroundColor Yellow
Write-Host "1. Run verification queries to validate data" -ForegroundColor White
Write-Host "2. Deploy web application" -ForegroundColor White
Write-Host "3. Test dashboard functionality" -ForegroundColor White