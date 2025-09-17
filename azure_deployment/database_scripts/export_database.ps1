# Database Export Script for Azure Migration
# This script exports data from local SQL Server to CSV for Azure import

param(
    [string]$LocalServer = "SRIKIRANREDDY\SQLEXPRESS",
    [string]$Database = "NSE_DATA",
    [string]$OutputDirectory = ".\database_export"
)

Write-Host "[SUCCESS] Starting NSE Database Export for Azure Migration" -ForegroundColor Green
Write-Host "=================================================================="

# Create output directory
if (!(Test-Path $OutputDirectory)) {
    New-Item -ItemType Directory -Path $OutputDirectory -Force
    Write-Host "[INFO] Created export directory: $OutputDirectory" -ForegroundColor Yellow
}

# Function to execute SQL and export to CSV
function Export-TableToCSV {
    param(
        [string]$TableName,
        [string]$Query,
        [string]$OutputFile
    )
    
    try {
        Write-Host "[EXPORT] Exporting $TableName..." -ForegroundColor Cyan
        
        # Execute SQL query and export to CSV
        $command = "sqlcmd -S `"$LocalServer`" -E -d `"$Database`" -Q `"$Query`" -s`",`" -W -h -1 -o `"$OutputFile`""
        
        Invoke-Expression $command
        
        if (Test-Path $OutputFile) {
            $lineCount = (Get-Content $OutputFile | Measure-Object -Line).Lines
            Write-Host "   [SUCCESS] Exported $lineCount records to $OutputFile" -ForegroundColor Green
        } else {
            Write-Host "   [ERROR] Failed to create $OutputFile" -ForegroundColor Red
        }
    }
    catch {
        Write-Host "   [ERROR] Error exporting $TableName : $($_.Exception.Message)" -ForegroundColor Red
    }
}

# 1. Export main step03 table
Write-Host "`n1. Exporting step03_compare_monthvspreviousmonth table" -ForegroundColor Yellow

$step03Query = @"
SELECT 
    [current_trade_date],
    [symbol],
    [series],
    [current_prev_close],
    [current_open_price],
    [current_high_price],
    [current_low_price],
    [current_last_price],
    [current_close_price],
    [current_avg_price],
    [current_ttl_trd_qnty],
    [current_turnover_lacs],
    [current_no_of_trades],
    [current_deliv_qty],
    [current_deliv_per],
    [current_source_file],
    [previous_baseline_date],
    [previous_prev_close],
    [previous_open_price],
    [previous_high_price],
    [previous_low_price],
    [previous_last_price],
    [previous_close_price],
    [previous_avg_price],
    [previous_ttl_trd_qnty],
    [previous_turnover_lacs],
    [previous_no_of_trades],
    [previous_deliv_qty],
    [previous_deliv_per],
    [previous_source_file],
    [delivery_increase_abs],
    [delivery_increase_pct],
    [comparison_type],
    [created_at],
    [index_name],
    [category],
    [sector]
FROM step03_compare_monthvspreviousmonth
ORDER BY current_trade_date DESC, symbol
"@

Export-TableToCSV -TableName "step03_compare_monthvspreviousmonth" -Query $step03Query -OutputFile "$OutputDirectory\step03_data.csv"

# 2. Get table statistics
Write-Host "`n2. Generating database statistics" -ForegroundColor Yellow

$statsQuery = @"
SELECT 
    'step03_compare_monthvspreviousmonth' as table_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT symbol) as unique_symbols,
    MIN(current_trade_date) as earliest_date,
    MAX(current_trade_date) as latest_date,
    COUNT(DISTINCT category) as unique_categories,
    COUNT(DISTINCT sector) as unique_sectors,
    COUNT(DISTINCT index_name) as unique_indices
FROM step03_compare_monthvspreviousmonth
"@

Export-TableToCSV -TableName "database_statistics" -Query $statsQuery -OutputFile "$OutputDirectory\database_stats.csv"

# 3. Export unique symbols with their categories and sectors
Write-Host "`n3. Exporting symbol metadata" -ForegroundColor Yellow

$symbolQuery = @"
SELECT DISTINCT
    symbol,
    category,
    sector,
    index_name,
    COUNT(*) as record_count,
    AVG(CAST(delivery_increase_pct as float)) as avg_delivery_increase,
    MAX(current_trade_date) as last_trade_date
FROM step03_compare_monthvspreviousmonth
WHERE symbol IS NOT NULL
GROUP BY symbol, category, sector, index_name
ORDER BY symbol
"@

Export-TableToCSV -TableName "symbol_metadata" -Query $symbolQuery -OutputFile "$OutputDirectory\symbol_metadata.csv"

# 4. Create data validation queries
Write-Host "`n4. Creating data validation scripts" -ForegroundColor Yellow

$validationSQL = @"
-- Data Validation Queries for Azure Import
-- Run these queries in Azure SQL Database after import

-- 1. Record count validation
SELECT 'Total Records' as validation_check, COUNT(*) as count 
FROM step03_compare_monthvspreviousmonth;

-- 2. Unique symbols validation
SELECT 'Unique Symbols' as validation_check, COUNT(DISTINCT symbol) as count 
FROM step03_compare_monthvspreviousmonth;

-- 3. Date range validation
SELECT 
    'Date Range' as validation_check,
    MIN(current_trade_date) as min_date,
    MAX(current_trade_date) as max_date
FROM step03_compare_monthvspreviousmonth;

-- 4. Categories validation
SELECT 'Categories' as validation_check, category, COUNT(*) as count
FROM step03_compare_monthvspreviousmonth
WHERE category IS NOT NULL
GROUP BY category
ORDER BY count DESC;

-- 5. Sectors validation
SELECT 'Sectors' as validation_check, sector, COUNT(*) as count
FROM step03_compare_monthvspreviousmonth
WHERE sector IS NOT NULL
GROUP BY sector
ORDER BY count DESC;

-- 6. Data quality checks
SELECT 
    'Data Quality' as validation_check,
    COUNT(CASE WHEN symbol IS NULL THEN 1 END) as null_symbols,
    COUNT(CASE WHEN current_trade_date IS NULL THEN 1 END) as null_dates,
    COUNT(CASE WHEN delivery_increase_pct IS NULL THEN 1 END) as null_delivery_pct
FROM step03_compare_monthvspreviousmonth;
"@

$validationSQL | Out-File -FilePath "$OutputDirectory\azure_validation_queries.sql" -Encoding UTF8

# 5. Create import ready files
Write-Host "`n5. Creating Azure import ready files" -ForegroundColor Yellow

# Create BCP format file
$bcpFormat = @"
12.0
37
1   SQLCHAR  0   10   "\t"   1     current_trade_date              ""
2   SQLCHAR  0   50   "\t"   2     symbol                          ""
3   SQLCHAR  0   10   "\t"   3     series                          ""
4   SQLCHAR  0   30   "\t"   4     current_prev_close              ""
5   SQLCHAR  0   30   "\t"   5     current_open_price              ""
6   SQLCHAR  0   30   "\t"   6     current_high_price              ""
7   SQLCHAR  0   30   "\t"   7     current_low_price               ""
8   SQLCHAR  0   30   "\t"   8     current_last_price              ""
9   SQLCHAR  0   30   "\t"   9     current_close_price             ""
10  SQLCHAR  0   30   "\t"   10    current_avg_price               ""
11  SQLCHAR  0   20   "\t"   11    current_ttl_trd_qnty            ""
12  SQLCHAR  0   30   "\t"   12    current_turnover_lacs           ""
13  SQLCHAR  0   20   "\t"   13    current_no_of_trades            ""
14  SQLCHAR  0   20   "\t"   14    current_deliv_qty               ""
15  SQLCHAR  0   20   "\t"   15    current_deliv_per               ""
16  SQLCHAR  0   255  "\t"   16    current_source_file             ""
17  SQLCHAR  0   10   "\t"   17    previous_baseline_date          ""
18  SQLCHAR  0   30   "\t"   18    previous_prev_close             ""
19  SQLCHAR  0   30   "\t"   19    previous_open_price             ""
20  SQLCHAR  0   30   "\t"   20    previous_high_price             ""
21  SQLCHAR  0   30   "\t"   21    previous_low_price              ""
22  SQLCHAR  0   30   "\t"   22    previous_last_price             ""
23  SQLCHAR  0   30   "\t"   23    previous_close_price            ""
24  SQLCHAR  0   30   "\t"   24    previous_avg_price              ""
25  SQLCHAR  0   20   "\t"   25    previous_ttl_trd_qnty           ""
26  SQLCHAR  0   30   "\t"   26    previous_turnover_lacs          ""
27  SQLCHAR  0   20   "\t"   27    previous_no_of_trades           ""
28  SQLCHAR  0   20   "\t"   28    previous_deliv_qty              ""
29  SQLCHAR  0   20   "\t"   29    previous_deliv_per              ""
30  SQLCHAR  0   255  "\t"   30    previous_source_file            ""
31  SQLCHAR  0   20   "\t"   31    delivery_increase_abs           ""
32  SQLCHAR  0   20   "\t"   32    delivery_increase_pct           ""
33  SQLCHAR  0   50   "\t"   33    comparison_type                 ""
34  SQLCHAR  0   30   "\t"   34    created_at                      ""
35  SQLCHAR  0   100  "\t"   35    index_name                      ""
36  SQLCHAR  0   100  "\t"   36    category                        ""
37  SQLCHAR  0   100  "\r\n" 37    sector                          ""
"@

$bcpFormat | Out-File -FilePath "$OutputDirectory\step03_format.fmt" -Encoding ASCII

# 6. Create Azure import instructions
$importInstructions = @"
# Azure SQL Database Import Instructions

## Files Generated:
1. step03_data.csv - Main data export
2. database_stats.csv - Database statistics
3. symbol_metadata.csv - Symbol metadata
4. azure_validation_queries.sql - Validation queries
5. step03_format.fmt - BCP format file

## Import Methods:

### Method 1: Using Azure Data Studio
1. Connect to Azure SQL Database
2. Right-click database → Import Wizard
3. Select step03_data.csv
4. Map columns to table structure
5. Execute import

### Method 2: Using BCP (Bulk Copy Program)
```cmd
bcp dbo.step03_compare_monthvspreviousmonth in "step03_data.csv" -S your-server.database.windows.net -d your-database -U your-username -P your-password -f step03_format.fmt -c
```

### Method 3: Using PowerShell (Recommended)
```powershell
# See azure_import_script.ps1 in this directory
```

### Method 4: Using Azure Data Factory
1. Create Azure Data Factory instance
2. Create pipeline with Copy Data activity
3. Source: CSV file in Blob Storage
4. Sink: Azure SQL Database table

## Post-Import Validation:
Run the queries in azure_validation_queries.sql to verify data integrity.

## Estimated Import Time:
- Small dataset (<100K records): 1-2 minutes
- Medium dataset (100K-1M records): 5-10 minutes
- Large dataset (>1M records): 15-30 minutes
"@

$importInstructions | Out-File -FilePath "$OutputDirectory\IMPORT_INSTRUCTIONS.txt" -Encoding UTF8

# 7. Final summary
Write-Host "`n[COMPLETE] Database Export Completed Successfully!" -ForegroundColor Green
Write-Host "=" * 60

Write-Host "[OUTPUT] Export Directory: $OutputDirectory" -ForegroundColor Cyan
Write-Host "[FILES] Files Created:" -ForegroundColor Cyan

Get-ChildItem $OutputDirectory | ForEach-Object {
    $size = if ($_.Length -gt 1MB) { "{0:N2} MB" -f ($_.Length / 1MB) } else { "{0:N2} KB" -f ($_.Length / 1KB) }
    Write-Host "   [FILE] $($_.Name) ($size)" -ForegroundColor White
}

Write-Host "`n[NEXT] Next Steps:" -ForegroundColor Yellow
Write-Host "1. [AZURE] Create Azure SQL Database" -ForegroundColor White
Write-Host "2. [TABLE] Run 01_create_table.sql to create table structure" -ForegroundColor White
Write-Host "3. [IMPORT] Import step03_data.csv using preferred method" -ForegroundColor White
Write-Host "4. [VERIFY] Run azure_validation_queries.sql to verify import" -ForegroundColor White
Write-Host "5. [DEPLOY] Deploy Azure Web App with database connection" -ForegroundColor White

Write-Host "`n[READY] Ready for Azure deployment!" -ForegroundColor Green