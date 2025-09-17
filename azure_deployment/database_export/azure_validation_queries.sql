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
