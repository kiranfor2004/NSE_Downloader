# NSE Pipeline - Quick Reference Guide

## **Step Summary**

| Step | Purpose | Input | Output | Status |
|------|---------|-------|--------|--------|
| **01** | Equity Data Collection | NSE BhavCopy files | `step01_equity_daily` table | ✅ Complete |
| **02** | Monthly Analysis | Daily equity data | `step02_monthly_analysis` table | ✅ Complete |
| **03** | Comparative Analysis | Daily + Monthly data | `step03_compare_monthvspreviousmonth` table | ✅ Complete |
| **04** | F&O Data Validation | F&O BhavCopy files | `step04_fo_udiff_daily` table | ✅ Complete |

## **Quick Commands**

### **Load All Data**
```bash
python step01_equity_data_loader.py      # Load equity data
python step02_database_loader.py         # Generate monthly analysis  
python step03_daily_vs_monthly_analyzer.py  # Run comparisons
python step04_fo_validation_loader.py    # Validate F&O data
```

### **Custom Analysis**
```bash
python step03_march_vs_february_analyzer.py  # March vs Feb comparison
```

## **Database Tables Quick Reference**

### **step01_equity_daily**
- **Records**: Daily equity trading data
- **Key Fields**: symbol, trade_date, volume, delivery, prices
- **Usage**: Source for all equity analysis

### **step02_monthly_analysis** 
- **Records**: Monthly aggregated statistics
- **Key Fields**: symbol, month, baseline_volume, baseline_delivery
- **Usage**: Baseline for comparative analysis

### **step03_compare_monthvspreviousmonth**
- **Records**: Exceedance detection results
- **Key Fields**: daily_values, baseline_values, exceedance_flags, increase_metrics
- **Usage**: Anomaly analysis and reporting

### **step04_fo_udiff_daily**
- **Records**: 757,755+ F&O derivatives data
- **Key Fields**: symbol, instrument, expiry, strike, volumes, open_interest
- **Usage**: F&O analysis and derivatives research

## **File Locations**

### **Main Scripts**
- `step01_equity_data_loader.py` - Equity data loader
- `step02_database_loader.py` - Monthly aggregation
- `step03_daily_vs_monthly_analyzer.py` - Main comparison engine
- `step04_fo_validation_loader.py` - **Official F&O loader**

### **Data Directories**
- `step01_equity_downloads/` - Equity source files
- `fo_udiff_downloads/` - F&O source files
- `step02_monthly_analysis/` - Monthly analysis outputs
- `step03_monthly_comparisons/` - Comparison analysis modules

### **Documentation**
- `NSE_PIPELINE_COMPLETE_GUIDE.md` - **Complete detailed guide**
- `PROJECT_PIPELINE_OVERVIEW.md` - Project overview
- `STEP04_README.md` - Step 4 technical details

## **Common Queries**

### **Check Data Availability**
```sql
-- Equity data coverage
SELECT MIN(trade_date), MAX(trade_date), COUNT(*) 
FROM step01_equity_daily;

-- Monthly analysis coverage  
SELECT DISTINCT analysis_month 
FROM step02_monthly_analysis 
ORDER BY analysis_month;

-- F&O data coverage
SELECT MIN(trade_date), MAX(trade_date), COUNT(*) 
FROM step04_fo_udiff_daily;
```

### **Find Exceedances**
```sql
-- Top volume exceedances
SELECT TOP 10 symbol, trade_date, daily_ttl_trd_qnty, monthly_volume_baseline
FROM step03_compare_monthvspreviousmonth 
WHERE volume_exceeded = 1 
ORDER BY volume_increase_pct DESC;
```

## **Troubleshooting**

### **Common Issues**
- **Database connection**: Check `database_config.json`
- **Missing files**: Verify source file directories
- **Validation failures**: Use Step 4 retry logic
- **Date format errors**: Check date conversion in scripts

### **Quick Fixes**
- **Restart validation**: Run Step 4 loader again
- **Check logs**: Review console output for detailed errors
- **Verify data**: Use SQL queries to check record counts

---

**For detailed information, see**: `NSE_PIPELINE_COMPLETE_GUIDE.md`