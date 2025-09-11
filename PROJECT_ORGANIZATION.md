# 📊 NSE Data Analysis Project - Organized Structure

## 🗂️ Project Overview
This project performs comprehensive NSE (National Stock Exchange) data analysis for 2025 with three main activities:

1. **Data Downloads** - Monthly NSE data collection
2. **Monthly Analysis** - Unique symbol analysis for each month
3. **Comparison Analysis** - Month-to-month growth tracking

---

## 📁 Folder Structure

### 1️⃣ `01_Data_Downloads/`
**Purpose**: NSE monthly data downloaders and raw data storage
- **Scripts**: Monthly downloader files (`nse_*_2025_downloader.py`)
- **Data Folders**: Raw CSV files organized by month
- **Coverage**: January 2025 - August 2025 (8 months)
- **Records**: 460,000+ trading records across 164 CSV files

### 2️⃣ `02_Monthly_Analysis/`
**Purpose**: Individual month unique symbol analysis
- **Scripts**: Comprehensive analysis files (`unique_symbol_analysis_*_comprehensive.py`)
- **Output**: Excel files with unique symbol analysis (`NSE_*2025_data_Unique_Symbol_Analysis_*.xlsx`)
- **Features**: 
  - Peak volume (TTL_TRD_QNTY) identification per symbol
  - Peak delivery (DELIV_QTY) identification per symbol
  - EQ series filtering only
  - No duplicate symbols per analysis

### 3️⃣ `03_Comparison_Analysis/`
**Purpose**: Month-to-month comparison showing only increased values
- **Scripts**: Consecutive month comparison files (`*_increased_only.py`)
- **Output**: Excel files showing only symbols with increases (`*_Increases_Only_*.xlsx`)
- **Coverage**: 
  - January → February 2025
  - February → March 2025
  - March → April 2025
  - April → May 2025
  - May → June 2025
  - June → July 2025
- **Format**: Standardized 2-sheet Excel format (Volume_Increases, Delivery_Increases)

### 4️⃣ `04_Archive/`
**Purpose**: Experimental files, tests, and deprecated scripts

---

## 📈 Analysis Results Summary

### Monthly Unique Analysis (Activity 2)
- ✅ **July 2025**: 2,140 unique symbols analyzed
- ✅ **January 2025**: 2,140 unique symbols analyzed  
- ✅ **February 2025**: 2,140 unique symbols analyzed
- ✅ **March 2025**: 2,140 unique symbols analyzed
- ✅ **April 2025**: 2,140 unique symbols analyzed
- ✅ **May 2025**: 2,140 unique symbols analyzed
- ✅ **June 2025**: 2,163 unique symbols analyzed

### Month-to-Month Comparisons (Activity 3)
| Comparison | Volume Increases | Delivery Increases | Top Volume Gainer | Top Delivery Gainer |
|------------|------------------|-------------------|-------------------|-------------------|
| Jan→Feb | 915 | 961 | IDEA | IDEA |
| Feb→Mar | 1,167 | 1,265 | IDEA | IDEA |
| Mar→Apr | 999 | 844 | IDEA | YESBANK |
| Apr→May | 1,241 | 1,281 | IFCI | ITC |
| May→Jun | 1,076 | 1,108 | VMM | APTUS |
| Jun→Jul | 967 | 916 | JPPOWER | JAYNECOIND |

---

## 🚀 Key Market Insights

### Evolution Pattern:
1. **Early 2025**: IDEA dominated most categories with massive percentage increases
2. **Mid 2025**: Market diversification with multiple high performers (ITC, IFCI, VMM)
3. **Recent**: Sector-specific leaders emerging (JPPOWER, JAYNECOIND, APTUS)

### Performance Highlights:
- **Highest Volume Increase**: VMM (+1.1B, May→June)
- **Highest Delivery %**: APTUS (+8,065%, May→June) 
- **Most Consistent**: IDEA (dominated Jan-Mar comparisons)
- **Emerging Leader**: JAYNECOIND (+18,497% volume, June→July)

---

## 🛠️ Usage Instructions

### Running Analysis:
1. **Data Download**: Use scripts in `01_Data_Downloads/`
2. **Monthly Analysis**: Use scripts in `02_Monthly_Analysis/`
3. **Comparison**: Use scripts in `03_Comparison_Analysis/`

### File Naming Convention:
- **Downloads**: `nse_[month]_2025_downloader.py`
- **Analysis**: `unique_symbol_analysis_[month]_comprehensive.py`
- **Comparisons**: `[month1]_[month2]_increased_only.py`

### Output Format:
- **Monthly**: `NSE_[MONTH]2025_data_Unique_Symbol_Analysis_[timestamp].xlsx`
- **Comparison**: `[Month1]_[Month2]_Increases_Only_[timestamp].xlsx`

---

## 📊 Technical Specifications

### Data Processing:
- **Language**: Python 3.x
- **Libraries**: pandas, openpyxl, glob
- **Filtering**: SERIES='EQ' only
- **Analysis**: Peak value identification per symbol
- **Output**: Excel with multiple sheets

### System Requirements:
- Python 3.7+
- pandas >= 1.3.0
- openpyxl >= 3.0.0
- Minimum 8GB RAM (for large dataset processing)

---

## 📅 Project Timeline
- **Start**: July 2025 data download
- **Expansion**: January-August 2025 complete dataset
- **Analysis Phase**: Monthly unique symbol analysis (7 months completed)
- **Comparison Phase**: Month-to-month tracking (6 comparisons completed)
- **Organization**: September 11, 2025 - Project restructured

---

## 🎯 Next Steps
1. **July → August 2025** comparison (pending)
2. **Quarterly analysis** aggregation
3. **Trend analysis** across complete 2025 dataset
4. **Automated pipeline** for future months

---

*Last Updated: September 11, 2025*
*Total Files Organized: 50+ scripts and Excel files*
*Data Coverage: 8 months of NSE trading data (460,000+ records)*
