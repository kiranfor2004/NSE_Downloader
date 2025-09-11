# 📊 Monthly Analysis Folder

## Purpose
Individual month comprehensive unique symbol analysis - one record per symbol showing peak performance.

## Scripts Overview

### Analysis Scripts
- `unique_symbol_analysis_january_comprehensive.py` - January 2025 analysis
- `unique_symbol_analysis_february_comprehensive.py` - February 2025 analysis
- `unique_symbol_analysis_march_comprehensive.py` - March 2025 analysis
- `unique_symbol_analysis_april_comprehensive.py` - April 2025 analysis
- `unique_symbol_analysis_may_comprehensive.py` - May 2025 analysis (⚡ Latest)
- `unique_symbol_analysis_june_comprehensive.py` - June 2025 analysis
- `unique_symbol_analysis_july.py` - July 2025 analysis

### Legacy Scripts
- `unique_symbol_analysis.py` - Original analysis script
- `unique_symbol_analysis_january.py` - Early January version

## Excel Output Files

### Completed Analysis
| Month | File | Symbols | Status |
|-------|------|---------|--------|
| July 2025 | `NSE_JULY2025_data_Unique_Symbol_Analysis_20250911_003120.xlsx` | 2,140 | ✅ |
| January 2025 | `NSE_JANUARY2025_data_Unique_Symbol_Analysis_20250911_112727.xlsx` | 2,140 | ✅ |
| February 2025 | `NSE_FEBRUARY2025_data_Unique_Symbol_Analysis_20250911_113430.xlsx` | 2,140 | ✅ |
| March 2025 | `NSE_MARCH2025_data_Unique_Symbol_Analysis_20250911_113959.xlsx` | 2,140 | ✅ |
| April 2025 | `NSE_APRIL2025_data_Unique_Symbol_Analysis_20250911_121027.xlsx` | 2,140 | ✅ |
| May 2025 | `NSE_MAY2025_data_Unique_Symbol_Analysis_20250911_121541.xlsx` | 2,140 | ✅ |
| June 2025 | `NSE_JUNE2025_data_Unique_Symbol_Analysis_20250911_151422.xlsx` | 2,163 | ✅ |
| August 2025 | `NSE_AUGUST2025_data_Unique_Symbol_Analysis_20250911_000857.xlsx` | 2,140 | ✅ |

## Analysis Features

### Unique Symbol Logic
- **No Duplicates**: One record per symbol per month
- **Peak Volume**: Highest TTL_TRD_QNTY for each symbol
- **Peak Delivery**: Highest DELIV_QTY for each symbol
- **EQ Only**: Filters SERIES='EQ' equity stocks only
- **Complete Data**: Preserves all original columns

### Excel Structure
Each analysis file contains 2 sheets:
1. **Unique_TTL_TRD_QNTY**: Peak volume analysis per symbol
2. **Unique_DELIV_QTY**: Peak delivery analysis per symbol

### Columns Included
- Symbol, Date, Series, Open, High, Low, Close, Last, PrevClose
- TTL_TRD_QNTY, TTL_TRD_VAL, DELIV_QTY, DELIV_PER
- Plus additional NSE standard columns

## Usage
Run monthly analysis after downloading data:
```bash
python unique_symbol_analysis_may_comprehensive.py
```

## Performance Stats
- **Processing Time**: ~30-60 seconds per month
- **Memory Usage**: ~500MB-1GB depending on data size
- **Output Size**: ~15-25MB Excel files
- **Symbols Covered**: 2,140-2,163 unique symbols per month

*Last Updated: September 11, 2025*
