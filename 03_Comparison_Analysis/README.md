# 📈 Comparison Analysis Folder

## Purpose
Month-to-month comparison analysis showing only symbols with increased values between consecutive months.

## Scripts Overview

### Month-to-Month Increase Analysis (Main Series)
- `january_february_increased_only.py` - Jan→Feb increases only
- `february_march_increased_only.py` - Feb→Mar increases only  
- `march_april_increased_only.py` - Mar→Apr increases only
- `april_may_increased_only.py` - Apr→May increases only
- `may_june_increased_only.py` - May→Jun increases only (⚡ Latest)
- `june_july_increased_only.py` - Jun→Jul increases only

### Legacy Comparison Scripts
- `january_february_comparison.py` - Original Jan-Feb comparison
- `july_august_comparison.py` - Jul-Aug comparison
- `horizontal_july_august_comparison.py` - Horizontal format
- `vertical_july_august_comparison.py` - Vertical format
- `simple_july_august_comparison.py` - Simplified version
- `july_february_january_comparison.py` - Multi-month comparison

### Analysis Scripts
- `excel_comparison_analysis.py` - Excel comparison tools

## Excel Output Files

### Consecutive Month Increases (Primary Results)
| Comparison | File | Volume Increases | Delivery Increases | Top Performer |
|------------|------|------------------|-------------------|---------------|
| Jan→Feb | `January_February_Increases_Only_20250911_163431.xlsx` | 915 | 961 | IDEA |
| Feb→Mar | `February_March_Increases_Only_20250911_164319.xlsx` | 1,167 | 1,265 | IDEA |
| Mar→Apr | `March_April_Increases_Only_20250911_164928.xlsx` | 999 | 844 | IDEA |
| Apr→May | `April_May_Increases_Only_20250911_165458.xlsx` | 1,241 | 1,281 | IFCI/ITC |
| May→Jun | `May_June_Increases_Only_20250911_172950.xlsx` | 1,076 | 1,108 | VMM/APTUS |
| Jun→Jul | `June_July_Increases_Only_20250911_173342.xlsx` | 967 | 916 | JPPOWER/JAYNECOIND |

### Legacy July-August Analysis
- `NSE_July_August_Comparison_Analysis_20250911_004057.xlsx` - Comprehensive Jul-Aug
- `Detailed_July_August_Comparison_20250911_071117.xlsx` - Detailed format
- `Horizontal_July_August_Comparison_20250911_073057.xlsx` - Horizontal layout
- `Simple_July_August_Comparison_20250911_065219.xlsx` - Simple format
- `Vertical_July_August_Comparison_20250911_080244.xlsx` - Vertical layout

### Historical January-February Versions
- Multiple `January_February_Comparison_*.xlsx` files (development iterations)

## Analysis Features

### Increase-Only Logic
- **Filters**: Only symbols with actual increases (not decreases or equal)
- **Comparison**: Current month peak vs previous month peak
- **Metrics**: Both volume (TTL_TRD_QNTY) and delivery (DELIV_QTY) tracking
- **Calculations**: Absolute increase + percentage increase + times higher

### Excel Structure
Each file contains 2 sheets:
1. **Volume_Increases**: TTL_TRD_QNTY increases only
2. **Delivery_Increases**: DELIV_QTY increases only

### Columns Included
- Symbol, Previous Month Value, Current Month Value
- Absolute Increase, Percentage Increase, Times Higher
- Comparison text, Dates for both months

## Key Market Insights

### Evolution Pattern
1. **Q1 2025 (Jan-Mar)**: IDEA dominated with massive gains
2. **Q2 2025 (Apr-Jun)**: Market diversification, multiple leaders
3. **Jul 2025**: Sector-specific emergence (JPPOWER, JAYNECOIND)

### Performance Highlights
- **Highest Single Increase**: VMM +1.1B volume (May→Jun)
- **Highest Percentage**: APTUS +8,065% delivery (May→Jun)
- **Most Consistent**: IDEA (dominated Q1 comparisons)
- **Latest Leader**: JAYNECOIND +18,497% volume (Jun→Jul)

### Trend Analysis
- **Increasing Diversity**: More symbols showing gains in recent months
- **Sector Rotation**: From telecom (IDEA) to infrastructure/power/jewellery
- **Volume Growth**: Consistently high number of symbols with increases

## Usage
Run consecutive month comparison:
```bash
python may_june_increased_only.py
```

## Performance Stats
- **Processing Time**: ~60-90 seconds per comparison
- **Data Volume**: 40,000-50,000 records processed per comparison
- **Output Size**: ~5-15MB Excel files
- **Success Rate**: 100% completion for all 6 consecutive months

*Last Updated: September 11, 2025*
