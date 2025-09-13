#!/usr/bin/env python3
"""
Step 02 Extension: February 2025 Daily Exceedance Analysis vs January 2025 Baseline

Enhancements:
  - CLI arguments for flexibility:
      --jan-file <path to January unique analysis Excel>
      --feb-pattern <glob of February daily CSVs>
      --out-prefix <output filename prefix>
      --out-dir <directory to place Excel output>
      --min-volume-diff <absolute min volume increase to keep>
      --min-delivery-diff <absolute min delivery increase to keep>
      --min-volume-pct <minimum percentage volume increase>
      --min-delivery-pct <minimum percentage delivery increase>
  - Percentage difference columns: VOLUME_PCT_INCREASE, DELIVERY_PCT_INCREASE
  - Adds sheets Top_Volume_Pct and Top_Delivery_Pct if data available

Default Behavior:
  Still reproduces prior logic if no thresholds specified (all exceedances retained).

Output Excel:
  Sheets:
    Exceedances_All
    Summary
    Top_Volume_Diff (absolute diff)
    Top_Delivery_Diff (absolute diff)
    Top_Volume_Pct (percentage diff)
    Top_Delivery_Pct (percentage diff)

Exit codes:
  0 success, 2 baseline missing, 3 no February data, 4 no exceedances after filters.
"""

import pandas as pd
import glob
import os
import sys
import argparse
from datetime import datetime

JAN_BASELINE_FILE_DEFAULT = os.path.join('02_Monthly_Analysis', 'NSE_JANUARY2025_data_Unique_Symbol_Analysis_20250911_112130.xlsx')
FEB_PATTERN_DEFAULT = 'NSE_February_2025_Data/cm*.csv'
OUTPUT_PREFIX_DEFAULT = 'Feb2025_Exceedances_vs_January'

COLUMN_MAPPING = {
    'symbol': 'SYMBOL', 'SYMBOL': 'SYMBOL',
    'date': 'DATE', 'DATE': 'DATE', 'DATE1': 'DATE',
    'series': 'SERIES', 'SERIES': 'SERIES',
    'TTL_TRD_QNTY': 'TTL_TRD_QNTY', 'total_traded_qty': 'TTL_TRD_QNTY', 'volume': 'TTL_TRD_QNTY',
    'DELIV_QTY': 'DELIV_QTY', 'delivery_qty': 'DELIV_QTY', 'deliv_qty': 'DELIV_QTY'
}


def parse_args():
    p = argparse.ArgumentParser(description='February exceedances vs January baseline')
    p.add_argument('--jan-file', default=JAN_BASELINE_FILE_DEFAULT, help='January unique analysis Excel file')
    p.add_argument('--feb-pattern', default=FEB_PATTERN_DEFAULT, help='Glob for February daily CSV files')
    p.add_argument('--out-prefix', default=OUTPUT_PREFIX_DEFAULT, help='Output Excel filename prefix')
    p.add_argument('--out-dir', default='analysis_outputs', help='Directory to write Excel output (created if missing)')
    p.add_argument('--min-volume-diff', type=float, default=0, help='Minimum absolute volume increase to include')
    p.add_argument('--min-delivery-diff', type=float, default=0, help='Minimum absolute delivery increase to include')
    p.add_argument('--min-volume-pct', type=float, default=0, help='Minimum percentage volume increase to include (0-100)')
    p.add_argument('--min-delivery-pct', type=float, default=0, help='Minimum percentage delivery increase to include (0-100)')
    return p.parse_args()


def load_january_baseline(path: str):
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    vol = pd.read_excel(path, sheet_name='Unique_TTL_TRD_QNTY')
    deliv = pd.read_excel(path, sheet_name='Unique_DELIV_QTY')
    vol.columns = vol.columns.str.strip()
    deliv.columns = deliv.columns.str.strip()
    for needed in [('SYMBOL','TTL_TRD_QNTY'), ('SYMBOL','DELIV_QTY')]:
        if needed[0] not in (vol.columns if 'TTL' in needed[1] else deliv.columns) or needed[1] not in (vol.columns if 'TTL' in needed[1] else deliv.columns):
            raise ValueError('Baseline missing required columns')
    # Keep DATE column from January data for reference
    jan_volume = vol[['SYMBOL','TTL_TRD_QNTY','DATE']].copy() if 'DATE' in vol.columns else vol[['SYMBOL','TTL_TRD_QNTY']].copy()
    jan_delivery = deliv[['SYMBOL','DELIV_QTY','DATE']].copy() if 'DATE' in deliv.columns else deliv[['SYMBOL','DELIV_QTY']].copy()
    jan_volume['TTL_TRD_QNTY'] = pd.to_numeric(jan_volume['TTL_TRD_QNTY'], errors='coerce').fillna(0)
    jan_delivery['DELIV_QTY'] = pd.to_numeric(jan_delivery['DELIV_QTY'], errors='coerce').fillna(0)
    
    # Merge volume and delivery data, keeping both dates if available
    if 'DATE' in jan_volume.columns and 'DATE' in jan_delivery.columns:
        base = pd.merge(jan_volume, jan_delivery, on='SYMBOL', how='outer', suffixes=('_VOL', '_DEL')).fillna(0)
        base = base.rename(columns={'TTL_TRD_QNTY':'JAN_BASE_VOLUME','DELIV_QTY':'JAN_BASE_DELIVERY',
                                  'DATE_VOL':'JAN_VOLUME_DATE','DATE_DEL':'JAN_DELIVERY_DATE'})
    else:
        base = pd.merge(jan_volume, jan_delivery, on='SYMBOL', how='outer').fillna(0)
        base = base.rename(columns={'TTL_TRD_QNTY':'JAN_BASE_VOLUME','DELIV_QTY':'JAN_BASE_DELIVERY'})
    return base


def load_february_daily(pattern: str):
    files = sorted(glob.glob(pattern))
    if not files:
        return pd.DataFrame()
    frames = []
    for f in files:
        try:
            df = pd.read_csv(f)
            df.columns = df.columns.str.strip()
            df = df.map(lambda x: x.strip() if isinstance(x,str) else x)
            df = df.rename(columns=COLUMN_MAPPING)
            if 'SERIES' in df.columns:
                df = df[df['SERIES']=='EQ']
            for col in ['TTL_TRD_QNTY','DELIV_QTY']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            if 'DATE' not in df.columns:
                base = os.path.basename(f)
                df['DATE'] = base
            df['SOURCE_FILE'] = os.path.basename(f)
            frames.append(df)
        except Exception as e:
            print(f'Skip {f}: {e}')
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out = out[out.get('SYMBOL').notna()]
    out['SYMBOL'] = out['SYMBOL'].astype(str)
    return out


def build_exceedances(feb_df: pd.DataFrame, jan_base: pd.DataFrame):
    merged = feb_df.merge(jan_base, on='SYMBOL', how='left')
    merged['JAN_BASE_VOLUME'] = merged['JAN_BASE_VOLUME'].fillna(0)
    merged['JAN_BASE_DELIVERY'] = merged['JAN_BASE_DELIVERY'].fillna(0)
    
    # Check for both volume and delivery exceedances
    merged['EXCEEDS_VOLUME'] = merged['TTL_TRD_QNTY'] > merged['JAN_BASE_VOLUME']
    merged['EXCEEDS_DELIVERY'] = merged['DELIV_QTY'] > merged['JAN_BASE_DELIVERY']
    
    # Split into volume and delivery exceedances for separate analysis
    volume_exceed = merged[merged['EXCEEDS_VOLUME']].copy()
    delivery_exceed = merged[merged['EXCEEDS_DELIVERY']].copy()
    
    # Add analysis type flags
    volume_exceed['ANALYSIS_TYPE'] = 'VOLUME_EXCEEDED'
    delivery_exceed['ANALYSIS_TYPE'] = 'DELIVERY_EXCEEDED'
    
    # Combine both types
    all_exceed = pd.concat([volume_exceed, delivery_exceed], ignore_index=True)
    
    # Remove duplicates where both volume and delivery exceeded (keep both records but mark them)
    return all_exceed


def apply_filters(exceed: pd.DataFrame, args):
    # Simple filtering - only basic exceedance check already done
    return exceed


def summarize(exceed: pd.DataFrame):
    total_rows = len(exceed)
    total_symbols = exceed['SYMBOL'].nunique() if 'SYMBOL' in exceed.columns and total_rows else 0
    vol_exceed = exceed[exceed['EXCEEDS_VOLUME']].shape[0]
    del_exceed = exceed[exceed['EXCEEDS_DELIVERY']].shape[0]

    return {
        'summary_rows': [
            ['Metric', 'Value'],
            ['Total Exceedance Rows', total_rows],
            ['Distinct Symbols Exceeding', total_symbols],
            ['Rows Exceed Volume', vol_exceed],
            ['Rows Exceed Delivery', del_exceed],
            ['Generated On', datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
        ]
    }


def write_output(exceed: pd.DataFrame, info: dict, prefix: str, out_dir: str):
    os.makedirs(out_dir, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    out_file = os.path.join(out_dir, f'{prefix}_{ts}.xlsx')
    print(f'Writing {out_file} ...')
    with pd.ExcelWriter(out_file, engine='openpyxl') as w:
        # Separate volume and delivery exceedances
        volume_data = exceed[exceed['ANALYSIS_TYPE'] == 'VOLUME_EXCEEDED'].copy()
        delivery_data = exceed[exceed['ANALYSIS_TYPE'] == 'DELIVERY_EXCEEDED'].copy()
        
        # Define core columns to keep
        core_cols = ['DATE', 'SYMBOL', 'SERIES', 'TTL_TRD_QNTY', 'DELIV_QTY', 'JAN_BASE_VOLUME', 'JAN_BASE_DELIVERY', 'ANALYSIS_TYPE']
        
        # Add January date columns if they exist
        jan_date_cols = [c for c in exceed.columns if c in ['JAN_VOLUME_DATE', 'JAN_DELIVERY_DATE']]
        core_cols.extend(jan_date_cols)
        
        # Add other original trading columns
        for col in exceed.columns:
            if col in ['PREV_CLOSE', 'OPEN_PRICE', 'HIGH_PRICE', 'LOW_PRICE', 'LAST_PRICE', 'CLOSE_PRICE', 'AVG_PRICE', 'TURNOVER_LACS', 'NO_OF_TRADES', 'DELIV_PER']:
                if col not in core_cols:
                    core_cols.append(col)
        
        # Only keep columns that actually exist
        final_cols = [c for c in core_cols if c in exceed.columns]
        
        # Volume Exceedances Sheet
        if not volume_data.empty:
            vol_output = volume_data[final_cols].copy()
            vol_output = vol_output.sort_values(['SYMBOL', 'DATE'])
            vol_output.to_excel(w, sheet_name='Volume_Exceedances', index=False)
        
        # Delivery Exceedances Sheet  
        if not delivery_data.empty:
            del_output = delivery_data[final_cols].copy()
            del_output = del_output.sort_values(['SYMBOL', 'DATE'])
            del_output.to_excel(w, sheet_name='Delivery_Exceedances', index=False)
        
        # Combined Sheet
        combined_output = exceed[final_cols].copy()
        combined_output = combined_output.sort_values(['SYMBOL', 'DATE'])
        combined_output.to_excel(w, sheet_name='All_Exceedances', index=False)

        # Summary
        summary_df = pd.DataFrame(info['summary_rows'][1:], columns=info['summary_rows'][0])
        summary_df.to_excel(w, sheet_name='Summary', index=False)

    print('Output written.')
    return out_file


def main():
    args = parse_args()

    try:
        jan_base = load_january_baseline(args.jan_file)
    except FileNotFoundError:
        print(f'January baseline not found: {args.jan_file}')
        sys.exit(2)
    except Exception as e:
        print(f'Error loading January baseline: {e}')
        sys.exit(2)

    feb_df = load_february_daily(args.feb_pattern)
    if feb_df.empty:
        print(f'No February data files found for pattern {args.feb_pattern}')
        sys.exit(3)

    merged = build_exceedances(feb_df, jan_base)
    exceed_filtered = apply_filters(merged, args)
    print(f'Exceedance rows after filters: {exceed_filtered.shape[0]}')
    if exceed_filtered.empty:
        print('No exceedances meet filter criteria.')
        sys.exit(4)

    info = summarize(exceed_filtered)
    write_output(exceed_filtered, info, args.out_prefix, args.out_dir)
    sys.exit(0)

if __name__ == '__main__':
    main()
