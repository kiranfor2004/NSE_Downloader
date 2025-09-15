#!/usr/bin/env python3

import pyodbc
import json

def analyze_column_order_and_compliance():
    """Analyze current table structure vs NSE UDiFF source column order"""
    
    # NSE UDiFF source columns in exact order provided
    source_columns = [
        'TradDt', 'BizDt', 'Sgmt', 'Src', 'FinInstrmTp', 'FinInstrmId', 'ISIN', 'TckrSymb', 
        'SctySrs', 'XpryDt', 'FininstrmActlXpryDt', 'StrkPric', 'OptnTp', 'FinInstrmNm', 
        'OpnPric', 'HghPric', 'LwPric', 'ClsPric', 'LastPric', 'PrvsClsgPric', 'UndrlygPric', 
        'SttlmPric', 'OpnIntrst', 'ChngInOpnIntrst', 'TtlTradgVol', 'TtlTrfVal', 'TtlNbOfTxsExctd', 
        'SsnId', 'NewBrdLotQty', 'Rmks', 'Rsvd1', 'Rsvd2', 'Rsvd3', 'Rsvd4'
    ]
    
    print("🔍 NSE F&O UDiFF COLUMN ORDER ANALYSIS")
    print("="*70)
    
    # Load database configuration
    with open('database_config.json', 'r') as f:
        config = json.load(f)

    try:
        conn_str = f"DRIVER={config['driver']};SERVER={config['server']};DATABASE={config['database']};Trusted_Connection=yes;"
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        # Get current table columns in order
        cursor.execute("""
        SELECT COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'step04_fo_udiff_daily' 
        ORDER BY ORDINAL_POSITION
        """)
        
        current_columns = cursor.fetchall()
        current_column_names = [col[0] for col in current_columns]
        
        print(f"📊 SOURCE vs CURRENT COMPARISON:")
        print("-" * 70)
        print(f"NSE UDiFF Source Columns: {len(source_columns)}")
        print(f"Current Table Columns: {len(current_column_names)}")
        
        # Column mapping between source names and current names
        column_mapping = {
            'TradDt': 'trade_date',
            'BizDt': 'BizDt',
            'Sgmt': 'Sgmt', 
            'Src': 'Src',
            'FinInstrmTp': 'instrument',
            'FinInstrmId': 'FinInstrmId',
            'ISIN': 'ISIN',
            'TckrSymb': 'symbol',
            'SctySrs': 'SctySrs',
            'XpryDt': 'expiry_date',
            'FininstrmActlXpryDt': 'FinInstrmActlXpryDt',  # Note: source has lowercase 'i'
            'StrkPric': 'strike_price',
            'OptnTp': 'option_type',
            'FinInstrmNm': 'FinInstrmNm',
            'OpnPric': 'open_price',
            'HghPric': 'high_price',
            'LwPric': 'low_price',
            'ClsPric': 'close_price',
            'LastPric': 'LastPric',
            'PrvsClsgPric': 'PrvsClsgPric',
            'UndrlygPric': 'UndrlygPric',
            'SttlmPric': 'settle_price',
            'OpnIntrst': 'open_interest',
            'ChngInOpnIntrst': 'change_in_oi',
            'TtlTradgVol': 'contracts_traded',
            'TtlTrfVal': 'value_in_lakh',
            'TtlNbOfTxsExctd': 'TtlNbOfTxsExctd',
            'SsnId': 'SsnId',
            'NewBrdLotQty': 'NewBrdLotQty',
            'Rmks': 'Rmks',
            'Rsvd1': 'Rsvd01',  # Current table has Rsvd01, source has Rsvd1
            'Rsvd2': 'Rsvd02',  # Current table has Rsvd02, source has Rsvd2
            'Rsvd3': 'Rsvd03',  # Current table has Rsvd03, source has Rsvd3
            'Rsvd4': 'Rsvd04'   # Current table has Rsvd04, source has Rsvd4
        }
        
        print(f"\n🔍 DETAILED COLUMN ANALYSIS:")
        print("-" * 70)
        print(f"{'#':<3} {'Source Column':<25} {'Current Column':<25} {'Status'}")
        print("-" * 70)
        
        missing_from_current = []
        new_columns_need_approval = []
        naming_discrepancies = []
        
        for i, source_col in enumerate(source_columns, 1):
            if source_col in column_mapping:
                current_col = column_mapping[source_col]
                if current_col in current_column_names:
                    status = "✅ Mapped"
                    if source_col != current_col:
                        naming_discrepancies.append((source_col, current_col))
                else:
                    status = "❌ Missing"
                    missing_from_current.append(source_col)
            else:
                status = "❓ Unmapped"
                missing_from_current.append(source_col)
                current_col = "NOT FOUND"
            
            print(f"{i:<3} {source_col:<25} {current_col:<25} {status}")
        
        # Check for extra columns in current table not in source
        mapped_current_columns = set(column_mapping.values())
        table_specific_columns = {'id', 'underlying', 'source_file', 'created_at'}
        
        for current_col in current_column_names:
            if (current_col not in mapped_current_columns and 
                current_col not in table_specific_columns):
                new_columns_need_approval.append(current_col)
        
        # Issues Summary
        print(f"\n🚨 ISSUES IDENTIFIED:")
        print("-" * 70)
        
        if missing_from_current:
            print(f"❌ MISSING COLUMNS ({len(missing_from_current)}):")
            for col in missing_from_current:
                print(f"   - {col}")
        
        if naming_discrepancies:
            print(f"\n⚠️  NAMING DISCREPANCIES ({len(naming_discrepancies)}):")
            for source_name, current_name in naming_discrepancies:
                print(f"   - Source: '{source_name}' → Current: '{current_name}'")
        
        if new_columns_need_approval:
            print(f"\n❓ COLUMNS NEEDING APPROVAL ({len(new_columns_need_approval)}):")
            for col in new_columns_need_approval:
                print(f"   - {col} (not in source specification)")
        
        # Column order issues
        print(f"\n📋 COLUMN ORDER ANALYSIS:")
        print("-" * 70)
        
        # Check if we can maintain source order
        current_udiff_columns = []
        for source_col in source_columns:
            if source_col in column_mapping and column_mapping[source_col] in current_column_names:
                current_udiff_columns.append(column_mapping[source_col])
        
        print(f"Source order maintainable: {len(current_udiff_columns)}/{len(source_columns)} columns")
        
        # Specific issues found
        issues = []
        
        # 1. Check FininstrmActlXpryDt vs FinInstrmActlXpryDt
        if 'FininstrmActlXpryDt' in source_columns:
            issues.append("Source uses 'FininstrmActlXpryDt' (lowercase 'i'), current uses 'FinInstrmActlXpryDt'")
        
        # 2. Check Rsvd naming
        rsvd_issues = []
        for i in range(1, 5):
            source_rsvd = f'Rsvd{i}'
            current_rsvd = f'Rsvd{i:02d}'
            if current_rsvd in current_column_names:
                rsvd_issues.append(f"Source: '{source_rsvd}' vs Current: '{current_rsvd}'")
        
        if rsvd_issues:
            issues.append("Reserved column naming mismatch: " + ", ".join(rsvd_issues))
        
        if issues:
            print(f"\n🔧 SPECIFIC ISSUES TO FIX:")
            print("-" * 70)
            for i, issue in enumerate(issues, 1):
                print(f"{i}. {issue}")
        
        # Recommendations
        print(f"\n💡 RECOMMENDATIONS:")
        print("-" * 70)
        print(f"1. Rename 'FinInstrmActlXpryDt' to 'FininstrmActlXpryDt' (match source)")
        print(f"2. Rename 'Rsvd01-04' to 'Rsvd1-4' (match source)")
        print(f"3. Consider reordering columns to match source sequence")
        print(f"4. Verify all column mappings are correct")
        
        conn.close()
        
        print(f"\n{'='*70}")
        print(f"📊 SUMMARY:")
        print(f"   Source columns: {len(source_columns)}")
        print(f"   Missing: {len(missing_from_current)}")
        print(f"   Naming issues: {len(naming_discrepancies)}")
        print(f"   Need approval: {len(new_columns_need_approval)}")
        print(f"   Issues found: {len(issues)}")
        print(f"{'='*70}")
        
        return {
            'missing': missing_from_current,
            'naming_issues': naming_discrepancies,
            'need_approval': new_columns_need_approval,
            'issues': issues
        }
        
    except Exception as e:
        print(f"❌ Error during analysis: {e}")
        return None

if __name__ == "__main__":
    analyze_column_order_and_compliance()
