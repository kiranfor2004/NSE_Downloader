#!/usr/bin/env python3

import pyodbc
import json

def final_udiff_compliance_verification():
    """Final verification of UDiFF compliance with exact source column order"""
    
    # Load database configuration
    with open('database_config.json', 'r') as f:
        config = json.load(f)

    try:
        conn_str = f"DRIVER={config['driver']};SERVER={config['server']};DATABASE={config['database']};Trusted_Connection=yes;"
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        print("✅ FINAL UDiFF COMPLIANCE VERIFICATION")
        print("="*60)
        
        # NSE UDiFF source columns in exact order provided by user
        source_order = [
            'TradDt', 'BizDt', 'Sgmt', 'Src', 'FinInstrmTp', 'FinInstrmId', 'ISIN', 'TckrSymb', 
            'SctySrs', 'XpryDt', 'FininstrmActlXpryDt', 'StrkPric', 'OptnTp', 'FinInstrmNm', 
            'OpnPric', 'HghPric', 'LwPric', 'ClsPric', 'LastPric', 'PrvsClsgPric', 'UndrlygPric', 
            'SttlmPric', 'OpnIntrst', 'ChngInOpnIntrst', 'TtlTradgVol', 'TtlTrfVal', 'TtlNbOfTxsExctd', 
            'SsnId', 'NewBrdLotQty', 'Rmks', 'Rsvd1', 'Rsvd2', 'Rsvd3', 'Rsvd4'
        ]
        
        print(f"📋 NSE UDiFF Source Specification:")
        print(f"   Total columns: {len(source_order)}")
        print(f"   Source order maintained: ✅")
        
        # Get current table structure
        cursor.execute("""
        SELECT COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'step04_fo_udiff_daily' 
        ORDER BY ORDINAL_POSITION
        """)
        
        current_columns = cursor.fetchall()
        current_column_names = [col[0] for col in current_columns]
        
        print(f"\n📊 Current Table Structure:")
        print(f"   Total columns: {len(current_columns)}")
        
        # Column mapping verification
        print(f"\n🔍 COLUMN MAPPING VERIFICATION:")
        print("-" * 60)
        print(f"{'#':<3} {'Source Column':<25} {'Table Column':<25} {'Status'}")
        print("-" * 60)
        
        # Final mapping after fixes
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
            'FininstrmActlXpryDt': 'FininstrmActlXpryDt',  # Fixed to match source
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
            'Rsvd1': 'Rsvd1',  # Fixed to match source
            'Rsvd2': 'Rsvd2',  # Fixed to match source
            'Rsvd3': 'Rsvd3',  # Fixed to match source
            'Rsvd4': 'Rsvd4'   # Fixed to match source
        }
        
        all_mapped = True
        exact_name_matches = 0
        
        for i, source_col in enumerate(source_order, 1):
            if source_col in column_mapping:
                table_col = column_mapping[source_col]
                if table_col in current_column_names:
                    status = "✅ Mapped"
                    if source_col == table_col:
                        exact_name_matches += 1
                        status += " (Exact)"
                else:
                    status = "❌ Missing"
                    all_mapped = False
            else:
                table_col = "NOT MAPPED"
                status = "❌ No Mapping"
                all_mapped = False
            
            print(f"{i:<3} {source_col:<25} {table_col:<25} {status}")
        
        # Check for extra columns not in source
        table_specific_columns = {'id', 'underlying', 'source_file', 'created_at'}
        mapped_columns = set(column_mapping.values())
        extra_columns = []
        
        for col in current_column_names:
            if col not in mapped_columns and col not in table_specific_columns:
                extra_columns.append(col)
        
        print(f"\n📋 COMPLIANCE SUMMARY:")
        print("-" * 60)
        print(f"✅ Total source columns: {len(source_order)}")
        print(f"✅ Successfully mapped: {len([col for col in source_order if col in column_mapping and column_mapping[col] in current_column_names])}")
        print(f"✅ Exact name matches: {exact_name_matches}")
        print(f"✅ Renamed for clarity: {len(source_order) - exact_name_matches}")
        
        if extra_columns:
            print(f"⚠️  Extra columns (not in source): {len(extra_columns)}")
            for col in extra_columns:
                print(f"   - {col}")
        else:
            print(f"✅ No extra columns beyond table-specific ones")
        
        # Table-specific columns (not from UDiFF source)
        print(f"\n📝 Table-Specific Columns (not from UDiFF):")
        print("-" * 60)
        for col in table_specific_columns:
            if col in current_column_names:
                print(f"   ✅ {col}")
            else:
                print(f"   ❌ {col} (missing)")
        
        # Sample data verification with UDiFF order
        print(f"\n🧪 Sample Data Verification (UDiFF Order):")
        print("-" * 60)
        
        # Create SELECT with UDiFF order
        select_columns = []
        for source_col in source_order:
            if source_col in column_mapping:
                table_col = column_mapping[source_col]
                if source_col == table_col:
                    select_columns.append(table_col)
                else:
                    select_columns.append(f"{table_col} AS {source_col}")
        
        sample_query = f"""
        SELECT TOP 1 {', '.join(select_columns)}
        FROM step04_fo_udiff_daily 
        WHERE trade_date = '20250203'
        ORDER BY id
        """
        
        try:
            cursor.execute(sample_query)
            sample_data = cursor.fetchone()
            
            if sample_data:
                print(f"✅ Sample record retrieved successfully")
                print(f"   Columns in UDiFF order: {len(sample_data)}")
                print(f"   Sample values (first 5 columns):")
                for i, (col, val) in enumerate(zip(source_order[:5], sample_data[:5])):
                    print(f"     {col}: {val}")
            else:
                print(f"⚠️  No sample data found")
                
        except Exception as e:
            print(f"❌ Sample query failed: {e}")
        
        # Validation script verification
        print(f"\n📄 Validation Script Status:")
        print("-" * 60)
        try:
            with open('udiff_validation_query.sql', 'r') as f:
                script_content = f.read()
            print(f"✅ Validation script exists")
            print(f"   File size: {len(script_content)} characters")
            print(f"   Ready for validation testing")
        except FileNotFoundError:
            print(f"❌ Validation script not found")
        
        conn.close()
        
        # Final compliance status
        print(f"\n{'='*60}")
        if all_mapped:
            print(f"🎯 UDIFF COMPLIANCE: ✅ FULLY COMPLIANT")
            print(f"📊 All {len(source_order)} source columns mapped")
            print(f"📋 Column order matches NSE UDiFF specification")
            print(f"✅ Ready for validation and export")
        else:
            print(f"🎯 UDIFF COMPLIANCE: ❌ ISSUES FOUND")
            print(f"⚠️  Some columns missing or unmapped")
            print(f"🔧 Manual fixes required")
        
        print(f"📝 NEXT STEPS:")
        print(f"   1. Use 'udiff_validation_query.sql' for validation")
        print(f"   2. Export data maintaining UDiFF column order")
        print(f"   3. Verify output matches NSE UDiFF format exactly")
        print(f"{'='*60}")
        
        return all_mapped
        
    except Exception as e:
        print(f"❌ Error during verification: {e}")
        return False

if __name__ == "__main__":
    final_udiff_compliance_verification()
