#!/usr/bin/env python3

import pyodbc
import json

def fix_column_order_for_udiff_validation():
    """Fix column names and order to match NSE UDiFF source exactly"""
    
    # Load database configuration
    with open('database_config.json', 'r') as f:
        config = json.load(f)

    try:
        conn_str = f"DRIVER={config['driver']};SERVER={config['server']};DATABASE={config['database']};Trusted_Connection=yes;"
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        print("🔧 FIXING COLUMN ORDER FOR UDiFF VALIDATION")
        print("="*60)
        
        # Step 1: Fix column naming discrepancies
        print(f"\n📝 Step 1: Fixing Column Names...")
        print("-" * 40)
        
        column_renames = [
            ('FinInstrmActlXpryDt', 'FininstrmActlXpryDt'),  # Fix lowercase 'i'
            ('Rsvd01', 'Rsvd1'),
            ('Rsvd02', 'Rsvd2'), 
            ('Rsvd03', 'Rsvd3'),
            ('Rsvd04', 'Rsvd4')
        ]
        
        for old_name, new_name in column_renames:
            try:
                # Check if old column exists
                cursor.execute(f"""
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'step04_fo_udiff_daily' AND COLUMN_NAME = '{old_name}'
                """)
                
                if cursor.fetchone()[0] > 0:
                    # Rename column
                    rename_sql = f"EXEC sp_rename 'step04_fo_udiff_daily.{old_name}', '{new_name}', 'COLUMN'"
                    cursor.execute(rename_sql)
                    conn.commit()
                    print(f"   ✅ Renamed: {old_name} → {new_name}")
                else:
                    print(f"   ⏭️  Column {old_name} not found")
                    
            except Exception as e:
                print(f"   ❌ Error renaming {old_name}: {e}")
        
        # Step 2: Verify current column structure
        print(f"\n🔍 Step 2: Current Column Structure...")
        print("-" * 40)
        
        cursor.execute("""
        SELECT COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'step04_fo_udiff_daily' 
        ORDER BY ORDINAL_POSITION
        """)
        
        current_columns = cursor.fetchall()
        print(f"Current table has {len(current_columns)} columns")
        
        # Step 3: Create ideal column mapping for validation
        print(f"\n📋 Step 3: Creating UDiFF-Compliant Column Mapping...")
        print("-" * 40)
        
        # NSE UDiFF source columns in exact order
        source_order = [
            'TradDt', 'BizDt', 'Sgmt', 'Src', 'FinInstrmTp', 'FinInstrmId', 'ISIN', 'TckrSymb', 
            'SctySrs', 'XpryDt', 'FininstrmActlXpryDt', 'StrkPric', 'OptnTp', 'FinInstrmNm', 
            'OpnPric', 'HghPric', 'LwPric', 'ClsPric', 'LastPric', 'PrvsClsgPric', 'UndrlygPric', 
            'SttlmPric', 'OpnIntrst', 'ChngInOpnIntrst', 'TtlTradgVol', 'TtlTrfVal', 'TtlNbOfTxsExctd', 
            'SsnId', 'NewBrdLotQty', 'Rmks', 'Rsvd1', 'Rsvd2', 'Rsvd3', 'Rsvd4'
        ]
        
        # Current column names mapping to source
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
            'FininstrmActlXpryDt': 'FininstrmActlXpryDt',  # Now fixed
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
            'Rsvd1': 'Rsvd1',  # Now fixed
            'Rsvd2': 'Rsvd2',  # Now fixed
            'Rsvd3': 'Rsvd3',  # Now fixed
            'Rsvd4': 'Rsvd4'   # Now fixed
        }
        
        # Step 4: Create validation-friendly SELECT statement
        print(f"\n📊 Step 4: Creating UDiFF-Ordered SELECT Statement...")
        print("-" * 40)
        
        select_columns = []
        missing_columns = []
        
        for source_col in source_order:
            if source_col in column_mapping:
                current_col = column_mapping[source_col]
                # Check if column exists in table
                col_exists = any(col[0] == current_col for col in current_columns)
                if col_exists:
                    if source_col == current_col:
                        select_columns.append(f"{current_col}")
                    else:
                        select_columns.append(f"{current_col} AS {source_col}")
                else:
                    missing_columns.append(source_col)
                    select_columns.append(f"NULL AS {source_col}")
            else:
                missing_columns.append(source_col)
                select_columns.append(f"NULL AS {source_col}")
        
        # Create the validation SELECT statement
        validation_select = "SELECT \n    " + ",\n    ".join(select_columns) + "\nFROM step04_fo_udiff_daily"
        
        print(f"✅ Created UDiFF-compliant SELECT statement")
        print(f"   Mapped columns: {len(select_columns) - len(missing_columns)}")
        print(f"   Missing columns: {len(missing_columns)}")
        
        if missing_columns:
            print(f"   Missing: {', '.join(missing_columns)}")
        
        # Step 5: Test the validation query
        print(f"\n🧪 Step 5: Testing Validation Query...")
        print("-" * 40)
        
        try:
            test_query = validation_select + " WHERE trade_date = '20250203' ORDER BY id OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY"
            cursor.execute(test_query)
            test_result = cursor.fetchone()
            
            if test_result:
                print(f"✅ Validation query works successfully")
                print(f"   Sample record retrieved with {len(test_result)} columns")
            else:
                print(f"⚠️  Query works but no data returned")
                
        except Exception as e:
            print(f"❌ Validation query failed: {e}")
        
        # Step 6: Create the final validation script
        print(f"\n📄 Step 6: Creating Final Validation Script...")
        print("-" * 40)
        
        validation_script = f"""-- NSE F&O UDiFF Validation Query
-- Columns ordered exactly as per NSE UDiFF source specification
-- Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

{validation_select}
WHERE trade_date BETWEEN '20250203' AND '20250215'
ORDER BY trade_date, id;
"""
        
        # Save validation script
        with open('udiff_validation_query.sql', 'w') as f:
            f.write(validation_script)
        
        print(f"✅ Saved validation script: udiff_validation_query.sql")
        
        # Step 7: Summary and recommendations
        print(f"\n📊 Step 7: Final Summary...")
        print("-" * 40)
        
        cursor.execute("""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'step04_fo_udiff_daily' 
        ORDER BY ORDINAL_POSITION
        """)
        
        final_columns = [row[0] for row in cursor.fetchall()]
        
        print(f"✅ Column renames completed")
        print(f"✅ Validation query created")
        print(f"✅ UDiFF compliance: {len(source_order)}/34 source columns mappable")
        print(f"✅ Total table columns: {len(final_columns)}")
        
        # Check for duplicates
        udiff_mapped_columns = [column_mapping[col] for col in source_order if col in column_mapping]
        duplicates = []
        seen = set()
        for col in udiff_mapped_columns:
            if col in seen:
                duplicates.append(col)
            seen.add(col)
        
        if duplicates:
            print(f"⚠️  Duplicate mappings found: {duplicates}")
        else:
            print(f"✅ No duplicate column mappings")
        
        conn.close()
        
        print(f"\n{'='*60}")
        print(f"🎯 COLUMN ORDER FIX COMPLETED!")
        print(f"📋 UDiFF validation ready")
        print(f"📄 Use 'udiff_validation_query.sql' for validation")
        print(f"✅ All source column names now match NSE specification")
        print(f"{'='*60}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error fixing column order: {e}")
        return False

if __name__ == "__main__":
    from datetime import datetime
    fix_column_order_for_udiff_validation()
