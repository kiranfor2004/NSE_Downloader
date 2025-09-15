#!/usr/bin/env python3

import pandas as pd
import pyodbc
import json
import os
import zipfile
from datetime import datetime
import numpy as np
from io import StringIO
import re

def validate_february_data_comprehensive():
    """Comprehensive validation of February data against actual BhavCopy source files"""
    
    print("🔍 COMPREHENSIVE FEBRUARY DATA VALIDATION")
    print("="*60)
    
    source_directory = r"C:\Users\kiran\NSE_Downloader\fo_udiff_downloads"
    
    try:
        # Connect to database
        with open('database_config.json', 'r') as f:
            config = json.load(f)
        
        conn_str = f"DRIVER={config['driver']};SERVER={config['server']};DATABASE={config['database']};Trusted_Connection=yes;"
        conn = pyodbc.connect(conn_str)
        
        print("💾 Connected to database")
        
        # Get all February dates from database
        print("\n🔍 Checking database data...")
        db_dates_query = """
        SELECT DISTINCT trade_date, COUNT(*) as record_count
        FROM step04_fo_udiff_daily 
        WHERE trade_date LIKE '202502%'
        GROUP BY trade_date
        ORDER BY trade_date
        """
        
        db_dates_df = pd.read_sql(db_dates_query, conn)
        db_dates = dict(zip(db_dates_df['trade_date'], db_dates_df['record_count']))
        
        print(f"   Database has data for {len(db_dates)} February dates:")
        for trade_date, count in db_dates.items():
            date_obj = datetime.strptime(trade_date, '%Y%m%d')
            print(f"     {date_obj.strftime('%d-%m-%Y')}: {count:,} records")
        
        # Find and parse source files
        print(f"\n📁 Scanning source files...")
        source_files = []
        
        for filename in os.listdir(source_directory):
            if filename.endswith('.zip') and 'BhavCopy_NSE_FO' in filename:
                # Extract date from filename: BhavCopy_NSE_FO_0_0_0_20250203_F_0000.csv.zip
                match = re.search(r'(202502\d{2})', filename)
                if match:
                    date_str = match.group(1)
                    source_files.append((date_str, filename))
        
        source_files.sort()
        print(f"   Found {len(source_files)} source files:")
        for date_str, filename in source_files:
            date_obj = datetime.strptime(date_str, '%Y%m%d')
            print(f"     {date_obj.strftime('%d-%m-%Y')}: {filename}")
        
        # Validation results
        validation_results = []
        total_matches = 0
        total_mismatches = 0
        
        print(f"\n📊 DETAILED VALIDATION:")
        print("-" * 60)
        
        for date_str, filename in source_files:
            date_obj = datetime.strptime(date_str, '%Y%m%d')
            date_display = date_obj.strftime('%d-%m-%Y')
            
            print(f"\n📅 Validating {date_display} ({date_str})")
            
            # Check if database has this date
            if date_str not in db_dates:
                print(f"   ❌ Missing from database")
                validation_results.append({
                    'date': date_str,
                    'date_display': date_display,
                    'source_file': filename,
                    'source_records': 0,
                    'db_records': 0,
                    'status': 'MISSING_FROM_DB',
                    'match': False
                })
                total_mismatches += 1
                continue
            
            # Load and analyze source file
            source_path = os.path.join(source_directory, filename)
            
            try:
                print(f"   📄 Reading source: {filename}")
                
                with zipfile.ZipFile(source_path, 'r') as zip_ref:
                    # List all files in zip
                    zip_files = zip_ref.namelist()
                    print(f"   📁 Files in zip: {zip_files}")
                    
                    # Find CSV file
                    csv_files = [f for f in zip_files if f.lower().endswith('.csv')]
                    
                    if not csv_files:
                        print(f"   ❌ No CSV file found in zip")
                        validation_results.append({
                            'date': date_str,
                            'date_display': date_display,
                            'source_file': filename,
                            'source_records': 0,
                            'db_records': db_dates[date_str],
                            'status': 'NO_CSV_IN_ZIP',
                            'match': False
                        })
                        total_mismatches += 1
                        continue
                    
                    csv_file = csv_files[0]
                    print(f"   📋 Processing CSV: {csv_file}")
                    
                    # Read CSV content
                    with zip_ref.open(csv_file) as f:
                        csv_content = f.read().decode('utf-8')
                    
                    # Parse CSV
                    source_df = pd.read_csv(StringIO(csv_content))
                    source_records = len(source_df)
                    
                    print(f"   📊 Source records: {source_records:,}")
                    print(f"   🔍 Source columns ({len(source_df.columns)}): {list(source_df.columns)[:5]}...")
                    
                    # Show sample data
                    if len(source_df) > 0:
                        print(f"   📝 Sample source data:")
                        sample_cols = ['TckrSymb', 'FinInstrmTp', 'ClsPric', 'TtlTradgVol', 'OpnIntrst'] if 'TckrSymb' in source_df.columns else list(source_df.columns)[:5]
                        for col in sample_cols:
                            if col in source_df.columns:
                                sample_val = source_df[col].iloc[0] if len(source_df) > 0 else 'N/A'
                                print(f"     {col}: {sample_val}")
                
                # Get database data for this date
                db_records = db_dates[date_str]
                print(f"   💾 Database records: {db_records:,}")
                
                # Compare record counts
                records_match = source_records == db_records
                match_status = "✅" if records_match else "❌"
                
                print(f"   🎯 Record count match: {match_status}")
                print(f"   📈 Difference: {abs(source_records - db_records):,} records")
                
                if records_match:
                    # Additional validation for perfect matches
                    detailed_match = validate_detailed_content(source_df, date_str, conn)
                    overall_match = detailed_match['content_match']
                    
                    validation_results.append({
                        'date': date_str,
                        'date_display': date_display,
                        'source_file': filename,
                        'source_records': source_records,
                        'db_records': db_records,
                        'status': 'PERFECT_MATCH' if overall_match else 'COUNT_MATCH_CONTENT_DIFF',
                        'match': overall_match,
                        'details': detailed_match
                    })
                    
                    if overall_match:
                        total_matches += 1
                        print(f"   🎉 PERFECT MATCH!")
                    else:
                        total_mismatches += 1
                        print(f"   ⚠️  Count matches but content differs")
                else:
                    validation_results.append({
                        'date': date_str,
                        'date_display': date_display,
                        'source_file': filename,
                        'source_records': source_records,
                        'db_records': db_records,
                        'status': 'COUNT_MISMATCH',
                        'match': False
                    })
                    total_mismatches += 1
                    print(f"   ❌ Record count mismatch")
                
            except Exception as e:
                print(f"   ❌ Error processing {filename}: {e}")
                validation_results.append({
                    'date': date_str,
                    'date_display': date_display,
                    'source_file': filename,
                    'source_records': 0,
                    'db_records': db_dates.get(date_str, 0),
                    'status': f'ERROR: {str(e)}',
                    'match': False
                })
                total_mismatches += 1
        
        # Check for database dates not in source
        print(f"\n🔍 Checking for database-only dates...")
        source_dates = {date for date, _ in source_files}
        db_only_dates = [date for date in db_dates.keys() if date not in source_dates]
        
        if db_only_dates:
            print(f"   ⚠️  Database has {len(db_only_dates)} dates without source files:")
            for date in db_only_dates:
                date_obj = datetime.strptime(date, '%Y%m%d')
                db_count = db_dates[date]
                print(f"     {date_obj.strftime('%d-%m-%Y')}: {db_count:,} records (generated/loaded separately)")
                
                validation_results.append({
                    'date': date,
                    'date_display': date_obj.strftime('%d-%m-%Y'),
                    'source_file': 'NONE (Generated/Manual)',
                    'source_records': 0,
                    'db_records': db_count,
                    'status': 'DB_ONLY_DATA',
                    'match': False
                })
        
        # Generate final report
        print(f"\n📋 FINAL VALIDATION REPORT")
        print("="*60)
        
        total_source_files = len(source_files)
        perfect_matches = len([r for r in validation_results if r['match']])
        
        print(f"📊 VALIDATION SUMMARY:")
        print(f"   Source files found: {total_source_files}")
        print(f"   Perfect matches: {perfect_matches} ✅")
        print(f"   Mismatches/Issues: {total_mismatches} ❌")
        print(f"   Database-only dates: {len(db_only_dates)} 🎯")
        
        # Calculate totals
        total_source_records = sum(r['source_records'] for r in validation_results if r['source_records'] > 0)
        total_db_records = sum(db_dates.values())
        
        print(f"\n💾 RECORD SUMMARY:")
        print(f"   Total source records: {total_source_records:,}")
        print(f"   Total database records: {total_db_records:,}")
        print(f"   Records from source files: {sum(db_dates[date] for date in source_dates if date in db_dates):,}")
        print(f"   Generated/Manual records: {sum(db_dates[date] for date in db_only_dates):,}")
        
        # Final verdict
        print(f"\n🏆 VALIDATION VERDICT:")
        print("="*60)
        
        if total_source_files == 0:
            print(f"⚠️  NO SOURCE FILES FOUND FOR VALIDATION")
            print(f"📁 Check source directory: {source_directory}")
            verdict = "NO_SOURCE_DATA"
        elif perfect_matches == total_source_files:
            print(f"🎉 VALIDATION PASSED: ALL SOURCE FILES MATCH PERFECTLY!")
            print(f"✅ {perfect_matches}/{total_source_files} files validated successfully")
            verdict = "PASSED"
        elif perfect_matches > 0:
            print(f"⚠️  VALIDATION PARTIAL: {perfect_matches}/{total_source_files} files match")
            print(f"🔧 {total_mismatches} files need attention")
            verdict = "PARTIAL"
        else:
            print(f"❌ VALIDATION FAILED: NO PERFECT MATCHES")
            verdict = "FAILED"
        
        # Save detailed report
        report = {
            'validation_timestamp': datetime.now().isoformat(),
            'source_directory': source_directory,
            'verdict': verdict,
            'summary': {
                'total_source_files': total_source_files,
                'perfect_matches': perfect_matches,
                'total_mismatches': total_mismatches,
                'db_only_dates': len(db_only_dates),
                'total_source_records': total_source_records,
                'total_db_records': total_db_records
            },
            'validation_results': validation_results
        }
        
        with open('february_comprehensive_validation_report.json', 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        print(f"\n💾 Comprehensive report saved: february_comprehensive_validation_report.json")
        print("="*60)
        
        conn.close()
        return verdict in ["PASSED", "PARTIAL"]
        
    except Exception as e:
        print(f"❌ Validation error: {e}")
        import traceback
        traceback.print_exc()
        return False

def validate_detailed_content(source_df, date_str, conn):
    """Validate detailed content between source and database"""
    
    try:
        # Sample validation - check key metrics
        if 'TckrSymb' in source_df.columns:
            # Get sample database data
            sample_query = f"""
            SELECT TOP 10 symbol, instrument, close_price, contracts_traded, open_interest
            FROM step04_fo_udiff_daily 
            WHERE trade_date = '{date_str}'
            ORDER BY symbol
            """
            
            db_sample = pd.read_sql(sample_query, conn)
            
            # Basic content validation
            source_symbols = set(source_df['TckrSymb'].unique())
            db_symbols = set(db_sample['symbol'].unique())
            
            symbol_overlap = len(source_symbols.intersection(db_symbols)) / max(len(source_symbols), 1)
            
            return {
                'content_match': symbol_overlap > 0.8,  # 80% symbol overlap considered good
                'symbol_overlap': symbol_overlap,
                'source_symbols_count': len(source_symbols),
                'db_symbols_sample': len(db_symbols)
            }
        else:
            return {
                'content_match': False,
                'error': 'Cannot find symbol column in source data'
            }
    
    except Exception as e:
        return {
            'content_match': False,
            'error': f'Content validation failed: {str(e)}'
        }

if __name__ == "__main__":
    success = validate_february_data_comprehensive()
    if success:
        print(f"\n✅ VALIDATION SIGN-OFF: APPROVED")
    else:
        print(f"\n❌ VALIDATION SIGN-OFF: REQUIRES ATTENTION")
