import pandas as pd

# Load horizontal format
df = pd.read_excel('Horizontal_July_August_Comparison_20250911_073320.xlsx', sheet_name='Volume_Horizontal')

print('🎯 FINAL HORIZONTAL FORMAT - CORRECTED:')
print('=' * 50)
print('✅ Each August date = separate column')
print('✅ Exact August volume (not just maximum)')
print('✅ Easy Excel filtering and sorting')  
print('✅ Percentage increases in separate columns')
print()

print('📊 IDEA Example (corrected dates):')
idea = df[df['SYMBOL'] == 'IDEA'].iloc[0]
print(f'Symbol: {idea["SYMBOL"]}')
print(f'July Volume: {idea["JULY_TTL_TRD_QNTY"]} on {idea["JULY_DATE"]}')
print(f'August Wins: {idea["AUGUST_WIN_COUNT"]} dates')
print(f'Aug Date 1: {idea["AUG_DATE_1"]} = {idea["AUG_VOLUME_1"]} ({idea["AUG_PERCENT_1"]} increase)')
print(f'Aug Date 2: {idea["AUG_DATE_2"]} = {idea["AUG_VOLUME_2"]} ({idea["AUG_PERCENT_2"]} increase)')
print(f'Aug Date 3: {idea["AUG_DATE_3"]} = {idea["AUG_VOLUME_3"]} ({idea["AUG_PERCENT_3"]} increase)')

print()
print('🔄 FORMAT COMPARISON:')
print('❌ OLD: IDEA | 939,070,587 | 14-Jul-2025 | 1,659,929,445 | 22-08-2025, 25-08-2025, 26-08-2025')
print('✅ NEW: Each date in separate columns with exact volumes and percentages')
