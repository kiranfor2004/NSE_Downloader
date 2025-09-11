import pandas as pd

print("🔄 FORMAT COMPARISON - Horizontal vs Vertical:")
print("=" * 65)

# Load both formats
horizontal_df = pd.read_excel('Horizontal_July_August_Comparison_20250911_073320.xlsx', sheet_name='Volume_Horizontal')
vertical_df = pd.read_excel('Vertical_July_August_Comparison_20250911_080244.xlsx', sheet_name='Volume_Vertical')

# Show IDEA in both formats
print("📊 IDEA Example - Same data, different presentation:")
print()

# Horizontal format (cramped)
idea_h = horizontal_df[horizontal_df['SYMBOL'] == 'IDEA'].iloc[0]
print("❌ HORIZONTAL FORMAT (cramped, hard to read):")
print(f"SYMBOL: {idea_h['SYMBOL']}")
print(f"JULY_TTL_TRD_QNTY: {idea_h['JULY_TTL_TRD_QNTY']}")
print(f"AUGUST_WIN_COUNT: {idea_h['AUGUST_WIN_COUNT']}")
print(f"AUG_DATE_1: {idea_h['AUG_DATE_1']} | AUG_VOLUME_1: {idea_h['AUG_VOLUME_1']} | AUG_PERCENT_1: {idea_h['AUG_PERCENT_1']}")
print(f"AUG_DATE_2: {idea_h['AUG_DATE_2']} | AUG_VOLUME_2: {idea_h['AUG_VOLUME_2']} | AUG_PERCENT_2: {idea_h['AUG_PERCENT_2']}")
print(f"AUG_DATE_3: {idea_h['AUG_DATE_3']} | AUG_VOLUME_3: {idea_h['AUG_VOLUME_3']} | AUG_PERCENT_3: {idea_h['AUG_PERCENT_3']}")
print()

# Vertical format (readable)
idea_v = vertical_df[vertical_df['SYMBOL'] == 'IDEA']
print("✅ VERTICAL FORMAT (clean, easy to read):")
print("Each August win as separate row:")
for i, (_, row) in enumerate(idea_v.iterrows(), 1):
    print(f"  Row {i}: {row['SYMBOL']} | {row['AUGUST_DATE']} | {row['COMPARISON']}")

print()
print("🎯 ADVANTAGES OF VERTICAL FORMAT:")
print("✅ Each August date is a separate row")
print("✅ Easy to scan and understand")  
print("✅ Perfect for Excel filtering (filter by date, symbol, percentage)")
print("✅ Clear comparison column shows the improvement")
print("✅ No cramped horizontal scrolling")
print("✅ Natural reading flow (top to bottom)")

print(f"\n📊 SUMMARY:")
print(f"• Horizontal format: {len(horizontal_df)} symbols, up to 68 columns (cramped)")
print(f"• Vertical format: {len(vertical_df)} rows, 9 clear columns (readable)")
print(f"• Same data, much better presentation!")
