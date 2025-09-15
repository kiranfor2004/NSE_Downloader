import pyodbc

# Connect to database
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=SRIKIRANREDDY\\SQLEXPRESS;'
    'DATABASE=master;'
    'Trusted_Connection=yes;'
)
cursor = conn.cursor()

# Check sample data with non-null values
print("🔍 CHECKING F&O DATA QUALITY")
print("=" * 50)

cursor.execute("""
    SELECT TOP 5 
        symbol, instrument, open_price, high_price, low_price, 
        close_price, settle_price, contracts_traded, value_in_lakh, open_interest 
    FROM step04_fo_udiff_daily 
    WHERE open_price IS NOT NULL 
    ORDER BY trade_date
""")

rows = cursor.fetchall()
print("✅ Sample records with populated values:")
for row in rows:
    print(f"  {row[0]:<12} | {row[1]:<6} | Open: {row[2]:<8} | High: {row[3]:<8} | Close: {row[5]:<8} | Settle: {row[6]:<8} | Contracts: {row[7]:<8} | OI: {row[9]}")

# Check for NULL values
cursor.execute("""
    SELECT 
        COUNT(*) as total_records,
        SUM(CASE WHEN open_price IS NULL THEN 1 ELSE 0 END) as null_open_price,
        SUM(CASE WHEN high_price IS NULL THEN 1 ELSE 0 END) as null_high_price,
        SUM(CASE WHEN close_price IS NULL THEN 1 ELSE 0 END) as null_close_price,
        SUM(CASE WHEN settle_price IS NULL THEN 1 ELSE 0 END) as null_settle_price,
        SUM(CASE WHEN contracts_traded IS NULL THEN 1 ELSE 0 END) as null_contracts,
        SUM(CASE WHEN open_interest IS NULL THEN 1 ELSE 0 END) as null_open_interest
    FROM step04_fo_udiff_daily
""")

stats = cursor.fetchone()
print(f"\n📊 NULL VALUE STATISTICS:")
print(f"  Total Records: {stats[0]:,}")
print(f"  NULL Open Price: {stats[1]:,} ({stats[1]/stats[0]*100:.1f}%)")
print(f"  NULL High Price: {stats[2]:,} ({stats[2]/stats[0]*100:.1f}%)")
print(f"  NULL Close Price: {stats[3]:,} ({stats[3]/stats[0]*100:.1f}%)")
print(f"  NULL Settle Price: {stats[4]:,} ({stats[4]/stats[0]*100:.1f}%)")
print(f"  NULL Contracts: {stats[5]:,} ({stats[5]/stats[0]*100:.1f}%)")
print(f"  NULL Open Interest: {stats[6]:,} ({stats[6]/stats[0]*100:.1f}%)")

conn.close()
