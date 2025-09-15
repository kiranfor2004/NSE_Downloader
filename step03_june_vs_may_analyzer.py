"""
Step 3: Compare June 2025 daily delivery to May 2025 peak delivery
This script identifies delivery volume exceedances where June daily delivery exceeds May peak delivery
"""

import pandas as pd
from datetime import datetime, date
from nse_database_integration import NSEDatabaseManager

def get_may_2025_baselines(cursor):
    """Get May 2025 peak delivery and volume for each symbol"""
    print("Calculating May 2025 baselines...")
    
    query = """
    SELECT 
        SYMBOL,
        MAX(DELIVERY_VOLUME) as max_delivery_volume,
        MAX(VOLUME) as max_volume
    FROM nse_fo_udiff 
    WHERE DATE >= '2025-05-01' AND DATE <= '2025-05-31'
    GROUP BY SYMBOL
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    baselines = {}
    for row in results:
        symbol = row[0]
        max_delivery = row[1] if row[1] is not None else 0
        max_volume = row[2] if row[2] is not None else 0
        baselines[symbol] = {
            'max_delivery_volume': max_delivery,
            'max_volume': max_volume
        }
    
    print(f"May 2025 baselines calculated for {len(baselines)} symbols")
    return baselines

def get_june_2025_data(cursor):
    """Get June 2025 daily data"""
    print("Fetching June 2025 daily data...")
    
    query = """
    SELECT 
        DATE,
        SYMBOL,
        DELIVERY_VOLUME,
        VOLUME
    FROM nse_fo_udiff 
    WHERE DATE >= '2025-06-01' AND DATE <= '2025-06-30'
    ORDER BY DATE, SYMBOL
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    june_data = []
    for row in results:
        june_data.append({
            'date': row[0],
            'symbol': row[1],
            'delivery_volume': row[2] if row[2] is not None else 0,
            'volume': row[3] if row[3] is not None else 0
        })
    
    print(f"June 2025 data fetched: {len(june_data)} records")
    return june_data

def analyze_exceedances(may_baselines, june_data):
    """Find records where June delivery/volume exceeds May peaks"""
    print("Analyzing delivery and volume exceedances...")
    
    exceedances = []
    delivery_exceedances = 0
    volume_exceedances = 0
    both_exceeded = 0
    
    for record in june_data:
        symbol = record['symbol']
        
        if symbol not in may_baselines:
            continue
            
        may_max_delivery = may_baselines[symbol]['max_delivery_volume']
        may_max_volume = may_baselines[symbol]['max_volume']
        
        june_delivery = record['delivery_volume']
        june_volume = record['volume']
        
        delivery_exceeds = june_delivery > may_max_delivery if may_max_delivery > 0 else False
        volume_exceeds = june_volume > may_max_volume if may_max_volume > 0 else False
        
        if delivery_exceeds or volume_exceeds:
            # Calculate percentage increases
            delivery_increase_pct = ((june_delivery - may_max_delivery) / may_max_delivery * 100) if may_max_delivery > 0 else 0
            volume_increase_pct = ((june_volume - may_max_volume) / may_max_volume * 100) if may_max_volume > 0 else 0
            
            exceedance = {
                'comparison_type': 'JUN_VS_MAY_2025',
                'current_date': record['date'],
                'symbol': symbol,
                'current_delivery_volume': june_delivery,
                'current_volume': june_volume,
                'previous_max_delivery_volume': may_max_delivery,
                'previous_max_volume': may_max_volume,
                'delivery_exceeds': delivery_exceeds,
                'volume_exceeds': volume_exceeds,
                'delivery_increase_percentage': delivery_increase_pct,
                'volume_increase_percentage': volume_increase_pct
            }
            
            exceedances.append(exceedance)
            
            if delivery_exceeds:
                delivery_exceedances += 1
            if volume_exceeds:
                volume_exceedances += 1
            if delivery_exceeds and volume_exceeds:
                both_exceeded += 1
    
    print(f"Analysis complete:")
    print(f"  Total exceedance records: {len(exceedances)}")
    print(f"  Delivery exceedances: {delivery_exceedances}")
    print(f"  Volume exceedances: {volume_exceedances}")
    print(f"  Both exceeded: {both_exceeded}")
    
    return exceedances

def insert_exceedances(cursor, exceedances):
    """Insert exceedance records into step03_compare_monthvspreviousmonth table"""
    if not exceedances:
        print("No exceedances to insert")
        return
    
    print(f"Inserting {len(exceedances)} exceedance records...")
    
    insert_query = """
    INSERT INTO step03_compare_monthvspreviousmonth (
        comparison_type,
        current_date,
        symbol,
        current_delivery_volume,
        current_volume,
        previous_max_delivery_volume,
        previous_max_volume,
        delivery_exceeds,
        volume_exceeds,
        delivery_increase_percentage,
        volume_increase_percentage
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    batch_size = 1000
    inserted_count = 0
    
    for i in range(0, len(exceedances), batch_size):
        batch = exceedances[i:i + batch_size]
        
        values = []
        for exc in batch:
            values.append((
                exc['comparison_type'],
                exc['current_date'],
                exc['symbol'],
                exc['current_delivery_volume'],
                exc['current_volume'],
                exc['previous_max_delivery_volume'],
                exc['previous_max_volume'],
                exc['delivery_exceeds'],
                exc['volume_exceeds'],
                exc['delivery_increase_percentage'],
                exc['volume_increase_percentage']
            ))
        
        cursor.executemany(insert_query, values)
        cursor.commit()
        inserted_count += len(batch)
        print(f"  Inserted {inserted_count}/{len(exceedances)} records")
    
    print("All exceedance records inserted successfully")

def show_top_performers(exceedances):
    """Show top 10 delivery increase performers"""
    if not exceedances:
        return
    
    print("\nTop 10 Delivery Increase Performers (June vs May 2025):")
    print("-" * 80)
    
    # Sort by delivery increase percentage
    delivery_sorted = sorted(exceedances, key=lambda x: x['delivery_increase_percentage'], reverse=True)
    
    for i, exc in enumerate(delivery_sorted[:10]):
        print(f"{i+1:2}. {exc['symbol']:15} {exc['delivery_increase_percentage']:>12.1f}% "
              f"({exc['current_delivery_volume']:>8} vs {exc['previous_max_delivery_volume']:>8})")

def main():
    """Main execution function"""
    print("=" * 60)
    print("NSE FO June 2025 vs May 2025 Delivery Analysis")
    print("=" * 60)
    
    try:
        # Initialize database manager
        print("Connecting to database...")
        db_manager = NSEDatabaseManager()
        
        # Get May 2025 baselines
        may_baselines = get_may_2025_baselines(db_manager.cursor)
        
        # Get June 2025 data
        june_data = get_june_2025_data(db_manager.cursor)
        
        # Analyze exceedances
        exceedances = analyze_exceedances(may_baselines, june_data)
        
        # Insert results
        insert_exceedances(db_manager.cursor, exceedances)
        
        # Show top performers
        show_top_performers(exceedances)
        
        print("\nJune vs May 2025 analysis completed successfully!")
        print(f"Results stored in step03_compare_monthvspreviousmonth table")
        
    except Exception as e:
        print(f"Error during analysis: {str(e)}")
        raise
    finally:
        if 'db_manager' in locals():
            db_manager.close()

if __name__ == "__main__":
    main()