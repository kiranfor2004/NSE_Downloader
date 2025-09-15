"""
Add category column to step03_compare_monthvspreviousmonth table
by matching symbols with index_symbol_masterdata table
"""

import pyodbc
import json
import sys

class CategoryUpdater:
    def __init__(self, config_file='database_config.json'):
        """Initialize database connection using config file"""
        try:
            with open(config_file, 'r') as f:
                self.config = json.load(f)
            self.connection = None
            self.connect()
        except FileNotFoundError:
            print(f"Error: {config_file} not found")
            sys.exit(1)
        except Exception as e:
            print(f"Error loading config: {e}")
            sys.exit(1)
    
    def connect(self):
        """Establish database connection"""
        try:
            # Build connection string for Windows Authentication
            conn_str = (
                f"DRIVER={{{self.config['driver']}}};"
                f"SERVER={self.config['server']};"
                f"DATABASE={self.config['database']};"
                f"Trusted_Connection=yes;"
            )
            
            self.connection = pyodbc.connect(conn_str)
            print(f"✅ Connected to database: {self.config['database']}")
            
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            sys.exit(1)
    
    def check_existing_columns(self):
        """Check if category column already exists"""
        try:
            cursor = self.connection.cursor()
            
            # Check current table structure
            cursor.execute("""
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'step03_compare_monthvspreviousmonth'
                AND COLUMN_NAME IN ('category', 'index_name')
                ORDER BY COLUMN_NAME
            """)
            
            existing_columns = cursor.fetchall()
            
            print("📋 Checking existing columns...")
            has_index_name = False
            has_category = False
            
            for col in existing_columns:
                if col[0] == 'index_name':
                    has_index_name = True
                    print("  ✅ index_name column exists")
                elif col[0] == 'category':
                    has_category = True
                    print("  ✅ category column exists")
            
            if not has_index_name:
                print("  ❌ index_name column not found")
            if not has_category:
                print("  ❌ category column not found")
            
            cursor.close()
            return has_index_name, has_category
            
        except Exception as e:
            print(f"❌ Error checking columns: {e}")
            return False, False
    
    def add_category_column(self):
        """Add category column to step03 table if it doesn't exist"""
        try:
            cursor = self.connection.cursor()
            
            print("🔧 Adding category column to step03_compare_monthvspreviousmonth table...")
            
            alter_sql = """
            ALTER TABLE step03_compare_monthvspreviousmonth 
            ADD category NVARCHAR(50) NULL
            """
            
            cursor.execute(alter_sql)
            self.connection.commit()
            print("✅ category column added successfully")
            cursor.close()
            return True
            
        except Exception as e:
            if "column name" in str(e).lower() and "already exists" in str(e).lower():
                print("ℹ️ category column already exists")
                return True
            else:
                print(f"❌ Error adding category column: {e}")
                return False
    
    def update_categories(self):
        """Update category column by matching symbols with master data"""
        try:
            cursor = self.connection.cursor()
            
            print("🔄 Updating category column...")
            
            # First, update all to "Other" as default where category is NULL
            cursor.execute("""
                UPDATE step03_compare_monthvspreviousmonth 
                SET category = 'Other' 
                WHERE category IS NULL
            """)
            
            print("✅ Set default 'Other' category for all records")
            
            # Update with actual categories for matching symbols
            # Cross-database update from NSE.index_symbol_masterdata to master.step03_compare_monthvspreviousmonth
            # For symbols that appear in multiple indices with different categories, we'll prioritize:
            # 1. Broad Market over Sectoral
            # 2. Alphabetically first if same priority
            update_sql = """
            UPDATE s3
            SET s3.category = ism.category
            FROM step03_compare_monthvspreviousmonth s3
            INNER JOIN (
                SELECT symbol, 
                       CASE 
                           WHEN MIN(CASE WHEN category = 'Broad Market' THEN 1 ELSE 2 END) = 1 
                           THEN 'Broad Market'
                           ELSE MIN(category)
                       END as category
                FROM NSE.dbo.index_symbol_masterdata
                GROUP BY symbol
            ) ism ON s3.symbol = ism.symbol
            """
            
            cursor.execute(update_sql)
            rows_updated = cursor.rowcount
            
            self.connection.commit()
            print(f"✅ Updated {rows_updated} records with matching categories")
            
            cursor.close()
            return True
            
        except Exception as e:
            print(f"❌ Error updating categories: {e}")
            return False
    
    def verify_category_updates(self):
        """Verify the category updates and show statistics"""
        try:
            cursor = self.connection.cursor()
            
            # Get total count
            cursor.execute("SELECT COUNT(*) FROM step03_compare_monthvspreviousmonth")
            total_count = cursor.fetchone()[0]
            
            # Count by category
            cursor.execute("""
                SELECT category, COUNT(*) as count
                FROM step03_compare_monthvspreviousmonth
                GROUP BY category
                ORDER BY count DESC
            """)
            
            category_counts = cursor.fetchall()
            
            # Count by index_name and category combination
            cursor.execute("""
                SELECT index_name, category, COUNT(*) as count
                FROM step03_compare_monthvspreviousmonth
                WHERE index_name != 'Other Index'
                GROUP BY index_name, category
                ORDER BY count DESC
            """)
            
            index_category_combinations = cursor.fetchall()
            
            # Get examples of categorized symbols
            cursor.execute("""
                SELECT TOP 10 
                    symbol, 
                    index_name,
                    category, 
                    comparison_type, 
                    delivery_increase_pct
                FROM step03_compare_monthvspreviousmonth
                WHERE category != 'Other'
                ORDER BY delivery_increase_pct DESC
            """)
            
            categorized_examples = cursor.fetchall()
            
            # Get examples by category
            cursor.execute("""
                SELECT 
                    category,
                    COUNT(*) as count,
                    AVG(delivery_increase_pct) as avg_increase,
                    MAX(delivery_increase_pct) as max_increase
                FROM step03_compare_monthvspreviousmonth
                WHERE category != 'Other'
                GROUP BY category
                ORDER BY count DESC
            """)
            
            category_stats = cursor.fetchall()
            
            print(f"\n📊 Category Update Verification:")
            print(f"Total Records: {total_count}")
            
            print(f"\n📈 Category Distribution:")
            for category, count in category_counts:
                percentage = (count / total_count) * 100
                print(f"  {category}: {count} records ({percentage:.1f}%)")
            
            if index_category_combinations:
                print(f"\n🔗 Index-Category Combinations:")
                for index_name, category, count in index_category_combinations:
                    print(f"  {index_name} ({category}): {count} records")
            
            if category_stats:
                print(f"\n📊 Category Performance Statistics:")
                print("-" * 80)
                print(f"{'Category':<15} {'Records':<10} {'Avg % Inc':<12} {'Max % Inc'}")
                print("-" * 80)
                for category, count, avg_inc, max_inc in category_stats:
                    print(f"{category:<15} {count:<10} {avg_inc:.2f}%{'':<6} {max_inc:.2f}%")
            
            if categorized_examples:
                print(f"\n✅ Examples of Categorized Symbols:")
                print("-" * 100)
                print(f"{'Symbol':<15} {'Index':<20} {'Category':<15} {'Comparison':<20} {'% Inc'}")
                print("-" * 100)
                for record in categorized_examples:
                    print(f"{record[0]:<15} {record[1]:<20} {record[2]:<15} {record[3]:<20} {record[4]:.2f}%")
            
            cursor.close()
            
        except Exception as e:
            print(f"❌ Error verifying category updates: {e}")
    
    def show_sample_queries(self):
        """Show sample queries that can be run with the new category column"""
        print(f"\n💡 Sample Queries with Category Data:")
        print("-" * 50)
        
        queries = [
            ("Broad Market vs Sectoral Performance", """
SELECT category, 
       COUNT(*) as records,
       AVG(delivery_increase_pct) as avg_increase,
       MAX(delivery_increase_pct) as max_increase
FROM step03_compare_monthvspreviousmonth 
WHERE category != 'Other'
GROUP BY category;
            """),
            ("Top Performing Broad Market Stocks", """
SELECT TOP 10 symbol, index_name, delivery_increase_pct, comparison_type
FROM step03_compare_monthvspreviousmonth 
WHERE category = 'Broad Market'
ORDER BY delivery_increase_pct DESC;
            """),
            ("Sectoral Analysis", """
SELECT index_name, category, COUNT(*) as exceedances
FROM step03_compare_monthvspreviousmonth 
WHERE category = 'Sectoral'
GROUP BY index_name, category
ORDER BY exceedances DESC;
            """)
        ]
        
        for title, query in queries:
            print(f"\n{title}:")
            print(query.strip())
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            print("\n🔐 Database connection closed")

def main():
    """Main function"""
    print("🚀 Add Category to Monthly Comparison Table")
    print("=" * 50)
    
    # Initialize updater
    updater = CategoryUpdater()
    
    try:
        # Check existing columns
        has_index_name, has_category = updater.check_existing_columns()
        
        if not has_index_name:
            print("\n❌ index_name column not found. Please run add_index_names_to_monthly_comparison.py first.")
            return
        
        # Add category column if it doesn't exist
        if not has_category:
            if not updater.add_category_column():
                return
        
        # Update categories
        if updater.update_categories():
            print("\n🎉 Category updates completed successfully!")
            
            # Verify updates
            updater.verify_category_updates()
            
            # Show sample queries
            updater.show_sample_queries()
        else:
            print("\n❌ Category updates failed!")
    
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
    
    finally:
        updater.close()

if __name__ == "__main__":
    main()