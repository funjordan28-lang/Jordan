#!/usr/bin/env python3
"""
Fetch All Population Data - Simple Sequential Version
Extracts all unique universalGemRateId values from sales table
and fetches population data for each one from GemRate API
"""

import requests
import json
from utils import SQLDBHandler
import time

# Import headers from config
try:
    from config import API_HEADERS as headers
except ImportError:
    print("⚠️  Warning: Could not import API_HEADERS from config.py")
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

class PopulationFetcher:
    def __init__(self):
        """Initialize the population fetcher"""
        try:
            from config import MYSQL_CONFIG
            self.sql_handler = SQLDBHandler(**MYSQL_CONFIG)
        except ImportError:
            print("📋 No config.py found, using default settings")
            self.sql_handler = SQLDBHandler()
        
        self.base_url = "https://www.gemrate.com/card-details?gemrate_id="
        self.stats = {
            'total_found': 0,
            'processed': 0,
            'successful': 0,
            'errors': 0,
            'already_exists': 0
        }
    
    def create_population_table(self):
        """Create the population table if it doesn't exist"""
        connection = self.sql_handler._get_connection()
        if not connection:
            print("❌ Failed to get database connection")
            return False
        
        try:
            cursor = connection.cursor()
            
            # Drop table if it exists to ensure correct schema
            drop_query = "DROP TABLE IF EXISTS population"
            cursor.execute(drop_query)
            print("🗑️  Dropped existing population table")
            
            create_table_query = """
            CREATE TABLE population (
                id INT AUTO_INCREMENT PRIMARY KEY,
                search_query TEXT,
                gemrate_id VARCHAR(255) NOT NULL,
                grading_company VARCHAR(50) NOT NULL,
                grade_type VARCHAR(20) NOT NULL,
                grade_value DECIMAL(3,1),
                grade_count INT NOT NULL,
                card_description TEXT,
                card_name VARCHAR(255),
                card_number VARCHAR(50),
                card_year VARCHAR(10),
                set_name VARCHAR(255),
                parallel VARCHAR(255),
                category VARCHAR(100),
                card_gem_rate DECIMAL(10,6),
                card_gems INT,
                card_total_grades INT,
                last_population_change DATE,
                date_fetched DATE DEFAULT (CURRENT_DATE),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_search_query (search_query(100)),
                INDEX idx_gemrate_id (gemrate_id),
                INDEX idx_grading_company (grading_company),
                INDEX idx_grade_value (grade_value),
                INDEX idx_card_name (card_name),
                INDEX idx_card_year (card_year),
                UNIQUE KEY unique_grade (gemrate_id, grading_company, grade_type, grade_value)
            )
            """
            cursor.execute(create_table_query)
            connection.commit()
            cursor.close()
            print("✅ Population table created successfully with correct schema")
            return True
        except Exception as e:
            print(f"❌ Error creating population table: {str(e)}")
            return False
        finally:
            self.sql_handler._return_connection(connection)
    
    def get_unique_gemrate_ids_with_queries(self):
        """
        Get all unique universalGemRateId values from the sales table with their search queries
        
        Returns:
            List of tuples (gemrate_id, search_query)
        """
        connection = self.sql_handler._get_connection()
        if not connection:
            print("❌ Failed to get database connection")
            return []
        
        try:
            cursor = connection.cursor()
            query = """
            SELECT DISTINCT universalGemRateId, search_query
            FROM sales 
            WHERE universalGemRateId IS NOT NULL 
            AND universalGemRateId != '' 
            AND universalGemRateId != 'null'
            ORDER BY universalGemRateId
            """
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            
            # Return list of tuples (gemrate_id, search_query)
            gemrate_data = [(row[0], row[1]) for row in results if row[0]]
            return gemrate_data
            
        except Exception as e:
            print(f"❌ Error getting unique gemrate IDs with queries: {str(e)}")
            return []
        finally:
            self.sql_handler._return_connection(connection)
    
    def check_gemrate_exists(self, gemrate_id):
        """
        Check if a gemrate_id already exists in the population table
        
        Returns:
            bool: True if exists, False otherwise
        """
        connection = self.sql_handler._get_connection()
        if not connection:
            return False
        
        try:
            cursor = connection.cursor()
            query = "SELECT COUNT(*) FROM population WHERE gemrate_id = %s"
            cursor.execute(query, (gemrate_id,))
            result = cursor.fetchone()
            cursor.close()
            exists = result[0] > 0 if result else False
            return exists
        except Exception as e:
            print(f"❌ Error checking if gemrate exists: {str(e)}")
            return False
        finally:
            self.sql_handler._return_connection(connection)
    
    def fetch_population_data(self, gemrate_id):
        """
        Fetch population data for a specific gemrate_id
        
        Args:
            gemrate_id: The gemrate ID to fetch
            
        Returns:
            dict: Population data or None if failed
        """
        url = f"{self.base_url}{gemrate_id}"
        
        try:
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                return data
            else:
                with self.print_lock:
                    print(f"❌ HTTP {response.status_code} for gemrate_id: {gemrate_id}")
                return None
                
        except requests.exceptions.Timeout:
            with self.print_lock:
                print(f"❌ Timeout for gemrate_id: {gemrate_id}")
            return None
        except requests.exceptions.RequestException as e:
            with self.print_lock:
                print(f"❌ Request error for gemrate_id {gemrate_id}: {str(e)}")
            return None
        except Exception as e:
            with self.print_lock:
                print(f"❌ Error fetching gemrate_id {gemrate_id}: {str(e)}")
            return None
    
    def parse_grade_value(self, grade_key, grading_company):
        """
        Parse grade value from grade key
        
        Args:
            grade_key: The grade key (e.g., 'g8', 'g9_5', 'g10')
            grading_company: The grading company (psa, beckett, sgc)
            
        Returns:
            tuple: (grade_type, grade_value)
        """
        # Special grades mapping
        special_grades = {
            'auth': ('auth', None),
            'g10': ('gem_mint', 10.0 if grading_company == 'psa' else 9.5),
            'g10b': ('gem_mint', 9.5),  # BGS Black Label
            'g10p': ('pristine', 10.0),  # BGS Pristine
            'perfect': ('perfect', 10.0),  # SGC Perfect
            'gA': ('auth', None)  # SGC Authentic
        }
        
        if grade_key in special_grades:
            return special_grades[grade_key]
        
        # Handle numeric grades
        if grade_key.startswith('g'):
            grade_part = grade_key[1:]  # Remove 'g' prefix
            
            if '_' in grade_part:
                # Handle half grades like g8_5 -> 8.5
                parts = grade_part.split('_')
                if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
                    grade_value = float(f"{parts[0]}.{parts[1]}")
                    return ('numeric', grade_value)
            elif grade_part.isdigit():
                # Handle whole grades like g8 -> 8.0
                grade_value = float(grade_part)
                return ('numeric', grade_value)
        
        return ('unknown', None)
    
    def process_population_data(self, data, search_query):
        """
        Process population data and extract records for database insertion
        
        Args:
            data: Population data from API
            search_query: Search query associated with this gemrate_id
            
        Returns:
            List of records to insert
        """
        if not data or 'population_data' not in data:
            return []
        
        records = []
        gemrate_id = data.get('gemrate_id', '')
        description = data.get('description', '')
        
        for pop_data in data['population_data']:
            grading_company = pop_data.get('grader', '')
            
            # Map grading company names
            company_mapping = {
                'psa': 'PSA',
                'beckett': 'BGS', 
                'sgc': 'SGC'
            }
            grading_company = company_mapping.get(grading_company.lower(), grading_company.upper())
            
            # Process all grades
            grades = pop_data.get('grades', {})
            
            for grade_key, count in grades.items():
                if count > 0:  # Only include grades with count > 0
                    grade_type, grade_value = self.parse_grade_value(grade_key, grading_company.lower())
                    
                    record = {
                        'search_query': search_query,
                        'gemrate_id': gemrate_id,
                        'grading_company': grading_company,
                        'grade_type': grade_type,
                        'grade_value': grade_value,
                        'grade_count': count,
                        'card_description': description,
                        'card_name': pop_data.get('name', ''),
                        'card_number': pop_data.get('card_number', ''),
                        'card_year': pop_data.get('year', ''),
                        'set_name': pop_data.get('set_name', ''),
                        'parallel': pop_data.get('parallel', ''),
                        'category': pop_data.get('category', ''),
                        'card_gem_rate': float(pop_data.get('card_gem_rate', 0)),
                        'card_gems': pop_data.get('card_gems', 0),
                        'card_total_grades': pop_data.get('card_total_grades', 0),
                        'last_population_change': pop_data.get('last_population_change')
                    }
                    records.append(record)
        
        return records
    
    def save_population_records(self, records):
        """
        Save population records to database
        
        Args:
            records: List of population records
            
        Returns:
            dict: Save results
        """
        if not records:
            return {"inserted": 0, "duplicates": 0, "errors": 0}
        
        connection = self.sql_handler._get_connection()
        if not connection:
            return {"inserted": 0, "duplicates": 0, "errors": len(records)}
        
        inserted_count = 0
        duplicate_count = 0
        error_count = 0
        
        try:
            cursor = connection.cursor(buffered=True)
            
            for record in records:
                try:
                    columns = ', '.join(record.keys())
                    placeholders = ', '.join(['%s'] * len(record))
                    
                    insert_query = f"""
                    INSERT IGNORE INTO population ({columns}) 
                    VALUES ({placeholders})
                    """
                    
                    cursor.execute(insert_query, list(record.values()))
                    
                    if cursor.rowcount > 0:
                        inserted_count += 1
                    else:
                        duplicate_count += 1
                        
                except Exception as e:
                    error_count += 1
                    print(f"❌ Error inserting record: {str(e)}")
            
            connection.commit()
            cursor.close()
            
        except Exception as e:
            print(f"❌ Error in batch save: {str(e)}")
            error_count = len(records)
        finally:
            self.sql_handler._return_connection(connection)
        
        return {
            "inserted": inserted_count,
            "duplicates": duplicate_count,
            "errors": error_count
        }
    
    def process_single_gemrate_with_query(self, gemrate_id, search_query):
        """
        Process a single gemrate ID with search query
        
        Args:
            gemrate_id: The gemrate ID to process
            search_query: The search query associated with this gemrate_id
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Fetch data
            data = self.fetch_population_data(gemrate_id)
            if not data:
                return False
            
            # Process data with search query
            records = self.process_population_data(data, search_query)
            if not records:
                print(f"⚠️  No valid records found for gemrate_id: {gemrate_id}")
                return False
            
            # Save to database
            result = self.save_population_records(records)
            
            if result['inserted'] > 0:
                return True
            else:
                if result['errors'] > 0:
                    return False
                return True  # Duplicates are considered success
            
        except Exception as e:
            print(f"❌ Error processing {gemrate_id}: {str(e)}")
            return False
    
    def process_single_gemrate_id(self, gemrate_id):
        """
        Process a single gemrate ID
        
        Args:
            gemrate_id: The gemrate ID to process
        """
        try:
            # Check if already exists
            if self.check_gemrate_exists(gemrate_id):
                self.stats['already_exists'] += 1
                return
            
            # Fetch data
            data = self.fetch_population_data(gemrate_id)
            if not data:
                self.stats['errors'] += 1
                return
            
            # Process data
            records = self.process_population_data(data)
            if not records:
                with self.print_lock:
                    print(f"⚠️  No valid records found for gemrate_id: {gemrate_id}")
                self.stats['errors'] += 1
                return
            
            # Save to database
            result = self.save_population_records(records)
            
            with self.print_lock:
                if result['inserted'] > 0:
                    print(f"✅ {gemrate_id}: {result['inserted']} records inserted")
                    self.stats['successful'] += 1
                else:
                    print(f"ℹ️  {gemrate_id}: {result['duplicates']} duplicates, {result['errors']} errors")
                    if result['errors'] > 0:
                        self.stats['errors'] += 1
            
            self.stats['processed'] += 1
            
        except Exception as e:
            with self.print_lock:
                print(f"❌ Error processing {gemrate_id}: {str(e)}")
            self.stats['errors'] += 1
    
    def fetch_all_population_data(self):
        """
        Main function to fetch all population data
        """
        print("🔄 Starting population data fetch from sales table")
        
        # Connect to database
        if not self.sql_handler.connect():
            print("❌ Database connection failed")
            return
        
        # Create population table
        if not self.create_population_table():
            print("❌ Failed to create population table")
            return
        
        try:
            # Get unique gemrate IDs with search queries
            print("📊 Getting unique gemrate IDs with search queries from sales table...")
            gemrate_data = self.get_unique_gemrate_ids_with_queries()
            
            if not gemrate_data:
                print("⚠️  No gemrate IDs found in sales table")
                return
            
            self.stats['total_found'] = len(gemrate_data)
            print(f"📊 Found {len(gemrate_data)} unique gemrate IDs")
            
            # Process each gemrate ID sequentially
            print("🔄 Starting sequential processing...")
            
            for index, (gemrate_id, search_query) in enumerate(gemrate_data, 1):
                print(f"📦 Processing {index}/{len(gemrate_data)}: {gemrate_id[:20]}...")
                
                try:
                    # Check if already exists
                    if self.check_gemrate_exists(gemrate_id):
                        print(f"✅ {index}: Already exists, skipping")
                        self.stats['already_exists'] += 1
                        continue
                    
                    # Fetch and process data
                    success = self.process_single_gemrate_with_query(gemrate_id, search_query)
                    
                    if success:
                        print(f"✅ {index}: Successfully processed")
                        self.stats['successful'] += 1
                    else:
                        print(f"❌ {index}: Failed to process")
                        self.stats['errors'] += 1
                    
                    self.stats['processed'] += 1
                    
                    # Small delay to be respectful to the API
                    time.sleep(1)
                    
                except Exception as e:
                    print(f"❌ {index}: Error processing {gemrate_id}: {str(e)}")
                    self.stats['errors'] += 1
            
            # Final summary
            print(f"\n🎉 POPULATION FETCH COMPLETED")
            print(f"📊 Total IDs found: {self.stats['total_found']}")
            print(f"📊 Processed: {self.stats['processed']}")
            print(f"📊 Successful: {self.stats['successful']}")
            print(f"📊 Already existed: {self.stats['already_exists']}")
            print(f"📊 Errors: {self.stats['errors']}")
            
        except Exception as e:
            print(f"❌ Fatal error: {str(e)}")
        finally:
            self.sql_handler.close_connection()

if __name__ == "__main__":
    fetcher = PopulationFetcher()
    fetcher.fetch_all_population_data()