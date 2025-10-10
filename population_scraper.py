#!/usr/bin/env python3
"""
Population Data Scraper
Scrapes population/gem rate data and saves to SQL database
Maps grading companies and converts grades to numeric format
"""

import requests
import json
from utils import SQLDBHandler
import time
from urllib.parse import quote

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

class PopulationDBHandler(SQLDBHandler):
    """Extended SQLDBHandler for population data"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.population_table = "population_data"
    
    def create_population_table(self):
        """Create the population_data table if it doesn't exist"""
        try:
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {self.population_table} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                gemrate_id VARCHAR(255) NOT NULL,
                search_query TEXT,
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
                total_population INT,
                total_gems INT,
                gem_rate DECIMAL(10,8),
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_gemrate_id (gemrate_id),
                INDEX idx_search_query (search_query(100)),
                INDEX idx_grading_company (grading_company),
                INDEX idx_grade_type (grade_type),
                INDEX idx_grade_value (grade_value),
                INDEX idx_card_name (card_name),
                INDEX idx_set_name (set_name(100)),
                UNIQUE KEY unique_pop_entry (gemrate_id, grading_company, grade_type, grade_value)
            )
            """
            
            # Get a connection for table creation
            connection = self._get_connection()
            if connection:
                cursor = connection.cursor()
                cursor.execute(create_table_query)
                connection.commit()
                cursor.close()
                self._return_connection(connection)
                print(f"✅ Population table '{self.population_table}' created/verified")
            
        except Exception as e:
            print(f"❌ Error creating population table: {str(e)}")
    
    def parse_grade_data(self, grades_dict, grader):
        """
        Parse grade data and convert to standardized format
        
        Args:
            grades_dict: Dictionary of grades from API
            grader: Grading company (psa, beckett, sgc)
            
        Returns:
            List of grade entries
        """
        grade_entries = []
        
        # Grade mapping for different companies
        grade_mappings = {
            'psa': {
                'gem_mint': 10.0,  # PSA 10
                'g10': 10.0,
                'g9': 9.0,
                'g8': 8.0,
                'g7': 7.0,
                'g6': 6.0,
                'g5': 5.0,
                'g4': 4.0,
                'g3': 3.0,
                'g2': 2.0,
                'g1': 1.0,
                'auth': 0.0  # Authentic (no grade)
            },
            'beckett': {
                'gem_mint': 9.5,  # BGS 9.5
                'g10b': 10.0,  # BGS 10 (Black Label)
                'g10p': 10.0,  # BGS 10 (Pristine)
                'g9_5': 9.5,
                'g9': 9.0,
                'g8_5': 8.5,
                'g8': 8.0,
                'g7_5': 7.5,
                'g7': 7.0,
                'g6_5': 6.5,
                'g6': 6.0,
                'g5_5': 5.5,
                'g5': 5.0,
                'g4_5': 4.5,
                'g4': 4.0,
                'g3_5': 3.5,
                'g3': 3.0,
                'g2_5': 2.5,
                'g2': 2.0,
                'g1_5': 1.5,
                'g1': 1.0
            },
            'sgc': {
                'gem_mint': 9.5,  # SGC 9.5 equivalent
                'g10': 10.0,
                'g10p': 10.0,  # SGC Perfect (ignore per instructions)
                'g9_5': 9.5,
                'g9': 9.0,
                'g8_5': 8.5,
                'g8': 8.0,
                'g7_5': 7.5,
                'g7': 7.0,
                'g6_5': 6.5,
                'g6': 6.0,
                'g5_5': 5.5,
                'g5': 5.0,
                'g4_5': 4.5,
                'g4': 4.0,
                'g3_5': 3.5,
                'g3': 3.0,
                'g2_5': 2.5,
                'g2': 2.0,
                'g1_5': 1.5,
                'g1': 1.0,
                'gA': 0.0  # Authentic
            }
        }
        
        mapping = grade_mappings.get(grader.lower(), {})
        
        for grade_key, count in grades_dict.items():
            if count > 0:  # Only include grades with actual population
                grade_value = mapping.get(grade_key.lower())
                
                if grade_value is not None:
                    # Skip Perfect and Pristine as instructed (unless they're 10.0)
                    if grade_key.lower() in ['g10p'] and grader.lower() == 'sgc':
                        continue  # Skip SGC Perfect
                    if grade_key.lower() in ['g10p'] and grader.lower() == 'beckett':
                        continue  # Skip BGS Pristine (actually keep it as 10.0)
                    
                    grade_entries.append({
                        'grade_type': 'numeric' if grade_value > 0 else 'auth',
                        'grade_value': grade_value,
                        'grade_count': count
                    })
        
        return grade_entries
    
    def save_population_data(self, population_json, search_query=None):
        """
        Save population data to database
        
        Args:
            population_json: JSON data from API
            search_query: The search query associated with this data
        """
        try:
            gemrate_id = population_json.get('gemrate_id', '')
            description = population_json.get('description', '')
            total_population = population_json.get('total_population', 0)
            total_gems = population_json.get('total_gems_or_greater', 0)
            
            population_data_list = population_json.get('population_data', [])
            
            for card_data in population_data_list:
                # Generate unique card ID
                card_unique_id = self.generate_card_unique_id(card_data)
                
                # Extract basic card info
                grader = card_data.get('grader', '').lower()
                card_name = card_data.get('name', '')
                card_number = card_data.get('card_number', '')
                year = card_data.get('year', '')
                set_name = card_data.get('set_name', '')
                parallel = card_data.get('parallel', '')
                category = card_data.get('category', '')
                card_total_grades = card_data.get('card_total_grades', 0)
                card_gems = card_data.get('card_gems', 0)
                card_gem_rate = float(card_data.get('card_gem_rate', 0))
                
                # Parse grades
                grades_dict = card_data.get('grades', {})
                grade_entries = self.parse_grade_data(grades_dict, grader)
                
                # Map grader names
                grader_mapping = {
                    'psa': 'PSA',
                    'beckett': 'BGS', 
                    'sgc': 'SGC'
                }
                grading_company = grader_mapping.get(grader, grader.upper())
                
                # Save each grade entry
                for grade_entry in grade_entries:
                    self.insert_population_record({
                        'gemrate_id': gemrate_id,
                        'search_query': search_query,
                        'grading_company': grading_company,
                        'grade_type': grade_entry['grade_type'],
                        'grade_value': grade_entry['grade_value'],
                        'grade_count': grade_entry['grade_count'],
                        'card_description': description,
                        'card_name': card_name,
                        'card_number': card_number,
                        'card_year': year,
                        'set_name': set_name,
                        'parallel': parallel,
                        'category': category,
                        'total_population': card_total_grades,
                        'total_gems': card_gems,
                        'gem_rate': card_gem_rate
                    })
                    
        except Exception as e:
            print(f"❌ Error saving population data: {str(e)}")
    
    def insert_population_record(self, record_data):
        """Insert a single population record"""
        connection = self._get_connection()
        if not connection:
            print("❌ Failed to get database connection for population insert")
            return False
            
        try:
            cursor = connection.cursor()
            
            # Build INSERT ON DUPLICATE KEY UPDATE query
            columns = ', '.join(record_data.keys())
            placeholders = ', '.join(['%s'] * len(record_data))
            
            # Update columns for ON DUPLICATE KEY UPDATE
            update_columns = ', '.join([f"{k}=VALUES({k})" for k in record_data.keys() if k not in ['id']])
            
            insert_query = f"""
            INSERT INTO {self.population_table} ({columns}) 
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE {update_columns}
            """
            
            cursor.execute(insert_query, list(record_data.values()))
            connection.commit()
            cursor.close()
            return True
            
        except Exception as e:
            print(f"❌ Error inserting population record: {str(e)}")
            return False
        finally:
            self._return_connection(connection)

def fetch_population_data_from_gemrate_api(gemrate_id):
    """
    Fetch population data from GemRate API for a specific gemrate_id
    
    Args:
        gemrate_id: The gemrate ID to fetch data for
        
    Returns:
        JSON data from API or None if failed
    """
    try:
        url = f"https://www.gemrate.com/card-details?gemrate_id={gemrate_id}"
        
        # Make API request
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"❌ API error {response.status_code} for gemrate_id: {gemrate_id}")
            return None
            
    except Exception as e:
        print(f"❌ Error fetching data for gemrate_id {gemrate_id}: {str(e)}")
        return None

def get_unique_gemrate_ids_with_queries():
    """
    Get unique universalGemRateId values from sales table along with their search queries
    
    Returns:
        List of tuples (gemrate_id, search_query)
    """
    try:
        from config import MYSQL_CONFIG
        sales_handler = SQLDBHandler(pool_size=5, **MYSQL_CONFIG)
    except ImportError:
        print("📋 No config.py found, using default settings")
        sales_handler = SQLDBHandler(pool_size=5)
    
    if not sales_handler.connect():
        print("❌ Failed to connect to sales database")
        return []
    
    connection = sales_handler._get_connection()
    if not connection:
        print("❌ Failed to get database connection")
        return []
    
    try:
        cursor = connection.cursor()
        
        # Get unique universalGemRateId values with their associated search queries
        query = """
        SELECT DISTINCT universalGemRateId, search_query 
        FROM sales 
        WHERE universalGemRateId IS NOT NULL 
        AND universalGemRateId != '' 
        AND search_query IS NOT NULL
        ORDER BY universalGemRateId
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        
        # Convert to list of tuples
        gemrate_data = [(row[0], row[1]) for row in results if row[0]]
        
        print(f"📊 Found {len(gemrate_data)} unique gemrate IDs in sales table")
        return gemrate_data
        
    except Exception as e:
        print(f"❌ Error fetching gemrate IDs: {str(e)}")
        return []
    finally:
        sales_handler._return_connection(connection)
        sales_handler.close_connection()

def main():
    """
    Main function to fetch population data for all gemrate IDs from sales table
    """
    print("🚀 Starting Population Data Scraper")
    print("=" * 60)
    
    # Get unique gemrate IDs from sales table
    print("📖 Reading unique gemrate IDs from sales table...")
    gemrate_data = get_unique_gemrate_ids_with_queries()
    
    if not gemrate_data:
        print("⚠️ No gemrate IDs found in sales table")
        return
    
    # Initialize population database handler
    try:
        from config import MYSQL_CONFIG
        pop_handler = PopulationDBHandler(pool_size=20, **MYSQL_CONFIG)
    except ImportError:
        print("📋 No config.py found, using default settings")
        pop_handler = PopulationDBHandler(pool_size=20)
    
    if not pop_handler.connect():
        print("❌ Database connection failed")
        return
    
    # Create population table
    pop_handler.create_population_table()
    
    # Process each gemrate ID
    processed_count = 0
    error_count = 0
    
    print(f"🔄 Processing {len(gemrate_data)} gemrate IDs...")
    
    for i, (gemrate_id, search_query) in enumerate(gemrate_data, 1):
        try:
            print(f"📦 {i}/{len(gemrate_data)}: Processing {gemrate_id}")
            
            # Fetch population data from API
            population_data = fetch_population_data_from_gemrate_api(gemrate_id)
            
            if population_data:
                # Save to database with search query
                pop_handler.save_population_data(population_data, search_query)
                processed_count += 1
                print(f"✅ Saved population data for {gemrate_id}")
            else:
                error_count += 1
                print(f"❌ Failed to fetch data for {gemrate_id}")
            
            # Small delay to be respectful to API
            time.sleep(1)
            
        except Exception as e:
            error_count += 1
            print(f"❌ Error processing {gemrate_id}: {str(e)}")
    
    # Final summary
    print(f"\n🎉 POPULATION SCRAPING COMPLETED")
    print(f"📊 Total gemrate IDs: {len(gemrate_data)}")
    print(f"✅ Successfully processed: {processed_count}")
    print(f"❌ Errors: {error_count}")
    
    # Close connection
    pop_handler.close_connection()
    print("🔒 Database connection closed")

def test_with_sample_data():
    """Test the population scraper with the provided sample data"""
    
    # Sample data from your boss
    sample_data = {
        "combined_totals": {
            "total_1": 0,
            "total_1_5": 0,
            "total_2": 0,
            "total_2_5": 0,
            "total_3": 0,
            "total_3_5": 0,
            "total_4": 1,
            "total_4_5": 0,
            "total_5": 0,
            "total_5_5": 0,
            "total_6": 5,
            "total_6_5": 0,
            "total_7": 4,
            "total_7_5": 6,
            "total_8": 23,
            "total_8_5": 14,
            "total_9": 38,
            "total_auth": 3,
            "total_gem_mint": 9,
            "total_mint_plus": 0,
            "total_perfect": 0,
            "total_pristine": 0,
            "total_q1": 0,
            "total_q2": 0,
            "total_q3": 0,
            "total_q4": 0,
            "total_q5": 0,
            "total_q6": 0,
            "total_q7": 0,
            "total_q8": 0,
            "total_q9": 0
        },
        "date": "2025-10-09",
        "description": "1996 Flair Showcase Legacy Collection Michael Jordan Row 0 23",
        "gemrate_id": "ebde2b67046bad69ba36c2e0a0fce4a6d3c20ee4",
        "graders_included": [
            "psa",
            "beckett",
            "sgc"
        ],
        "last_population_change": "2025-09-16",
        "population_data": [
            {
                "auto_grades": {
                    "auth": 0,
                    "g1": 0,
                    "g10": 0,
                    "g2": 0,
                    "g3": 0,
                    "g4": 0,
                    "g5": 0,
                    "g6": 0,
                    "g7": 0,
                    "g8": 0,
                    "g9": 0
                },
                "auto_halves": {
                    "g1_5": 0,
                    "g2_5": 0,
                    "g3_5": 0,
                    "g4_5": 0,
                    "g5_5": 0,
                    "g6_5": 0,
                    "g7_5": 0,
                    "g8_5": 0
                },
                "auto_qualifiers": {
                    "q1": 0,
                    "q15": 0,
                    "q2": 0,
                    "q3": 0,
                    "q4": 0,
                    "q5": 0,
                    "q6": 0,
                    "q7": 0,
                    "q8": 0,
                    "q9": 0
                },
                "card_gem_rate": "0.13157894736842105",
                "card_gems": 5,
                "card_number": "23",
                "card_total_grades": 38,
                "category": "basketball-cards",
                "description": "1996 Flair Showcase Legacy Collection Michael Jordan Row 0 23",
                "gemrate_id": "ebde2b67046bad69ba36c2e0a0fce4a6d3c20ee4",
                "grader": "psa",
                "grades": {
                    "auth": 3,
                    "g1": 0,
                    "g10": 5,
                    "g2": 0,
                    "g3": 0,
                    "g4": 0,
                    "g5": 0,
                    "g6": 5,
                    "g7": 2,
                    "g8": 8,
                    "g9": 14
                },
                "halves": {
                    "g1_5": 0,
                    "g2_5": 0,
                    "g3_5": 0,
                    "g4_5": 0,
                    "g5_5": 0,
                    "g6_5": 0,
                    "g7_5": 0,
                    "g8_5": 1
                },
                "last_population_change": "2025-09-16",
                "name": "Michael Jordan",
                "non_auto_grades": {
                    "auth": 3,
                    "g1": 0,
                    "g10": 5,
                    "g2": 0,
                    "g3": 0,
                    "g4": 0,
                    "g5": 0,
                    "g6": 5,
                    "g7": 2,
                    "g8": 8,
                    "g9": 14
                },
                "non_auto_halves": {
                    "g1_5": 0,
                    "g2_5": 0,
                    "g3_5": 0,
                    "g4_5": 0,
                    "g5_5": 0,
                    "g6_5": 0,
                    "g7_5": 0,
                    "g8_5": 1
                },
                "non_auto_qualifiers": {
                    "q1": 0,
                    "q15": 0,
                    "q2": 0,
                    "q3": 0,
                    "q4": 0,
                    "q5": 0,
                    "q6": 0,
                    "q7": 0,
                    "q8": 0,
                    "q9": 0
                },
                "parallel": "Row 0",
                "pop_results": True,
                "qualifiers": {
                    "q1": 0,
                    "q15": 0,
                    "q2": 0,
                    "q3": 0,
                    "q4": 0,
                    "q5": 0,
                    "q6": 0,
                    "q7": 0,
                    "q8": 0,
                    "q9": 0
                },
                "set_name": "Flair Showcase Legacy Collection",
                "set_url": "https://www.psacard.com/pop/basketball-cards/1996/flair-showcase-legacy-collection/59138",
                "specid": "362249",
                "year": "1996"
            },
            {
                "card_gem_rate": "0.06349206349206349",
                "card_gems": 4,
                "card_number": "23",
                "card_total_grades": 63,
                "category": "basketball",
                "description": "1996 Flair Showcase Legacy Collection Row 0 Michael Jordan 23",
                "gemrate_id": "ebd8f570aed4b7fc0d60280d4cf73935d31ad041",
                "grader": "beckett",
                "grades": {
                    "g1": 0,
                    "g10b": 0,
                    "g10p": 0,
                    "g1_5": 0,
                    "g2": 0,
                    "g2_5": 0,
                    "g3": 0,
                    "g3_5": 0,
                    "g4": 1,
                    "g4_5": 0,
                    "g5": 0,
                    "g5_5": 0,
                    "g6": 0,
                    "g6_5": 0,
                    "g7": 2,
                    "g7_5": 6,
                    "g8": 14,
                    "g8_5": 12,
                    "g9": 24,
                    "g9_5": 4
                },
                "halves": None,
                "last_population_change": "2025-06-29",
                "name": "Michael Jordan",
                "parallel": "",
                "pop_results": True,
                "qualifiers": None,
                "set_name": "Flair Showcase Legacy Collection Row 0",
                "set_url": "https://www.beckett.com/grading/set_match/3126732",
                "year": "1996"
            },
            {
                "card_gem_rate": "0",
                "card_gems": 0,
                "card_number": "23",
                "card_total_grades": 2,
                "category": "basketball",
                "description": "1996 Flair Showcase Michael Jordan Legacy Collection Row 0 23",
                "gemrate_id": "72cc87dcb186d41146ad91e3e13bd353603bd0ed",
                "grader": "sgc",
                "grades": {
                    "g1": 0,
                    "g10": 0,
                    "g10p": 0,
                    "g1_5": 0,
                    "g2": 0,
                    "g2_5": 0,
                    "g3": 0,
                    "g3_5": 0,
                    "g4": 0,
                    "g4_5": 0,
                    "g5": 0,
                    "g5_5": 0,
                    "g6": 0,
                    "g6_5": 0,
                    "g7": 0,
                    "g7_5": 0,
                    "g8": 1,
                    "g8_5": 1,
                    "g9": 0,
                    "g9_5": 0,
                    "gA": 0
                },
                "halves": None,
                "last_population_change": "2025-08-04",
                "name": "Michael Jordan",
                "parallel": "Legacy Collection Row 0",
                "pop_results": True,
                "qualifiers": None,
                "set_name": "Flair Showcase",
                "set_url": "https://gosgc.com/pop-report/result/1996-97%20Flair%20Showcase/Basketball",
                "year": "1996"
            }
        ],
        "population_type": "universal",
        "total_gems_or_greater": 9,
        "total_population": 103
    }
    
    print("🧪 Testing Population Scraper with sample data")
    print("=" * 60)
    
    # Initialize database handler
    try:
        from config import MYSQL_CONFIG
        pop_handler = PopulationDBHandler(pool_size=10, **MYSQL_CONFIG)
    except ImportError:
        print("📋 No config.py found, using default settings")
        pop_handler = PopulationDBHandler(pool_size=10)
    
    if not pop_handler.connect():
        print("❌ Database connection failed")
        return
    
    # Create population table
    pop_handler.create_population_table()
    
    # Save sample data
    print("💾 Saving sample population data...")
    pop_handler.save_population_data(sample_data)
    
    print("✅ Sample data saved successfully!")
    print(f"📊 Processed gemrate_id: {sample_data['gemrate_id']}")
    print(f"📊 Card: {sample_data['description']}")
    
    # Close connection
    pop_handler.close_connection()
    print("🔒 Database connection closed")

if __name__ == "__main__":
    main()