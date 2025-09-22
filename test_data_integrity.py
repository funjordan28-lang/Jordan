

import requests
from urllib.parse import quote, unquote, parse_qs, urlparse
from utils import MongoDBHandler, read_excel_to_dict
import time

# API headers (same as main.py)
headers = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "en-US,en;q=0.9",
    "authorization": "Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6IjUwMDZlMjc5MTVhMTcwYWIyNmIxZWUzYjgxZDExNjU0MmYxMjRmMjAiLCJ0eXAiOiJKV1QifQ.eyJpc3MiOiJodHRwczovL3NlY3VyZXRva2VuLmdvb2dsZS5jb20vY2FyZGxhZGRlci03MWQ1MyIsImF1ZCI6ImNhcmRsYWRkZXItNzFkNTMiLCJhdXRoX3RpbWUiOjE3NTgzNTA5ODAsInVzZXJfaWQiOiJwTTUzUjRXQ21nUXhqUVdwR0pRWjFSeVFWcE8yIiwic3ViIjoicE01M1I0V0NtZ1F4alFXcEdKUVoxUnlRVnBPMiIsImlhdCI6MTc1ODM1NDI4MSwiZXhwIjoxNzU4MzU3ODgxLCJlbWFpbCI6ImZ1bmpvcmRhbjI4QGdtYWlsLmNvbSIsImVtYWlsX3ZlcmlmaWVkIjpmYWxzZSwiZmlyZWJhc2UiOnsiaWRlbnRpdGllcyI6eyJlbWFpbCI6WyJmdW5qb3JkYW4yOEBnbWFpbC5jb20iXX0sInNpZ25faW5fcHJvdmlkZXIiOiJwYXNzd29yZCJ9fQ.jtrzg9IWu3Za_XKcYeba-RjP7H_i8V28OUQq4jBbybc0bdLuUvrn2sZuytyhjvNouhqTKQBhoSrUM4seZvdTtGuvt4Evv7r5jX0oVBtIZO4EVhcBCHepIfYPA58ZoYmqeGvDXO-nNVc0IeVn7vz2chqzZ0PasdnVK4C-AcTjV5alxyeD72ys4Yh9UqYMzuGYc1EPsqMWWWqAcoYC-ZCm_3ws74eBTFtkfyKIdlAQ0zrXB-T0EJ7mIKedS8M2tkM9P2NHn7O4hX5_rDD8sEIWDd0QeRbNKiCyDM5Q3WkAfjuUtsfxXWWg0mmB1hPtucnwtAWQvt-8JKXCJIK-l0_VIA",
    "origin": "https://app.cardladder.com",
    "priority": "u=1, i",
    "referer": "https://app.cardladder.com/",
    "sec-ch-ua": "\"Chromium\";v=\"140\", \"Not=A?Brand\";v=\"24\", \"Brave\";v=\"140\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "cross-site",
    "sec-gpc": "1",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
}

def get_api_total_for_query(query):
    """
    Get the total number of records available for a query from the API
    
    Args:
        query: Search query string
        
    Returns:
        int: Total number of records available, or -1 if error
    """
    try:
        # Make API call to page 0 with limit 1 to get total count
        url = f"https://search-zzvl7ri3bq-uc.a.run.app/search?index=salesarchive&query={quote(query)}&page=0&limit=1&filters=&sort=date&direction=desc"
        
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            total_hits = data.get('totalHits', 0)
            return total_hits
        else:
            print(f"❌ API Error for query '{query}': Status {response.status_code}")
            return -1
            
    except Exception as e:
        print(f"❌ Exception getting API total for query '{query}': {str(e)}")
        return -1

def get_mongodb_count_for_query(mongo_handler, query):
    """
    Get the number of records saved in MongoDB for a specific query
    
    Args:
        mongo_handler: MongoDBHandler instance
        query: Search query string
        
    Returns:
        int: Number of records in MongoDB for this query
    """
    try:
        # Create search pattern for the query in search_url field
        search_pattern = f"query={quote(query)}"
        
        # Count documents where search_url contains this query
        count = mongo_handler.collection.count_documents({
            "search_url": {"$regex": search_pattern}
        })
        
        return count
        
    except Exception as e:
        print(f"❌ Error counting MongoDB records for query '{query}': {str(e)}")
        return -1

def extract_unique_queries_from_db(mongo_handler):
    """
    Extract all unique queries that have been processed and saved to MongoDB
    
    Args:
        mongo_handler: MongoDBHandler instance
        
    Returns:
        list: List of unique query strings
    """
    try:
        # Get all distinct search_url values
        search_urls = mongo_handler.collection.distinct("search_url")
        
        unique_queries = set()
        for search_url in search_urls:
            if search_url:
                try:
                    parsed_url = urlparse(search_url)
                    params = parse_qs(parsed_url.query)
                    if 'query' in params:
                        query = unquote(params['query'][0])
                        unique_queries.add(query)
                except:
                    pass
        
        return list(unique_queries)
        
    except Exception as e:
        print(f"❌ Error extracting queries from MongoDB: {str(e)}")
        return []

def test_data_integrity():
    """
    Test data integrity by comparing MongoDB counts with API totals for queries from input.xlsx
    Returns list of mismatched queries for reprocessing
    """
    print("🔍 Starting Data Integrity Test")
    print("=" * 60)
    
    # Initialize MongoDB handler
    mongo_handler = MongoDBHandler()
    if not mongo_handler.connect():
        print("❌ Failed to connect to MongoDB. Exiting.")
        return []
    
    try:
        # Read queries from input.xlsx
        input_data = read_excel_to_dict('input.xlsx', columns=['Queries'])
        queries_to_test = input_data['Queries']
        
        if not queries_to_test:
            print("⚠️ No queries found in input.xlsx")
            return []
        
        print(f"📊 Testing {len(queries_to_test)} queries from input.xlsx")
        print("Checking data integrity...\n")
        
        # Test results tracking
        total_tests = 0
        passed_tests = 0
        failed_tests = 0
        error_tests = 0
        
        test_results = []
        mismatched_queries = []  # Queries that need reprocessing
        
        # Test each query
        for i, query in enumerate(queries_to_test, 1):
            print(f"🔄 Testing query {i}/{len(queries_to_test)}: '{query[:50]}...'")
            
            # Get counts from both sources
            api_total = get_api_total_for_query(query)
            mongodb_count = get_mongodb_count_for_query(mongo_handler, query)
            
            total_tests += 1
            
            # Compare results
            if api_total == -1 or mongodb_count == -1:
                status = "ERROR"
                error_tests += 1
                result_icon = "❌"
                mismatched_queries.append(query)  # Add to reprocessing list
            elif api_total == mongodb_count:
                status = "PASS"
                passed_tests += 1
                result_icon = "✅"
            else:
                status = "FAIL"
                failed_tests += 1
                result_icon = "❌"
                mismatched_queries.append(query)  # Add to reprocessing list
            
            # Store result
            test_result = {
                'query': query,
                'api_total': api_total,
                'mongodb_count': mongodb_count,
                'status': status
            }
            test_results.append(test_result)
            
            print(f"{result_icon} {status}: API={api_total}, MongoDB={mongodb_count}")
            
            # Small delay to be respectful to API
            time.sleep(0.1)  # Reduced delay
        
        # Print summary
        print("\n" + "=" * 60)
        print("📋 DATA INTEGRITY TEST SUMMARY")
        print("=" * 60)
        print(f"Total queries tested: {total_tests}")
        print(f"✅ Passed: {passed_tests}")
        print(f"❌ Failed: {failed_tests}")
        print(f"⚠️ Errors: {error_tests}")
        print(f"🔄 Queries needing reprocessing: {len(mismatched_queries)}")
        
        if failed_tests > 0:
            print(f"\n🔍 FAILED TESTS DETAILS:")
            print("-" * 40)
            for result in test_results:
                if result['status'] == 'FAIL':
                    print(f"Query: '{result['query'][:50]}...'")
                    print(f"  API Total: {result['api_total']}")
                    print(f"  MongoDB Count: {result['mongodb_count']}")
                    print(f"  Difference: {result['api_total'] - result['mongodb_count']}")
                    print()
        
        if error_tests > 0:
            print(f"\n⚠️ ERROR TESTS DETAILS:")
            print("-" * 40)
            for result in test_results:
                if result['status'] == 'ERROR':
                    print(f"Query: '{result['query'][:50]}...'")
                    print(f"  API Total: {result['api_total']}")
                    print(f"  MongoDB Count: {result['mongodb_count']}")
                    print()
        
        # Save mismatched queries for reprocessing
        if mismatched_queries:
            print(f"\n📝 QUERIES NEEDING REPROCESSING:")
            print("-" * 40)
            for i, query in enumerate(mismatched_queries, 1):
                print(f"{i}. {query}")
            
            # Save to a file for easy reprocessing
            try:
                import pandas as pd
                df = pd.DataFrame({'Queries': mismatched_queries})
                df.to_excel('reprocess_queries.xlsx', index=False)
                print(f"\n💾 Saved {len(mismatched_queries)} mismatched queries to 'reprocess_queries.xlsx'")
                print("   You can use this file as input for main.py to reprocess only failed queries.")
            except Exception as e:
                print(f"❌ Error saving reprocess file: {str(e)}")
        
        # Overall result
        if failed_tests == 0 and error_tests == 0:
            print("\n🎉 ALL TESTS PASSED! Data integrity is perfect.")
        elif failed_tests > 0 or error_tests > 0:
            print(f"\n⚠️ {len(mismatched_queries)} queries need reprocessing.")
            
        return mismatched_queries
            
    except Exception as e:
        print(f"❌ Fatal error during testing: {str(e)}")
        return []
    finally:
        mongo_handler.close_connection()

if __name__ == "__main__":
    mismatched_queries = test_data_integrity()
    
    if mismatched_queries:
        print(f"\n🚀 To reprocess the {len(mismatched_queries)} mismatched queries:")
        print("1. Use 'reprocess_queries.xlsx' as input file")
        print("2. Or manually copy the listed queries to your input file")
        print("3. Run main.py to process only the failed queries")
    else:
        print("\n✅ No reprocessing needed - all data is consistent!")