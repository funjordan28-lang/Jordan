import requests
from urllib.parse import urlencode, quote
from utils import read_excel_to_dict, save_data_to_mongodb, MongoDBHandler
import math
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
url = "https://search-zzvl7ri3bq-uc.a.run.app/search?index=salesarchive&query=1984%20Star%20Michael%20Jordan%20101&page=1&limit=20&filters=&sort=date&direction=desc"

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
def fetch_all_data_for_query(query, headers):
    """
    Fetch all pages of data for a given query with limit=50 per page.
    
    Args:
        query: Search query string
        headers: HTTP headers for the request
    
    Returns:
        List of all hits data from all pages
    """
    all_data = []
    page = 0
    limit = 50
    
    while True:
        # Construct URL with current page and limit=50
        url = f"https://search-zzvl7ri3bq-uc.a.run.app/search?index=salesarchive&query={quote(query)}&page={page}&limit={limit}&filters=&sort=date&direction=desc"
        
        try:
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                data = response.json()
                hits = data.get('hits', [])
                total_hits = data.get('totalHits', 0)
                
                # Add search_url field to each hit and add to all_data
                for hit in hits:
                    hit['search_url'] = url
                    if "cardId" in hit and hit["cardId"] != "" and hit["cardId"] is not None:
                        hit["Verified"] = True 
                    else:
                        hit["Verified"] = False
                        
                all_data.extend(hits)
                
                # Check if we've reached the end
                if len(hits) < limit:
                    break
                
                # Calculate if there are more pages
                total_pages = math.ceil(total_hits / limit)
                if page >= total_pages:
                    break
                
                page += 1
                
                # Add a small delay between requests to be respectful
                time.sleep(0.5)
                
            else:
                print(f"❌ Failed to fetch page {page} for query '{query}'. Status code: {response.status_code}")
                break
                
        except Exception as e:
            print(f"❌ Error fetching page {page} for query '{query}': {str(e)}")
            break
    
    return all_data


def fetch_data_multithreaded():
    """
    Fetch data using multithreading with batches of 50 queries and save to MongoDB
    """
    # Initialize MongoDB handler
    mongo_handler = MongoDBHandler()
    if not mongo_handler.connect():
        print("❌ Failed to connect to MongoDB. Exiting.")
        return
    
    try:
        # Read input queries
        input_data = read_excel_to_dict('input.xlsx', columns=['Queries'])
        
        # Get already processed queries from MongoDB
        processed_queries = mongo_handler.get_processed_queries_from_db()
        
        # Filter out already processed queries
        queries_to_process = [query for query in input_data['Queries'] if query not in processed_queries]
        
        if not queries_to_process:
            print("✓ All queries have already been processed!")
            return
        
        # Process queries in batches of 50
        batch_size = 50
        total_batches = math.ceil(len(queries_to_process) / batch_size)
        
        global_stats = {
            'total_records': 0,
            'total_inserted': 0,
            'total_duplicates': 0,
            'total_errors': 0
        }
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(queries_to_process))
            batch_queries = queries_to_process[start_idx:end_idx]
            
            # Process batch with multithreading
            batch_results = process_queries_batch(batch_queries, headers, mongo_handler)
            
            # Update global statistics
            global_stats['total_records'] += batch_results['total_records']
            global_stats['total_inserted'] += batch_results['total_inserted']
            global_stats['total_duplicates'] += batch_results['total_duplicates']
            global_stats['total_errors'] += batch_results['total_errors']
            
            # Small delay between batches to be respectful
            if batch_num < total_batches - 1:
                time.sleep(2)
        
        # Final summary
        total_db_records = mongo_handler.get_total_records()
        print(f"\n{'='*60}")
        print(f"FINAL SUMMARY")
        print(f"{'='*60}")
        print(f"Total queries processed: {len(queries_to_process)}")
        print(f"Total records fetched: {global_stats['total_records']}")
        print(f"New records inserted: {global_stats['total_inserted']}")
        print(f"Duplicates skipped: {global_stats['total_duplicates']}")
        print(f"Errors encountered: {global_stats['total_errors']}")
        print(f"Total records in MongoDB: {total_db_records}")
        print(f"Database: Jordan, Collection: sales")
        
    except Exception as e:
        print(f"❌ Fatal error in fetch_data_multithreaded: {str(e)}")
    finally:
        mongo_handler.close_connection()


def process_queries_batch(queries, headers, mongo_handler):
    """
    Process a batch of queries using multithreading
    
    Args:
        queries: List of queries to process
        headers: HTTP headers for requests
        mongo_handler: MongoDBHandler instance
        
    Returns:
        Dictionary with batch processing results
    """
    batch_results = {
        'total_records': 0,
        'total_inserted': 0,
        'total_duplicates': 0,
        'total_errors': 0
    }
    
    # Thread lock for safe printing and stats updating
    print_lock = Lock()
    stats_lock = Lock()
    
    def process_single_query(query_info):
        """Process a single query in a thread"""
        query_index, query = query_info
        
        try:
            # Fetch all data for this query
            query_data = fetch_all_data_for_query(query, headers)
            
            if query_data:
                # Save to MongoDB
                save_result = save_data_to_mongodb(query_data, mongo_handler)
                
                with stats_lock:
                    batch_results['total_records'] += len(query_data)
                    batch_results['total_inserted'] += save_result['inserted']
                    batch_results['total_duplicates'] += save_result['duplicates']
                    batch_results['total_errors'] += save_result['errors']
                    
        except Exception as e:
            with stats_lock:
                batch_results['total_errors'] += 1
            with print_lock:
                print(f"❌ Error processing query '{query[:50]}...': {str(e)}")
    
    # Process queries with thread pool
    with ThreadPoolExecutor(max_workers=min(len(queries), 50)) as executor:
        # Submit all queries
        query_info_list = [(i + 1, query) for i, query in enumerate(queries)]
        futures = {executor.submit(process_single_query, query_info): query_info for query_info in query_info_list}
        
        # Wait for completion
        for future in as_completed(futures):
            try:
                future.result()  # This will raise any exception that occurred
            except Exception as e:
                query_info = futures[future]
                with print_lock:
                    print(f"❌ Exception in thread for query '{query_info[1][:50]}...': {str(e)}")
    
    return batch_results

if __name__ == "__main__":
    fetch_data_multithreaded()