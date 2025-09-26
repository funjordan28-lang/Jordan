import requests
from urllib.parse import urlencode, quote
from utils import read_excel_to_dict, save_data_to_mongodb, MongoDBHandler
import math
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
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

def create_robust_session():
    """
    Create a requests session with retry strategy for handling network failures
    
    Returns:
        requests.Session: Session configured with retry logic
    """
    session = requests.Session()
    
    # Define retry strategy
    retry_strategy = Retry(
        total=3,  # Total number of retries
        backoff_factor=2,  # Exponential backoff factor (2^n seconds)
        status_forcelist=[429, 500, 502, 503, 504],  # HTTP status codes to retry
        allowed_methods=["HEAD", "GET", "OPTIONS"],  # HTTP methods to retry
        raise_on_redirect=False,
        raise_on_status=False
    )
    
    # Mount the adapter
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

def make_api_request_with_retry(url, headers, timeout=30):
    """
    Make API request with retry logic for network failures
    
    Args:
        url (str): The URL to request
        headers (dict): Request headers
        timeout (int): Request timeout in seconds
        
    Returns:
        requests.Response or None: Response object if successful, None if failed
    """
    session = create_robust_session()
    
    for attempt in range(1, 4):  # 3 attempts total
        try:
            print(f"🔄 Attempt {attempt}/3 for URL: {url[:100]}...")
            
            response = session.get(url, headers=headers, timeout=timeout)
            
            if response.status_code == 200:
                print(f"✅ Success on attempt {attempt}")
                return response
            else:
                print(f"⚠️ HTTP {response.status_code} on attempt {attempt}")
                if attempt < 3:
                    wait_time = 2 ** attempt  # Exponential backoff: 2, 4, 8 seconds
                    print(f"⏳ Waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                    
        except requests.exceptions.Timeout:
            print(f"⏰ Timeout on attempt {attempt}")
            if attempt < 3:
                wait_time = 2 ** attempt
                print(f"⏳ Waiting {wait_time}s before retry...")
                time.sleep(wait_time)
                
        except requests.exceptions.ConnectionError as e:
            print(f"🌐 Connection error on attempt {attempt}: {str(e)}")
            if attempt < 3:
                wait_time = 2 ** attempt
                print(f"⏳ Waiting {wait_time}s before retry...")
                time.sleep(wait_time)
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Request error on attempt {attempt}: {str(e)}")
            if attempt < 3:
                wait_time = 2 ** attempt
                print(f"⏳ Waiting {wait_time}s before retry...")
                time.sleep(wait_time)
    
    print(f"❌ All 3 attempts failed for URL: {url[:100]}...")
    return None

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

def check_if_item_exists_in_db(mongo_handler, query, item_id):
    """
    Check if a specific itemId already exists in MongoDB for a given query
    
    Args:
        mongo_handler: MongoDBHandler instance
        query: Search query string
        item_id: The itemId to check
        
    Returns:
        bool: True if item exists, False otherwise
    """
    try:
        search_pattern = f"query={quote(query)}"
        
        # Check if document exists with this itemId and query
        count = mongo_handler.collection.count_documents({
            "search_url": {"$regex": search_pattern},
            "itemId": item_id
        })
        
        return count > 0
        
    except Exception as e:
        print(f"❌ Error checking if item exists: {str(e)}")
        return False

def fetch_all_data_for_query_optimized(query, headers, mongo_handler):
    """
    Optimized: Fetch data for query with smart early stopping and minimal API calls
    
    Args:
        query: Search query string
        headers: HTTP headers for the request
        mongo_handler: MongoDBHandler instance
    
    Returns:
        List of all hits data from all pages (new records only)
    """
    # Step 1: Get current DB count
    db_count = get_mongodb_count_for_query(mongo_handler, query)
    if db_count == -1:
        print(f"❌ Error getting DB count for query '{query}' - skipping")
        return []
    
    all_data = []
    page = 0
    limit = 50
    found_existing = False
    
    while not found_existing:
        # Construct URL with current page and limit=50
        if page == 0:

            url = f"https://search-zzvl7ri3bq-uc.a.run.app/search?index=salesarchive&query={quote(query)}&limit={limit}&filters=&sort=date&direction=desc"
        else:
            url = f"https://search-zzvl7ri3bq-uc.a.run.app/search?index=salesarchive&query={quote(query)}&page={page}&limit={limit}&filters=&sort=date&direction=desc"

        try:
            response = make_api_request_with_retry(url, headers)
            
            if response and response.status_code == 200:
                data = response.json()
                hits = data.get('hits', [])
                total_hits = data.get('totalHits', 0)
                
                # Smart decision: Check if update needed (only on first page)
                if page == 0:
                    if db_count >= total_hits:
                        print(f"✅ Query '{query[:50]}...' already complete: DB={db_count}, API={total_hits}")
                        return []  # No update needed
                    else:
                        print(f"🔄 Query '{query[:50]}...' needs processing: DB={db_count}, API={total_hits}")

                if not hits:
                    print("❌ No hits found, stopping.")
                    break
                
                # Add search_url field to each hit and add to all_data
                for hit in hits:
                    hit['search_url'] = url
                    hit['search_query'] = query
                    if "cardId" in hit and hit["cardId"] != "" and hit["cardId"] is not None:
                        hit["Verified"] = True 
                    else:
                        hit["Verified"] = False
                    if "date" in hit:
                        hit["date_normal"] = hit["date"].split("T")[0]
                    all_data.append(hit)
                
            
                
                # Check if we've reached the end
                if len(hits) < limit:
                    print("❌ Limit reached, stopping.")
                    break
                
                # Calculate if there are more pages
                total_pages = math.ceil(total_hits / limit)
                if page >= total_pages:
                    break
                
                page += 1
                
                # Add a small delay between requests to be respectful
                time.sleep(0.5)
                
            elif response:
                print(f"❌ Failed to fetch page {page} for query '{query}'. Status code: {response.status_code}")
                break
            else:
                print(f"❌ Failed to fetch page {page} for query '{query}' after 3 retry attempts")
                break
                
        except Exception as e:
            print(f"❌ Error fetching page {page} for query '{query}': {str(e)}")
            break
    
    return all_data


def fetch_data_multithreaded():
    """
    Fetch data using optimized multithreading with batches of 50 queries and save to MongoDB
    """
    # Initialize MongoDB handler
    mongo_handler = MongoDBHandler()
    if not mongo_handler.connect():
        print("❌ Failed to connect to MongoDB. Exiting.")
        return
    
    try:
        # Read input queries
        print("📖 Reading queries from input.xlsx...")
        input_data = read_excel_to_dict('input.xlsx', columns=['Queries'])
        all_queries = input_data['Queries']
        
        if not all_queries:
            print("⚠️ No queries found in input.xlsx")
            return
        
        print(f"📊 Found {len(all_queries)} queries to process")
        print("🚀 Starting optimized multithreaded processing...\n")
        
        # Process queries in batches of 50
        batch_size = 50
        total_batches = math.ceil(len(all_queries) / batch_size)
        
        global_stats = {
            'total_queries_processed': 0,
            'queries_up_to_date': 0,
            'queries_processed': 0,
            'total_records': 0,
            'total_inserted': 0,
            'total_duplicates': 0,
            'total_errors': 0
        }
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(all_queries))
            batch_queries = all_queries[start_idx:end_idx]
            
            print(f"Processing batch {batch_num + 1}/{total_batches} ({len(batch_queries)} queries)")
            
            # Process batch with multithreading
            batch_results = process_queries_batch_optimized(batch_queries, headers, mongo_handler)
            
            # Update global statistics
            global_stats['total_queries_processed'] += len(batch_queries)
            global_stats['total_records'] += batch_results['total_records']
            global_stats['total_inserted'] += batch_results['total_inserted']
            global_stats['total_duplicates'] += batch_results['total_duplicates']
            global_stats['total_errors'] += batch_results['total_errors']
            
            # Count queries that were processed vs already up-to-date
            queries_with_data = batch_results['queries_processed']
            global_stats['queries_processed'] += queries_with_data
            global_stats['queries_up_to_date'] += len(batch_queries) - queries_with_data
            
            print(f"✅ Batch {batch_num + 1} completed - Records: {batch_results['total_records']}, Inserted: {batch_results['total_inserted']}")
            
            # Small delay between batches to be respectful
            if batch_num < total_batches - 1:
                time.sleep(2)
        
        # Final comprehensive summary
        total_db_records = mongo_handler.get_total_records()
        print(f"\n{'='*70}")
        print(f"🎉 PROCESSING COMPLETED SUCCESSFULLY")
        print(f"{'='*70}")
        print(f"📊 Processing Summary:")
        print(f"   Total queries in input.xlsx: {len(all_queries)}")
        print(f"   Queries processed: {global_stats['total_queries_processed']}")
        print(f"   Queries already up-to-date: {global_stats['queries_up_to_date']}")
        print(f"   Queries that needed processing: {global_stats['queries_processed']}")
        print(f"")
        print(f"📈 Data Summary:")
        print(f"   Total records fetched: {global_stats['total_records']}")
        print(f"   New records inserted: {global_stats['total_inserted']}")
        print(f"   Duplicates skipped: {global_stats['total_duplicates']}")
        print(f"   Errors encountered: {global_stats['total_errors']}")
        print(f"")
        print(f"🗄️ Database Status:")
        print(f"   Total records in MongoDB: {total_db_records:,}")
        print(f"   Database: Jordan, Collection: sales")
        
        if global_stats['total_inserted'] > 0:
            print(f"\n✅ Successfully processed {global_stats['total_inserted']:,} new records!")
        else:
            print(f"\n✅ All queries were already up-to-date!")
        
    except Exception as e:
        print(f"❌ Fatal error in fetch_data_multithreaded: {str(e)}")
    finally:
        mongo_handler.close_connection()
        print(f"\n🔒 MongoDB connection closed")


def process_queries_batch_optimized(queries, headers, mongo_handler):
    """
    Process a batch of queries using optimized multithreading approach
    
    Args:
        queries: List of queries to process
        headers: HTTP headers for requests
        mongo_handler: MongoDBHandler instance
        
    Returns:
        Dictionary with batch processing results
    """
    batch_results = {
        'queries_processed': 0,
        'total_records': 0,
        'total_inserted': 0,
        'total_duplicates': 0,
        'total_errors': 0
    }
    
    # Thread lock for safe printing and stats updating
    print_lock = Lock()
    stats_lock = Lock()
    
    def process_single_query_optimized(query_info):
        """Process a single query in a thread with optimized approach"""
        query_index, query = query_info
        
        try:
            # Fetch data using optimized approach
            query_data = fetch_all_data_for_query_optimized(query, headers, mongo_handler)
            
            if query_data:
                # Save to MongoDB
                save_result = save_data_to_mongodb(query_data, mongo_handler)
                
                with stats_lock:
                    batch_results['queries_processed'] += 1
                    batch_results['total_records'] += len(query_data)
                    batch_results['total_inserted'] += save_result['inserted']
                    batch_results['total_duplicates'] += save_result['duplicates']
                    batch_results['total_errors'] += save_result['errors']
                
                with print_lock:
                    print(f"✅ Query {query_index}: {len(query_data)} records, {save_result['inserted']} inserted")
            # No need to print anything for up-to-date queries (already handled in fetch function)
                    
        except Exception as e:
            with stats_lock:
                batch_results['total_errors'] += 1
            with print_lock:
                print(f"❌ Error processing query '{query[:50]}...': {str(e)}")
    
    # Process queries with thread pool
    with ThreadPoolExecutor(max_workers=min(len(queries), 50)) as executor:
        # Submit all queries
        query_info_list = [(i + 1, query) for i, query in enumerate(queries)]
        futures = {executor.submit(process_single_query_optimized, query_info): query_info for query_info in query_info_list}
        
        # Wait for completion
        for future in as_completed(futures):
            try:
                future.result()  # This will raise any exception that occurred
            except Exception as e:
                query_info = futures[future]
                with print_lock:
                    print(f"❌ Exception in thread for query '{query_info[1][:50]}...': {str(e)}")
                with stats_lock:
                    batch_results['total_errors'] += 1
    
    return batch_results

if __name__ == "__main__":
    fetch_data_multithreaded()