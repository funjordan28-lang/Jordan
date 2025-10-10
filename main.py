import requests
from urllib.parse import urlencode, quote
from utils import read_excel_to_dict, save_data_to_sql, SQLDBHandler
import math
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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
            response = session.get(url, headers=headers, timeout=timeout)
            
            if response.status_code == 200:
                return response
            else:
                if attempt == 3:  # Only log on final failure
                    print(f"❌ HTTP {response.status_code} after 3 attempts: {url[:80]}...")
                if attempt < 3:
                    time.sleep(2 ** attempt)  # Exponential backoff: 2, 4 seconds
                    
        except requests.exceptions.Timeout:
            if attempt == 3:
                print(f"❌ Timeout after 3 attempts: {url[:80]}...")
            if attempt < 3:
                time.sleep(2 ** attempt)
                
        except requests.exceptions.ConnectionError as e:
            if attempt == 3:
                print(f"❌ Connection error after 3 attempts: {url[:80]}...")
            if attempt < 3:
                time.sleep(2 ** attempt)
                
        except requests.exceptions.RequestException as e:
            if attempt == 3:
                print(f"❌ Request error after 3 attempts: {str(e)}")
            if attempt < 3:
                time.sleep(2 ** attempt)
    
    return None

def get_sql_count_for_query(sql_handler, query):
    """
    Get the number of records saved in SQL database for a specific query
    
    Args:
        sql_handler: SQLDBHandler instance
        query: Search query string
        
    Returns:
        int: Number of records in SQL database for this query
    """
    try:
        count = sql_handler.get_count_for_query(query)
        return count
        
    except Exception as e:
        print(f"❌ Error counting SQL records for query '{query}': {str(e)}")
        return -1

def check_if_item_exists_in_db(sql_handler, query, item_id):
    """
    Check if a specific itemId already exists in SQL database for a given query
    
    Args:
        sql_handler: SQLDBHandler instance
        query: Search query string
        item_id: The itemId to check
        
    Returns:
        bool: True if item exists, False otherwise
    """
    try:
        exists = sql_handler.check_item_exists(item_id)
        return exists
        
    except Exception as e:
        print(f"❌ Error checking if item exists: {str(e)}")
        return False

def fetch_all_data_for_query_optimized(queryAndTierTuple, headers, sql_handler):
    """
    Optimized: Fetch data for query with smart early stopping and minimal API calls
    
    Args:
        query: Search query string
        headers: HTTP headers for the request
        sql_handler: SQLDBHandler instance
    
    Returns:
        List of all hits data from all pages (new records only)
    """
    # Step 1: Get current DB count
    query, tier = queryAndTierTuple
    db_count = get_sql_count_for_query(sql_handler, query)
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
                        return []  # No update needed, silent return
                    # Only show progress for queries that need processing
                
                if not hits:
                    break
                
                # Add search_url field to each hit and add to all_data
                for hit in hits:
                    hit['search_url'] = url
                    hit['search_query'] = query
                    hit['Tier'] = tier
                    if "cardId" in hit and hit["cardId"] != "" and hit["cardId"] is not None:
                        hit["Verified"] = True 
                    else:
                        hit["Verified"] = False
                    if "date" in hit:
                        hit["date_normal"] = hit["date"].split("T")[0]
                    all_data.append(hit)
                
            
                
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
                
            elif response:
                print(f"❌ API error {response.status_code} for query '{query[:30]}...'")
                break
            else:
                print(f"❌ Failed to fetch data for query '{query[:30]}...' after retries")
                break
                
        except Exception as e:
            print(f"❌ Error processing query '{query[:30]}...': {str(e)}")
            break
    
    return all_data


def fetch_data_multithreaded():
    """
    Fetch data using optimized multithreading with batches of 50 queries and save to SQL database
    """
    # Try to load config if it exists
    try:
        from config import MYSQL_CONFIG
        # Use larger pool size for better multithreading performance
        sql_handler = SQLDBHandler(pool_size=60, **MYSQL_CONFIG)
    except ImportError:
        print("📋 No config.py found, using default settings")
        sql_handler = SQLDBHandler(pool_size=60)
    
    if not sql_handler.connect():
        print("❌ Database connection failed")
        return
    
    try:
        # Read input queries
        input_data = read_excel_to_dict('input.xlsx', columns=['Queries', 'Tier'])
        all_queries = input_data['Queries']
        all_tiers = input_data['Tier']

        if not all_queries or not all_tiers:
            print("⚠️ No queries found in input.xlsx")
            return
        
        print(f"📊 Processing {len(all_queries)} queries from input.xlsx")
        
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
            tiers = all_tiers[start_idx:end_idx]
            combinedTierandQuery =[(q, t) for q, t in zip(batch_queries, tiers)]
            print(f"🔄 Batch {batch_num + 1}/{total_batches} ({len(batch_queries)} queries)")
            
            # Process batch with multithreading
            batch_results = process_queries_batch_optimized(combinedTierandQuery, headers, sql_handler)
            
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
            
            print(f"📦 Batch {batch_num + 1}/{total_batches}: {batch_results['total_inserted']} new records")
            
            # Small delay between batches to be respectful
            if batch_num < total_batches - 1:
                time.sleep(2)
        
        # Final summary
        total_db_records = sql_handler.get_total_records()
        print(f"\n🎉 COMPLETED - {global_stats['total_inserted']:,} new records added")
        print(f"� Total queries: {len(all_queries)} | New records: {global_stats['total_inserted']:,} | Total in DB: {total_db_records:,}")
        if global_stats['total_errors'] > 0:
            print(f"⚠️  {global_stats['total_errors']} errors encountered")
        
    except Exception as e:
        print(f"❌ Fatal error: {str(e)}")
    finally:
        sql_handler.close_connection()


def process_queries_batch_optimized(queries, headers, sql_handler):
    """
    Process a batch of queries using optimized multithreading approach
    
    Args:
        queries: List of queries to process
        headers: HTTP headers for requests
        sql_handler: SQLDBHandler instance
        
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
            query_data = fetch_all_data_for_query_optimized(query, headers, sql_handler)
            
            if query_data:
                # Save to SQL database
                save_result = save_data_to_sql(query_data, sql_handler)
                
                with stats_lock:
                    batch_results['queries_processed'] += 1
                    batch_results['total_records'] += len(query_data)
                    batch_results['total_inserted'] += save_result['inserted']
                    batch_results['total_duplicates'] += save_result['duplicates']
                    batch_results['total_errors'] += save_result['errors']
                
                with print_lock:
                    # Only show output for queries that actually had work to do
                    print(f"✅ Query {query_index}: {len(query_data)} records, {save_result['inserted']} new")
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