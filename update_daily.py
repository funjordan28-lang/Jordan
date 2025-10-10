#!/usr/bin/env python3
"""
Daily Update Script
Efficiently updates the database by:
1. Getting mismatched queries from test_data_integrity
2. For each query, fetching only new sales (stopping when we find existing cardId)
3. Since API returns latest first, we can stop early when we hit existing data
"""

import requests
from urllib.parse import quote
from utils import SQLDBHandler, save_data_to_sql, read_excel_to_dict
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import math

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

def get_api_total_for_query(query):
    """
    Get the total number of records available for a query from the API (page 0)
    
    Args:
        query: Search query string
        
    Returns:
        int: Total number of records available, or -1 if error
    """
    try:
        # Make API call to page 0 with limit 1 to get total count
        url = f"https://search-zzvl7ri3bq-uc.a.run.app/search?index=salesarchive&query={quote(query)}&limit=1&filters=&sort=date&direction=desc"
        
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

def fetch_new_data_for_query_optimized(query_and_tier_tuple, headers, sql_handler):
    """
    Optimized: Make single API call to determine if update needed, then process efficiently
    
    Args:
        query_and_tier_tuple: Tuple containing (query, tier) 
        headers: HTTP headers for the request
        sql_handler: SQLDBHandler instance
    
    Returns:
        List of new data (items not yet in database)
    """
    # Extract query and tier from tuple
    if isinstance(query_and_tier_tuple, tuple):
        query, tier = query_and_tier_tuple
    else:
        # Fallback for backward compatibility
        query = query_and_tier_tuple
        tier = ""
    
    # Step 1: Get current DB count
    db_count = get_sql_count_for_query(sql_handler, query)
    if db_count == -1:
        print(f"❌ Error getting DB count for query '{query}' - skipping")
        return []
    
    new_data = []
    page = 0
    limit = 50
    found_existing = False
    
    while not found_existing:
        # Construct URL for current page
        if page == 0:

            url = f"https://search-zzvl7ri3bq-uc.a.run.app/search?index=salesarchive&query={quote(query)}&limit={limit}&filters=&sort=date&direction=desc"
        else:
            url = f"https://search-zzvl7ri3bq-uc.a.run.app/search?index=salesarchive&query={quote(query)}&page={page}&limit={limit}&filters=&sort=date&direction=desc"
        
        try:
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                data = response.json()
                hits = data.get('hits', [])
                total_hits = data.get('totalHits', 0)
                
                # Smart decision: Check if update needed (only on first page)
                if page == 0:
                    if db_count >= total_hits:
                        print(f"✅ Query '{query[:50]}...' already complete: DB={db_count}, API={total_hits}")
                        return []  # No update needed
                    else:
                        print(f"🔄 Query '{query[:50]}...' needs update: DB={db_count}, API={total_hits}")

                if not hits:
                    break
                
                # Process each hit in this page
                for hit in hits:
                    # Add metadata
                    hit['search_url'] = url
                    hit['search_query'] = query  # Add search query
                    hit['Tier'] = tier  # Add tier information (capital T to match main.py)
                    
                    if "cardId" in hit and hit["cardId"] != "" and hit["cardId"] is not None:
                        hit["Verified"] = True
                    else:
                        hit["Verified"] = False
                    if "date" in hit:
                        hit["date_normal"] = hit["date"].split("T")[0]
                    
                    # Check if this item already exists in our database
                    if "itemId" in hit and hit["itemId"]:
                        if check_if_item_exists_in_db(sql_handler, query, hit["itemId"]):
                            found_existing = True
                            print(f"✅ Found existing itemId '{hit['itemId']}' - stopping pagination")
                            break
                    
                    # Add to new data
                    new_data.append(hit)
                
                # If we found existing data, stop
                if found_existing:
                    break
                
                # Check if we've reached the end naturally
                if len(hits) < limit:
                    break
                
                # Calculate if there are more pages
                total_pages = math.ceil(total_hits / limit)
                if page >= total_pages:
                    break
                
                page += 1
                time.sleep(0.5)  # Be respectful to API
                
            else:
                print(f"❌ Failed to fetch page {page} for query '{query}'. Status code: {response.status_code}")
                break
                
        except Exception as e:
            print(f"❌ Error fetching page {page} for query '{query}': {str(e)}")
            break
    
    return new_data

def process_single_query_update(query_info, sql_handler, print_lock, stats_lock, batch_results):
    """
    Process a single query for daily update with optimized logic
    """
    query_index, query_and_tier_tuple = query_info
    
    # Extract query from tuple for display
    if isinstance(query_and_tier_tuple, tuple):
        query, tier = query_and_tier_tuple
    else:
        query = query_and_tier_tuple
        tier = ""
    
    try:
        # Fetch new data using optimized approach (single API call decision)
        new_data = fetch_new_data_for_query_optimized(query_and_tier_tuple, headers, sql_handler)
        
        if new_data:
            # Save to SQL database
            save_result = save_data_to_sql(new_data, sql_handler)
            
            with stats_lock:
                batch_results['total_records'] += len(new_data)
                batch_results['total_inserted'] += save_result['inserted']
                batch_results['total_duplicates'] += save_result['duplicates']
                batch_results['total_errors'] += save_result['errors']
            
            with print_lock:
                print(f"✅ Query {query_index}: {len(new_data)} new records, {save_result['inserted']} inserted")
        else:
            with print_lock:
                print(f"ℹ️ Query {query_index}: No new records found")
                
    except Exception as e:
        with stats_lock:
            batch_results['total_errors'] += 1
        with print_lock:
            print(f"❌ Error processing query '{query[:50]}...': {str(e)}")

def update_daily():
    """
    Main function for daily updates with optimized single-pass logic
    """
    print("🔄 Starting Daily Update Process")
    print("=" * 60)
    
    # Try to load config if it exists
    try:
        from config import MYSQL_CONFIG
        print("📋 Using configuration from config.py")
        sql_handler = SQLDBHandler(pool_size=30, **MYSQL_CONFIG)
    except ImportError:
        print("📋 No config.py found, using default settings")
        sql_handler = SQLDBHandler(pool_size=30)
    
    if not sql_handler.connect():
        print("❌ Failed to connect to SQL database. Exiting.")
        print("💡 Try running 'python test_mysql_connection.py' to test your connection")
        return
    
    try:
        # Step 1: Read queries and tiers from input.xlsx
        print("📖 Reading queries and tiers from input.xlsx...")
        input_data = read_excel_to_dict('input.xlsx', columns=['Queries', 'Tier'])
        all_queries = input_data['Queries']
        all_tiers = input_data['Tier']
        
        if not all_queries:
            print("⚠️ No queries found in input.xlsx")
            return
        
        # Create combined list of (query, tier) tuples
        combined_query_tier = []
        for i in range(len(all_queries)):
            query = all_queries[i]
            tier = all_tiers[i] if i < len(all_tiers) else ""
            combined_query_tier.append((query, tier))
        
        print(f"📊 Found {len(combined_query_tier)} queries to process")
        print("🚀 Starting optimized update process...\n")
        
        # Global statistics tracking
        global_stats = {
            'total_queries_processed': 0,
            'queries_up_to_date': 0,
            'queries_updated': 0,
            'total_records': 0,
            'total_inserted': 0,
            'total_duplicates': 0,
            'total_errors': 0
        }
        
        # Process queries in batches for better performance
        batch_size = 20
        total_batches = math.ceil(len(combined_query_tier) / batch_size)
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(combined_query_tier))
            batch_queries = combined_query_tier[start_idx:end_idx]
            
            print(f"Processing batch {batch_num + 1}/{total_batches} ({len(batch_queries)} queries)")
            
            # Process batch with multithreading
            batch_results = {
                'total_records': 0,
                'total_inserted': 0,
                'total_duplicates': 0,
                'total_errors': 0
            }
            
            print_lock = Lock()
            stats_lock = Lock()
            
            with ThreadPoolExecutor(max_workers=min(len(batch_queries), 20)) as executor:
                query_info_list = [(i + start_idx + 1, query_tier_tuple) for i, query_tier_tuple in enumerate(batch_queries)]
                futures = {
                    executor.submit(
                        process_single_query_update, 
                        query_info, 
                        sql_handler, 
                        print_lock, 
                        stats_lock, 
                        batch_results
                    ): query_info for query_info in query_info_list
                }
                
                # Wait for completion
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        query_info = futures[future]
                        with print_lock:
                            print(f"❌ Exception in thread for query '{query_info[1][:50]}...': {str(e)}")
                        with stats_lock:
                            batch_results['total_errors'] += 1
            
            # Update global statistics
            global_stats['total_queries_processed'] += len(batch_queries)
            global_stats['total_records'] += batch_results['total_records']
            global_stats['total_inserted'] += batch_results['total_inserted']
            global_stats['total_duplicates'] += batch_results['total_duplicates']
            global_stats['total_errors'] += batch_results['total_errors']
            
            # Count queries that were updated vs already up-to-date
            queries_with_data = sum(1 for i in range(len(batch_queries)) 
                                  if batch_results['total_records'] > 0)
            global_stats['queries_updated'] += queries_with_data
            global_stats['queries_up_to_date'] += len(batch_queries) - queries_with_data
            
            print(f"✅ Batch {batch_num + 1} completed - Records: {batch_results['total_records']}, Inserted: {batch_results['total_inserted']}")
            
            # Small delay between batches
            if batch_num < total_batches - 1:
                time.sleep(1)
        
        # Final comprehensive summary
        total_db_records = sql_handler.get_total_records()
        print(f"\n{'='*70}")
        print(f"🎉 DAILY UPDATE COMPLETED SUCCESSFULLY")
        print(f"{'='*70}")
        print(f"📊 Processing Summary:")
        print(f"   Total queries in input.xlsx: {len(all_queries)}")
        print(f"   Queries processed: {global_stats['total_queries_processed']}")
        print(f"   Queries already up-to-date: {global_stats['queries_up_to_date']}")
        print(f"   Queries that needed updates: {global_stats['queries_updated']}")
        print(f"")
        print(f"📈 Data Summary:")
        print(f"   New records fetched: {global_stats['total_records']}")
        print(f"   New records inserted: {global_stats['total_inserted']}")
        print(f"   Duplicates skipped: {global_stats['total_duplicates']}")
        print(f"   Errors encountered: {global_stats['total_errors']}")
        print(f"")
        print(f"🗄️ Database Status:")
        print(f"   Total records in SQL database: {total_db_records:,}")
        print(f"   Database: Jordan, Table: sales")
        
        # Success message
        if global_stats['total_inserted'] > 0:
            print(f"\n✅ Successfully updated database with {global_stats['total_inserted']:,} new records!")
        elif global_stats['queries_up_to_date'] == len(all_queries):
            print(f"\n✅ All queries are already up-to-date! No new records found.")
        else:
            print(f"\n✅ Update process completed.")
        
    except Exception as e:
        print(f"❌ Fatal error in daily update: {str(e)}")
    finally:
        sql_handler.close_connection()
        print(f"\n🔒 SQL database connection closed")

if __name__ == "__main__":
    update_daily()