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
from utils import MongoDBHandler, save_data_to_mongodb, read_excel_to_dict
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import math

# API headers (same as main.py)
headers = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "en-US,en;q=0.9",
    "authorization": "Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6IjA1NTc3MjZmYWIxMjMxZmEyZGNjNTcyMWExMDgzZGE2ODBjNGE3M2YiLCJ0eXAiOiJKV1QifQ.eyJpc3MiOiJodHRwczovL3NlY3VyZXRva2VuLmdvb2dsZS5jb20vY2FyZGxhZGRlci03MWQ1MyIsImF1ZCI6ImNhcmRsYWRkZXItNzFkNTMiLCJhdXRoX3RpbWUiOjE3NTg4ODgyNzMsInVzZXJfaWQiOiJwTTUzUjRXQ21nUXhqUVdwR0pRWjFSeVFWcE8yIiwic3ViIjoicE01M1I0V0NtZ1F4alFXcEdKUVoxUnlRVnBPMiIsImlhdCI6MTc1ODg4ODI3MywiZXhwIjoxNzU4ODkxODczLCJlbWFpbCI6ImZ1bmpvcmRhbjI4QGdtYWlsLmNvbSIsImVtYWlsX3ZlcmlmaWVkIjpmYWxzZSwiZmlyZWJhc2UiOnsiaWRlbnRpdGllcyI6eyJlbWFpbCI6WyJmdW5qb3JkYW4yOEBnbWFpbC5jb20iXX0sInNpZ25faW5fcHJvdmlkZXIiOiJwYXNzd29yZCJ9fQ.jSy0bb6WXkF_HfEelcGDVfgj9XqsPayjEDUzP1zQH28pgz87CXxPjjyQbSw4bwhJdvfLawbVRaX0HRn1hIUbvzF9T20SQM1JrTAf5sFG2u3Q8FM_oP9xGtpuQcMhOGyX_UgiVHWNZ_RD0AR4DsubaYiuFVFLwqeLRbi0YCotq0P4iZgpHzfp4MOcgmr-B4Gdgw9jBvOOzcyK_TQSPZbD-WvLd-ryQlzDt8oR5XRa3pgenLNZoJ4Oa5QGgy6TykTxK-ZDTw3sNtWTnmwcjGX2Jvwn8U280PmekGoFz6NLIwQflMw40n_ufxsCszXsw2vdnVoyZQECkDpqzCC8mNwuyg",
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

def fetch_new_data_for_query_optimized(query, headers, mongo_handler):
    """
    Optimized: Make single API call to determine if update needed, then process efficiently
    
    Args:
        query: Search query string
        headers: HTTP headers for the request
        mongo_handler: MongoDBHandler instance
    
    Returns:
        List of new data (items not yet in database)
    """
    # Step 1: Get current DB count
    db_count = get_mongodb_count_for_query(mongo_handler, query)
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
                    if "cardId" in hit and hit["cardId"] != "" and hit["cardId"] is not None:
                        hit["Verified"] = True
                    else:
                        hit["Verified"] = False
                    if "date" in hit:
                        hit["date_normal"] = hit["date"].split("T")[0]
                    
                    # Check if this item already exists in our database
                    if "itemId" in hit and hit["itemId"]:
                        if check_if_item_exists_in_db(mongo_handler, query, hit["itemId"]):
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

def process_single_query_update(query_info, mongo_handler, print_lock, stats_lock, batch_results):
    """
    Process a single query for daily update with optimized logic
    """
    query_index, query = query_info
    
    try:
        # Fetch new data using optimized approach (single API call decision)
        new_data = fetch_new_data_for_query_optimized(query, headers, mongo_handler)
        
        if new_data:
            # Save to MongoDB
            save_result = save_data_to_mongodb(new_data, mongo_handler)
            
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
    
    # Initialize MongoDB handler
    mongo_handler = MongoDBHandler()
    if not mongo_handler.connect():
        print("❌ Failed to connect to MongoDB. Exiting.")
        return
    
    try:
        # Step 1: Read queries from input.xlsx
        print("📖 Reading queries from input.xlsx...")
        input_data = read_excel_to_dict('input.xlsx', columns=['Queries'])
        all_queries = input_data['Queries']
        
        if not all_queries:
            print("⚠️ No queries found in input.xlsx")
            return
        
        print(f"📊 Found {len(all_queries)} queries to process")
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
        total_batches = math.ceil(len(all_queries) / batch_size)
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(all_queries))
            batch_queries = all_queries[start_idx:end_idx]
            
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
                query_info_list = [(i + start_idx + 1, query) for i, query in enumerate(batch_queries)]
                futures = {
                    executor.submit(
                        process_single_query_update, 
                        query_info, 
                        mongo_handler, 
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
        total_db_records = mongo_handler.get_total_records()
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
        print(f"   Total records in MongoDB: {total_db_records:,}")
        print(f"   Database: Jordan, Collection: sales")
        
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
        mongo_handler.close_connection()
        print(f"\n🔒 MongoDB connection closed")

if __name__ == "__main__":
    update_daily()