import requests
from urllib.parse import urlencode, quote
from utils import read_excel_to_dict, write_data_to_excel, load_existing_data, get_processed_queries
import math
import time
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
        
        print(f"Fetching page {page} for query '{query}'...")
        print(f"URL: {url}")
        
        try:
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                data = response.json()
                hits = data.get('hits', [])
                total_hits = data.get('totalHits', 0)
                
                print(f"Page {page}: Found {len(hits)} items (Total: {total_hits})")
                
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
                    print(f"Reached end of results for query '{query}'. Total pages: {page}")
                    break
                
                # Calculate if there are more pages
                total_pages = math.ceil(total_hits / limit)
                if page >= total_pages:
                    print(f"Reached maximum pages ({total_pages}) for query '{query}'")
                    break
                
                page += 1
                
                # Add a small delay between requests to be respectful
                time.sleep(0.5)
                
            else:
                print(f"Failed to fetch page {page} for query '{query}'. Status code: {response.status_code}")
                break
                
        except Exception as e:
            print(f"Error fetching page {page} for query '{query}': {str(e)}")
            break
    
    return all_data


def fetch_data():
    input_data = read_excel_to_dict('input.xlsx', columns=['Queries'])
    output_file = 'output.xlsx'
    
    # Load existing data to check what's already been processed
    existing_data = load_existing_data(output_file)
    processed_queries = get_processed_queries(existing_data)
    
    print(f"Total queries to process: {len(input_data['Queries'])}")
    print(f"Already processed queries: {len(processed_queries)}")
    
    # Start with existing data
    all_collected_data = existing_data
    
    for query_index, query in enumerate(input_data['Queries'], 1):
        # Skip if already processed
        if query in processed_queries:
            print(f"\nQuery {query_index}/{len(input_data['Queries'])}: '{query}' - SKIPPED (already processed)")
            continue
            
        print(f"\n{'='*60}")
        print(f"Processing query {query_index}/{len(input_data['Queries'])}: '{query}'")
        print(f"{'='*60}")
        
        try:
            query_data = fetch_all_data_for_query(query, headers)
            all_collected_data.extend(query_data)
            
            print(f"Collected {len(query_data)} items for query '{query}'")
            
            # Save data after each query
            if all_collected_data:
                write_data_to_excel(all_collected_data, output_file)
                print(f"✓ Data saved after processing query {query_index}/{len(input_data['Queries'])}")
                print(f"✓ Total records in file: {len(all_collected_data)}")
            else:
                print("No data to save yet.")
                
        except Exception as e:
            print(f"ERROR processing query '{query}': {str(e)}")
            print(f"Continuing with next query...")
            continue
    
    print(f"\n{'='*60}")
    print(f"FINAL SUMMARY")
    print(f"{'='*60}")
    print(f"Total queries processed: {len(input_data['Queries']) - len(processed_queries)}")
    print(f"Total records in final file: {len(all_collected_data)}")
    print(f"Output saved to: {output_file}")

if __name__ == "__main__":
    fetch_data()