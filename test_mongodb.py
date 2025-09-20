#!/usr/bin/env python3
"""
Test script to validate MongoDB setup for the Jordan scraper
"""

from utils import MongoDBHandler

def test_mongodb_connection():
    """Test MongoDB connection and basic operations"""
    print("🔍 Testing MongoDB connection...")
    
    # Initialize MongoDB handler
    mongo_handler = MongoDBHandler()
    
    # Test connection
    if mongo_handler.connect():
        print("✅ MongoDB connection successful!")
        
        # Test getting total records
        total_records = mongo_handler.get_total_records()
        print(f"📊 Current records in collection: {total_records}")
        
        # Test getting processed queries
        processed_queries = mongo_handler.get_processed_queries_from_db()
        print(f"📝 Previously processed queries: {len(processed_queries)}")
        
        # Test inserting a sample record
        test_data = [{
            "itemId": "test_item_123",
            "title": "Test Item",
            "price": 100,
            "search_url": "https://example.com/search?query=test",
            "Verified": True
        }]
        
        print("\n🧪 Testing data insertion...")
        result = mongo_handler.insert_data_batch(test_data)
        print(f"Insert test result: {result}")
        
        # Try inserting the same data again to test uniqueness
        print("\n🔄 Testing duplicate itemId handling...")
        result2 = mongo_handler.insert_data_batch(test_data)
        print(f"Duplicate test result: {result2}")
        
        # Clean up test data
        try:
            mongo_handler.collection.delete_one({"itemId": "test_item_123"})
            print("🧹 Cleaned up test data")
        except Exception as e:
            print(f"⚠️ Could not clean up test data: {e}")
        
        mongo_handler.close_connection()
        print("\n✅ All tests completed successfully!")
        return True
    else:
        print("❌ MongoDB connection failed!")
        print("\n📋 Troubleshooting steps:")
        print("1. Make sure MongoDB is installed and running")
        print("2. Check if MongoDB service is started")
        print("3. Verify connection string (default: mongodb://localhost:27017/)")
        print("4. Install pymongo: pip install pymongo")
        return False

if __name__ == "__main__":
    test_mongodb_connection()