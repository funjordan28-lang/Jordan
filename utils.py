from openpyxl import load_workbook
import pandas as pd
from pymongo import MongoClient, errors
import logging
from urllib.parse import unquote, parse_qs, urlparse


def read_excel_to_dict(file_path, columns=None):
    """
    Read columns from Sheet1 of an Excel file and return data in dictionary format.
    
    Args:
        file_path: Path to the Excel file
        columns: List of column names to read. If None, reads all columns
    
    Returns:
        Dictionary where keys are column headers and values are lists of cell values
    
    Example:
        # Read all columns
        data = read_excel_to_dict('input.xlsx')
        
        # Read specific columns
        data = read_excel_to_dict('input.xlsx', columns=['Name', 'Age', 'Email'])
    """
    workbook = load_workbook(filename=file_path, read_only=True, data_only=True)
    worksheet = workbook['Sheet1']
    
    # Read headers from first row
    headers = []
    for col in range(1, worksheet.max_column + 1):
        cell_value = worksheet.cell(row=1, column=col).value
        if cell_value is not None:
            headers.append(str(cell_value))
        else:
            headers.append(f"Column_{col}")
    
    # Determine which columns to read
    if columns is None:
        # Read all columns
        column_indices = list(range(1, len(headers) + 1))
        column_headers = headers
    else:
        column_indices = []
        column_headers = []
        for col in columns:
            if col in headers:
                col_index = headers.index(col) + 1
                column_indices.append(col_index)
                column_headers.append(col)
    
    # Initialize result dictionary
    result_dict = {}
    for header in column_headers:
        result_dict[header] = []
    
    # Read data from row 2 onwards
    for row in range(2, worksheet.max_row + 1):
        for i, col_index in enumerate(column_indices):
            cell_value = worksheet.cell(row=row, column=col_index).value
            if cell_value is None:
                cell_value = ""
            result_dict[column_headers[i]].append(cell_value)
    
    workbook.close()
    return result_dict


def write_data_to_excel(data_list, output_file='output.xlsx'):
    """
    Write list of dictionaries to Excel file.
    
    Args:
        data_list: List of dictionaries containing the scraped data
        output_file: Path to the output Excel file
    
    Returns:
        None
    """
    if not data_list:
        print("No data to write to Excel")
        return
    
    # Convert list of dictionaries to DataFrame
    df = pd.DataFrame(data_list)
    
    # Write to Excel file
    df.to_excel(output_file, index=False, engine='openpyxl')
    print(f"Data successfully written to {output_file}")
    print(f"Total records: {len(data_list)}")


def load_existing_data(output_file='output.xlsx'):
    """
    Load existing data from Excel file if it exists.
    
    Args:
        output_file: Path to the output Excel file
    
    Returns:
        List of dictionaries containing existing data, empty list if file doesn't exist
    """
    try:
        df = pd.read_excel(output_file, engine='openpyxl')
        return df.to_dict('records')
    except FileNotFoundError:
        print(f"No existing file found at {output_file}. Starting fresh.")
        return []
    except Exception as e:
        print(f"Error loading existing data: {str(e)}. Starting fresh.")
        return []


def get_processed_queries(existing_data):
    """
    Get list of unique queries that have already been processed.
    
    Args:
        existing_data: List of dictionaries containing existing data
    
    Returns:
        Set of processed query strings
    """
    processed_queries = set()
    for item in existing_data:
        search_url = item.get('search_url', '')
        if search_url:
            # Extract query from search_url
            try:
                parsed_url = urlparse(search_url)
                params = parse_qs(parsed_url.query)
                if 'query' in params:
                    query = unquote(params['query'][0])
                    processed_queries.add(query)
            except:
                pass
    return processed_queries


# MongoDB utility functions
class MongoDBHandler:
    def __init__(self, connection_string="mongodb://localhost:27017/", database_name="Jordan", collection_name="sales"):
        """
        Initialize MongoDB connection
        
        Args:
            connection_string: MongoDB connection string
            database_name: Name of the database
            collection_name: Name of the collection
        """
        self.connection_string = connection_string
        self.database_name = database_name
        self.collection_name = collection_name
        self.client = None
        self.db = None
        self.collection = None
        
    def connect(self):
        """Establish connection to MongoDB"""
        try:
            self.client = MongoClient(self.connection_string)
            # Test the connection
            self.client.admin.command('ping')
            self.db = self.client[self.database_name]
            self.collection = self.db[self.collection_name]
            
            # Create unique index on itemId to ensure uniqueness
            try:
                self.collection.create_index("itemId", unique=True)
            except errors.OperationFailure:
                # Index might already exist
                pass
                
            return True
        except Exception as e:
            print(f"❌ Failed to connect to MongoDB: {str(e)}")
            return False
    
    def insert_data_batch(self, data_list):
        """
        Insert batch of data with itemId uniqueness handling
        
        Args:
            data_list: List of dictionaries to insert
            
        Returns:
            Dictionary with insertion results
        """
        if not data_list:
            return {"inserted": 0, "duplicates": 0, "errors": 0}
        
        inserted_count = 0
        duplicate_count = 0
        error_count = 0
        
        for item in data_list:
            try:
                if 'itemId' in item:
                    # Try to insert, will fail if itemId already exists
                    self.collection.insert_one(item)
                    inserted_count += 1
                else:
                    error_count += 1
            except errors.DuplicateKeyError:
                duplicate_count += 1
            except Exception as e:
                error_count += 1
                print(f"❌ Error inserting item {item.get('itemId', 'Unknown')}: {str(e)}")
        
        result = {
            "inserted": inserted_count,
            "duplicates": duplicate_count,
            "errors": error_count
        }
        
        return result
    
    def get_processed_queries_from_db(self):
        """
        Get list of unique queries that have already been processed from MongoDB
        
        Returns:
            Set of processed query strings
        """
        processed_queries = set()
        try:
            # Get distinct search_url values
            search_urls = self.collection.distinct("search_url")
            
            for search_url in search_urls:
                if search_url:
                    try:
                        parsed_url = urlparse(search_url)
                        params = parse_qs(parsed_url.query)
                        if 'query' in params:
                            query = unquote(params['query'][0])
                            processed_queries.add(query)
                    except:
                        pass
        except Exception as e:
            print(f"❌ Error getting processed queries from DB: {str(e)}")
        
        return processed_queries
    
    def get_total_records(self):
        """Get total number of records in the collection"""
        try:
            return self.collection.count_documents({})
        except Exception as e:
            print(f"❌ Error getting record count: {str(e)}")
            return 0
    
    def close_connection(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()


def save_data_to_mongodb(data_list, mongo_handler):
    """
    Save list of dictionaries to MongoDB with itemId uniqueness handling
    
    Args:
        data_list: List of dictionaries containing the scraped data
        mongo_handler: MongoDBHandler instance
    
    Returns:
        Dictionary with insertion results
    """
    if not data_list:
        return {"inserted": 0, "duplicates": 0, "errors": 0}
    
    result = mongo_handler.insert_data_batch(data_list)
    return result
