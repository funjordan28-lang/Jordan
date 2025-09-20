from openpyxl import load_workbook
import pandas as pd


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
                from urllib.parse import unquote, parse_qs, urlparse
                parsed_url = urlparse(search_url)
                params = parse_qs(parsed_url.query)
                if 'query' in params:
                    query = unquote(params['query'][0])
                    processed_queries.add(query)
            except:
                pass
    return processed_queries
