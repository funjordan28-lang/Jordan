# CardLadder Michael Jordan Card Sales Archive Scraper

A Python web scraper designed to extract comprehensive sales data for Michael Jordan trading cards from the CardLadder search API. This tool processes multiple search queries, fetches all available data across paginated results, and exports everything to Excel format with resume capability.

## 🚀 Features

- **Comprehensive Data Extraction**: Scrapes all pages of results with 50 items per page (increased from default 20)
- **Resume Capability**: Automatically skips already processed queries if script is interrupted
- **Progress Tracking**: Shows detailed progress with query counts and record totals
- **Search URL Tracking**: Each record includes the search URL used to fetch that data
- **Excel Export**: Clean Excel output with proper column headers
- **Error Handling**: Robust error handling with detailed logging
- **Rate Limiting**: Built-in delays between requests to be respectful to the API

## 📁 Project Structure

```
JordanScraper/
├── main.py              # Main scraper script
├── utils.py             # Utility functions for Excel I/O and data processing
├── requirements.txt     # Python dependencies
├── input.xlsx          # Input file with search queries
├── output.xlsx         # Generated output file with scraped data
└── README.md           # This documentation
```

## 🛠️ Installation

1. **Clone or download the repository**
   ```bash
   cd JordanScraper
   ```

2. **Set up Python virtual environment (recommended)**
   ```bash
   python -m venv .venv
   .venv\Scripts\activate  # On Windows
   source .venv/bin/activate  # On macOS/Linux
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

## 📋 Requirements

- Python 3.7+
- pandas >= 1.5.0
- requests >= 2.25.0
- openpyxl >= 3.0.0

## 📖 Usage

### 1. Prepare Input File

Create or modify `input.xlsx` with a column named "Queries" containing your search terms:

| Queries |
|---------|
| jordan linchpins |
| 1998 Metal Universe Linchpins Michael Jordan 8 |
| 1984 Star Michael Jordan 101 |

### 2. Run the Scraper

```bash
python main.py
```

### 3. Output

The scraper will:
- Process each query sequentially
- Fetch all pages of results (50 items per page)
- Save data after each completed query
- Create `output.xlsx` with all scraped data

## 📊 Output Data Structure

Each row in the output Excel file contains:

| Field | Description |
|-------|-------------|
| itemId | Unique identifier for the item |
| title | Item title/description |
| price | Sale price |
| date | Sale date |
| platform | Marketplace (eBay, Goldin, Fanatics, etc.) |
| seller | Seller information |
| condition | Card condition/grade |
| gradingCompany | Grading company (PSA, BGS, etc.) |
| image | Image URL |
| url | Item listing URL |
| search_url | API search URL used to fetch this record |
| ...and many more fields |

## 🔄 Resume Functionality

If the script is interrupted:

1. **Automatic Detection**: The script detects existing output data
2. **Query Skipping**: Already processed queries are automatically skipped
3. **Progress Display**: Shows which queries were already completed
4. **Continuation**: Resumes from the next unprocessed query

Example output:
```
Total queries to process: 379
Already processed queries: 5
Query 6/379: '1997 Metal Universe Precious Metal Gems Michael Jordan 23' - Processing...
```

## ⚙️ Configuration

### Headers and Authentication

The script includes pre-configured headers with authentication token. If you need to update the token:

1. Open `main.py`
2. Update the `authorization` field in the `headers` dictionary
3. Replace with your valid Bearer token

### Rate Limiting

Default delay between requests is 0.5 seconds. To modify:
```python
time.sleep(0.5)  # Adjust this value in fetch_all_data_for_query()
```

### Output Limits

- **Items per page**: 50 (configurable via `limit` variable)
- **Maximum queries**: Currently limited to first 5 queries for testing (remove `[:5]` in line 97 for full processing)

## 🐛 Error Handling

The scraper handles various error scenarios:

- **Network timeouts**: Continues with next page/query
- **Invalid responses**: Logs error and continues
- **File access issues**: Detailed error messages
- **Data corruption**: Validates data before saving

## 📝 Example Usage

```bash
# Run the full scraper
python main.py

# Check progress
# The script will show output like:
Processing query 15/379: '1995 Metal Michael Jordan Silver Spotlight 13'
Page 2: Found 50 items (Total: 127)
Collected 127 items for query '1995 Metal Michael Jordan Silver Spotlight 13'
✓ Data saved after processing query 15/379
✓ Total records in file: 2,847
```

## 🔍 Data Sources

This scraper connects to the CardLadder search API:
- **Base URL**: `https://search-zzvl7ri3bq-uc.a.run.app/search`
- **Index**: salesarchive
- **Sort**: date (descending)
- **Pagination**: Automatic handling of all pages

## ⚠️ Important Notes

1. **Authentication**: Requires valid CardLadder authentication token
2. **Rate Limiting**: Be respectful with request frequency
3. **Data Volume**: Some queries return thousands of records
4. **File Size**: Output Excel files can become large with extensive data
5. **Legal Compliance**: Ensure scraping complies with CardLadder's terms of service

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## 📄 License

This project is for educational and personal use. Please respect CardLadder's terms of service and rate limits.

## 🆘 Troubleshooting

### Common Issues

1. **"Failed to fetch" errors**
   - Check internet connection
   - Verify authentication token is valid
   - Ensure CardLadder API is accessible

2. **"Permission denied" errors**
   - Check file permissions
   - Ensure Excel file isn't open in another program
   - Verify write permissions in output directory

3. **"No data collected" messages**
   - Verify query syntax in input.xlsx
   - Check if queries return results manually
   - Ensure proper column naming in input file

### Debug Mode

For detailed debugging, add print statements or modify the logging level in the script.

---

**Last Updated**: September 2025
**Version**: 1.0.0
