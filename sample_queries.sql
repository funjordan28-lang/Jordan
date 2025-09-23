-- sample_queries.sql
-- Run these inside DuckDB (CLI or any SQL IDE connected to scraped.duckdb)

-- See tables
PRAGMA show_tables;

-- Inspect table structure
DESCRIBE scraped;

-- Count records
SELECT COUNT(*) AS row_count FROM scraped;

-- Example: top values for a categorical column (replace `source` with a real column name)
-- SELECT source, COUNT(*) FROM scraped GROUP BY source ORDER BY COUNT(*) DESC;

-- Example: recent items by date (replace `published_at` with a real timestamp column)
-- SELECT * FROM scraped ORDER BY published_at DESC LIMIT 25;

-- If you stored arrays as JSON in a column named `items`, this shows how to inspect one element path:
-- SELECT _id, json_extract(items, '$[0]') AS first_item FROM scraped LIMIT 10;

-- If you used --explode items, the exploded child table keeps a per-item JSON column `item_json`:
-- SELECT parent_id, json_extract(item_json, '$.name') AS item_name FROM scraped_items LIMIT 20;