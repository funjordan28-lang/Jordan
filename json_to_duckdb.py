#!/usr/bin/env python3
"""
json_to_duckdb.py

Purpose
-------
Pull documents from MongoDB, flatten nested JSON to a tabular shape, write them to Parquet,
and build/refresh a DuckDB database so you can run standard SQL.

Quick Start (Windows PowerShell)
--------------------------------
# 1) Install Python packages (in your project folder or venv)
pip install pymongo duckdb pandas pyarrow python-dotenv

# 2) Create a .env file next to this script (or edit the defaults below)
copy .env.example .env   # Windows PowerShell (or create manually)

# 3) Run the script
python json_to_duckdb.py --db your_db --collection your_collection

# 4) Open scraped.duckdb in a SQL tool (DuckDB CLI, VS Code SQL extension, or DBeaver) and run SQL:
-- Example (DuckDB CLI):
-- duckdb scraped.duckdb
-- .schema
-- SELECT COUNT(*) FROM scraped;

Notes
-----
- This script flattens nested objects (dicts) into columns using "__" as a separator.
- Arrays/lists are stored as JSON strings by default (so nothing is lost). You can still query
  those with DuckDB's JSON functions (json_extract, json_each) or choose to explode a specific
  array field into a separate child table with --explode <fieldname>.
- Re-running the script will *replace* the Parquet export and refresh the DuckDB table by default.
  (Use --append-parquet if you prefer to append to Parquet instead of replacing.)

"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import duckdb
import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.collection import Collection

# -------------------------
# Utilities
# -------------------------

def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def _as_json_str(x: Any) -> Any:
    # Convert lists/dicts to JSON strings to keep full fidelity in one column
    if isinstance(x, (list, dict)):
        try:
            return json.dumps(x, ensure_ascii=False)
        except Exception:
            return str(x)
    return x

def flatten_docs(docs: List[Dict[str, Any]], sep: str = "__") -> pd.DataFrame:
    if not docs:
        return pd.DataFrame()
    # Convert ObjectId to str and normalize
    for d in docs:
        if "_id" in d:
            d["_id"] = str(d["_id"])
    df = pd.json_normalize(docs, sep=sep, max_level=None)

    # Convert lists/dicts to JSON strings so schema remains rectangular
    for col in df.columns:
        df[col] = df[col].map(_as_json_str)

    # Best-effort datetime conversion without breaking non-date strings
    for col in df.columns:
        if df[col].dtype == "object":
            try:
                converted = pd.to_datetime(df[col], errors="ignore", utc=True)
                df[col] = converted
            except Exception:
                pass
    return df

def get_mongo_collection(uri: str, db: str, collection: str) -> Collection:
    client = MongoClient(uri)
    return client[db][collection]

def batched_find(coll: Collection, query: Dict[str, Any], projection: Optional[Dict[str, int]], batch_size: int) -> Iterable[List[Dict[str, Any]]]:
    cursor = coll.find(query, projection=projection, no_cursor_timeout=True).batch_size(batch_size)
    batch: List[Dict[str, Any]] = []
    try:
        for doc in cursor:
            batch.append(doc)
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch
    finally:
        cursor.close()

# -------------------------
# Main ETL
# -------------------------

def main() -> int:
    load_dotenv(override=False)

    parser = argparse.ArgumentParser(description="Flatten MongoDB JSON into DuckDB via Parquet.")
    parser.add_argument("--mongo-uri", default=os.getenv("MONGODB_URI", "mongodb://localhost:27017"), help="MongoDB connection URI")
    parser.add_argument("--db", required=False, default=os.getenv("MONGODB_DB"), help="MongoDB database name")
    parser.add_argument("--collection", required=False, default=os.getenv("MONGODB_COLLECTION"), help="MongoDB collection name")
    parser.add_argument("--query", default=os.getenv("MONGODB_QUERY", "{}"), help="JSON query filter, e.g. '{\"status\":\"active\"}'")
    parser.add_argument("--projection", default=os.getenv("MONGODB_PROJECTION", None), help="JSON projection, e.g. '{\"_id\":1, \"title\":1}'")
    parser.add_argument("--batch-size", type=int, default=int(os.getenv("BATCH_SIZE", "5000")), help="Mongo batch size")
    parser.add_argument("--out-parquet", default=os.getenv("OUT_PARQUET", "export/export.parquet"), help="Parquet output file (or directory if --append-parquet)")
    parser.add_argument("--append-parquet", action="store_true", help="Append batches to a Parquet dataset (directory) instead of replacing a single file")
    parser.add_argument("--duckdb-file", default=os.getenv("DUCKDB_FILE", "scraped.duckdb"), help="DuckDB file path")
    parser.add_argument("--table", default=os.getenv("DUCKDB_TABLE", "scraped"), help="DuckDB table name to create/refresh")
    parser.add_argument("--explode", default=os.getenv("EXPLODE_FIELD", None), help="OPTIONAL: name of an array field to explode into a child table")
    parser.add_argument("--explode-table", default=os.getenv("EXPLODE_TABLE", "scraped_items"), help="Name of the child table for exploded array")
    args = parser.parse_args()

    if not args.db or not args.collection:
        print("ERROR: You must provide --db and --collection (or set MONGODB_DB/MONGODB_COLLECTION in .env).", file=sys.stderr)
        return 2

    try:
        query_dict = json.loads(args.query)
    except json.JSONDecodeError as e:
        print(f"ERROR: --query is not valid JSON: {e}", file=sys.stderr)
        return 2

    projection_dict: Optional[Dict[str, int]] = None
    if args.projection:
        try:
            projection_dict = json.loads(args.projection)
        except json.JSONDecodeError as e:
            print(f"ERROR: --projection is not valid JSON: {e}", file=sys.stderr)
            return 2

    out_parquet_path = Path(args.out_parquet)
    duckdb_path = Path(args.duckdb_file)
    _ensure_dir(out_parquet_path.parent)

    coll = get_mongo_collection(args.mongo_uri, args.db, args.collection)

    # If not appending, remove previous parquet to avoid schema conflicts
    if not args.append_parquet and out_parquet_path.exists():
        out_parquet_path.unlink()

    total = 0
    part = 0
    parquet_files: List[str] = []

    for batch in batched_find(coll, query_dict, projection_dict, args.batch_size):
        df = flatten_docs(batch)

        if df.empty:
            continue

        if args.append_parquet:
            # Write as a dataset of parquet files
            part += 1
            part_path = out_parquet_path if out_parquet_path.suffix == "" else out_parquet_path.parent
            if out_parquet_path.suffix != "":
                # If user provided a file path, switch to a directory sibling named without suffix
                part_path = out_parquet_path.with_suffix("")
            _ensure_dir(Path(part_path))

            file_path = Path(part_path) / f"part_{part:05d}.parquet"
            df.to_parquet(file_path, index=False)
            parquet_files.append(str(file_path))
        else:
            # Single file overwrite on first batch, append by combining in-memory thereafter
            if total == 0:
                df.to_parquet(out_parquet_path, index=False)
                parquet_files = [str(out_parquet_path)]
            else:
                # Append by reading old + concat (OK for moderate sizes)
                old = pd.read_parquet(out_parquet_path)
                all_df = pd.concat([old, df], ignore_index=True)
                all_df.to_parquet(out_parquet_path, index=False)
            part += 1

        total += len(df)

    if total == 0:
        print("No documents found with the given query/projection.")
        return 0

    # Build/refresh DuckDB table from Parquet(s)
    con = duckdb.connect(str(duckdb_path))
    con.execute(f"PRAGMA threads=4;")  # modest parallelism

    if args.append_parquet:
        parquet_glob = str((out_parquet_path if out_parquet_path.suffix == "" else out_parquet_path.with_suffix("")).resolve() / "*.parquet")
        con.execute(f"""
            CREATE OR REPLACE TABLE {args.table} AS
            SELECT * FROM read_parquet('{parquet_glob}', union_by_name=true);
        """)
    else:
        con.execute(f"""
            CREATE OR REPLACE TABLE {args.table} AS
            SELECT * FROM read_parquet('{str(out_parquet_path.resolve())}', union_by_name=true);
        """)

    # Optional: explode a JSON array field into a child table
    if args.explode:
        # We stored arrays as JSON strings; use DuckDB JSON functions to explode
        con.execute(f"DROP TABLE IF EXISTS {args.explode_table};")
        con.execute(f"""
            CREATE TABLE {args.explode_table} AS
            WITH base AS (
                SELECT *, json_parse({args.table}.{args.explode}) AS arr_json
                FROM {args.table}
                WHERE {args.explode} IS NOT NULL
            ),
            exploded AS (
                SELECT
                    base._id AS parent_id,
                    json_extract(e.value, '$') AS item_json
                FROM base,
                LATERAL json_each(base.arr_json) AS e
            )
            SELECT * FROM exploded;
        """)

    # Helpful output
    print(f"Exported {total} documents into {len(parquet_files) if args.append_parquet else 1} Parquet file(s).")
    print(f"DuckDB file: {duckdb_path.resolve()}  |  Table: {args.table}")
    if args.explode:
        print(f"Exploded child table created: {args.explode_table}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
