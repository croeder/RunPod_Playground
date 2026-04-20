#!/usr/bin/env python3
"""
Exploration script: loads Monarch subset into an in-memory DuckDB instance.
Run after download_monarch_subset.py.
"""
import duckdb
import pandas as pd
from pathlib import Path

DB_PATH = "data/monarch_subset.duckdb"

for p in [DB_PATH]:
    if not Path(p).exists():
        raise FileNotFoundError(f"Run load_monarch_to_duckdb.py first to create {p}")

con = duckdb.connect()  # in-memory
con.execute(f"ATTACH '{DB_PATH}' AS monarch (READ_ONLY)")
con.execute("USE monarch")

print(con.execute("SHOW TABLES").df())
print(con.execute("SELECT COUNT(*) FROM edges").df())
print(con.execute("DESCRIBE edges").df())
print(con.execute("SELECT COUNT(*) FROM nodes").df())
print(con.execute("DESCRIBE nodes").df())
