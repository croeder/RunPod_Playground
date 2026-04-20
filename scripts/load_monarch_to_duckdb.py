#!/usr/bin/env python3
"""
Loads locally downloaded Monarch subset TSVs into a persistent DuckDB database.
Run after download_monarch_subset.py.
"""
import duckdb
import pandas as pd
from pathlib import Path

DB_PATH = "data/monarch_subset.duckdb"

if not Path("data/monarch_subset_edges.tsv").exists():
    raise FileNotFoundError("Run download_monarch_subset.py first.")
if not Path("data/monarch_subset_nodes.tsv").exists():
    raise FileNotFoundError("Run download_monarch_subset.py first.")

con = duckdb.connect(DB_PATH)

# --- EDGES ---
print("Loading edges...")
edges = pd.read_csv("data/monarch_subset_edges.tsv", sep="\t", low_memory=False)
con.register("edges_df", edges)
con.execute("CREATE OR REPLACE TABLE edges AS SELECT * FROM edges_df")
con.unregister("edges_df")
print(f"  Loaded {len(edges)} edges")
print(f"EDGES columns {edges.columns.tolist()}")

result = con.execute("""
    SELECT predicate, COUNT(*) as count
    FROM edges
    GROUP BY predicate
    ORDER BY count DESC
""").df()
print("\nEdge counts by predicate:")
print(result)

# --- NODES ---
print("\nLoading nodes...")
nodes = pd.read_csv("data/monarch_subset_nodes.tsv", sep="\t", low_memory=False)
print(f"  Loaded {len(nodes)} nodes")

# Clean up Neo4j-style column names
nodes = nodes.rename(columns={
    "category:string[]": "category",
    "xref:string[]": "xref",
    "synonym:string[]": "synonym",
    "exact_synonym:string[]": "exact_synonym",
    "broad_synonym:string[]": "broad_synonym",
    "narrow_synonym:string[]": "narrow_synonym",
    "related_synonym:string[]": "related_synonym",
    "has_attribute:string[]": "has_attribute",
    "has_gene:string[]": "has_gene",
    "same_as:string[]": "same_as",
    "deprecated:boolean": "deprecated",
})
print(f"NODES columns {nodes.columns.tolist()}")
con.register("nodes_df", nodes)
con.execute("CREATE OR REPLACE TABLE nodes AS SELECT * FROM nodes_df")
con.unregister("nodes_df")

result = con.execute("""
    SELECT category, COUNT(*) as count
    FROM nodes
    GROUP BY category
    ORDER BY count DESC
""").df()
print("\nNode counts by category:")
print(result)




con.close()
print(f"\nDatabase saved to {DB_PATH}")
