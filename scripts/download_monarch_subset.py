#!/usr/bin/env python3
"""
Downloads a subset of the Monarch KG to local TSV files.
Edges are downloaded first by association type, then nodes are filtered
to only those referenced by those edges.
Run once; outputs to data/.
"""
import pandas as pd
from pathlib import Path

Path("data").mkdir(exist_ok=True)

BASE = "https://data.monarchinitiative.org/monarch-kg/latest/tsv/all_associations"
NODES_URL = "https://data.monarchinitiative.org/monarch-kg/latest/monarch-kg_nodes.neo4j.csv"

EDGE_FILES = [
    "causal_gene_to_disease_association.all.tsv.gz",
    "disease_to_phenotypic_feature_association.all.tsv.gz",
    "gene_to_phenotypic_feature_association.all.tsv.gz",
    "pairwise_gene_to_gene_interaction.all.tsv.gz",
    "correlated_gene_to_disease_association.all.tsv.gz",
    "gene_to_gene_homology_association.all.tsv.gz",
]

# --- EDGES ---
dfs = []
for f in EDGE_FILES:
    print(f"Downloading {f}...")
    df = pd.read_csv(f"{BASE}/{f}", sep="\t", compression="gzip", low_memory=False)
    print(f"  {len(df)} rows, columns: {list(df.columns)}")
    dfs.append(df)

edges = pd.concat(dfs, ignore_index=True)
print(f"\nTotal edges: {len(edges)}")
edges.to_csv("data/monarch_subset_edges.tsv", sep="\t", index=False)
print("Saved to data/monarch_subset_edges.tsv")

# --- NODES ---
# Get the unique node IDs referenced in our edges
node_ids = set(edges["subject"]).union(set(edges["object"]))
print(f"\nUnique node IDs referenced in edges: {len(node_ids)}")

print("Downloading full nodes file (this may take a while)...")
nodes = pd.read_csv(NODES_URL, low_memory=False)
print(f"  Full node count: {len(nodes)}")

nodes_filtered = nodes[nodes["id"].isin(node_ids)]
print(f"  Filtered node count: {len(nodes_filtered)}")

nodes_filtered.to_csv("data/monarch_subset_nodes.tsv", sep="\t", index=False)
print("Saved to data/monarch_subset_nodes.tsv")
