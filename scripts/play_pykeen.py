#!/usr/bin/env python3
"""
Exploration script: trains a PyKEEN model on the Monarch subset.
Run after load_monarch_to_duckdb.py.
"""
import duckdb
import pandas as pd
from pathlib import Path
from pykeen.triples import TriplesFactory
from pykeen.pipeline import pipeline

DB_PATH = "data/monarch_subset.duckdb"

if not Path(DB_PATH).exists():
    raise FileNotFoundError(f"Run load_monarch_to_duckdb.py first to create {DB_PATH}")

con = duckdb.connect()  # in-memory
con.execute(f"ATTACH '{DB_PATH}' AS monarch (READ_ONLY)")
con.execute("USE monarch")

# Pull subject/predicate/object triples from edges
triples_df = con.execute("""
    SELECT subject, predicate, object
    FROM edges
    WHERE predicate IN ('biolink:has_phenotype') 
""").df()
    #WHERE predicate IN ('biolink:has_phenotype', 'biolink:causes') 
    #USING SAMPLE 50 PERCENT
con.close()

print(f"Triples: {len(triples_df)}")
print(triples_df.head())

# Build PyKEEN TriplesFactory from the DataFrame
tf = TriplesFactory.from_labeled_triples(
    triples_df.values,
    create_inverse_triples=True,
)
print(f"Entities: {tf.num_entities}, Relations: {tf.num_relations}")

training, testing = tf.split([0.9, 0.1], random_state=42)

result = pipeline(
    training=training,
    testing=testing,
    model="TransE",
    training_kwargs=dict(num_epochs=50, batch_size=512, pin_memory=False),
    device="mps",
)
    #training_kwargs=dict(num_epochs=5, batch_size=512, pin_memory=False),

print(result.metric_results.to_df())
