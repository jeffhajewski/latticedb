# Performance Tuning

This guide covers the key parameters for tuning LatticeDB performance.

## Cache Size

The `cache_size_mb` parameter controls how many database pages are cached in memory. Larger caches reduce disk I/O.

```python
db = Database("mydb.db", cache_size_mb=200)  # 200 MB cache
db.open()
```

**Guidelines:**
- Default is 100 MB in the C API and every binding (64 MB in the CLI), which handles most workloads well
- For large databases (1M+ nodes), increase to 200-500 MB
- For memory-constrained environments, reduce to 50 MB or less
- The cache stores fixed-size pages (4 KB each at the default page size), so 100 MB holds ~25,000 pages
- The `LATTICE_BUFFER_POOL_MB` environment variable overrides the size, including an explicit `cache_size_mb`
- In-memory (`:memory:`) databases cap the cache at 64 pages whatever you ask for

## Vector Search: ef_search

The `ef_search` parameter controls the accuracy/speed tradeoff for HNSW vector search.

| ef_search | Mean Latency (1M vectors) | Recall@10 |
|-----------|--------------------------|-----------|
| 16 | 506 us | 57% |
| 32 | 1.9 ms | 79% |
| 64 | 990 us | 100% |
| 128 | 3.2 ms | 100% |
| 256 | 11.6 ms | 100% |

**Guidelines:**
- Default is 64, which achieves 100% recall at 1M vectors
- For latency-sensitive applications, try 32 (79% recall)
- For maximum recall in critical applications, use 128
- Values above 128 rarely improve recall but increase latency

```python
# Programmatic API
results = db.vector_search(query_vec, k=10, ef_search=128)
```

`ef_search` is the only search setting that can change after the index exists. The
index's `m` (connections per node, 2 to 32, default 16) and `ef_construction` (the
candidate list while inserting, 1 to 4096, default 200) are chosen when the database
is created (`vector_m` and `vector_ef_construction` in the bindings, `--vector-m`
and `--vector-ef-construction` in `lattice create`) and are fixed afterwards. A
larger `m` or `ef_construction` builds a better-connected graph, at the cost of
slower inserts and, for `m`, more memory.

## Batch Insert

When loading large amounts of vector-bearing nodes, `batch_insert_vectors` creates a node and stores its vector for every row in one call:

```python
import numpy as np

with db.write() as txn:
    # Fast: ~248 inserts/sec at 1M scale
    vectors = np.random.rand(10000, 128).astype(np.float32)
    node_ids = txn.batch_insert_vectors("Document", vectors)
    txn.commit()
```

It saves one binding call per node. It does not amortize HNSW work: each vector is still inserted into the index individually, exactly as `set_vector` would.

## Query Caching

Use parameterized queries so the cache can reuse the parsed query. The cache is
keyed by query text and holds the parsed, analysed query (not an execution plan):

```python
# Good: parsed once, reused
for name in names:
    db.query("MATCH (n:Person) WHERE n.name = $name RETURN n", parameters={"name": name})

# Bad: every distinct query text is parsed again
for name in names:
    db.query(f"MATCH (n:Person) WHERE n.name = '{name}' RETURN n")
```

Monitor cache effectiveness:

```python
stats = db.cache_stats()
print(f"Hit rate: {stats['hits'] / (stats['hits'] + stats['misses']):.1%}")
```

## Indexing Strategy

### Full-Text Search

Declare a full-text index only over properties you actually search. Every declared
index is maintained on every write that touches its property, so an index nobody
queries is pure cost:

```python
# The text people search
db.create_node_fts_index("Chunk", "text")
# Not metadata, IDs, or timestamps
```

Then write queries that can use it. A `@@` predicate reads the index directly when
it is the whole `WHERE` or a branch of an `AND`, and falls back to examining every
node carrying the label when it sits under an `OR` beside a non-text condition.
See [Full-Text Search](./full-text-search.md#performance).

### Labels

Use specific labels for nodes you query frequently. Label scans are fast because they use a dedicated index:

```cypher
// Fast: scans only Chunk nodes
MATCH (c:Chunk) WHERE c.source = "web" RETURN c

// Slower: scans all nodes
MATCH (n) WHERE n.source = "web" RETURN n
```

A label narrows a `<=>` vector search as well: the rows the pattern produces are
the candidates, and the search widens until it has the candidates the predicate
asks for. A label that is rare among the vectors therefore costs more searching
than a common one. To get the nearest `k`, write the distance in the `WHERE` and
end with `ORDER BY` that same distance and `LIMIT k`: the search then stops once it
has `k` rows, instead of finding every candidate under the threshold and sorting
them. Write the distance in the `RETURN` the same way as in the `WHERE` to reuse
the value the search measured. See [Vector Search in Cypher](../cypher/vector-search.md).

## Transaction Scope

Keep write transactions short. Only one write transaction can be open at a time, and while one is open, beginning another fails immediately with a lock-timeout error rather than waiting:

```python
# Good: small, focused transactions
for batch in chunks(data, 1000):
    with db.write() as txn:
        for item in batch:
            txn.create_node(labels=["Item"], properties=item)
        txn.commit()

# Bad: one giant transaction
with db.write() as txn:
    for item in all_data:  # millions of items
        txn.create_node(labels=["Item"], properties=item)
    txn.commit()
```

## Memory Usage

Vector storage dominates memory at scale:

| Scale | Memory |
|-------|--------|
| 1,000 vectors (128d) | 1 MB |
| 10,000 vectors | 10 MB |
| 100,000 vectors | 101 MB |
| 1,000,000 vectors | 1,040 MB |

Plan your `vector_dimensions` and scale accordingly. Lower dimensions use less memory but capture less semantic information.
