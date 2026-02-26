# Performance Tuning

This guide covers the key parameters for tuning LatticeDB performance.

## Cache Size

The `cache_size_mb` parameter controls how many database pages are cached in memory. Larger caches reduce disk I/O.

```python
db = Database("mydb.db", cache_size_mb=200)  # 200 MB cache
```

**Guidelines:**
- Default is 100 MB, which handles most workloads well
- For large databases (1M+ nodes), increase to 200-500 MB
- For memory-constrained environments, reduce to 50 MB or less
- The cache stores fixed-size pages (4 KB each), so 100 MB holds ~25,000 pages

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

## Batch Insert

When loading large amounts of data, use `batch_insert` instead of individual creates:

```python
import numpy as np

with db.write() as txn:
    # Fast: ~248 inserts/sec at 1M scale
    vectors = np.random.rand(10000, 128).astype(np.float32)
    node_ids = txn.batch_insert("Document", vectors)
    txn.commit()
```

Batch insert is significantly faster than individual node creation because it amortizes HNSW index updates.

## Query Plan Caching

Use parameterized queries to enable plan caching:

```python
# Good: plan is cached and reused
for name in names:
    db.query("MATCH (n:Person) WHERE n.name = $name RETURN n", parameters={"name": name})

# Bad: new plan compiled for each query
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

Only index text that you need to search. Indexing unnecessary text wastes memory and slows inserts:

```python
# Index only the searchable text field
txn.fts_index(chunk.id, chunk_text)
# Don't index metadata, IDs, etc.
```

### Labels

Use specific labels for nodes you query frequently. Label scans are fast because they use a dedicated index:

```cypher
-- Fast: scans only Chunk nodes
MATCH (c:Chunk) WHERE c.embedding <=> $q < 0.5 RETURN c

-- Slower: scans all nodes
MATCH (n) WHERE n.embedding <=> $q < 0.5 RETURN n
```

## Transaction Scope

Keep write transactions short. Long-running write transactions hold the write lock and block other writes:

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
