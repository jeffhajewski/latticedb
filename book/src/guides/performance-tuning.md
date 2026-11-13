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
- The `LATTICE_BUFFER_POOL_MB` environment variable sets the size when the program does not; an explicit `cache_size_mb` wins over it
- In-memory (`:memory:`) databases cap the cache at 64 pages whatever you ask for

## Vector Search: ef_search

The `ef_search` parameter controls the accuracy/speed tradeoff for HNSW vector search.

Mean latency and recall@10 over 128-dimensional clustered vectors, measured
through the C API with the default 100 MB cache (0.17.0, Apple M5; method and
more scales on [Benchmarks](../performance/benchmarks.md#vector-search)):

| ef_search | 100,000 vectors | Recall@10 | 1,000,000 vectors | Recall@10 |
|-----------|----------------:|----------:|------------------:|----------:|
| 16 | 55 us | 90.7% | 0.71 ms | 72.5% |
| 32 | 86 us | 96.3% | 1.05 ms | 87.7% |
| 64 (default) | 123 us | 98.3% | 1.54 ms | 95.6% |
| 128 | 180 us | 98.5% | 2.0 ms | 98.4% |
| 256 | 266 us | 98.5% | 2.5 ms | 98.5% |

**Guidelines:**
- Default is 64: about 98% recall at 100K vectors on this data, about 96% at 1M
- At a million vectors, 128 buys back the last few points of recall for about a third more time
- Below 64, recall falls quickly as the index grows
- Past 128, latency rises and recall does not
- At 1M vectors the cache matters more than `ef_search`: see Memory Usage below

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

### Property Indexes

Declare a property index on what queries filter or sort by. Cypher reads it for
equality, ranges (`<`, `>=`, ...), `STARTS WITH` and `ORDER BY ... LIMIT` in
either direction, on nodes of a label and on relationships of a type. An index
created by 0.16.0 answers equality only until `lattice compact
--upgrade-indexes` rebuilds it. See [Property Indexes](./property-indexes.md).

### Labels

Use specific labels for nodes you query frequently. Label scans are fast because they use a dedicated index:

```cypher
// Fast: scans only Chunk nodes
MATCH (c:Chunk) WHERE c.source = "web" RETURN c

// Slower: scans all nodes
MATCH (n) WHERE n.source = "web" RETURN n
```

A label narrows a `<=>` vector search as well: the rows the pattern produces are
the candidates, and the search looks only among them. A small candidate set is
measured node by node, exactly, and a large one is searched through the HNSW
graph with the set as a filter, so a rare label is cheap rather than costly
(about 10 to 20 µs for `LIMIT 10` over 100 labelled nodes among 100,000
vectors). To get the nearest `k`, write the distance in the `WHERE` and
end with `ORDER BY` that same distance and `LIMIT k`: the search then stops once it
has `k` rows, instead of finding every candidate under the threshold and sorting
them. Write the distance in the `RETURN` the same way as in the `WHERE` to reuse
the value the search measured. See [Vector Search in Cypher](../cypher/vector-search.md).

## Bulk Loading and the Changefeed

Every committed change is also recorded in the changefeed, which by default keeps
every event and can make up most of a file. A database nothing reads the history
of can keep less, or none (`changefeed_retention`); see
[The Changefeed](./changefeed.md). A load you can rerun from its source can also
run without the log and with a large cache; see
[Bulk loading](../configuration/durability.md#bulk-loading).

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

A 128-dimensional vector takes about 1.1 KB of file with its index. What a process
holds is bounded by the page cache, plus a few MB:

| Vectors (128-d) | File | Memory, default 100 MB cache | Search p50, default cache | With a larger cache |
|----------------:|-----:|-----------------------------:|--------------------------:|--------------------:|
| 1,000 | 1.2 MB | 3 MB | 35 us | |
| 10,000 | 11 MB | 13 MB | 66 us | |
| 100,000 | 108 MB | 108 MB | 122 us | 121 us (1 GB cache, 119 MB) |
| 1,000,000 | 1,074 MB | 185 MB | 1.34 ms | 0.21 ms (2 GB cache, 1.1 GB) |

"Memory" is what opening the database and searching added to the process. Once the
file outgrows the cache, searches read pages back from the operating system's file
cache (or the disk) and get several times slower, and inserts slow down far more
(see [Benchmarks](../performance/benchmarks.md#vector-search)): building a million
vectors took 75 minutes with the default cache and 18 with a 2 GB one. For a
million vectors, give the cache about the size of the file. Lower `vector_dimensions` use
less of both, but capture less semantic information.
