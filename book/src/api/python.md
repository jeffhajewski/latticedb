# Python API

Python bindings for LatticeDB, an embedded knowledge graph database for AI/RAG applications.

## Installation

```bash
pip install latticedb
```

The published wheels bundle the native shared library for Linux (x86_64, aarch64), macOS (x86_64, arm64) and Windows (x64, ARM64), so nothing else needs installing. From a source checkout, build it with `zig build shared`; the binding finds `zig-out/lib` (or set `LATTICE_LIB_PATH`).

## Quick Start

```python
import numpy as np
from latticedb import Database

with Database("knowledge.db", create=True, enable_vectors=True, vector_dimensions=4) as db:
    # Create nodes, edges, and index content
    db.create_node_fts_index("Person", "bio")

    with db.write() as txn:
        alice = txn.create_node(
            labels=["Person"],
            properties={"name": "Alice", "age": 30},
        )
        bob = txn.create_node(
            labels=["Person"],
            properties={"name": "Bob", "age": 25},
        )
        txn.create_edge(alice.id, bob.id, "KNOWS")

        # Index text for full-text search
        txn.set_property(alice.id, "bio", "Alice works on machine learning research")
        txn.set_property(bob.id, "bio", "Bob studies deep learning and neural networks")

        # Store vector embeddings
        txn.set_vector(alice.id, "embedding", np.array([1.0, 0.0, 0.0, 0.0], dtype=np.float32))
        txn.set_vector(bob.id, "embedding", np.array([0.0, 1.0, 0.0, 0.0], dtype=np.float32))

        txn.commit()

    # Query with Cypher
    result = db.query("MATCH (n:Person) WHERE n.age > 20 RETURN n.name, n.age")
    for row in result:
        print(row)

    # Vector similarity search
    query_vec = np.array([0.9, 0.1, 0.0, 0.0], dtype=np.float32)
    for r in db.vector_search(query_vec, k=2):
        print(f"Node {r.node_id}: distance={r.distance:.4f}")

    # Full-text search
    for r in db.fts_search("Person", "bio", "machine learning"):
        print(f"Node {r.node_id}: score={r.score:.4f}")

    # Fuzzy search (typo-tolerant)
    for r in db.fts_search_fuzzy("Person", "bio", "machin lerning"):
        print(f"Node {r.node_id}: score={r.score:.4f}")
```

## API Reference

### Database

```python
Database(
    path: str | Path,
    *,
    create: bool = False,        # Create if doesn't exist
    read_only: bool = False,     # Open in read-only mode
    cache_size_mb: int | None = None, # Page cache size; None = LATTICE_BUFFER_POOL_MB or 100
    enable_wal: bool = True,     # log commits so they survive a crash
    enable_adjacency_cache: bool = False, # Accepted; no effect on queries
    enable_vectors: bool | None = None, # Create a vector index if the file has none
    enable_vector: bool | None = None,  # Deprecated compatibility alias
    vector_dimensions: int = 128, # Vector dimensions (0 = the file's)
    lock: bool = True,           # Take the file lock (see below)
    sync_mode: str | None = None,       # "full" or "normal"; None = LATTICE_SYNC_MODE or full
    full_fsync: bool | None = None,     # macOS F_FULLFSYNC; None = LATTICE_FULL_FSYNC or off
    vector_metric: str | None = None,   # "cosine", "euclidean" or "dot_product"
    vector_m: int | None = None,        # HNSW connections per node, 2..32 (default 16)
    vector_ef_construction: int | None = None, # HNSW build candidate list, 1..4096 (default 200)
    changefeed_retention: str | None = None, # "all", "off", "last:N", "age:7d"; None = the file's
)
```

Constructing a `Database` does not open it: use it as a context manager or call
`open()` first.

#### Vector index options

A database file records its vector index: dimensions, metric, `vector_m` and
`vector_ef_construction`. A database that has an index opens with it whether or
not `enable_vectors` is set, so reopening needs no vector options at all, also
after a crash. `enable_vectors` creates an index if the database has none, and
`vector_metric`, `vector_m` and `vector_ef_construction` configure that new
index; once it exists they are fixed. Any vector option you do pass (and
`vector_dimensions` when `enable_vectors` is set) must match the file, or opening
raises `LatticeConfigMismatchError`.

```python
import numpy as np
from latticedb import Database

with Database("vectors.db", create=True, enable_vectors=True,
              vector_dimensions=2, vector_metric="euclidean") as db:
    with db.write() as txn:
        a = txn.create_node(labels=["Doc"])
        txn.set_vector(a.id, "embedding", np.array([1.0, 0.0], dtype=np.float32))
        b = txn.create_node(labels=["Doc"])
        txn.set_vector(b.id, "embedding", np.array([0.0, 2.0], dtype=np.float32))
        txn.commit()

# No vector options: the file says how its index was built.
with Database("vectors.db") as db:
    for r in db.vector_search(np.array([1.0, 0.0], dtype=np.float32), k=2):
        print(f"Node {r.node_id}: distance={r.distance:.4f}")
# Node 1: distance=0.0000
# Node 2: distance=2.2361
```

The metric decides search distances, smallest first: cosine distance (vectors
stored at unit length), L2 distance, or the negative dot product (vectors stored
as given).

#### Durability options

`sync_mode="normal"` returns from a commit once the operating system has it: the
commit survives the process being killed, but a power cut or OS crash can lose
the most recent commits. The default, `"full"`, syncs the log on every commit.
`full_fsync=True` makes each sync on macOS ask the drive to flush its cache. A
value you pass wins over the `LATTICE_SYNC_MODE` and `LATTICE_FULL_FSYNC`
environment variables; they apply only when you leave the option unset. See
[Durability and the Log](../configuration/durability.md).

`enable_wal=False` logs nothing. Transactions, rollback, streams and the
changefeed work as with the log, but a commit reaches the file at the next
flush (`close()`, or when half the page cache holds changes) rather than when it
returns; see
[Running without the log](../configuration/durability.md#running-without-the-log).

`sync_mode`, `full_fsync`, the three vector index options and
`changefeed_retention` need a native library with `lattice_open_v5`; against an
older one, setting any of them raises `LatticeUnsupportedError`.
`latticedb.deserialize()` takes them too, through `lattice_deserialize_v5`, with
the same fallback against an older library.

#### Read-only opens

`read_only=True` on a database that was not closed cleanly recovers it in
memory: the handle sees every committed transaction, and nothing is written to
the files. If that recovery needs more than 64 MB plus four times the size of
the log and journal, opening raises `LatticeFullError`; open the database
read-write once to recover it on disk. A file that a newer release has marked as
not writable by this one opens only with `read_only=True`; a writable open
raises `LatticeNeedsNewerWriterError` and writes nothing.

#### In-memory databases

```python
import latticedb

db = latticedb.Database(":memory:")
db.open()
```

Pass `:memory:` as the path and the database has no files behind it. Nothing is
written to disk and nothing survives closing it, which suits a scratch database, a
test, or one you pulled out of object storage and will hand back as bytes.

It behaves like any other database — transactions, the write-ahead log, and
serialization all work. The differences are that it disappears when closed, and
that nothing locks it, since no other process can reach it.

#### The file lock

A database can only be open in one process at a time. Opening takes a lock on the
file, so a second process gets `LatticeDatabaseLockedError` rather than quietly
corrupting your data. A read-only handle shares the lock with other readers, but
is still refused while a writer holds the database, because what it would read is
a stale file that a checkpoint may be rewriting underneath it.

Pass `lock=False` only on filesystems where locking does not work. It does not
make concurrent access safe; it removes the thing that was going to tell you it
was not.

#### Methods

- `open()` / `close()` - Open/close the database (also works as context manager)
- `read()` - Start a read-only transaction (context manager)
- `write()` - Start a read-write transaction (context manager)
- `query(cypher, parameters=None)` - Execute a Cypher query in its own transaction, read-write only if the query writes. If the commit fails, the transaction is rolled back before the error is raised, so the writer slot is free for the next write
- `serialize()` - Return the whole database as bytes
- `get_nodes_by_label(label)` - IDs of every node with a label
- `vector_search(vector, *, k=10, ef_search=64)` - k-NN vector search
- `fts_search(label, prop, query, *, limit=10)` - Full-text search of one declared index
- `fts_search_fuzzy(label, prop, query, *, limit=10, max_distance=0, min_term_length=0)` - Fuzzy full-text search
- `create_node_fts_index(label, property)` / `drop_node_fts_index(...)` / `has_node_fts_index(...)` - Manage node full-text indexes
- `create_edge_fts_index(edge_type, property)` / `drop_edge_fts_index(...)` / `has_edge_fts_index(...)` - Manage relationship full-text indexes. A node index and a relationship index cannot share a name and property: the second raises `LatticeAlreadyExistsError`
- `create_node_property_index(label, property_key)` / `drop_node_property_index(...)` - Manage explicit node property indexes, used for equality lookups and by Cypher for ranges, prefixes and sorting
- `create_edge_property_index(edge_type, property_key)` / `drop_edge_property_index(...)` - Manage explicit edge property indexes
- `read_stream(stream, *, after_sequence=0, limit=100, timeout_ms=0)` - Read durable stream records by cursor
- `get_stream_offset(stream, consumer)` - Read a committed consumer offset
- `changes(*, after_sequence=0, limit=100, timeout_ms=0)` - Read the built-in graph changefeed. Raises `LatticeChangesTrimmedError` (a `LatticeNotFoundError`) when retention has dropped the events after `after_sequence`, and `LatticeUnsupportedError` when the policy is `"off"`
- `changefeed_status()` - The changefeed's policy and range: `ChangefeedStatus(retention, oldest_sequence, last_sequence)`, `oldest_sequence` None when it holds no events
- `trim_changefeed()` - Apply the retention policy in full now; returns the number of events dropped
- `cache_clear()` - Clear the query cache
- `cache_stats()` - Get cache hit/miss statistics

`latticedb.deserialize(data, *, copy=True, ...)` opens a database from bytes
produced by `serialize()`, taking the same options as `Database` apart from `path`,
`create` and `read_only`. `latticedb.version()` returns the native library version.

### Transaction

#### Read Operations

- `get_node(node_id)` - Get a node by ID, returns `Node` or `None`
- `node_exists(node_id)` - Check if a node exists
- `get_property(node_id, key)` - Get a property value
- `get_vector(node_id, key="embedding")` - The node's vector as a float32 array, or `None` if it has none. Raises `LatticeNotFoundError` if the node does not exist. Under the cosine metric, the default, a committed vector reads back normalised to unit length (`[3, 4]` reads back as `[0.6, 0.8]`); an uncommitted one reads back as written.
- `get_outgoing_edges(node_id)` - Get outgoing edges from a node
- `get_incoming_edges(node_id)` - Get incoming edges to a node
- `get_outgoing_edges_by_type(node_id, edge_type, *, limit=0)` / `get_incoming_edges_by_type(...)` - Edges of one type (0 means no limit)
- `get_nodes_by_label(label)` - IDs of every node with a label, as this transaction sees them
- `query(cypher, parameters=None)` - Run Cypher inside this transaction
- `vector_search(...)` / `fts_search(...)` / `fts_search_fuzzy(...)` - Searches inside this transaction
- `find_nodes_by_label_property(label, property_key, value, limit=100)` - Indexed node equality lookup
- `find_edges_by_type_property(edge_type, property_key, value, limit=100)` - Indexed edge equality lookup
- `is_read_only` / `is_active` - Transaction state

#### Write Operations

- `create_node(labels=[], properties=None)` - Create a node
- `delete_node(node_id, *, detach=False)` - Delete a node. A node that still has relationships raises `LatticeHasRelationshipsError` and nothing is deleted, as Cypher's `DELETE` does; `detach=True` deletes its relationships with it, as `DETACH DELETE` does. Deleting a node that does not exist does nothing.
- `set_property(node_id, key, value)` - Set a property on a node
- `set_vector(node_id, key, vector)` - Set the node's vector embedding. A node has one vector, which Cypher reads as `n.embedding`; `key` must be `"embedding"` (or `""`), and any other key raises `LatticeUnsupportedError`. A vector with NaN or infinite components raises `LatticeInvalidArgError`. On a database created without a vector index, `set_vector`, `batch_insert_vectors` (before it creates any node) and `vector_search` raise `LatticeUnsupportedError` with a message that says so; before 0.18.1 they raised `LatticeIOError`. Opening the database with `enable_vectors=True` and `vector_dimensions` adds an index.
- `remove_vector(node_id, key="embedding")` - Remove the node's vector. The node keeps its labels and properties, and vector search no longer returns it. Removing a vector the node does not have does nothing. Raises `LatticeNotFoundError` if the node does not exist.
- `batch_insert_vectors(label, vectors)` - Insert vector-bearing nodes in one call
- `batch_insert(label, vectors)` - Deprecated compatibility alias for `batch_insert_vectors`
- `create_edge(source_id, target_id, edge_type, properties=None)` - Create an edge and return its stable edge ID on the `Edge`. Raises `LatticeNotFoundError` unless both ends exist as the transaction sees them
- `delete_edge(source_id, target_id, edge_type)` - Delete an edge
- `set_edge_property(edge_id, key, value)` - Set an edge property by stable edge ID
- `get_edge_property(edge_id, key)` - Get an edge property by stable edge ID
- `remove_edge_property(edge_id, key)` - Remove an edge property by stable edge ID
- `publish_stream(stream, payload, kind="message")` - Publish a durable stream record
- `publish_stream_get_sequence(stream, payload, kind="message")` - Publish and return the record's sequence number
- `set_stream_offset(stream, consumer, sequence)` - Commit a durable consumer offset
- `trim_stream(stream, through_sequence)` - Delete stream records through a sequence
- `commit()` / `rollback()` - Commit or rollback the transaction. A commit that raises `LatticeTxnAbortedError` has ended the transaction, and rolling it back afterwards does nothing; after any other failure it stays open, and leaving the `with` block rolls it back

### Bulk Vector Insertion

Insert many nodes with vectors in a single efficient call:

```python
import numpy as np
from latticedb import Database

with Database("vectors.db", create=True, enable_vectors=True, vector_dimensions=128) as db:
    with db.write() as txn:
        vectors = np.random.rand(1000, 128).astype(np.float32)
        node_ids = txn.batch_insert_vectors("Document", vectors)
        print(f"Created {len(node_ids)} nodes")
        txn.commit()
```

### Full-Text Search

#### Exact Search

```python
results = db.fts_search("Person", "bio", "machine learning", limit=10)
for r in results:
    print(f"Node {r.node_id}: score={r.score:.4f}")
```

#### Fuzzy Search (Typo-Tolerant)

```python
# Finds "machine learning" even with typos
results = db.fts_search_fuzzy("Person", "bio", "machne lerning", limit=10)

# Control fuzzy matching sensitivity
results = db.fts_search_fuzzy(
    "Person",
    "bio",
    "machne",
    limit=10,
    max_distance=2,      # Max edit distance (default: 2)
    min_term_length=4,   # Min term length for fuzzy matching (default: 4)
)
```

### Embeddings

LatticeDB includes a built-in hash embedding function and an HTTP client for external embedding services.

#### Hash Embeddings (Built-in)

Deterministic, no external service needed. Useful for testing or simple keyword-based similarity:

```python
from latticedb.embedding import hash_embed

vec = hash_embed("hello world", dimensions=128)
print(vec.shape)  # (128,)
```

#### HTTP Embedding Client

Connect to Ollama, OpenAI, or compatible APIs:

```python
from latticedb.embedding import EmbeddingClient, EmbeddingApiFormat

# Ollama (default). The endpoint is the full URL the client POSTs to.
with EmbeddingClient("http://localhost:11434/api/embeddings") as client:
    vec = client.embed("hello world")

# OpenAI-compatible API
with EmbeddingClient(
    "https://api.openai.com/v1/embeddings",
    model="text-embedding-3-small",
    api_format=EmbeddingApiFormat.OPENAI,
    api_key="sk-...",
) as client:
    vec = client.embed("hello world")
```

The endpoint is POSTed to exactly as given; nothing is appended, so a server's
base URL alone (`http://localhost:11434`) sends the request to `/` and `embed`
fails. `timeout_ms` is how long `embed` waits for the response to begin: it bounds the wait for the server to start answering (default 30 seconds), not connecting or reading the rest of a response that has begun, and is not applied on Windows.

### Edge Traversal

```python
with db.read() as txn:
    outgoing = txn.get_outgoing_edges(node_id)
    for edge in outgoing:
        print(f"{edge.id}: {edge.source_id} --[{edge.edge_type}]--> {edge.target_id}")

    incoming = txn.get_incoming_edges(node_id)
    for edge in incoming:
        print(f"{edge.id}: {edge.source_id} --[{edge.edge_type}]--> {edge.target_id}")
```

### Edge Properties

Edge properties are addressed by stable edge ID. `create_edge` returns an
`Edge` object containing that ID, and traversal results include it.

```python
with db.write() as txn:
    edge = txn.create_edge(alice.id, bob.id, "KNOWS")
    txn.set_edge_property(edge.id, "since", 2020)
    assert txn.get_edge_property(edge.id, "since") == 2020
    txn.commit()
```

### Durable Streams

```python
with db.write() as txn:
    txn.publish_stream("jobs", {"id": 1, "status": "queued"}, kind="job.queued")
    txn.set_stream_offset("jobs", "worker-a", 1)
    txn.commit()

records = db.read_stream("jobs", after_sequence=0, limit=100)
changes = db.changes(after_sequence=0, limit=100)
```

`changes()` returns each transaction's net effect (a property set several times
is one event, from its value before the transaction to its last value), in an
order that can be replayed as it comes: the edges it deletes, the nodes that
exist after it, the edges that exist after it, then the nodes it deletes, each
group by ascending id. How much of the changefeed to keep is set with
`changefeed_retention`; see [The Changefeed](../guides/changefeed.md).

### Cypher Queries

```python
# Pattern matching
result = db.query("MATCH (n:Person) RETURN n.name")

# With parameters
result = db.query(
    "MATCH (n:Person) WHERE n.name = $name RETURN n",
    parameters={"name": "Alice"},
)

# Vector similarity in Cypher
result = db.query(
    "MATCH (n:Document) WHERE n.embedding <=> $vec < 0.5 RETURN n.title",
    parameters={"vec": query_vector},
)

# Full-text search in Cypher
result = db.query(
    'MATCH (n:Document) WHERE n.content @@ "machine learning" RETURN n.title'
)

# Data mutation
db.query("CREATE (n:Person {name: 'Charlie', age: 35})")
db.query("MATCH (n:Person {name: 'Charlie'}) SET n.age = 36")
db.query("MATCH (n:Person {name: 'Charlie'}) DETACH DELETE n")
```

### Query Cache

```python
# Get cache statistics
stats = db.cache_stats()
print(f"Entries: {stats['entries']}, Hits: {stats['hits']}, Misses: {stats['misses']}")

# Clear the cache
db.cache_clear()
```

### Checkpoints

```python
stats = db.checkpoint("truncate")   # fold the log into the file now
db.set_auto_checkpoint(10_000)      # commits checkpoint every 10,000 frames
db.set_auto_checkpoint(0)           # commits never checkpoint
```

`"passive"` gives way if a changed page is in use, `"full"` writes every changed
page and leaves the log, and `"truncate"` (the default) also empties it.
`CheckpointStats` has `pages_flushed`, `duration_ns`, `checkpoint_lsn` and
`wal_truncated`; `set_auto_checkpoint` takes `min_interval_ms` as well.
Without the setter, the `LATTICE_AUTO_CHECKPOINT_FRAMES` environment variable sets the frame count (default 1000). A larger interval means fewer pauses, each longer, a longer `-wal` file and more to replay after a crash; see [Checkpointing](../configuration/durability.md#checkpointing).

## Supported Property Types

- `None` - Null value
- `bool` - Boolean
- `int` - 64-bit integer; a value outside the signed 64-bit range raises `OverflowError`
- `float` - 64-bit float
- `str` - UTF-8 string
- `bytes` - Binary data
- NumPy `ndarray` (`float32`) - Vector
- `list` - List of any of these values, nested to any depth
- `dict` with `str` keys - Map of any of these values, nested to any depth

## Error Handling

```python
from latticedb import Database, LatticeError, LatticeNotFoundError, LatticeIOError

try:
    with Database("nonexistent.db") as db:
        pass
except LatticeNotFoundError:
    print("Database not found")
except LatticeIOError:
    print("I/O error")
except LatticeError as e:
    print(f"Error: {e}")
```

Every exception is a `LatticeError` and carries the C error code as `.code`.
The more specific classes include:

| Exception | Code | Raised when |
|-----------|------|-------------|
| `LatticeNotFoundError` | -4 | The database file is missing, or `create_edge` names an end that does not exist |
| `LatticeDatabaseLockedError` | -16 | Another process holds the file |
| `LatticeHasRelationshipsError` | -17 | `delete_node` without `detach=True` on a node with relationships |
| `LatticeConfigMismatchError` | -18 | Open options that contradict the file's vector index |
| `LatticeNeedsNewerWriterError` | -19 | A writable open of a file only a newer release may write |
| `LatticeVersionMismatchError` | -11 | An open, read-only included, of a file or log in a format only a newer release can read |
| `LatticeFullError` | -10 | A read-only open whose crash recovery needs more memory than it may use |
| `LatticeCorruptionError` | -3 | An open whose log is damaged where it had been synced, or holds a committed change that cannot be replayed |
| `LatticeQueryError` | varies | Any failed Cypher query (below) |

### Query errors

A failed query raises `LatticeQueryError`, which says what failed and where:

```python
from latticedb import Database, LatticeQueryError

with Database("graph.db", create=True) as db:
    try:
        db.query("RETURN 1/0")
    except LatticeQueryError as e:
        print(e)                    # Division by zero: 1 / 0
        print(e.code)               # -1
        print(e.stage)              # execution
        print(e.diagnostic_code)    # DivisionByZero
        print(e.location)           # {'line': 1, 'column': 9, 'length': 1}
```

`stage` is `"parse"`, `"semantic"`, `"plan"` or `"execution"`. A query that does
not parse or does not make sense (an unknown function such as `lenght(x)` is
`unknown_function` at the semantic stage) has code -6; one that fails while
running has code -1, except that a `DELETE` of a node that still has
relationships has code -17 and diagnostic code `NodeHasRelationships`, and a
relationship created to a node deleted earlier in the query has code -4 and
`NotFound`, the codes `delete_node` and `create_edge` give. A storage failure
under a query has code -2 and an allocation failure -13; these are still raised
as `LatticeQueryError`, so check `.code` to tell them from a fault in the
query. `location` is `None` when the failure does not belong to one place in the
text.

## Requirements

- Python 3.9+
- NumPy (a required dependency)
- The native LatticeDB library (`liblattice.dylib` / `liblattice.so` / `lattice.dll`), bundled in the published wheels
