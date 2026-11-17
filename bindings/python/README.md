# LatticeDB Python Bindings

Python bindings for [LatticeDB](https://github.com/jeffhajewski/latticedb), an embedded single-file property-graph database with native vector and BM25 full-text search.

## Installation

```bash
pip install latticedb
```

Published wheels bundle the native shared library for Linux (x86_64, aarch64), macOS (x86_64, arm64) and Windows (x64, ARM64).

If you are installing from a source checkout, the package build can either:

- bundle a prebuilt `liblattice` from `LATTICE_BUNDLE_LIB_DIR` / `LATTICE_BUNDLE_LIB_PATH`, or
- build `liblattice` with Zig during the wheel build

For example, to bundle a staged installed library into a locally built wheel:

```bash
export LATTICE_BUNDLE_LIB_DIR=/tmp/lattice-install/lib
pip wheel . -w dist
pip install dist/latticedb-*.whl
```

At runtime, explicit library discovery overrides still work via `LATTICE_LIB_PATH`, `LATTICE_PREFIX`, and `pkg-config`.

Migration note: embedding helpers now live in the dedicated `latticedb.embedding` module. See [../../docs/client_api_migration.md](../../docs/client_api_migration.md) for the preferred API names and deprecated compatibility aliases.

Installed-prefix workflow:

```bash
zig build install --prefix /tmp/lattice-install
export LATTICE_PREFIX=/tmp/lattice-install
```

`LATTICE_PREFIX` is searched under `<prefix>/lib`. On Windows, where `zig build install`
puts `lattice.dll` in `<prefix>/bin` and only the import library in `<prefix>/lib`,
`<prefix>/bin` is searched first, then `<prefix>/lib`.

Alternatively, discovery can use `pkg-config`:

```bash
export PKG_CONFIG_PATH=/tmp/lattice-install/lib/pkgconfig
```

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

        # Writing the indexed property is what makes it searchable.
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
    lock: bool = True,           # Take a file lock (exclusive for read-write, shared for read-only)
    sync_mode: str | None = None,       # "full" or "normal"; None = LATTICE_SYNC_MODE or full
    full_fsync: bool | None = None,     # macOS F_FULLFSYNC; None = LATTICE_FULL_FSYNC or off
    vector_metric: str | None = None,   # "cosine", "euclidean" or "dot_product"
    vector_m: int | None = None,        # HNSW connections per node, 2..32 (default 16)
    vector_ef_construction: int | None = None, # HNSW build candidate list, 1..4096 (default 200)
    changefeed_retention: str | None = None, # "all", "off", "last:N", "age:7d"; None = the file's
)
```

Constructing a `Database` does not open it. Use it as a context manager
(`with Database(...) as db:`) or call `db.open()` before using it.

A database file records its vector index: dimensions, metric, `vector_m` and
`vector_ef_construction`. A database that has an index opens with it whether or
not `enable_vectors` is set, so reopening needs no vector options at all. Any
vector option you do pass (and `vector_dimensions` when `enable_vectors` is set)
must match the file, or opening raises `LatticeConfigMismatchError`. The metric
decides search distances, smallest first: cosine distance (vectors stored at unit
length), L2 distance, or the negative dot product (vectors stored as given).

`sync_mode="normal"` returns from a commit once the operating system has it: the
commit survives the process being killed, but a power cut or OS crash can lose
the most recent commits. The default, `"full"`, syncs the log on every commit.

A `sync_mode` or `full_fsync` you pass wins over the `LATTICE_SYNC_MODE` and
`LATTICE_FULL_FSYNC` environment variables, which apply only when the option is
left unset.

`enable_wal=False` logs nothing. Transactions, rollback, streams and the
changefeed work as with the log, but a commit reaches the file at the next
flush (`close()`, or when half the page cache holds changes) rather than when it
returns.

`changefeed_retention` sets how much of the changefeed the file keeps (see
[Durable Streams and Changefeeds](#durable-streams-and-changefeeds)).

`sync_mode`, `full_fsync`, the three vector index options and
`changefeed_retention` need a native library with `lattice_open_v5`; against an
older one, setting any of them raises `LatticeUnsupportedError`.

`read_only=True` on a database that was not closed cleanly recovers it in
memory: the handle sees every committed transaction and nothing is written to
the files. A recovery that needs more than 64 MB plus four times the size of the
log and journal raises `LatticeFullError`; open the database read-write once to
recover it on disk.

#### Methods

- `open()` / `close()` - Open/close the database (also works as context manager)
- `read()` - Start a read-only transaction (context manager)
- `write()` - Start a read-write transaction (context manager)
- `query(cypher, parameters=None)` - Execute a Cypher query in its own transaction, read-write only if the query writes. If the commit fails, the transaction is rolled back before the error is raised, so the writer slot is free for the next write
- `get_nodes_by_label(label)` - IDs of every node with a label
- `vector_search(vector, *, k=10, ef_search=64)` - k-NN vector search
- `create_node_fts_index(label, prop)` / `drop_node_fts_index(label, prop)` / `has_node_fts_index(label, prop)` - Manage full-text indexes on node properties
- `create_edge_fts_index(edge_type, prop)` / `drop_edge_fts_index(...)` / `has_edge_fts_index(...)` - Manage full-text indexes on edge properties. A node index and a relationship index cannot share a name and property: the second raises `LatticeAlreadyExistsError`
- `fts_search(label, prop, query, *, limit=10)` - Full-text search over one declared index
- `fts_search_fuzzy(label, prop, query, *, limit=10, max_distance=0, min_term_length=0)` - Fuzzy full-text search (0 selects the defaults, 2 and 4)
- `create_node_property_index(label, property_key)` / `drop_node_property_index(...)` - Manage explicit node property indexes
- `create_edge_property_index(edge_type, property_key)` / `drop_edge_property_index(...)` - Manage explicit edge property indexes
- `read_stream(stream, *, after_sequence=0, limit=100, timeout_ms=0)` - Read durable stream records by cursor
- `get_stream_offset(stream, consumer)` - Read a committed consumer offset
- `changes(*, after_sequence=0, limit=100, timeout_ms=0)` - Read the built-in graph changefeed. Raises `LatticeChangesTrimmedError` (a `LatticeNotFoundError`) when retention has dropped the events after `after_sequence`, and `LatticeUnsupportedError` when the policy is `"off"`
- `changefeed_status()` - The changefeed's policy and range: `ChangefeedStatus(retention, oldest_sequence, last_sequence)`, `oldest_sequence` None when it holds no events
- `trim_changefeed()` - Apply the retention policy in full now; returns the number of events dropped
- `serialize()` - Return the whole database as bytes
- `checkpoint(mode="truncate")` - Fold the log into the database file now; returns `CheckpointStats` (see [Checkpoints](#checkpoints))
- `set_auto_checkpoint(max_wal_frames, min_interval_ms=0)` - Set when commits checkpoint from now on; `0` frames switches automatic checkpoints off
- `cache_clear()` - Clear the query cache
- `cache_stats()` - Get cache hit/miss statistics

The module-level `latticedb.deserialize(data, *, copy=True, ...)` opens a
database from bytes produced by `serialize()`; it takes the same options as
`Database` apart from `path`, `create` and `read_only`, with the same meaning.
Bytes that hold a vector index open with the configuration they record, and any
vector option you set must match it (`LatticeConfigMismatchError` otherwise);
`enable_vectors` with `vector_dimensions`, `vector_metric`, `vector_m` and
`vector_ef_construction` creates and configures an index in bytes that have
none. `sync_mode`, `full_fsync`, the vector metric, m and ef_construction, and
`changefeed_retention` need a native library with `lattice_deserialize_v5`; against an older one,
setting any of them raises `LatticeUnsupportedError`.

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
- `vector_search(vector, *, k=10, ef_search=64)` - k-NN search inside this transaction
- `fts_search(label, prop, query, *, limit=10)` / `fts_search_fuzzy(...)` - Full-text search inside this transaction
- `find_nodes_by_label_property(label, property_key, value, limit=100)` - Indexed node equality lookup
- `find_edges_by_type_property(edge_type, property_key, value, limit=100)` - Indexed edge equality lookup
- `is_read_only` / `is_active` - Transaction state

#### Write Operations

- `create_node(labels=[], properties=None)` - Create a node
- `delete_node(node_id, *, detach=False)` - Delete a node. A node that still has relationships raises `LatticeHasRelationshipsError` and nothing is deleted, as Cypher's `DELETE` does; `detach=True` deletes its relationships with it, as `DETACH DELETE` does. Deleting a node that does not exist does nothing.
- `set_property(node_id, key, value)` - Set a property on a node. If a full-text index is declared for one of the node's labels and this property, the index is kept current.
- `set_vector(node_id, key, vector)` - Set the node's vector embedding. A node has one vector, which Cypher reads as `n.embedding`; `key` must be `"embedding"` (or `""`), and any other key raises `LatticeUnsupportedError`. A vector with NaN or infinite components raises `LatticeInvalidArgError`. On a database created without a vector index, `set_vector`, `batch_insert_vectors` (before it creates any node) and `vector_search` raise `LatticeUnsupportedError` with a message that says so; before 0.18.1 they raised `LatticeIOError`. Opening the database with `enable_vectors=True` and `vector_dimensions` adds an index.
- `remove_vector(node_id, key="embedding")` - Remove the node's vector. The node keeps its labels and properties, and vector search no longer returns it. Removing a vector the node does not have does nothing. Raises `LatticeNotFoundError` if the node does not exist.
- `batch_insert_vectors(label, vectors)` - Insert vector-bearing nodes in one call
- `batch_insert(label, vectors)` - Deprecated compatibility alias for `batch_insert_vectors`
- `create_edge(source_id, target_id, edge_type, properties=None)` - Create an edge
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

### Property Indexes

Property indexes are explicit and durable. Create them outside an active write
transaction; lookup fails instead of silently scanning when the requested index
does not exist. The lookup methods find equal values; Cypher also reads the
indexes for ranges, `STARTS WITH` and `ORDER BY ... LIMIT`.

```python
db.create_node_property_index("Person", "email")

with db.read() as txn:
    node_ids = txn.find_nodes_by_label_property(
        "Person", "email", "alice@example.com", limit=10
    )

# Inline Cypher equality can use the same index.
rows = db.query(
    "MATCH (p:Person {email: $email}) RETURN p",
    {"email": "alice@example.com"},
)
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

LatticeDB includes a built-in hash embedding function and an HTTP client for external embedding services. For new code, prefer the dedicated `latticedb.embedding` module. The package root still exposes deprecated compatibility aliases.

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

### Edge Traversal

```python
with db.read() as txn:
    outgoing = txn.get_outgoing_edges(node_id)
    for edge in outgoing:
        print(f"{edge.source_id} --[{edge.edge_type}]--> {edge.target_id}")

    incoming = txn.get_incoming_edges(node_id)
    for edge in incoming:
        print(f"{edge.source_id} --[{edge.edge_type}]--> {edge.target_id}")
```

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

A commit is durable once it is in the log (the `-wal` file beside the database).
A checkpoint writes the pages the log's commits changed into the database file
and can then empty the log. By default a commit runs one once the log holds 1000
frames (one frame per page, 4096 bytes); the `LATTICE_AUTO_CHECKPOINT_FRAMES`
environment variable sets another default (`0` for none), and
`set_auto_checkpoint` wins over it. Closing a database always folds the log into
the file.

```python
db.set_auto_checkpoint(0)          # commits never checkpoint; the log grows
# ... a burst of commits ...
stats = db.checkpoint("truncate")  # write the changed pages, then empty the log
print(stats.pages_flushed, stats.duration_ns, stats.checkpoint_lsn, stats.wal_truncated)

db.set_auto_checkpoint(4000, min_interval_ms=1000)  # every 4000 frames, at most once a second
```

Modes: `"passive"` writes the changed pages unless one is in use (then it writes
nothing and reports 0 pages) and leaves the log in place; `"full"` writes every
changed page and leaves the log in place; `"truncate"` (the default) does what
`"full"` does and then empties the log, the only mode that bounds its size. Any
other mode raises `ValueError`. `checkpoint` raises `LatticeReadOnlyError` on a
read-only database and `LatticeLockTimeoutError` if a transaction already has
records in the log; `set_auto_checkpoint` has no effect on a read-only database
and raises `ValueError` for a negative argument.

The interval is a trade-off. A larger one means fewer checkpoints, each a longer
pause for the commit that runs it, a longer `-wal` file and more to replay after
a crash. A smaller one means shorter pauses, more often.

Both need a native library with `lattice_checkpoint`; against an older one they
raise `LatticeUnsupportedError`.

### Durable Streams and Changefeeds

Streams are durable named event logs stored inside the database file. Records are
published in write transactions, sequence numbers are per stream, and reads use
an explicit cursor. Reads do not acknowledge records; commit offsets separately
when your consumer has processed a batch.

```python
from latticedb import Database

with Database("events.db", create=True) as db:
    with db.write() as txn:
        txn.publish_stream("jobs", {"id": 1, "status": "queued"}, kind="job.queued")
        txn.commit()

    records = db.read_stream("jobs", after_sequence=0, limit=100, timeout_ms=0)

    with db.write() as txn:
        txn.set_stream_offset("jobs", "worker-a", records[-1].sequence)
        txn.trim_stream("jobs", records[-1].sequence - 1)
        txn.commit()
```

`db.changes()` reads the reserved `__lattice_changes` stream. It emits semantic
graph events such as `node.insert`, `node.property_set`, `edge.delete`, and
`edge.property_remove`, with payloads represented as normal Python values.

A transaction's events describe its net effect (a property set several times
is one event, from its value before the transaction to its last value) and
come in an order that can be replayed as it comes: the edges it deletes, then
the nodes that exist after it, then the edges that exist after it, then the
nodes it deletes, each group by ascending id. So an edge is created after both
its nodes and deleted before either. See the
[changefeed guide](https://docs.latticedb.org/guides/changefeed) for the details
and for retention policies.

## Supported Property Types

- `None` - Null value
- `bool` - Boolean
- `int` - 64-bit integer; a value outside the signed 64-bit range raises `OverflowError`
- `float` - 64-bit float
- `str` - UTF-8 string
- `bytes` - Binary data
- NumPy `ndarray` (`float32`) - Vector embeddings
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

Every exception carries the C error code as `.code`. Among the more specific ones:
`LatticeHasRelationshipsError` (a plain `delete_node` of a node with
relationships), `LatticeConfigMismatchError` (open options that contradict the
file's vector index), `LatticeNeedsNewerWriterError` (a writable open of a file
that a newer release has marked as not writable by this one; it still opens with
`read_only=True`), `LatticeVersionMismatchError` (a file in a format too new for
this release to read at all, read-only or not), `LatticeNotFoundError` (also `create_edge` when either end
does not exist) and `LatticeDatabaseLockedError` (another process holds the file).

A failed Cypher query raises `LatticeQueryError`, which says what failed and
where:

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

`stage` is `"parse"`, `"semantic"`, `"plan"` or `"execution"`. Parse and semantic
failures (an unknown function is `unknown_function`) have code -6 and execution
failures -1, except that a `DELETE` of a node with relationships has code -17
and a relationship created to a node deleted earlier in the query -4, as
`delete_node` and `create_edge` report them. A storage failure under a query
has code -2 and an allocation failure -13; `location` is `None` when the failure
does not belong to one place in the query.

## Requirements

- Python 3.9+
- NumPy (a required dependency)
- The native LatticeDB library (`liblattice.dylib` / `liblattice.so` / `lattice.dll`), bundled in the published wheels

## License

MIT
