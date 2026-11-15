# LatticeDB TypeScript Bindings

TypeScript/Node.js bindings for [LatticeDB](https://github.com/jeffhajewski/latticedb), an embedded single-file property-graph database with native vector and BM25 full-text search.

## Installation

```bash
npm install @hajewski/latticedb
```

Published package tarballs bundle the native shared library for Linux (x64, arm64), macOS (x64, arm64) and Windows (x64, arm64).

If you are working from a source checkout, stage the native library into the package with:

```bash
export LATTICE_BUNDLE_LIB_DIR=/tmp/lattice-install/lib
npm run bundle:native
```

If `LATTICE_BUNDLE_LIB_DIR` / `LATTICE_BUNDLE_LIB_PATH` is not set, `npm run bundle:native` will build the current platform library with Zig.

At runtime, explicit library discovery overrides still work via `LATTICE_LIB_PATH`, `LATTICE_PREFIX`, and `pkg-config`.

Migration note: embedding helpers now live in the dedicated `@hajewski/latticedb/embedding` entrypoint. See [../../docs/client_api_migration.md](../../docs/client_api_migration.md) for the preferred API names and deprecated compatibility aliases.

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

```typescript
import { Database } from "@hajewski/latticedb";

const db = new Database("knowledge.db", {
  create: true,
  enableVectors: true,
  vectorDimensions: 4,
});
await db.open();

// Create nodes, edges, and index content
await db.createNodeFtsIndex("Person", "bio");

await db.write(async (txn) => {
  const alice = await txn.createNode({
    labels: ["Person"],
    properties: { name: "Alice", age: 30 },
  });

  const bob = await txn.createNode({
    labels: ["Person"],
    properties: { name: "Bob", age: 25 },
  });

  await txn.createEdge(alice.id, bob.id, "KNOWS");

  // Writing the indexed property is what makes it searchable.
  await txn.setProperty(alice.id, "bio", "Alice works on machine learning research");
  await txn.setProperty(bob.id, "bio", "Bob studies deep learning and neural networks");

  // Store vector embeddings
  await txn.setVector(
    alice.id,
    "embedding",
    new Float32Array([1.0, 0.0, 0.0, 0.0])
  );
  await txn.setVector(
    bob.id,
    "embedding",
    new Float32Array([0.0, 1.0, 0.0, 0.0])
  );
});

// Query with Cypher
const result = await db.query(
  "MATCH (n:Person) WHERE n.age > 20 RETURN n.name, n.age"
);
for (const row of result.rows) {
  console.log(row);
}

// Vector similarity search
const results = await db.vectorSearch(
  new Float32Array([0.9, 0.1, 0.0, 0.0]),
  { k: 2 }
);
for (const r of results) {
  console.log(`Node ${r.nodeId}: distance=${r.distance.toFixed(4)}`);
}

// Full-text search
const ftsResults = await db.ftsSearch("Person", "bio", "machine learning");
for (const r of ftsResults) {
  console.log(`Node ${r.nodeId}: score=${r.score.toFixed(4)}`);
}

// Fuzzy search (typo-tolerant)
const fuzzyResults = await db.ftsSearchFuzzy("Person", "bio", "machin lerning");
for (const r of fuzzyResults) {
  console.log(`Node ${r.nodeId}: score=${r.score.toFixed(4)}`);
}

await db.close();
```

## Features

- **Property Graph** - Nodes and edges with labels and properties
- **Vector Search** - HNSW-based k-NN search for embeddings
- **Full-Text Search** - BM25-ranked search with tokenization
- **Fuzzy Search** - Typo-tolerant full-text search with configurable edit distance
- **Bulk Vector Insertion** - Efficient insertion of vector-bearing nodes
- **Embeddings** - Built-in hash embeddings and HTTP client for external services
- **Cypher Queries** - Pattern matching with `<=>` (vector) and `@@` (FTS) extensions
- **Transactions** - ACID-compliant read/write transactions
- **Query Cache** - Automatic caching of parsed queries
- **TypeScript** - Full type definitions included

## API Reference

### Database

```typescript
const db = new Database(path: string, options?: DatabaseOptions);

interface DatabaseOptions {
  create?: boolean;          // Create if not exists (default: false)
  readOnly?: boolean;        // Open read-only (default: false)
  cacheSizeMb?: number;      // Cache size in MB; unset: LATTICE_BUFFER_POOL_MB, else 100
  enableWal?: boolean;       // log commits so they survive a crash (default: true)
  enableAdjacencyCache?: boolean; // Accepted; no effect on queries (default: false)
  enableVectors?: boolean;   // Create a vector index if the file has none
  enableVector?: boolean;    // Deprecated compatibility alias
  vectorDimensions?: number; // Vector dimensions (default: 128; 0 = the file's)
  lock?: boolean;            // Take a file lock (default: true)
  syncMode?: 'full' | 'normal';   // Unset: LATTICE_SYNC_MODE, else 'full'
  fullFsync?: boolean;            // macOS F_FULLFSYNC; unset: LATTICE_FULL_FSYNC, else off
  vectorMetric?: 'cosine' | 'euclidean' | 'dot_product'; // For a new index (default: cosine)
  vectorM?: number;               // HNSW connections per node, 2..32 (default: 16)
  vectorEfConstruction?: number;  // HNSW build candidate list, 1..4096 (default: 200)
  changefeedRetention?: ChangefeedRetention; // 'all', 'off', 'last:N', 'age:7d' (default: the file's)
}
```

A database file records its vector index: dimensions, metric, `vectorM` and
`vectorEfConstruction`. A database that has an index opens with it whether or
not `enableVectors` is set, so reopening needs no vector options at all. Any
vector option you do pass (and `vectorDimensions` when `enableVectors` is set)
must match the file, or `open()` throws a `LatticeError` with code -18
(`ConfigMismatch`). The metric decides search distances, smallest first: cosine
distance (vectors stored at unit length), L2 distance, or the negative dot
product (vectors stored as given).

`syncMode: 'normal'` returns from a commit once the operating system has it: the
commit survives the process being killed, but a power cut or OS crash can lose
the most recent commits. The default, `'full'`, syncs the log on every commit.

A `syncMode` or `fullFsync` you pass wins over the `LATTICE_SYNC_MODE` and
`LATTICE_FULL_FSYNC` environment variables, which apply only when the option is
left unset.

`enableWal: false` logs nothing. Transactions, rollback, streams and the
changefeed work as with the log, but a commit reaches the file at the next
flush (`close()`, or when half the page cache holds changes) rather than when it
returns.

`changefeedRetention` sets how much of the changefeed the file keeps (see
[Durable Streams and Changefeeds](#durable-streams-and-changefeeds)).

`syncMode`, `fullFsync`, the three vector index options and
`changefeedRetention` need a native library with `lattice_open_v5`; against an
older one, setting any of them throws with code -14 (`Unsupported`). `deserialize()` takes the same options and passes
them through `lattice_deserialize_v5`, which needs the same newer library.

`readOnly: true` on a database that was not closed cleanly recovers it in
memory: the handle sees every committed transaction and nothing is written to
the files. A recovery that needs more than 64 MB plus four times the size of the
log and journal throws with code -10 (`Full`); open the database read-write once
to recover it on disk.

Node IDs, edge IDs and stream sequence numbers are `bigint`.

#### Methods

- `await db.open()` - Open the database connection
- `await db.close()` - Close the database connection
- `await db.read(fn)` - Execute a read-only transaction
- `await db.write(fn)` - Execute a read-write transaction
- `await db.query(cypher, params?)` - Execute a Cypher query
- `await db.getNodesByLabel(label)` - IDs of every node with a label
- `await db.vectorSearch(vector, options?)` - k-NN vector search
- `await db.createNodeFtsIndex(label, property)` / `dropNodeFtsIndex(...)` / `hasNodeFtsIndex(...)` - Manage full-text indexes on node properties
- `await db.createEdgeFtsIndex(edgeType, property)` / `dropEdgeFtsIndex(...)` / `hasEdgeFtsIndex(...)` - Manage full-text indexes on edge properties. A node index and a relationship index cannot share a name and property: the second is rejected as already existing
- `await db.ftsSearch(label, property, query, options?)` - Full-text search over one declared index
- `await db.ftsSearchFuzzy(label, property, query, options?)` - Fuzzy full-text search
- `await db.createNodePropertyIndex(label, property)` / `dropNodePropertyIndex(...)` - Manage explicit node property indexes
- `await db.createEdgePropertyIndex(edgeType, property)` / `dropEdgePropertyIndex(...)` - Manage explicit edge property indexes
- `await db.readStream(stream, options?)` - Read durable stream records by cursor. With `timeoutMs > 0` the wait runs on a worker thread, so the event loop keeps running and a commit from the same process can end the wait
- `await db.getStreamOffset(stream, consumer)` - Read a committed consumer offset
- `await db.changes(options?)` - Read the built-in graph changefeed. Rejects with `LatticeChangesTrimmedError` when retention has dropped the events after `afterSequence`, and with code -14 (`Unsupported`) when the policy is `'off'`
- `await db.changefeedStatus()` - The changefeed's policy and range: `{ retention, oldestSequence, lastSequence }`, `oldestSequence` null when it holds no events
- `await db.trimChangefeed()` - Apply the retention policy in full now; resolves to the number of events dropped
- `await db.checkpoint(mode = 'truncate')` - Fold the log into the database file now; resolves to `CheckpointStats` (see [Checkpoints](#checkpoints))
- `await db.setAutoCheckpoint(maxWalFrames, minIntervalMs = 0)` - Set when commits checkpoint from now on; `0` frames switches automatic checkpoints off
- `await db.cacheClear()` - Clear the query cache
- `await db.cacheStats()` - Get cache hit/miss statistics
- `db.isOpen()` - Check if database is open
- `db.getPath()` - Get database file path
- `db.serialize()` - Return the whole database as a `Uint8Array`

`deserialize(bytes, options?, copy = true)` opens a database from bytes produced by `serialize()`.
Its options mean what they mean to `new Database()`, apart from `create` and
`readOnly`, which it ignores. Bytes that hold a vector index open with the
configuration they record, and any vector option you pass must match it (code
-18, `ConfigMismatch`); `enableVectors` with the vector options creates and
configures an index in bytes that have none.

### Transaction

#### Read Operations

- `await txn.getNode(nodeId)` - Get a node by ID, returns `Node` or `null`
- `await txn.nodeExists(nodeId)` - Check if a node exists
- `await txn.getProperty(nodeId, key)` - Get a property value, or `null` when the node does not have it
- `await txn.getVector(nodeId, key = "embedding")` - The node's vector as a `Float32Array`, or `null` if it has none. Rejects with code -4 (`NotFound`) if the node does not exist. Under the cosine metric, the default, a committed vector reads back normalised to unit length (`[3, 4]` reads back as `[0.6, 0.8]`); an uncommitted one reads back as written.
- `await txn.getOutgoingEdges(nodeId)` - Get outgoing edges from a node
- `await txn.getIncomingEdges(nodeId)` - Get incoming edges to a node
- `await txn.getOutgoingEdgesByType(nodeId, edgeType, ...)` / `getIncomingEdgesByType(...)` - Edges of one type
- `await txn.getNodesByLabel(label)` - IDs of every node with a label, as this transaction sees them
- `await txn.query(cypher, params?)` - Run Cypher inside this transaction
- `await txn.vectorSearch(...)` / `ftsSearch(...)` / `ftsSearchFuzzy(...)` - Searches inside this transaction
- `await txn.findNodesByLabelProperty(label, property, value, limit?)` - Indexed node equality lookup
- `await txn.findEdgesByTypeProperty(edgeType, property, value, limit?)` - Indexed edge equality lookup
- `txn.isReadOnly()` / `txn.isActive()` - Transaction state

#### Write Operations

- `await txn.createNode({ labels, properties })` - Create a node
- `await txn.deleteNode(nodeId, { detach })` - Delete a node. A node that still has relationships is refused with code -17 (`HasRelationships`) and nothing is deleted, as Cypher's `DELETE` does; `{ detach: true }` deletes its relationships with it, as `DETACH DELETE` does. Deleting a node that does not exist does nothing.
- `await txn.setProperty(nodeId, key, value)` - Set a property. If a full-text index is declared for one of the node's labels and this property, the index is kept current.
- `await txn.setVector(nodeId, key, vector)` - Set the node's vector embedding. A node has one vector, which Cypher reads as `n.embedding`; `key` must be `"embedding"` (or `""`), and any other key is rejected with an unsupported-operation error. A vector with NaN or infinite components is rejected as an invalid argument.
- `await txn.removeVector(nodeId, key = "embedding")` - Remove the node's vector. The node keeps its labels and properties, and vector search no longer returns it. Removing a vector the node does not have does nothing. Rejects with code -4 (`NotFound`) if the node does not exist.
- `await txn.batchInsertVectors(label, vectors)` - Insert vector-bearing nodes in one call
- `await txn.batchInsert(label, vectors)` - Deprecated compatibility alias for `batchInsertVectors`
- `await txn.createEdge(sourceId, targetId, edgeType, options?)` - Create an edge
- `await txn.deleteEdge(sourceId, targetId, edgeType)` - Delete an edge
- `await txn.setEdgeProperty(edgeId, key, value)` - Set an edge property by stable edge ID
- `await txn.getEdgeProperty(edgeId, key)` - Get an edge property by stable edge ID
- `await txn.removeEdgeProperty(edgeId, key)` - Remove an edge property by stable edge ID
- `txn.publishStream(stream, payload, kind?)` - Publish a durable stream record
- `txn.publishStreamGetSequence(stream, payload, kind?)` - Publish and return the record's sequence (`bigint`)
- `txn.setStreamOffset(stream, consumer, sequence)` - Commit a durable consumer offset
- `txn.trimStream(stream, throughSequence)` - Delete stream records through a sequence
- `txn.commit()` / `txn.rollback()` - Commit or rollback. A commit that throws with code -7 (`TxnAborted`) has ended the transaction, and `rollback()` afterwards does nothing; after any other failure it stays open for `rollback()`

### Bulk Vector Insertion

Insert many nodes with vectors in a single efficient call:

```typescript
import { Database } from "@hajewski/latticedb";

const db = new Database("vectors.db", {
  create: true,
  enableVectors: true,
  vectorDimensions: 128,
});
await db.open();

await db.write(async (txn) => {
  const vectors = Array.from({ length: 1000 }, () =>
    Float32Array.from({ length: 128 }, () => Math.random())
  );
  const nodeIds = await txn.batchInsertVectors("Document", vectors);
  console.log(`Created ${nodeIds.length} nodes`);
});

await db.close();
```

### Property Indexes

Property indexes are explicit and durable. Create them outside an active write
transaction; lookup fails instead of silently scanning when the requested index
does not exist. The lookup methods find equal values; Cypher also reads the
indexes for ranges, `STARTS WITH` and `ORDER BY ... LIMIT`.

```typescript
await db.createNodePropertyIndex("Person", "email");

const nodeIds = await db.read((txn) =>
  txn.findNodesByLabelProperty("Person", "email", "alice@example.com", 10)
);

// Inline Cypher equality can use the same index.
const rows = await db.query(
  "MATCH (p:Person {email: $email}) RETURN p",
  { email: "alice@example.com" }
);
```

### Full-Text Search

#### Exact Search

```typescript
const results = await db.ftsSearch("Person", "bio", "machine learning", { limit: 10 });
for (const r of results) {
  console.log(`Node ${r.nodeId}: score=${r.score.toFixed(4)}`);
}
```

#### Fuzzy Search (Typo-Tolerant)

```typescript
// Finds "machine learning" even with typos
const results = await db.ftsSearchFuzzy("Person", "bio", "machne lerning", { limit: 10 });

// Control fuzzy matching sensitivity
const precise = await db.ftsSearchFuzzy("Person", "bio", "machne", {
  limit: 10,
  maxDistance: 2, // Max edit distance (default: 0 = auto)
  minTermLength: 4, // Min term length for fuzzy matching (default: 0 = auto)
});
```

### Embeddings

LatticeDB includes a built-in hash embedding function and an HTTP client for external embedding services. For new code, prefer the dedicated `@hajewski/latticedb/embedding` entrypoint. The package root still exposes deprecated compatibility aliases.

#### Hash Embeddings (Built-in)

Deterministic, no external service needed. Useful for testing or simple keyword-based similarity:

```typescript
import { hashEmbed } from "@hajewski/latticedb/embedding";

const vec = hashEmbed("hello world", 128);
console.log(vec.length); // 128
```

#### HTTP Embedding Client

Connect to Ollama, OpenAI, or compatible APIs:

```typescript
import { EmbeddingClient, EmbeddingApiFormat } from "@hajewski/latticedb/embedding";

// Ollama (default). The endpoint is the full URL the client POSTs to.
const client = new EmbeddingClient({
  endpoint: "http://localhost:11434/api/embeddings",
});
const vec = client.embed("hello world");
client.close();

// OpenAI-compatible API
const openaiClient = new EmbeddingClient({
  endpoint: "https://api.openai.com/v1/embeddings",
  model: "text-embedding-3-small",
  apiFormat: EmbeddingApiFormat.OpenAI,
  apiKey: "sk-...",
});
const embedding = openaiClient.embed("hello world");
openaiClient.close();
```

### Edge Traversal

```typescript
await db.read(async (txn) => {
  const outgoing = await txn.getOutgoingEdges(nodeId);
  for (const edge of outgoing) {
    console.log(`${edge.sourceId} --[${edge.type}]--> ${edge.targetId}`);
  }

  const incoming = await txn.getIncomingEdges(nodeId);
  for (const edge of incoming) {
    console.log(`${edge.sourceId} --[${edge.type}]--> ${edge.targetId}`);
  }
});
```

### Cypher Queries

```typescript
// Pattern matching
const result = await db.query("MATCH (n:Person) RETURN n.name");

// With parameters
const result = await db.query(
  "MATCH (n:Person) WHERE n.name = $name RETURN n",
  { name: "Alice" }
);

// Vector similarity in Cypher
const result = await db.query(
  "MATCH (n:Document) WHERE n.embedding <=> $vec < 0.5 RETURN n.title",
  { vec: new Float32Array([0.1, 0.2, 0.3, 0.4]) }
);

// Full-text search in Cypher
const result = await db.query(
  'MATCH (n:Document) WHERE n.content @@ "machine learning" RETURN n.title'
);

// Data mutation
await db.query('CREATE (n:Person {name: "Charlie", age: 35})');
await db.query('MATCH (n:Person {name: "Charlie"}) SET n.age = 36');
await db.query('MATCH (n:Person {name: "Charlie"}) DETACH DELETE n');
```

### Query Cache

```typescript
// Get cache statistics
const stats = await db.cacheStats();
console.log(
  `Entries: ${stats.entries}, Hits: ${stats.hits}, Misses: ${stats.misses}`
);

// Clear the cache
await db.cacheClear();
```

### Checkpoints

A commit is durable once it is in the log (the `-wal` file beside the database).
A checkpoint writes the pages the log's commits changed into the database file
and can then empty the log. By default a commit runs one once the log holds 1000
frames (one frame per page, 4096 bytes); the `LATTICE_AUTO_CHECKPOINT_FRAMES`
environment variable sets another default (`0` for none), and
`setAutoCheckpoint` wins over it. Closing a database always folds the log into
the file.

```typescript
import type { CheckpointStats } from '@hajewski/latticedb';

await db.setAutoCheckpoint(0);   // commits never checkpoint; the log grows
// ... a burst of commits ...
const stats: CheckpointStats = await db.checkpoint('truncate');
console.log(stats.pagesFlushed, stats.durationNs, stats.checkpointLsn, stats.walTruncated);

await db.setAutoCheckpoint(4000, 1000); // every 4000 frames, at most once a second
```

Modes (`CheckpointMode`): `'passive'` writes the changed pages unless one is in
use (then it writes nothing and reports 0 pages) and leaves the log in place;
`'full'` writes every changed page and leaves the log in place; `'truncate'` (the
default) does what `'full'` does and then empties the log, the only mode that
bounds its size. Any other mode throws a `TypeError`, and a negative or
fractional argument to `setAutoCheckpoint` a `RangeError`. `durationNs` and
`checkpointLsn` are `bigint`. `checkpoint` rejects with code -9 (`ReadOnly`) on a
read-only database and -8 (`LockTimeout`) if a transaction already has records
in the log; `setAutoCheckpoint` has no effect on a read-only database. Against a
native library without `lattice_checkpoint`, both reject with code -14
(`Unsupported`).

The interval is a trade-off. A larger one means fewer checkpoints, each a longer
pause for the commit that runs it, a longer `-wal` file and more to replay after
a crash. A smaller one means shorter pauses, more often.

### Durable Streams and Changefeeds

Streams are durable named event logs stored inside the database file. Records are
published in write transactions, sequence numbers are per stream, and reads use
an explicit cursor. Reads do not acknowledge records; commit offsets separately
when your consumer has processed a batch.

```typescript
const db = new Database("events.db", { create: true });
await db.open();

await db.write(async (txn) => {
  txn.publishStream("jobs", { id: 1, status: "queued" }, "job.queued");
});

const records = await db.readStream("jobs", {
  afterSequence: 0n,
  limit: 100,
  timeoutMs: 0,
});

await db.write(async (txn) => {
  txn.setStreamOffset("jobs", "worker-a", records.at(-1)!.sequence);
  txn.trimStream("jobs", records.at(-1)!.sequence - 1n);
});
```

`db.changes()` reads the reserved `__lattice_changes` stream. It emits semantic
graph events such as `node.insert`, `node.property_set`, `edge.delete`, and
`edge.property_remove`, with payloads represented as normal TypeScript values.

A transaction's events describe its net effect (a property set several times
is one event, from its value before the transaction to its last value) and
come in an order that can be replayed as it comes: the edges it deletes, then
the nodes that exist after it, then the edges that exist after it, then the
nodes it deletes, each group by ascending id. So an edge is created after both
its nodes and deleted before either. See the
[changefeed guide](https://docs.latticedb.org/guides/changefeed) for the details
and for retention policies.

## Supported Property Types

- `null` - Null value
- `boolean` - Boolean
- `number` - Integer or float
- `bigint` - 64-bit integer
- `string` - UTF-8 string
- `Uint8Array` - Binary data
- `Float32Array` - Vector embeddings
- Arrays of any of these values, nested to any depth
- Plain objects with string keys, whose values are any of these, nested to any depth

## Error Handling

```typescript
import { Database, isLibraryAvailable } from "@hajewski/latticedb";

// Check if native library is available
if (!isLibraryAvailable()) {
  console.error("LatticeDB native library not found");
  process.exit(1);
}

try {
  const db = new Database("test.db", { create: true });
  await db.open();
  // ...
  await db.close();
} catch (error) {
  console.error("Database error:", error);
}
```

Errors from the native library are `LatticeError`s whose `code` is the C error
code; `LatticeErrorCode` (exported from the package root, with `QueryErrorStage`
for `LatticeQueryError.stage`; both are plain enums, readable at run time) names
them. For example -4 (not found, also from `createEdge` when either end does not
exist), -16 (another process holds the file), -17 (a plain `deleteNode` of a node
with relationships), -18 (open options that contradict the file's vector
index), -19 (`NeedsNewerWriter`: a writable open of a file that a newer
release has marked as not writable by this one; it still opens with
`readOnly: true`) and -11 (`VersionMismatch`: a file in a format too new for
this release to read at all, read-only or not).

A failed Cypher query throws a `LatticeQueryError`, a `LatticeError` that also
says where the query failed:

```typescript
import { Database, LatticeQueryError } from "@hajewski/latticedb";

try {
  await db.query("RETURN 1/0");
} catch (e) {
  if (e instanceof LatticeQueryError) {
    console.log(e.message);        // Division by zero: 1 / 0
    console.log(e.code);           // -1
    console.log(e.stage);          // 4 (execution)
    console.log(e.diagnosticCode); // DivisionByZero
    console.log(e.location);       // { line: 1, column: 9, length: 1 }
  }
}
```

`stage` is a `QueryErrorStage`: 1 parse, 2 semantic, 3 plan, 4 execution. Parse
and semantic failures have code -6 and execution failures -1, except that a
`DELETE` of a node with relationships has code -17 and a relationship created
to a node deleted earlier in the query -4, as `deleteNode` and `createEdge`
report them. A storage failure under a query has code -2 and an allocation
failure -13.

## Electron

These bindings run in Electron's **main process** (or a utility process). They
call into `liblattice` through koffi, which requires Node integration, so a
sandboxed renderer cannot open a database — expose the database over IPC instead.

koffi ships prebuilt binaries, so `electron-rebuild` is not needed.

### Unpack the native library from asar

`liblattice.{dylib,so,dll}` is a plain shared library, not a `.node` addon, so
Electron packagers do **not** unpack it automatically. Left inside `app.asar` it
is invisible to `dlopen()` / `LoadLibraryW()` even though `fs.existsSync()`
reports it as present. Unpack it:

electron-builder:

```json
{
  "build": {
    "asarUnpack": ["**/node_modules/@hajewski/latticedb/lib/**"]
  }
}
```

`@electron/packager` (Electron Forge):

```js
module.exports = {
  packagerConfig: {
    asar: { unpack: "**/node_modules/@hajewski/latticedb/lib/**" },
  },
};
```

Nothing else is required at runtime: the loader rewrites any `app.asar` path to
its `app.asar.unpacked` twin. The same glob is exported as
`ELECTRON_ASAR_UNPACK_GLOB` for packager configs generated from code.

### Bundlers

Electron Forge's Webpack and Vite templates rewrite `__dirname`, which breaks the
lookup of the library bundled beside the module. Either mark the package external
so it is required from `node_modules` at runtime (webpack `externals`, Vite
`build.rollupOptions.external`), or ship the library through `extraResources` and
let the loader find it under `process.resourcesPath`:

```json
{
  "build": {
    "extraResources": [
      { "from": "node_modules/@hajewski/latticedb/lib/darwin-arm64", "to": "lib" }
    ]
  }
}
```

Pick the source directory for the platform you are building. The published package
uses `lib/linux-x64` and `lib/linux-arm64` for Linux (glibc builds only); a package
staged locally with `npm run bundle:native` may carry a libc suffix instead
(`linux-x64-gnu`). The loader also accepts
`resources/lib/<platform>-<arch>/`, `resources/<platform>-<arch>/`, and the
library sitting next to the executable (electron-builder `extraFiles`).

`LATTICE_LIB_PATH` remains the explicit escape hatch — set it before the first
`db.open()` if the library lives somewhere else entirely.

### Diagnosing packaging problems

```typescript
import { isElectronRuntime, resolveLibraryPath } from "@hajewski/latticedb";

console.log(isElectronRuntime(), resolveLibraryPath());
```

`resolveLibraryPath()` runs the full search without loading anything and returns
the path the loader would use, or `null`. When the library exists only inside a
packed archive, opening a database fails with an error naming the archived path
and the `asarUnpack` glob to add.

### Windows

Windows x64 is built and tested in CI; Windows ARM64 is built but not run.
Published package tarballs bundle `lib/win32-x64/lattice.dll` and `lib/win32-arm64/lattice.dll`,
and koffi ships matching prebuilt `win32_x64` and `win32_arm64` binaries, so
nothing has to be compiled at install time.

Standalone release archives for Windows are `.zip` rather than `.tar.gz`, if you
would rather take `lattice.dll` from a release and ship it through
`extraResources`.

Building the library from a source checkout:

```bash
# From the package directory; picks the target for the host architecture
npm run bundle:native

# Or cross-compile one explicitly from the repository root
zig build shared -Dtarget=aarch64-windows-gnu -Doptimize=ReleaseFast
```

Note that `zig build shared` installs `lattice.dll` into `zig-out/bin` and leaves
only the import library `lattice.lib` in `zig-out/lib`. `bundle:native` and the
runtime loader both account for this; hand-written copy steps usually do not.

## Building from Source

Requires Node.js 18+ and the LatticeDB native library.

```bash
# From the latticedb root directory
zig build shared

# Build the TypeScript bindings
cd bindings/typescript
npm install
npm run build

# Run tests
npm test
```

## Requirements

- Node.js 18+, or Electron's main process (see [Electron](#electron); the loader's asar handling is tested without running Electron, and no Electron version is tested)
- The native LatticeDB library (`liblattice.dylib` / `liblattice.so` /
  `lattice.dll`)

## License

MIT
