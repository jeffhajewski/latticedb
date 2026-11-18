# TypeScript / Node.js API

TypeScript/Node.js bindings for LatticeDB, an embedded knowledge graph database for AI/RAG applications.

## Installation

```bash
npm install @hajewski/latticedb
```

The published package bundles the native shared library for Linux (x64, arm64), macOS (x64, arm64) and Windows (x64, arm64), so nothing else needs installing. From a source checkout, build it with `zig build shared`; the loader finds `zig-out/lib` (or set `LATTICE_LIB_PATH`).

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

  // Writing the indexed property is what makes it searchable
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
  lock?: boolean;            // Take the file lock (default: true)
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
not `enableVectors` is set, so reopening needs no vector options at all, also
after a crash. `enableVectors` creates an index if the database has none, and
`vectorMetric`, `vectorM` and `vectorEfConstruction` configure that new index;
once it exists they are fixed. Any vector option you do pass (and
`vectorDimensions` when `enableVectors` is set) must match the file, or `open()`
throws a `LatticeError` with code -18 (`ConfigMismatch`).

```typescript
const db = new Database("vectors.db", {
  create: true,
  enableVectors: true,
  vectorDimensions: 2,
  vectorMetric: "euclidean",
});
await db.open();
await db.write(async (txn) => {
  const a = await txn.createNode({ labels: ["Doc"] });
  await txn.setVector(a.id, "embedding", new Float32Array([1, 0]));
  const b = await txn.createNode({ labels: ["Doc"] });
  await txn.setVector(b.id, "embedding", new Float32Array([0, 2]));
});
await db.close();

// No vector options: the file says how its index was built.
const reopened = new Database("vectors.db");
await reopened.open();
console.log(await reopened.vectorSearch(new Float32Array([1, 0]), { k: 2 }));
// [ { nodeId: 1n, distance: 0 }, { nodeId: 2n, distance: 2.2360680103302 } ]
await reopened.close();
```

The metric decides search distances, smallest first: cosine distance (vectors
stored at unit length), L2 distance, or the negative dot product (vectors stored
as given).

`syncMode: 'normal'` returns from a commit once the operating system has it: the
commit survives the process being killed, but a power cut or OS crash can lose
the most recent commits. The default, `'full'`, syncs the log on every commit.
`fullFsync: true` makes each sync on macOS ask the drive to flush its cache. A
value you pass wins over the `LATTICE_SYNC_MODE` and `LATTICE_FULL_FSYNC`
environment variables; they apply only when you leave the option unset. See
[Durability and the Log](../configuration/durability.md).

`enableWal: false` logs nothing. Transactions, rollback, streams and the
changefeed work as with the log, but a commit reaches the file at the next
flush (`close()`, or when half the page cache holds changes) rather than when it
returns; see
[Running without the log](../configuration/durability.md#running-without-the-log).

`syncMode`, `fullFsync`, the three vector index options and
`changefeedRetention` need a native library with `lattice_open_v5`; against an
older one, setting any of them throws with code -14 (`Unsupported`).
`deserialize()` takes them too, through `lattice_deserialize_v5`, with the same
fallback against an older library.

`readOnly: true` on a database that was not closed cleanly recovers it in
memory: the handle sees every committed transaction, and nothing is written to
the files. If that recovery needs more than 64 MB plus four times the size of
the log and journal, `open()` throws with code -10 (`Full`); open the database
read-write once to recover it on disk. A file that a newer release has marked as
not writable by this one opens only with `readOnly: true`; a writable open
throws with code -19 (`NeedsNewerWriter`) and writes nothing.

#### In-memory databases

```typescript
const db = new Database(':memory:');
```

Pass `:memory:` as the path and the database has no files behind it. Nothing is
written to disk and nothing survives closing it, which suits a scratch database, a
test, or one you pulled out of object storage and will hand back as bytes.

It behaves like any other database — transactions, the write-ahead log, and
serialization all work. The differences are that it disappears when closed, and
that nothing locks it, since no other process can reach it.

#### The file lock

A database can only be open in one process at a time. Opening takes a lock on the
file, so a second process gets a `LatticeError` with
`code === -16` (`LATTICE_ERROR_DATABASE_LOCKED`) rather than quietly corrupting your
data. A read-only handle shares the lock with other readers, but is still refused
while a writer holds the database, because what it would read is a stale file that
a checkpoint may be rewriting underneath it.

Pass `lock: false` only on filesystems where locking does not work. It does not
make concurrent access safe; it removes the thing that was going to tell you it
was not.

#### Methods

- `await db.open()` - Open the database connection
- `await db.close()` - Close the database connection
- `db.serialize()` - Return the whole database as bytes
- `await db.read(fn)` - Execute a read-only transaction
- `await db.write(fn)` - Execute a read-write transaction
- `await db.query(cypher, params?)` - Execute a Cypher query
- `await db.getNodesByLabel(label)` - IDs of every node with a label
- `await db.vectorSearch(vector, options?)` - k-NN vector search
- `await db.ftsSearch(label, property, query, options?)` - Full-text search of one declared index
- `await db.ftsSearchFuzzy(label, property, query, options?)` - Fuzzy full-text search
- `await db.createNodeFtsIndex(label, property)` / `dropNodeFtsIndex(...)` / `hasNodeFtsIndex(...)` - Manage node full-text indexes
- `await db.createEdgeFtsIndex(edgeType, property)` / `dropEdgeFtsIndex(...)` / `hasEdgeFtsIndex(...)` - Manage relationship full-text indexes. A node index and a relationship index cannot share a name and property: the second is rejected as already existing
- `await db.createNodePropertyIndex(label, property)` / `dropNodePropertyIndex(...)` - Manage explicit node property indexes, used for equality lookups and by Cypher for ranges, prefixes and sorting
- `await db.createEdgePropertyIndex(edgeType, property)` / `dropEdgePropertyIndex(...)` - Manage explicit edge property indexes
- `await db.readStream(stream, options?)` - Read durable stream records by cursor. With `timeoutMs > 0` the wait runs on a worker thread, so the event loop keeps running and a commit from the same process can end the wait
- `await db.getStreamOffset(stream, consumer)` - Read a committed consumer offset
- `await db.changes(options?)` - Read the built-in graph changefeed. Rejects with `LatticeChangesTrimmedError` when retention has dropped the events after `afterSequence`, and with code -14 (`Unsupported`) when the policy is `'off'`
- `await db.changefeedStatus()` - The changefeed's policy and range: `{ retention, oldestSequence, lastSequence }`, `oldestSequence` null when it holds no events
- `await db.trimChangefeed()` - Apply the retention policy in full now; resolves to the number of events dropped
- `await db.cacheClear()` - Clear the query cache
- `await db.cacheStats()` - Get cache hit/miss statistics
- `db.isOpen()` - Check if database is open
- `db.getPath()` - Get database file path

`deserialize(bytes, options?, copy = true)`, exported from the package root, opens a
database from bytes produced by `serialize()`.

Node IDs, edge IDs and stream sequence numbers are `bigint`.

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

- `await txn.createNode({ labels, properties })` - Create a node. A property whose value is `null` (or `undefined`) is left out, as in Cypher's `CREATE`, and the returned `Node` leaves it out too
- `await txn.deleteNode(nodeId, { detach })` - Delete a node. A node that still has relationships is refused with code -17 (`HasRelationships`) and nothing is deleted, as Cypher's `DELETE` does; `{ detach: true }` deletes its relationships with it, as `DETACH DELETE` does. Deleting a node that does not exist does nothing.
- `await txn.setProperty(nodeId, key, value)` - Set a property. A `null` value removes the property, as Cypher's `SET n.x = null` does; removing one the node does not have does nothing. Before 0.18.1 it stored the `null`
- `await txn.setVector(nodeId, key, vector)` - Set the node's vector embedding. A node has one vector, which Cypher reads as `n.embedding`; `key` must be `"embedding"` (or `""`), and any other key is rejected with an unsupported-operation error. A vector with NaN or infinite components is rejected as an invalid argument. On a database created without a vector index, `setVector`, `batchInsertVectors` (before it creates any node) and `vectorSearch` reject with a `LatticeError` whose code is `Unsupported` and whose message says so; before 0.18.1 the code was `IoError`. Opening the database with `enableVectors: true` and `vectorDimensions` adds an index.
- `await txn.removeVector(nodeId, key = "embedding")` - Remove the node's vector. The node keeps its labels and properties, and vector search no longer returns it. Removing a vector the node does not have does nothing. Rejects with code -4 (`NotFound`) if the node does not exist.
- `await txn.batchInsertVectors(label, vectors)` - Insert vector-bearing nodes in one call
- `await txn.batchInsert(label, vectors)` - Deprecated compatibility alias for `batchInsertVectors`
- `await txn.createEdge(sourceId, targetId, edgeType, options?)` - Create an edge and return its stable edge ID on the `Edge`. Throws with code -4 (`NotFound`) unless both ends exist as the transaction sees them. A property whose value is `null` is left out
- `await txn.deleteEdge(sourceId, targetId, edgeType)` - Delete an edge
- `await txn.setEdgeProperty(edgeId, key, value)` - Set an edge property by stable edge ID. A `null` value removes it, as `setProperty` does
- `await txn.getEdgeProperty(edgeId, key)` - Get an edge property by stable edge ID
- `await txn.removeEdgeProperty(edgeId, key)` - Remove an edge property by stable edge ID
- `txn.publishStream(stream, payload, kind?)` - Publish a durable stream record
- `txn.publishStreamGetSequence(stream, payload, kind?)` - Publish and return the record's sequence (`bigint`)
- `txn.setStreamOffset(stream, consumer, sequence)` - Commit a durable consumer offset
- `txn.trimStream(stream, throughSequence)` - Delete stream records through a sequence
- `txn.commit()` / `txn.rollback()` - Commit or rollback. A commit that throws with code -7 (`TxnAborted`) has ended the transaction, and `rollback()` afterwards does nothing; after any other failure it stays open for `rollback()`

### Batch Insert

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

LatticeDB includes a built-in hash embedding function and an HTTP client for external embedding services.

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

The endpoint is POSTed to exactly as given; nothing is appended, so a server's
base URL alone (`http://localhost:11434`) sends the request to `/` and `embed`
throws. `timeoutMs` is how long `embed` waits for the response to begin: it bounds the wait for the server to start answering (default 30 seconds), not connecting or reading the rest of a response that has begun, and is not applied on Windows.

### Edge Traversal

```typescript
await db.read(async (txn) => {
  const outgoing = await txn.getOutgoingEdges(nodeId);
  for (const edge of outgoing) {
    console.log(`${edge.id}: ${edge.sourceId} --[${edge.type}]--> ${edge.targetId}`);
  }

  const incoming = await txn.getIncomingEdges(nodeId);
  for (const edge of incoming) {
    console.log(`${edge.id}: ${edge.sourceId} --[${edge.type}]--> ${edge.targetId}`);
  }
});
```

### Edge Properties

Edge properties are addressed by stable edge ID. `createEdge` returns an `Edge`
object containing that ID, and traversal results include it.

```typescript
await db.write(async (txn) => {
  const edge = await txn.createEdge(alice.id, bob.id, "KNOWS");
  await txn.setEdgeProperty(edge.id, "since", 2020);
  const since = await txn.getEdgeProperty(edge.id, "since");
});
```

### Durable Streams

```typescript
await db.write(async (txn) => {
  await txn.publishStream("jobs", { id: 1, status: "queued" }, "job.queued");
  await txn.setStreamOffset("jobs", "worker-a", 1n);
});

const records = await db.readStream("jobs", { afterSequence: 0n, limit: 100 });
const changes = await db.changes({ afterSequence: 0n, limit: 100 });
```

`changes()` returns each transaction's net effect (a property set several times
is one event, from its value before the transaction to its last value), in an
order that can be replayed as it comes: the edges it deletes, the nodes that
exist after it, the edges that exist after it, then the nodes it deletes, each
group by ascending id. How much of the changefeed to keep is set with
`changefeedRetention`; see [The Changefeed](../guides/changefeed.md).

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

```typescript
const stats = await db.checkpoint('truncate'); // fold the log into the file now
await db.setAutoCheckpoint(10000);             // commits checkpoint every 10,000 frames
await db.setAutoCheckpoint(0);                 // commits never checkpoint
```

`'passive'` gives way if a changed page is in use, `'full'` writes every changed
page and leaves the log, and `'truncate'` (the default) also empties it. The
stats report `pagesFlushed`, `durationNs`, `checkpointLsn` and `walTruncated`;
`setAutoCheckpoint` takes an optional minimum gap in milliseconds as its second
argument. Without the setter, the `LATTICE_AUTO_CHECKPOINT_FRAMES` environment variable sets the frame count (default 1000). A larger interval means fewer pauses, each longer, a longer `-wal` file and more to replay after a crash; see [Checkpointing](../configuration/durability.md#checkpointing).

## Supported Property Types

- `null` - Null value. A property is never stored as `null`: writing one removes the property (see `setProperty`). `null` inside an array or an object is kept
- `boolean` - Boolean
- `number` - Integer or float
- `bigint` - 64-bit integer
- `string` - UTF-8 string
- `Uint8Array` - Binary data
- `Float32Array` - Vector
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
code; `LatticeErrorCode`, exported from the package root, names them (with
`QueryErrorStage` for `LatticeQueryError.stage`; both are plain enums, readable
at run time). For example -4 (`NotFound`, also from `createEdge` when either end
does not exist), -10 (`Full`, a read-only open whose crash recovery needs more
memory than it may use), -11 (`VersionMismatch`, a file in a format only a newer
release can read), -16 (`DatabaseLocked`, another process holds the file), -17
(`HasRelationships`, a plain `deleteNode` of a node with relationships), -18
(`ConfigMismatch`, open options that contradict the file's vector index) and -19
(`NeedsNewerWriter`, a writable open of a file only a newer release may write).

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
and semantic failures (an unknown function is `unknown_function`) have code -6
and execution failures -1, except that a `DELETE` of a node with relationships
has code -17 and a relationship created to a node deleted earlier in the query
-4, as `deleteNode` and `createEdge` report them. A storage failure under a query
has code -2 and an allocation failure -13. `location` is undefined when the
failure does not belong to one place in the query.

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

- Node.js 18+
- The native LatticeDB library (`liblattice.dylib` / `liblattice.so` / `lattice.dll`), bundled in the published package
