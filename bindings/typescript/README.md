# LatticeDB TypeScript Bindings

TypeScript/Node.js bindings for [LatticeDB](https://github.com/jeffhajewski/latticedb), an embedded single-file property-graph database with native vector and BM25 full-text search.

## Installation

```bash
npm install @hajewski/latticedb
```

Published package tarballs are expected to bundle the native shared library for supported platforms.

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
  cacheSizeMb?: number;      // Cache size in MB (default: 100)
  enableVectors?: boolean;   // Preferred vector config flag
  enableVector?: boolean;    // Deprecated compatibility alias
  vectorDimensions?: number; // Vector dimensions (default: 128)
}
```

#### Methods

- `await db.open()` - Open the database connection
- `await db.close()` - Close the database connection
- `await db.read(fn)` - Execute a read-only transaction
- `await db.write(fn)` - Execute a read-write transaction
- `await db.query(cypher, params?)` - Execute a Cypher query
- `await db.vectorSearch(vector, options?)` - k-NN vector search
- `await db.ftsSearch(query, options?)` - Full-text search
- `await db.ftsSearchFuzzy(query, options?)` - Fuzzy full-text search
- `await db.createNodePropertyIndex(label, property)` / `dropNodePropertyIndex(...)` - Manage explicit node equality indexes
- `await db.createEdgePropertyIndex(edgeType, property)` / `dropEdgePropertyIndex(...)` - Manage explicit edge equality indexes
- `await db.readStream(stream, options?)` - Read durable stream records by cursor
- `await db.getStreamOffset(stream, consumer)` - Read a committed consumer offset
- `await db.changes(options?)` - Read the built-in graph changefeed
- `await db.cacheClear()` - Clear the query cache
- `await db.cacheStats()` - Get cache hit/miss statistics
- `db.isOpen()` - Check if database is open
- `db.getPath()` - Get database file path

### Transaction

#### Read Operations

- `await txn.getNode(nodeId)` - Get a node by ID, returns `Node` or `null`
- `await txn.nodeExists(nodeId)` - Check if a node exists
- `await txn.getProperty(nodeId, key)` - Get a property value
- `await txn.getOutgoingEdges(nodeId)` - Get outgoing edges from a node
- `await txn.getIncomingEdges(nodeId)` - Get incoming edges to a node
- `await txn.findNodesByLabelProperty(label, property, value, limit?)` - Indexed node equality lookup
- `await txn.findEdgesByTypeProperty(edgeType, property, value, limit?)` - Indexed edge equality lookup
- `txn.isReadOnly()` / `txn.isActive()` - Transaction state

#### Write Operations

- `await txn.createNode({ labels, properties })` - Create a node
- `await txn.deleteNode(nodeId)` - Delete a node
- `await txn.setProperty(nodeId, key, value)` - Set a property
- `await txn.setVector(nodeId, key, vector)` - Set a vector embedding
- `await txn.batchInsertVectors(label, vectors)` - Insert vector-bearing nodes in one call
- `await txn.batchInsert(label, vectors)` - Deprecated compatibility alias for `batchInsertVectors`
- `await db.createNodeFtsIndex(label, property)` - Declare a full-text index; writing that property keeps it current
- `await txn.createEdge(sourceId, targetId, edgeType, options?)` - Create an edge
- `await txn.deleteEdge(sourceId, targetId, edgeType)` - Delete an edge
- `await txn.setEdgeProperty(edgeId, key, value)` - Set an edge property by stable edge ID
- `await txn.getEdgeProperty(edgeId, key)` - Get an edge property by stable edge ID
- `await txn.removeEdgeProperty(edgeId, key)` - Remove an edge property by stable edge ID
- `txn.publishStream(stream, payload, kind?)` - Publish a durable stream record
- `txn.setStreamOffset(stream, consumer, sequence)` - Commit a durable consumer offset
- `txn.trimStream(stream, throughSequence)` - Delete stream records through a sequence
- `txn.commit()` / `txn.rollback()` - Commit or rollback

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

Property equality indexes are explicit and durable. Create them outside an
active write transaction; lookup fails instead of silently scanning when the
requested index does not exist.

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

// Ollama (default)
const client = new EmbeddingClient({
  endpoint: "http://localhost:11434",
});
const vec = client.embed("hello world");
client.close();

// OpenAI-compatible API
const openaiClient = new EmbeddingClient({
  endpoint: "https://api.openai.com/v1",
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

## Supported Property Types

- `null` - Null value
- `boolean` - Boolean
- `number` - Integer or float
- `string` - UTF-8 string
- `Uint8Array` - Binary data
- `Float32Array` - Vector embeddings

Nested arrays/objects are not currently exposed by the public bindings/C API.

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

Pick the source directory for the platform you are building; Linux bundles carry
a libc suffix (`linux-x64-gnu`, `linux-x64-musl`). The loader also accepts
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

Both Windows architectures are supported. Published package tarballs are
expected to bundle `lib/win32-x64/lattice.dll` and `lib/win32-arm64/lattice.dll`,
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

- Node.js 18+, or Electron 23+ (main process, Node 18 — see [Electron](#electron))
- The native LatticeDB library (`liblattice.dylib` / `liblattice.so` /
  `lattice.dll`)

## License

MIT
