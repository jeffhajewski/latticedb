# LatticeDB TypeScript Bindings

TypeScript/Node.js bindings for [LatticeDB](https://github.com/jeffhajewski/latticedb), an embedded knowledge graph database for AI/RAG applications.

## Installation

```bash
npm install @hajewski/latticedb
```

The native shared library (`liblattice.dylib` / `liblattice.so`) must be available on the system. Install it via the [install script](https://github.com/jeffhajewski/latticedb#installation) or build from source with `zig build shared`.

## Quick Start

```typescript
import { Database } from "@hajewski/latticedb";

const db = new Database("knowledge.db", {
  create: true,
  enableVector: true,
  vectorDimensions: 4,
});
await db.open();

// Create nodes, edges, and index content
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

  // Index text for full-text search
  await txn.ftsIndex(alice.id, "Alice works on machine learning research");
  await txn.ftsIndex(bob.id, "Bob studies deep learning and neural networks");

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
const ftsResults = await db.ftsSearch("machine learning");
for (const r of ftsResults) {
  console.log(`Node ${r.nodeId}: score=${r.score.toFixed(4)}`);
}

// Fuzzy search (typo-tolerant)
const fuzzyResults = await db.ftsSearchFuzzy("machin lerning");
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
- **Batch Insert** - Efficient bulk insertion of nodes with vectors
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
  enableVector?: boolean;    // Enable vector storage (default: false)
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
- `txn.isReadOnly()` / `txn.isActive()` - Transaction state

#### Write Operations

- `await txn.createNode({ labels, properties })` - Create a node
- `await txn.deleteNode(nodeId)` - Delete a node
- `await txn.setProperty(nodeId, key, value)` - Set a property
- `await txn.setVector(nodeId, key, vector)` - Set a vector embedding
- `await txn.batchInsert(label, vectors)` - Batch insert nodes with vectors
- `await txn.ftsIndex(nodeId, text)` - Index text for full-text search
- `await txn.createEdge(sourceId, targetId, edgeType)` - Create an edge
- `await txn.deleteEdge(sourceId, targetId, edgeType)` - Delete an edge
- `txn.commit()` / `txn.rollback()` - Commit or rollback

### Batch Insert

Insert many nodes with vectors in a single efficient call:

```typescript
import { Database } from "@hajewski/latticedb";

const db = new Database("vectors.db", {
  create: true,
  enableVector: true,
  vectorDimensions: 128,
});
await db.open();

await db.write(async (txn) => {
  const vectors = Array.from({ length: 1000 }, () =>
    Float32Array.from({ length: 128 }, () => Math.random())
  );
  const nodeIds = await txn.batchInsert("Document", vectors);
  console.log(`Created ${nodeIds.length} nodes`);
});

await db.close();
```

### Full-Text Search

#### Exact Search

```typescript
const results = await db.ftsSearch("machine learning", { limit: 10 });
for (const r of results) {
  console.log(`Node ${r.nodeId}: score=${r.score.toFixed(4)}`);
}
```

#### Fuzzy Search (Typo-Tolerant)

```typescript
// Finds "machine learning" even with typos
const results = await db.ftsSearchFuzzy("machne lerning", { limit: 10 });

// Control fuzzy matching sensitivity
const precise = await db.ftsSearchFuzzy("machne", {
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
import { getFFI } from "@hajewski/latticedb/ffi";

const ffi = getFFI();
const vec = ffi.hashEmbed("hello world", 128);
console.log(vec.length); // 128
```

#### HTTP Embedding Client

Connect to Ollama, OpenAI, or compatible APIs:

```typescript
import { getFFI, EmbeddingApiFormat } from "@hajewski/latticedb";

const ffi = getFFI();

// Ollama (default)
const client = ffi.embeddingClientCreate({
  endpoint: "http://localhost:11434",
});
const vec = ffi.embeddingClientEmbed(client, "hello world");
ffi.embeddingClientFree(client);

// OpenAI-compatible API
const openaiClient = ffi.embeddingClientCreate({
  endpoint: "https://api.openai.com/v1",
  model: "text-embedding-3-small",
  apiFormat: EmbeddingApiFormat.OpenAI,
  apiKey: "sk-...",
});
const embedding = ffi.embeddingClientEmbed(openaiClient, "hello world");
ffi.embeddingClientFree(openaiClient);
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

## Supported Property Types

- `null` - Null value
- `boolean` - Boolean
- `number` - Integer or float
- `string` - UTF-8 string
- `Uint8Array` - Binary data

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
- The native LatticeDB library (`liblattice.dylib` / `liblattice.so`)

## License

MIT
