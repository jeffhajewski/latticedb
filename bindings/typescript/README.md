# LatticeDB TypeScript Bindings

TypeScript/Node.js bindings for [LatticeDB](https://github.com/jeffhajewski/latticedb), an embedded knowledge graph database for AI and RAG applications.

## Installation

```bash
npm install lattice-db
```

## Quick Start

```typescript
import { Database } from 'lattice-db';

// Open or create a database
const db = new Database('knowledge.db', { create: true });
await db.open();

// Create nodes and relationships
await db.write(async (txn) => {
  const alice = await txn.createNode({
    labels: ['Person'],
    properties: { name: 'Alice', age: 30 }
  });

  const bob = await txn.createNode({
    labels: ['Person'],
    properties: { name: 'Bob', age: 25 }
  });

  await txn.createEdge(alice.id, bob.id, 'KNOWS');
});

// Query with Cypher
const result = await db.query('MATCH (n:Person) RETURN n.name, n.age');
for (const row of result.rows) {
  console.log(row);
}

await db.close();
```

## Features

- **Property Graph** - Nodes and edges with labels and properties
- **Vector Search** - HNSW-based k-NN search for embeddings
- **Full-Text Search** - BM25-ranked search with tokenization
- **Cypher Queries** - Pattern matching with `<=>` (vector) and `@@` (FTS) extensions
- **Transactions** - ACID-compliant read/write transactions
- **TypeScript** - Full type definitions included

## API Reference

### Database

```typescript
const db = new Database(path: string, options?: DatabaseOptions);

interface DatabaseOptions {
  create?: boolean;        // Create if not exists (default: false)
  readOnly?: boolean;      // Open read-only (default: false)
  cacheSizeMb?: number;    // Cache size in MB (default: 100)
  enableVector?: boolean;  // Enable vector storage (default: false)
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
- `db.isOpen()` - Check if database is open
- `db.getPath()` - Get database file path

### Transaction

```typescript
await db.write(async (txn) => {
  // Node operations
  const node = await txn.createNode({ labels: ['Label'], properties: { key: 'value' } });
  await txn.deleteNode(nodeId);
  const exists = await txn.nodeExists(nodeId);
  const node = await txn.getNode(nodeId);

  // Property operations
  await txn.setProperty(nodeId, 'key', 'value');
  const value = await txn.getProperty(nodeId, 'key');

  // Vector operations
  await txn.setVector(nodeId, 'embedding', new Float32Array([...]));

  // FTS operations
  await txn.ftsIndex(nodeId, 'text to index');

  // Edge operations
  const edge = await txn.createEdge(sourceId, targetId, 'EDGE_TYPE');
  await txn.deleteEdge(sourceId, targetId, 'EDGE_TYPE');
  const outgoing = await txn.getOutgoingEdges(nodeId);
  const incoming = await txn.getIncomingEdges(nodeId);
});
```

### Query Examples

```typescript
// Basic pattern matching
const result = await db.query('MATCH (n:Person) RETURN n.name');

// With parameters
const result = await db.query(
  'MATCH (n:Person) WHERE n.name = $name RETURN n',
  { name: 'Alice' }
);

// Vector similarity search
const result = await db.query(
  'MATCH (n:Document) WHERE n.embedding <=> $query < 0.5 RETURN n.title',
  { query: new Float32Array([...]) }
);

// Full-text search
const result = await db.query(
  'MATCH (n:Document) WHERE n.content @@ "machine learning" RETURN n.title'
);

// Data mutation
await db.query('CREATE (n:Person {name: "Alice", age: 30})');
await db.query('MATCH (n:Person {name: "Alice"}) SET n.age = 31');
await db.query('MATCH (n:Person {name: "Alice"}) REMOVE n.age');
await db.query('MATCH (n:Person {name: "Bob"}) DETACH DELETE n');
```

## Building from Source

Requires Node.js 18+ and the LatticeDB native library to be built.

```bash
# From the latticedb root directory
zig build shared

# Build the TypeScript bindings
cd bindings/typescript
npm install
npm run build:all

# Run tests
npm test
```

## License

MIT
