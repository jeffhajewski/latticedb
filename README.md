# LatticeDB

**Embedded property-graph database with native vector and full-text indexing.**

LatticeDB is a single-file local database for connected, semantic, and textual data. It lets you traverse relationships, run vector similarity search, and do BM25 full-text search over the same dataset in one engine and one query layer. It is designed for relationship-heavy workloads on a single machine, with zero-config operation and an embedded single-writer model.

LatticeDB is an embedded, single-file graph database that lets local applications query the same data by relationship, semantics, and text, then consume durable graph and application events from the same file. Workloads like Graph RAG, agent memory, and local knowledge tools are examples built on those primitives, not the definition of the engine.

- **One file.** Your entire database is a single portable file. No server, no configuration.
- **One query layer.** Graph traversal, HNSW vector similarity, and BM25 full-text — in the same query language.
- **One event log.** Durable named streams and a built-in graph changefeed share the same transaction/WAL path as graph writes.
- **Local-first.** Designed for one owning process on one machine, with WAL-backed durability.
- **Fast.** 0.2 μs reads by node id and 2.3 μs Cypher lookups through the C API; top-10 vector search over 100K vectors in 0.12 ms (see [Performance](#performance)).

```cypher
// Find chunks similar to a query, traverse to their document, then to the author
MATCH (chunk:Chunk)-[:PART_OF]->(doc:Document)-[:AUTHORED_BY]->(author:Person)
WHERE chunk.embedding <=> $query_vector < 0.3
  AND doc.content @@ "neural networks"
RETURN doc.title, chunk.text, author.name
ORDER BY chunk.embedding <=> $query_vector
LIMIT 10
```

## Install

**CLI**

```bash
curl -fsSL https://raw.githubusercontent.com/jeffhajewski/latticedb/main/dist/install.sh | bash
```

The installer runs on Linux and macOS. On Windows, download the `.zip` archive from the [releases page](https://github.com/jeffhajewski/latticedb/releases); it holds `lattice.exe` and `lattice.dll`.

**Python**

```bash
pip install latticedb
```

Published wheels bundle `liblattice` for Linux (x86_64, aarch64), macOS (x86_64, arm64) and Windows (x64, ARM64). Source installs can also bundle a staged native library during wheel builds with `LATTICE_BUNDLE_LIB_DIR=/path/to/lib`.

**TypeScript / Node.js**

```bash
npm install @hajewski/latticedb
```

The published package bundles `liblattice` for the same six platforms. Source checkouts can stage the native library into the package with `LATTICE_BUNDLE_LIB_DIR=/path/to/lib npm run bundle:native`.

Electron apps run the bindings in the main process and must unpack the native library from `app.asar`; see [Electron](bindings/typescript/README.md#electron).

**Java**

Requires JDK 21+. See [bindings/java/README.md](bindings/java/README.md) for the Maven build, which compiles the JNI bridge and stages `liblattice` from `zig-out/lib`. A runnable knowledge-graph example is in [bindings/java/src/main/java/io/latticedb/examples](bindings/java/src/main/java/io/latticedb/examples).

**Go**

See [bindings/go/README.md](bindings/go/README.md) for the current cgo workflow. The default consumer path uses installed `pkg-config` metadata; in-repo development can use `-tags repolocal` against `zig-out/lib`.
There is also a runnable graph/vector/text retrieval example in [examples/go](examples/go).

Version 0.5 moved embedding helpers into dedicated modules and subpackages. See [docs/client_api_migration.md](docs/client_api_migration.md) for the preferred imports and current compatibility aliases.

## Start Here

- [Getting Started](docs/getting_started.md) maps the shortest path for CLI, Python, TypeScript, Go, and Java.
- [CLI Quickstart](examples/cli/README.md) is the smallest copy-paste example in the repo.
- [Examples Overview](examples/README.md) covers the larger graph/vector/text retrieval demos.

## Example

A complete example: create a small knowledge graph with documents and authors, store embeddings, index text, then query across all three search modes.

The examples use the built-in hash embedding helper (`hash_embed` / `hashEmbed` / `Hash` / `Embedding.hashEmbed`) so they run with no
external service. It is a deterministic bag-of-words hash, not a semantic embedding: texts that share words get
nearby vectors, but synonyms and paraphrases do not, so a distance threshold is arbitrary and a
similarity query may match nothing.
Use a real embedding model for anything where the results should mean something — see
[Working with Embeddings](https://docs.latticedb.org/guides/embeddings).

### Python

```python
from latticedb import Database
from latticedb.embedding import hash_embed

with Database("knowledge.db", create=True, enable_vectors=True, vector_dimensions=128) as db:

    # --- Build the graph ---
    db.create_node_fts_index("Chunk", "text")

    with db.write() as txn:
        # Create authors
        alice = txn.create_node(labels=["Person"], properties={"name": "Alice", "field": "ML"})
        bob = txn.create_node(labels=["Person"], properties={"name": "Bob", "field": "Systems"})
        txn.create_edge(alice.id, bob.id, "COLLABORATES_WITH")

        # Create documents with chunks
        for title, text, author in [
            ("Attention Is All You Need", "The transformer architecture uses self-attention...", alice),
            ("Scaling Laws for LLMs", "We find that model performance scales predictably...", alice),
            ("Log-Structured Merge Trees", "LSM trees optimize write-heavy workloads...", bob),
        ]:
            doc = txn.create_node(labels=["Document"], properties={"title": title})
            chunk = txn.create_node(labels=["Chunk"], properties={"text": text})

            # The chunk's text property is already indexed by the declaration above.
            txn.set_vector(chunk.id, "embedding", hash_embed(text, dimensions=128))

            txn.create_edge(chunk.id, doc.id, "PART_OF")
            txn.create_edge(doc.id, author.id, "AUTHORED_BY")

        txn.commit()

    # --- Query: vector search + text match + graph traversal ---
    results = db.query("""
        MATCH (chunk:Chunk)-[:PART_OF]->(doc:Document)-[:AUTHORED_BY]->(author:Person)
        WHERE chunk.embedding <=> $query < 0.5
        RETURN doc.title, chunk.text, author.name
        ORDER BY chunk.embedding <=> $query
        LIMIT 5
    """, parameters={"query": hash_embed("transformer attention mechanism", dimensions=128)})

    for row in results:
        print(f"{row['doc.title']} by {row['author.name']}")

    # --- Full-text search ---
    for r in db.fts_search("Chunk", "text", "self-attention transformer"):
        print(f"Node {r.node_id}: score={r.score:.4f}")

    # --- Aggregations ---
    stats = db.query("""
        MATCH (doc:Document)-[:AUTHORED_BY]->(p:Person)
        RETURN p.name, count(doc) AS papers
        ORDER BY papers DESC
    """)
    for row in stats:
        print(f"{row['p.name']}: {row['papers']} papers")
```

### TypeScript

```typescript
import { Database } from "@hajewski/latticedb";
import { hashEmbed } from "@hajewski/latticedb/embedding";

const db = new Database("knowledge.db", {
  create: true,
  enableVectors: true,
  vectorDimensions: 128,
});
await db.open();

// Build a graph
await db.write(async (txn) => {
  const alice = await txn.createNode({
    labels: ["Person"],
    properties: { name: "Alice", field: "ML" },
  });
  const doc = await txn.createNode({
    labels: ["Document"],
    properties: { title: "Attention Is All You Need" },
  });
  const chunk = await txn.createNode({
    labels: ["Chunk"],
    properties: { text: "The transformer architecture uses self-attention..." },
  });

  await txn.setVector(chunk.id, "embedding", hashEmbed("transformer self-attention", 128));

  await txn.createEdge(chunk.id, doc.id, "PART_OF");
  await txn.createEdge(doc.id, alice.id, "AUTHORED_BY");
});

// Query across vector search + graph traversal
const results = await db.query(
  `MATCH (chunk:Chunk)-[:PART_OF]->(doc:Document)-[:AUTHORED_BY]->(author:Person)
   WHERE chunk.embedding <=> $query < 0.5
   RETURN doc.title, chunk.text, author.name
   ORDER BY chunk.embedding <=> $query
   LIMIT 5`,
  { query: hashEmbed("attention mechanism", 128) }
);

for (const row of results.rows) {
  console.log(`${row["doc.title"]} by ${row["author.name"]}`);
}

await db.close();
```

### Go

```go
// import latticeembedding "github.com/jeffhajewski/latticedb/bindings/go/embedding"
db, err := latticedb.Open("knowledge.db", latticedb.OpenOptions{
    Create: true,
    EnableVectors: true,
    VectorDimensions: 128,
})
if err != nil {
    log.Fatal(err)
}
defer db.Close()

if err := db.CreateNodeFTSIndex("Chunk", "text"); err != nil {
    log.Fatal(err)
}

err = db.Update(func(tx *latticedb.Tx) error {
    node, err := tx.CreateNode(latticedb.CreateNodeOptions{
        Labels: []string{"Chunk"},
        Properties: map[string]latticedb.Value{"text": "The transformer architecture uses self-attention..."},
    })
    if err != nil {
        return err
    }
    embedding, err := latticeembedding.Hash("The transformer architecture uses self-attention...", 128)
    if err != nil {
        return err
    }
    if err := tx.SetVector(node.ID, "embedding", embedding); err != nil {
        return err
    }
    return tx.SetProperty(node.ID, "text", "The transformer architecture uses self-attention...")
})
if err != nil {
    log.Fatal(err)
}
```

### Java

```java
import io.latticedb.*;
import java.util.List;
import java.util.Map;

try (Database db = Database.open("knowledge.db",
        OpenOptions.defaults().create(true).enableVectors(true).vectorDimensions(128))) {

    // The Chunk.text property is what full-text search reads.
    db.createNodeFtsIndex("Chunk", "text");

    // Build a graph
    try (Transaction txn = db.beginWrite()) {
        Node chunk = txn.createNode(List.of("Chunk"),
                Map.of("text", "The transformer architecture uses self-attention..."));
        txn.setVector(chunk.id(), "embedding",
                Embedding.hashEmbed("transformer self-attention", 128));
        txn.commit();
    }

    // Query across vector search + graph traversal
    QueryResult results = db.query(
            "MATCH (chunk:Chunk) " +
            "WHERE chunk.embedding <=> $query < 0.5 " +
            "RETURN chunk.text " +
            "ORDER BY chunk.embedding <=> $query LIMIT 5",
            Map.of("query", Embedding.hashEmbed("attention mechanism", 128)));

    for (Map<String, Object> row : results.rows()) {
        System.out.println(row.get("chunk.text"));
    }
}
```

See [bindings/java/README.md](bindings/java/README.md) for build instructions and the full runnable example.

## Performance

Measured on 0.17.0 through the public C API with the default options (log on and
fsynced on every commit, 100 MB cache), ReleaseFast, on an Apple M5 (10 cores,
24 GB, macOS 26.5), 2026-11-13. Medians of five runs; the full tables, the method
and the spreads are in [Benchmarks](book/src/performance/benchmarks.md), and
`zig build capi-benchmark` reproduces them.

| Operation | p50 | p99 |
|-----------|----:|----:|
| Create a node, one durable commit | 44 μs | 70 μs |
| Create nodes, 1,000 per transaction | 332,000 nodes/s | |
| Read a property by node id (C API, open read transaction) | 0.22 μs | 0.24 μs |
| Cypher lookup by an indexed property, 100K nodes | 2.3 μs | 2.5 μs |
| Cypher 2-hop count from an indexed node, 100K nodes / 500K edges | 8.8 μs | 12 μs |
| Cypher `*1..5` path count (3,905 paths), 100K nodes | 600 μs | 661 μs |
| Full-text top-10, one word, 10K / 100K documents | 8.5 / 104 μs | 103 μs / 1.2 ms |
| Vector top-10, 100K vectors, 128-d (recall@10 98.3%) | 122 μs | 192 μs |
| Vector top-10, 1M vectors, default 100 MB cache (recall@10 95.6%) | 1.34 ms | 3.1 ms |
| Vector top-10, 1M vectors, 2 GB cache | 0.21 ms | 0.46 ms |

With `sync_mode = normal` a single commit takes 6.4 μs. Loading 1M nodes and 2M
edges takes 34 s with the defaults and 8 s with the
[bulk-load recipe](book/src/configuration/durability.md#bulk-loading). Against
SQLite through its own C API with the same durability, LatticeDB is level for one
to three hops and about twice as fast for `*1..5`, while SQLite inserts rows several
times faster; see [Competitive Analysis](book/src/performance/competitive-analysis.md),
which also lists other systems' published figures, as citations kept apart from
these measurements.

## Features

**Graph**
- Nodes and edges with labels and arbitrary properties
- Durable property indexes on node and relationship properties, kept in order, so
  Cypher reads equality, ranges, `STARTS WITH` and `ORDER BY ... LIMIT` from them
- Multi-hop traversal, variable-length paths (`*1..3`)
- ACID transactions with commit/rollback and crash recovery
- Deleting a node that still has relationships is refused unless asked to detach
  them, and a relationship needs both of its ends
- MERGE, WITH, UNWIND, aggregations (`count`, `sum`, `avg`, `min`, `max`, `collect`)

**Vector Search**
- HNSW approximate nearest neighbor, with `ef_search` tunable per query
- Cosine, Euclidean or dot-product distance, chosen when the index is created; the
  file records the index, so reopening needs no vector options
- Built-in hash embeddings or HTTP client for Ollama/OpenAI
- Bulk vector node insertion for fast ingestion

**Full-Text Search**
- BM25-ranked inverted index; words split by Unicode, lowercased and accent-folded, with English stop words dropped
- One query language everywhere: `OR`, `-exclusions`, `"phrases"`, `prefix*` and `fuzzy~`
- Fuzzy search with configurable Levenshtein distance

**Cypher Query Language**
- MATCH, OPTIONAL MATCH, WHERE, RETURN, CREATE, DELETE, SET, REMOVE, MERGE, WITH, UNWIND, FOREACH
- ORDER BY, LIMIT, SKIP, DETACH DELETE, UNION, CASE, `CALL { }` subqueries
- Named paths, `shortestPath`, list and pattern comprehensions, `EXISTS`, `COUNT` and `COLLECT` subqueries, map projection, label expressions
- Vector distance operator: `<=>`
- Full-text search operator: `@@`
- Regular expressions: `=~`
- Parameters: `$name`
- Errors that give the stage, a message and the line and column

**Operations**
- Single-file storage with write-ahead log for crash recovery, tested by cutting
  the power at every sync point of a simulated disk
- Choose per database whether a commit waits for the disk (`sync_mode`), and on
  macOS whether syncs flush the drive's cache (`full_fsync`)
- Read-only opens of a crashed database see every committed transaction, without
  writing to it
- Continuous backup: ship changes to a directory and restore to a point in time
- Hot backup with `lattice backup`, taken without closing the database
- Serialize a database to bytes and open one from bytes, for keeping many small
  databases in object storage
- In-memory databases with `:memory:`, touching no files at all
- Durable named streams with explicit consumer offsets, manual trim, and graph changefeeds
- A compact changefeed with a retention policy (`all`, `last:N`, `age:7d` or `off`)
- Import and export JSON, JSONL, CSV and RDF (Turtle, N-Triples)
- Online freelist reuse plus `lattice compact` for safe physical tail reclamation
- Zero configuration — open a file and start working
- Embedded single-writer model for local applications
- Clean C API; Python, TypeScript, Go, and Java bindings wrap it

## Use Cases

- **Connected local data** — Notes, documents, catalogs, citation graphs, and entity graphs
- **Graph plus retrieval** — Relationship traversal, semantic search, and lexical search over the same dataset
- **Local knowledge tools** — Embedded apps that need graph structure without running a separate server
- **Agent memory and RAG pipelines** — One example class of workload built on the graph/vector/text substrate
- **Local development** — Lightweight alternative to Neo4j or Weaviate for prototyping on one machine

## When to Use Something Else

LatticeDB is fast, but speed is not the only thing that matters. Here are cases where a different tool is the better choice.

**You need multiple applications writing to the same database at the same time.**
LatticeDB is embedded with a single-writer model. One process opens the file and owns it. If you need many clients connecting over a network, use Neo4j, PostgreSQL, or another client-server database.

**Your data is fundamentally tabular.**
If your data fits naturally into rows and columns — sales records, user accounts, time series — a relational database like SQLite or PostgreSQL will be simpler and just as fast. Graph databases shine when relationships between records are the point, not an afterthought.

**You need to scale beyond a single machine.**
LatticeDB stores everything in one file on one machine. It can ship that file's changes elsewhere continuously, so a disk failure costs you seconds rather than everything, but that is backup rather than clustering. If you need sharding, multi-node replicas serving reads, or distributed queries across billions of nodes, look at Neo4j cluster, Dgraph, or a managed service like Neptune.

**You need the full Cypher language.**
LatticeDB implements most of the Cypher people write day to day, including `OPTIONAL MATCH`, `UNION`, `CASE`, `CALL { }` subqueries, list and pattern comprehensions, named paths and `shortestPath`, but not all of it. Quantified path patterns, `LOAD CSV`, user-defined procedures, temporal and spatial types, and schema statements are not implemented. If your queries depend on these, Neo4j is the complete implementation.

**You need mature tooling and ecosystem.**
Neo4j has visualization tools, admin dashboards, monitoring, drivers in every language, and years of community resources. PostgreSQL has decades of tooling. LatticeDB is new and lean — which is a strength for embedding, but a weakness if you need a rich operational ecosystem around your database.

## Building from Source

Written in Zig. Building needs only Zig (the engine links the C library).

```bash
git clone https://github.com/jeffhajewski/latticedb.git
cd latticedb
zig build                  # build everything
zig build test             # run tests
zig build -Doptimize=ReleaseFast   # optimized build
```

## Documentation

The full documentation lives at **[docs.latticedb.org](https://docs.latticedb.org)** — the Cypher
reference, the C, Python, TypeScript, Go, and Java API references, guides, and the storage engine
internals. [latticedb.org](https://latticedb.org) is the project site.

The links below are the in-repo copies and design notes.

- [Getting Started](docs/getting_started.md)
- [Durable Streams and Graph Changefeeds](docs/14_durable_streams.md)
- [Property Indexes](docs/property_index_design.md)
- [Examples Overview](examples/README.md)
- [CLI Quickstart](examples/cli/README.md)
- [Architecture Overview](docs/00_introduction.md)
- [0.17.0 Release Notes](docs/release_notes_0.17.0.md)
- [0.16.0 Release Notes](docs/release_notes_0.16.0.md)
- [0.15.0 Release Notes](docs/release_notes_0.15.0.md)
- [0.14.0 Release Notes](docs/release_notes_0.14.0.md)
- [0.13.0 Release Notes](docs/release_notes_0.13.0.md)
- [0.12.0 Release Notes](docs/release_notes_0.12.0.md)
- [0.11.1 Release Notes](docs/release_notes_0.11.1.md)
- [0.11.0 Release Notes](docs/release_notes_0.11.0.md)
- [0.10.0 Release Notes](docs/release_notes_0.10.0.md)
- [0.9.6 Release Notes](docs/release_notes_0.9.6.md)
- [0.9.5 Release Notes](docs/release_notes_0.9.5.md)
- [0.9.0 Release Notes](docs/release_notes_0.9.0.md)
- [0.8.7 Release Notes](docs/release_notes_0.8.7.md)
- [0.8.6 Release Notes](docs/release_notes_0.8.6.md)
- [0.8.5 Release Notes](docs/release_notes_0.8.5.md)
- [0.8.4 Release Notes](docs/release_notes_0.8.4.md)
- [0.8.2 Release Notes](docs/release_notes_0.8.2.md)
- [0.8.0 Release Notes](docs/release_notes_0.8.0.md)
- [Client API Migration Notes](docs/client_api_migration.md)
- [Python API Reference](bindings/python/README.md)
- [TypeScript API Reference](bindings/typescript/README.md)
- [Go API Reference](bindings/go/README.md)
- [Java API Reference](bindings/java/README.md)
- [C API Header](include/lattice.h)

## License

[MIT](LICENSE)
