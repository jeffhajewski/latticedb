# LatticeDB

**Embedded property graph with native vector, full-text, and durable event primitives.**

LatticeDB is a single-file local database for connected, semantic, and textual data. It lets applications traverse relationships, run HNSW vector similarity, run BM25 full-text search, and consume durable graph/application events from the same file.

- **One file.** Your entire database is a single portable file. No server, no configuration.
- **One query layer.** Graph traversal, HNSW vector similarity, and BM25 full-text — in one query.
- **One event log.** Durable named streams and the built-in graph changefeed share the same transaction/WAL path as graph writes.
- **Sub-millisecond.** 0.13 us node lookups. 0.83 ms vector search at 1M vectors with 100% recall.

```cypher
-- Find chunks similar to a query, traverse to their document, then to the author
MATCH (chunk:Chunk)-[:PART_OF]->(doc:Document)-[:AUTHORED_BY]->(author:Person)
WHERE chunk.embedding <=> $query_vector < 0.3
  AND doc.content @@ "neural networks"
RETURN doc.title, chunk.text, author.name
ORDER BY chunk.embedding <=> $query_vector
LIMIT 10
```

## Features

**Graph**
- Nodes and edges with labels and arbitrary properties
- Multi-hop traversal, variable-length paths (`*1..3`)
- ACID transactions with snapshot isolation
- MERGE, WITH, UNWIND, aggregations (`count`, `sum`, `avg`, `min`, `max`, `collect`)

**Vector Search**
- HNSW approximate nearest neighbor with configurable M, ef
- Built-in hash embeddings or HTTP client for Ollama/OpenAI
- Batch insert for bulk loading

**Full-Text Search**
- BM25-ranked inverted index with tokenization and stemming
- Fuzzy search with configurable Levenshtein distance

**Cypher Query Language**
- MATCH, WHERE, RETURN, CREATE, DELETE, SET, REMOVE
- ORDER BY, LIMIT, SKIP, DETACH DELETE
- Vector distance operator: `<=>`
- Full-text search operator: `@@`
- Parameters: `$name`

**Operations**
- Single-file storage with write-ahead log for crash recovery
- Durable streams and semantic graph changefeeds
- Zero configuration — open a file and start working
- Clean C API; Python, TypeScript, and Go bindings wrap it

## Use Cases

- **RAG Systems** — Vector search finds relevant chunks, graph traversal gathers context
- **Knowledge Graphs** — Linked notes and documents with semantic search
- **AI Agents** — Persistent memory with relationship awareness
- **Local Development** — Lightweight alternative to Neo4j or Weaviate for prototyping

## Getting Started

Head to the [Installation](./getting-started/installation.md) page to install LatticeDB, then follow the [Quick Start](./getting-started/quickstart.md) guide to build your first knowledge graph.
