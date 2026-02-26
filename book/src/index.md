# LatticeDB

**The embedded knowledge graph for AI.**

LatticeDB is a single-file database that combines a property graph, vector search, and full-text search. It's built for RAG, agents, and any application where relationships between data matter as much as the data itself.

- **One file.** Your entire database is a single portable file. No server, no configuration.
- **Three search modes.** Graph traversal, HNSW vector similarity, and BM25 full-text — in one query.
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
- Zero configuration — open a file and start working
- Clean C API; Python and TypeScript bindings wrap it

## Use Cases

- **RAG Systems** — Vector search finds relevant chunks, graph traversal gathers context
- **Knowledge Graphs** — Linked notes and documents with semantic search
- **AI Agents** — Persistent memory with relationship awareness
- **Local Development** — Lightweight alternative to Neo4j or Weaviate for prototyping

## Getting Started

Head to the [Installation](./getting-started/installation.md) page to install LatticeDB, then follow the [Quick Start](./getting-started/quickstart.md) guide to build your first knowledge graph.
