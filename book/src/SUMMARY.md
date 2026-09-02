# Summary

[Introduction](./index.md)

# Getting Started

- [Installation](./getting-started/installation.md)
- [Quick Start](./getting-started/quickstart.md)
- [The lattice Command](./getting-started/cli.md)
- [Core Concepts](./getting-started/concepts.md)
- [When to Use LatticeDB](./getting-started/when-to-use.md)

# Comparisons

- [Choosing an Embedded Graph Database](./comparisons/overview.md)
- [LatticeDB vs SQLite](./comparisons/vs-sqlite.md)
- [LatticeDB vs Kùzu and LadybugDB](./comparisons/vs-kuzu.md)
- [LatticeDB vs Neo4j](./comparisons/vs-neo4j.md)
- [LatticeDB vs Vector Databases](./comparisons/vs-vector-databases.md)

# Configuration

- [Opening a Database](./configuration/opening.md)
- [Storage Modes](./configuration/storage-modes.md)
- [In-Memory Databases](./configuration/in-memory.md)
- [Durability and the Log](./configuration/durability.md)

# Guides

- [Building a RAG System](./guides/rag-system.md)
- [Knowledge Graph Modeling](./guides/knowledge-graph.md)
- [Working with Embeddings](./guides/embeddings.md)
- [Full-Text Search](./guides/full-text-search.md)
- [Migrating to Per-Property FTS](./guides/migrating-to-per-property-fts.md)
- [Durable Streams](./guides/durable-streams.md)
- [Data Export](./guides/data-export.md)
- [Transactions and Durability](./guides/transactions.md)
- [Backup and Replication](./guides/backup-and-replication.md)
- [Property Indexes](./guides/property-indexes.md)
- [Performance Tuning](./guides/performance-tuning.md)

# Cypher Query Language

- [Overview](./cypher/overview.md)
- [MATCH and Patterns](./cypher/match.md)
- [WHERE and Filtering](./cypher/where.md)
- [RETURN and Projections](./cypher/return.md)
- [CREATE, SET, DELETE](./cypher/mutations.md)
- [MERGE](./cypher/merge.md)
- [WITH and Chaining](./cypher/with.md)
- [UNWIND](./cypher/unwind.md)
- [Aggregations](./cypher/aggregations.md)
- [Variable-Length Paths](./cypher/variable-length-paths.md)
- [Vector Search (<=>)](./cypher/vector-search.md)
- [Full-Text Search (@@)](./cypher/full-text-search.md)
- [Parameters](./cypher/parameters.md)
- [Functions](./cypher/functions.md)

# API Reference

- [C API](./api/c.md)
- [Python](./api/python.md)
- [TypeScript / Node.js](./api/typescript.md)
- [Go](./api/go.md)
- [Java](./api/java.md)

# Architecture

- [Overview](./architecture/overview.md)
- [Virtual File System](./architecture/vfs.md)
- [Portable Databases](./architecture/portable-databases.md)
- [Page Manager](./architecture/page-manager.md)
- [Buffer Pool](./architecture/buffer-pool.md)
- [B+Tree](./architecture/btree.md)
- [Write-Ahead Log](./architecture/wal.md)
- [Transaction Manager](./architecture/transaction-manager.md)
- [Checkpointing](./architecture/checkpointing.md)
- [Recovery](./architecture/recovery.md)
- [Graph Storage](./architecture/graph-storage.md)
- [Vector Search (HNSW)](./architecture/vector-search.md)
- [Full-Text Search](./architecture/full-text-search.md)
- [Query Execution](./architecture/query-execution.md)

# Performance

- [Benchmarks](./performance/benchmarks.md)
- [Competitive Analysis](./performance/competitive-analysis.md)

# Development

- [Building from Source](./development/building.md)
- [Running Tests](./development/testing.md)
- [Releasing](./development/releasing.md)
- [Contributing](./development/contributing.md)
