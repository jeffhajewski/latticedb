# Architecture Overview

This section explains how LatticeDB's storage engine works, from the ground up. Each chapter builds on the previous, showing how simple primitives combine to create a durable, transactional graph database with vector search, full-text search, and local event streams.

## The Stack

<img class="diagram" src="../assets/diagrams/engine-stack.svg"
     alt="The engine stack from top to bottom: the application calls the transaction manager, which drives the B+Tree, write-ahead log and checkpointer; those share the buffer pool, which sits on the page manager, the virtual file system, and finally the operating system">

## Chapters

### Storage Engine
1. [Virtual File System](./vfs.md) - Abstracting file I/O for portability and testing
1. [Portable Databases](./portable-databases.md) - Serializing to bytes, and running with no files at all
2. [Page Manager](./page-manager.md) - Fixed-size pages, allocation, and checksums
3. [Buffer Pool](./buffer-pool.md) - Caching pages in memory with eviction
4. [B+Tree](./btree.md) - Ordered key-value storage with efficient lookups

### Durability & Transactions
5. [Write-Ahead Log](./wal.md) - Durability through logging before data changes
6. [Transaction Manager](./transaction-manager.md) - ACID transactions with begin/commit/abort
7. [Checkpointing](./checkpointing.md) - Bounding recovery time by flushing dirty pages
8. [Recovery](./recovery.md) - ARIES-style crash recovery with redo/undo

### Data Model
9. [Graph Storage](./graph-storage.md) - Nodes, edges, labels, and properties
10. [Durable Streams](../guides/durable-streams.md) - Transactional event records and semantic graph changes

### Search Indexes
11. [Vector Search](./vector-search.md) - HNSW approximate nearest neighbor search
12. [Full-Text Search](./full-text-search.md) - BM25-scored inverted index, per-property declarations, and search as an access path

### Query System
13. [Query Execution](./query-execution.md) - Volcano iterator model, operators, and planning

## Design Principles

### Direct Page Manipulation

We don't deserialize pages into objects. Instead, we read/write bytes directly in page buffers. This is "zero-copy" - no intermediate representations, no serialization overhead.

<img class="diagram" src="../assets/diagrams/zero-copy-pages.svg"
     alt="Traditional access deserialises page bytes into an in-memory object, modifies it, and serialises back. LatticeDB modifies the page bytes in place through calculated offsets, with no intermediate object">

### Everything is Pages

The entire database is built on fixed-size pages (4KB by default):

- B+Tree nodes are pages
- Large B+Tree values spill into linked overflow pages
- WAL frames are pages
- The file header is a page
- Free list entries are pages

This uniformity simplifies the system - one caching layer, one I/O path, one checksum format.

### Durability Through WAL

Changes are logged before being applied. This means:

- Commit = log is on disk (fast, sequential write)
- Actual data pages can be written lazily
- Crash recovery replays the log

### Simple Concurrency Model

Each page has a reader-writer latch. Multiple readers OR one writer. No complex lock hierarchies - simplicity over maximum concurrency.
