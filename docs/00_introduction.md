# Lattice: How It Works

This documentation explains how Lattice's storage engine works, from the ground
up. Each chapter builds on the previous, showing how simple primitives combine
to create a durable, transactional database.

## The Stack

```
┌─────────────────────────────────────────────────────────────┐
│                      Application                            │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   Transaction Manager                       │
│              (ACID guarantees, isolation)                   │
└─────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
┌───────────────────┐ ┌─────────────┐ ┌─────────────────────┐
│      B+Tree       │ │     WAL     │ │    Checkpointer     │
│  (ordered data)   │ │ (durability)│ │  (recovery bounds)  │
└───────────────────┘ └─────────────┘ └─────────────────────┘
              │               │               │
              └───────────────┼───────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                      Buffer Pool                            │
│                (page caching in RAM)                        │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                     Page Manager                            │
│              (page allocation, checksums)                   │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                 Virtual File System                         │
│                    (file I/O)                               │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Operating System                         │
└─────────────────────────────────────────────────────────────┘
```

## Chapters

### Storage Engine

1. **Virtual File System** - Abstracting file I/O for portability and testing
2. **Page Manager** - Fixed-size pages, allocation, and checksums
3. **Buffer Pool** - Caching pages in memory with eviction
4. **B+Tree** - Ordered key-value storage with efficient lookups

### Durability & Transactions

5. **Write-Ahead Log** - Durability through logging before data changes
6. **Transaction Manager** - ACID transactions with begin/commit/abort
7. **Checkpointing** - Bounding recovery time by flushing dirty pages
8. **Recovery** - ARIES-style crash recovery with redo/undo

### Data Model

9. **Graph Storage** - Nodes, edges, labels, and properties

### Search Indexes

10. **Vector Search** - HNSW approximate nearest neighbor search
11. **Full-Text Search** - BM25-scored inverted index with tokenization

### Query System

12. **Query Execution** - Volcano iterator model, operators, and planning

## Design Principles

### Direct Page Manipulation

We don't deserialize pages into objects. Instead, we read/write bytes directly
in page buffers. This is "zero-copy" - no intermediate representations, no
serialization overhead.

```
Traditional approach:           Our approach:

Page bytes ──► Object         Page bytes ──► Direct access
              in memory                       via offsets
                  │                               │
                  ▼                               ▼
              Modify                          Modify bytes
                  │                               │
                  ▼                               ▼
Object ──► Serialize ──► Page   Already in page buffer!
```

### Everything is Pages

The entire database is built on fixed-size pages (4KB by default):

- B+Tree nodes are pages
- WAL frames are pages
- The file header is a page
- Free list entries are pages

This uniformity simplifies the system - one caching layer, one I/O path, one
checksum format.

### Durability Through WAL

Changes are logged before being applied. This means:

- Commit = log is on disk (fast, sequential write)
- Actual data pages can be written lazily
- Crash recovery replays the log

### Simple Concurrency Model

Each page has a reader-writer latch. Multiple readers OR one writer. No complex
lock hierarchies - simplicity over maximum concurrency.
