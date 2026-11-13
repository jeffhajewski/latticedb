# LatticeDB vs Vector Databases

Most RAG systems start with a vector database — Chroma, LanceDB, pgvector, or sqlite-vec — and that is usually the right first move. This page is about the point where a pure vector store stops being enough, which arrives sooner than most teams expect.

## Where vector-only retrieval breaks down

A vector database answers one question well: which chunks are semantically nearest this query. The problems show up around that answer.

**Chunks have context that similarity does not capture.** The nearest chunk came from a document, which has an author, a date, a version, and a place in a hierarchy. Retrieval quality usually improves when you can pull that context in — and in a vector-only store, that means a second database and a join in application code.

**Similarity misses exact terms.** Someone searching for an error code, a function name, or a product SKU wants lexical matching, not semantic neighbourhood. This is what BM25 is for, and it is why hybrid retrieval consistently outperforms pure vector search.

**Relationships are the answer sometimes.** "What else did this author write", "what supersedes this document", "what is two hops from this concept" are traversals. No amount of embedding quality answers them.

The usual response is to run a vector store plus a relational database plus a search index, and keep three systems consistent. LatticeDB's argument is that these are one question and should be one query.

```cypher
MATCH (chunk:Chunk)-[:PART_OF]->(doc:Document)-[:AUTHORED_BY]->(author:Person)
WHERE (chunk.embedding <=> $query_vector < 0.3
       OR chunk.text @@ 'connection pool exhausted')
  AND doc.version = $current_version
RETURN doc.title, author.name, chunk.text
ORDER BY chunk.embedding <=> $query_vector
LIMIT 10
```

Vector similarity, lexical matching, graph traversal, and a metadata filter, resolved together against one file. The full worked example is in [Building a RAG System](../guides/rag-system.md).

## Feature comparison

| | LatticeDB | Chroma | LanceDB | pgvector | sqlite-vec |
|---|---|---|---|---|---|
| Deployment | Embedded | Embedded / server | Embedded | Postgres extension | SQLite extension |
| Vector index | HNSW | HNSW | IVF / HNSW | HNSW / IVFFlat | Brute force |
| Full-text search | Native BM25 | BM25 and sparse vectors | Native | `tsvector` | FTS5, separate |
| Graph traversal | Native Cypher | No | No | Recursive CTE | Recursive CTE |
| Metadata filtering | Cypher `WHERE` | Dict filters | SQL-like | Full SQL | Full SQL |
| Durable streams | Native | No | No | Logical decoding | No |
| Transactions | ACID | Limited | Limited | ACID | ACID |
| Needs a server | No | Optional | No | Yes | No |

## Published latency figures

Every number below comes from the vendor's own documentation or a third-party benchmark, on hardware, data and settings we did not control, and none was measured here. They are order-of-magnitude orientation, not a head-to-head result — see [how to read the numbers](./overview.md#how-to-read-the-numbers).

| System | Published 10-NN latency at 1M vectors | Type |
|--------|----------------------------|------|
| FAISS HNSW (single thread) | 0.5–3 ms | Library |
| Weaviate | 1.4 ms mean, 3.1 ms p99 | Server |
| Qdrant | ~1–2 ms | Server |
| Milvus + SQ8 | 2.2 ms p99 | Server |
| LanceDB | 3–5 ms | Embedded |
| Chroma | 4–5 ms mean | Embedded |
| pgvector HNSW | ~5 ms at 99% recall | Extension |
| Pinecone P2 | ~15 ms including network | Cloud |
| sqlite-vec | 17 ms (brute force) | Extension |

Sources for each row are in [Competitive Analysis](../performance/competitive-analysis.md#cited-from-others). LatticeDB's own measurements, on synthetic clustered vectors, are in [Benchmarks](../performance/benchmarks.md#vector-search): at 1M vectors, 1.34 ms at p50 with 95.6% recall@10 and the default 100 MB cache, 0.21 ms with a 2 GB cache.

## When a vector database is still the better choice

**Your retrieval genuinely has no graph in it.** A flat corpus of independent chunks with no meaningful relationships is exactly what a vector store is for. Adding a graph model buys you nothing.

**You need to scale past one machine.** LatticeDB is one file on one host. Qdrant, Weaviate, Milvus, and Pinecone shard and replicate.

**You are already on Postgres.** If your metadata lives in Postgres, pgvector keeps everything in one database and one transaction. That consolidation is worth more than a few milliseconds.

**You need the ecosystem.** LangChain and LlamaIndex integrations, managed hosting, hybrid-search tuning knobs, and reranking pipelines are mature in the dedicated vector stores and thin here.

**Billions of vectors.** LatticeDB is benchmarked to 1M vectors; its index estimates about 1 GB of pages for them, and the benchmark ran with a 4 GB buffer pool. Beyond that scale you want a purpose-built distributed system.

## When LatticeDB fits

When your retrieval corpus has structure — documents, authors, versions, topics, citations, conversations — and you want that structure to participate in retrieval rather than sit in a separate database. When exact-term matching matters alongside semantic similarity. When you would rather ship one file than operate three services.

Start with [Building a RAG System](../guides/rag-system.md) for a worked example, or [Working with Embeddings](../guides/embeddings.md) for the vector-indexing details.
