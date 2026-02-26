# Building a RAG System

This guide walks through building a Retrieval-Augmented Generation (RAG) system with LatticeDB. We'll chunk documents, generate embeddings, store them with graph relationships, and query using vector search combined with graph traversal.

## Architecture

A typical RAG system with LatticeDB:

1. **Ingest** — Split documents into chunks, generate embeddings, store in the graph
2. **Link** — Create edges between chunks, documents, authors, topics
3. **Retrieve** — Vector search finds relevant chunks, graph traversal gathers context
4. **Generate** — Pass retrieved context to an LLM

## Step 1: Set Up the Database

```python
from latticedb import Database, hash_embed

db = Database(
    "rag.db",
    create=True,
    enable_vector=True,
    vector_dimensions=128,  # Match your embedding model's output
)
db.open()
```

For production, use a real embedding model via the HTTP client:

```python
from latticedb import EmbeddingClient

client = EmbeddingClient(
    "http://localhost:11434",
    model="nomic-embed-text",
)
```

## Step 2: Ingest Documents

Split documents into chunks and store them with graph relationships:

```python
def ingest_document(db, title, author_name, chunks):
    with db.write() as txn:
        # Create or find the author
        doc = txn.create_node(
            labels=["Document"],
            properties={"title": title},
        )

        author = txn.create_node(
            labels=["Person"],
            properties={"name": author_name},
        )
        txn.create_edge(doc.id, author.id, "AUTHORED_BY")

        # Create chunks with embeddings
        prev_chunk = None
        for i, text in enumerate(chunks):
            chunk = txn.create_node(
                labels=["Chunk"],
                properties={"text": text, "position": i},
            )

            # Store embedding
            embedding = hash_embed(text, dimensions=128)
            txn.set_vector(chunk.id, "embedding", embedding)

            # Index for full-text search
            txn.fts_index(chunk.id, text)

            # Link chunk to document
            txn.create_edge(chunk.id, doc.id, "PART_OF")

            # Link sequential chunks
            if prev_chunk is not None:
                txn.create_edge(prev_chunk.id, chunk.id, "NEXT")
            prev_chunk = chunk

        txn.commit()
```

## Step 3: Add Topic Links

Enrich the graph with topic relationships:

```python
with db.write() as txn:
    ml_topic = txn.create_node(
        labels=["Topic"],
        properties={"name": "Machine Learning"},
    )

    # Link documents to topics
    txn.create_edge(doc.id, ml_topic.id, "ABOUT")
    txn.commit()
```

## Step 4: Query — Vector Search + Graph Context

The key advantage of LatticeDB: retrieve by similarity, then traverse the graph for additional context.

```python
def retrieve_context(db, query_text, k=5):
    """Retrieve relevant chunks with their surrounding context."""
    query_vec = hash_embed(query_text, dimensions=128)

    # Find similar chunks and traverse to their documents and authors
    results = db.query("""
        MATCH (chunk:Chunk)-[:PART_OF]->(doc:Document)-[:AUTHORED_BY]->(author:Person)
        WHERE chunk.embedding <=> $query < 0.5
        RETURN chunk.text, doc.title, author.name
        ORDER BY chunk.embedding <=> $query
        LIMIT $k
    """, parameters={"query": query_vec, "k": k})

    return results
```

### Retrieve with Neighboring Chunks

Get surrounding chunks for more context:

```python
results = db.query("""
    MATCH (chunk:Chunk)-[:PART_OF]->(doc:Document)
    WHERE chunk.embedding <=> $query < 0.5
    WITH chunk, doc
    ORDER BY chunk.embedding <=> $query
    LIMIT 5
    MATCH (prev:Chunk)-[:NEXT]->(chunk)
    RETURN prev.text, chunk.text, doc.title
""", parameters={"query": query_vec})
```

### Combine Vector and Full-Text Search

```python
results = db.query("""
    MATCH (chunk:Chunk)-[:PART_OF]->(doc:Document)
    WHERE chunk.embedding <=> $query < 0.5
      AND chunk.text @@ $keywords
    RETURN chunk.text, doc.title
    ORDER BY chunk.embedding <=> $query
    LIMIT 10
""", parameters={
    "query": query_vec,
    "keywords": "transformer attention",
})
```

## Step 5: Pass to LLM

```python
context = retrieve_context(db, "How does self-attention work?")

# Build prompt with retrieved context
chunks = [f"[{r['doc.title']}] {r['chunk.text']}" for r in context]
prompt = f"""Answer the question based on the following context:

{chr(10).join(chunks)}

Question: How does self-attention work?"""

# Pass to your LLM of choice
# response = llm.generate(prompt)
```

## Batch Loading

For large datasets, use batch insert:

```python
import numpy as np

with db.write() as txn:
    # Insert 10,000 chunks at once
    vectors = np.array([hash_embed(text, 128) for text in all_chunks], dtype=np.float32)
    node_ids = txn.batch_insert("Chunk", vectors)

    # Set properties and create edges afterward
    for node_id, text in zip(node_ids, all_chunks):
        txn.set_property(node_id, "text", text)
        txn.fts_index(node_id, text)

    txn.commit()
```

## Performance Tips

- Use `batch_insert()` for bulk loading — significantly faster than individual creates
- Set `ef_search` based on your recall requirements (64 gives 100% recall at 1M vectors)
- Use `cache_size_mb` to control memory usage
- Index only the text fields you need to search with `fts_index()`
- Use parameters (`$name`) to enable query plan caching
