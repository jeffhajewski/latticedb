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
from latticedb import Database
from latticedb.embedding import hash_embed

db = Database(
    "rag.db",
    create=True,
    enable_vectors=True,
    vector_dimensions=128,  # Match your embedding model's output
)
db.open()

# Declare the full-text index once, before loading text into Chunk.text
db.create_node_fts_index("Chunk", "text")
```

For production, use a real embedding model via the HTTP client:

```python
from latticedb.embedding import EmbeddingClient

# The endpoint is the full URL the client POSTs to
client = EmbeddingClient(
    "http://localhost:11434/api/embeddings",
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

            # Link chunk to document
            txn.create_edge(chunk.id, doc.id, "PART_OF")

            # Link sequential chunks
            if prev_chunk is not None:
                txn.create_edge(prev_chunk.id, chunk.id, "NEXT")
            prev_chunk = chunk

        txn.commit()
    return doc


doc = ingest_document(
    db,
    "Attention Is All You Need",
    "Vaswani",
    [
        "The transformer architecture uses self-attention.",
        "Self-attention relates every position in a sequence to every other.",
        "Multi-head attention runs several attention functions in parallel.",
    ],
)
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
query_vec = hash_embed("How does self-attention work?", dimensions=128)

results = db.query("""
    MATCH (prev:Chunk)-[:NEXT]->(chunk:Chunk)-[:PART_OF]->(doc:Document)
    WHERE chunk.embedding <=> $query < 0.5
    RETURN prev.text, chunk.text, doc.title
    ORDER BY chunk.embedding <=> $query
    LIMIT 5
""", parameters={"query": query_vec})
```

The path can also be written in two `MATCH` clauses that share `chunk`; a node
variable bound in one `MATCH` stands for the same node in the next.

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

For large datasets, `batch_insert_vectors` creates a node and stores its vector
for every row in one call:

```python
import numpy as np

all_chunks = ["first chunk of text", "second chunk of text"]  # your chunk texts

with db.write() as txn:
    # Insert every chunk at once
    vectors = np.array([hash_embed(text, 128) for text in all_chunks], dtype=np.float32)
    node_ids = txn.batch_insert_vectors("Chunk", vectors)

    # Set properties and create edges afterward
    for node_id, text in zip(node_ids, all_chunks):
        txn.set_property(node_id, "text", text)

    txn.commit()
```

## Performance Tips

- Use `batch_insert_vectors()` for bulk loading — it saves one binding call per node; each vector is still inserted into the HNSW index individually
- Set `ef_search` based on your recall requirements: the default, 64, measured about 98% recall@10 at 100K vectors and 93–96% at 1M; 128 brought 1M to about 98% (see [Benchmarks](../performance/benchmarks.md#ef_search))
- End a retrieval query as above, with `ORDER BY` the same `<=>` distance as the `WHERE` and a `LIMIT`: the search then stops once it has enough rows, rather than finding every chunk under the threshold and sorting them. To return the distance, write it in the `RETURN` the same way (`chunk.embedding <=> $query AS dist`) and the measured value is reused
- Use `cache_size_mb` to control memory usage; once the file outgrows the cache (about 100K 128-d vectors at the default 100 MB), vector inserts and searches slow down several times over, so size it to the file for large indexes
- Declare a full-text index only over properties you actually search; each one costs write time
- Use parameters (`$name`) so the query text stays the same and its parsed form is reused from the query cache
