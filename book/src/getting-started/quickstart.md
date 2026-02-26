# Quick Start

This guide walks through creating a small knowledge graph with documents and authors, storing embeddings, indexing text, and querying across all three search modes.

## Python

```python
from latticedb import Database, hash_embed

with Database("knowledge.db", create=True, enable_vector=True, vector_dimensions=128) as db:

    # --- Build the graph ---
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

            # Store embedding and index text
            txn.set_vector(chunk.id, "embedding", hash_embed(text, dimensions=128))
            txn.fts_index(chunk.id, text)

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
    for r in db.fts_search("self-attention transformer"):
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

## TypeScript

```typescript
import { Database, hashEmbed } from "@hajewski/latticedb";

const db = new Database("knowledge.db", {
  create: true,
  enableVector: true,
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
  await txn.ftsIndex(chunk.id, "The transformer architecture uses self-attention...");

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

## Next Steps

- [Core Concepts](./concepts.md) — understand the data model
- [Cypher Overview](../cypher/overview.md) — learn the query language
- [Building a RAG System](../guides/rag-system.md) — end-to-end tutorial
