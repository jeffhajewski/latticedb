# Vector Search (`<=>`)

The `<=>` operator computes the distance between a stored vector embedding and a query vector. It integrates HNSW vector search into Cypher queries.

## Basic Usage

```cypher
MATCH (chunk:Chunk)
WHERE chunk.embedding <=> $query < 0.5
RETURN chunk.text
ORDER BY chunk.embedding <=> $query
LIMIT 10
```

This finds chunks whose embedding is within distance 0.5 of the query vector, sorted by proximity.

## How It Works

When the query planner encounters `<=>` in a `WHERE` clause, it converts the pattern into a specialized HNSW search operator rather than scanning all nodes. The distance threshold (e.g., `< 0.5`) filters results after the search.

## Distance Metric

LatticeDB uses **cosine distance** (1 - cosine_similarity). Values range from 0 (identical) to 2 (opposite).

| Distance | Meaning |
|----------|---------|
| 0.0 | Identical vectors |
| 0.1-0.3 | Very similar |
| 0.3-0.5 | Moderately similar |
| 0.5-1.0 | Dissimilar |
| 1.0-2.0 | Very dissimilar to opposite |

## Ordering by Distance

Use `ORDER BY` to sort results by similarity:

```cypher
MATCH (n:Document)
WHERE n.embedding <=> $query < 1.0
RETURN n.title
ORDER BY n.embedding <=> $query
LIMIT 10
```

## Combining with Graph Traversal

The real power is combining vector search with graph patterns:

```cypher
MATCH (chunk:Chunk)-[:PART_OF]->(doc:Document)-[:AUTHORED_BY]->(author:Person)
WHERE chunk.embedding <=> $query < 0.5
RETURN doc.title, chunk.text, author.name
ORDER BY chunk.embedding <=> $query
LIMIT 10
```

This finds similar chunks, then traverses to their documents and authors — all in one query.

## Combining with Full-Text Search

```cypher
MATCH (chunk:Chunk)-[:PART_OF]->(doc:Document)
WHERE chunk.embedding <=> $query < 0.5
  AND doc.content @@ "neural networks"
RETURN doc.title, chunk.text
ORDER BY chunk.embedding <=> $query
```

## Parameter Binding

Pass the query vector as a parameter:

**Python:**
```python
results = db.query(
    "MATCH (n) WHERE n.embedding <=> $q < 0.5 RETURN n",
    parameters={"q": query_vector}
)
```

**TypeScript:**
```typescript
const results = await db.query(
  "MATCH (n) WHERE n.embedding <=> $q < 0.5 RETURN n",
  { q: new Float32Array([0.1, 0.2, ...]) }
);
```
