# Full-Text Search (`@@`)

The `@@` operator performs BM25-scored full-text search on indexed text content.

## Basic Usage

```cypher
MATCH (d:Document)
WHERE d.content @@ "neural networks"
RETURN d.title
```

This searches all nodes that have had their text indexed with `fts_index()` and returns those matching the search terms.

## How It Works

When the query planner encounters `@@`, it uses the inverted index to find matching nodes directly — no full scan needed. Results are scored using BM25, which considers term frequency, inverse document frequency, and document length.

## String Queries

The search query is a space-separated list of terms. All terms must match (implicit AND):

```cypher
-- Both "neural" and "networks" must appear
MATCH (n) WHERE n.text @@ "neural networks" RETURN n
```

## Using Parameters

```cypher
MATCH (d:Document)
WHERE d.content @@ $search_text
RETURN d.title
```

**Python:**
```python
results = db.query(
    'MATCH (d:Document) WHERE d.content @@ $q RETURN d.title',
    parameters={"q": "machine learning"}
)
```

**TypeScript:**
```typescript
const results = await db.query(
  'MATCH (d:Document) WHERE d.content @@ $q RETURN d.title',
  { q: "machine learning" }
);
```

## Combining with Graph Traversal

```cypher
MATCH (chunk:Chunk)-[:PART_OF]->(doc:Document)-[:AUTHORED_BY]->(author:Person)
WHERE chunk.text @@ "transformer attention"
RETURN doc.title, author.name
```

## Combining with Vector Search

```cypher
MATCH (chunk:Chunk)
WHERE chunk.embedding <=> $query < 0.5
  AND chunk.text @@ "transformer"
RETURN chunk.text
ORDER BY chunk.embedding <=> $query
```

## Indexing Text

Text must be indexed before it can be searched. Use `fts_index()` within a write transaction:

**Python:**
```python
with db.write() as txn:
    txn.fts_index(node.id, "The transformer architecture uses self-attention mechanisms")
    txn.commit()
```

**TypeScript:**
```typescript
await db.write(async (txn) => {
  await txn.ftsIndex(node.id, "The transformer architecture uses self-attention mechanisms");
});
```

## Programmatic Search

In addition to the `@@` operator in Cypher, you can use the dedicated search APIs:

**Python:**
```python
# Exact search
results = db.fts_search("machine learning", limit=10)

# Fuzzy search (typo-tolerant)
results = db.fts_search_fuzzy("machin lerning", limit=10)
```

**TypeScript:**
```typescript
// Exact search
const results = await db.ftsSearch("machine learning", { limit: 10 });

// Fuzzy search (typo-tolerant)
const results = await db.ftsSearchFuzzy("machin lerning", { limit: 10 });
```
