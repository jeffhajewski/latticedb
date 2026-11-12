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

`n.embedding` is the node's vector, the one stored with `set_vector`. A node has
one vector, and `embedding` is the only property name that reads it; `set_vector`
accepts `"embedding"` (or an empty key) and rejects any other key as unsupported.
Once committed, the vector reads back normalised to unit length, because the index
stores only the normalised copy: a stored `[3, 4, 0, 0]` returns as
`[0.6, 0.8, 0, 0]`.

A query vector must have the database's dimensions and finite components. A
vector containing NaN or infinity fails the query, as it fails `set_vector`.

When the whole `WHERE` clause is a `<=>` predicate (`n.embedding <=> q < t`,
`<= t`, or `n.embedding <=> q` on its own), with `q` a parameter or a list literal
and `t` a number or a parameter, the planner answers it with the HNSW index. The
rows the `MATCH` pattern produces are the candidates, and the search widens
(doubling how many neighbours it asks for) until the answer is complete:

- **With a threshold**: every candidate within it. `<` excludes a distance of
  exactly `t` and `<=` includes it. A null threshold matches nothing.
- **Under a `RETURN` with a `LIMIT`**, and no `ORDER BY`, `DISTINCT` or
  aggregation in between: until it has `SKIP` + `LIMIT` candidate rows.
- **With neither**: the 100 nearest candidate nodes.

So the label narrows the search (`MATCH (c:Chunk)` returns Chunk nodes even when
other nodes are nearer), and a threshold query returns every match, however many.
If the search reaches the end of the index, candidates it did not return are
measured directly. What stays approximate is HNSW itself: when the search stops
before the end of the index, a candidate within the threshold can occasionally be
missed.

Any other form is evaluated row by row, exactly, by computing each candidate's
distance: a threshold that is an expression (`< 0.25 * 2`), or a `<=>` predicate
combined with others through `AND`. `<=>` has the same precedence as the
comparison operators and groups left to right, so write the distance first, or in
parentheses: `0.5 > n.embedding <=> $q` compares `0.5 > n.embedding` first.

For the nearest `k` across the whole database, whatever the labels, use the
`vector_search` API (`db.vector_search(vector, k=...)`). A `k` larger
than `ef_search` widens the search to `k`, so asking for 500 neighbours returns 500
when the database holds that many.

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
