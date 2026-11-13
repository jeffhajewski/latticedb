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

This finds chunks whose embedding is within distance 0.5 of the query vector, sorted by proximity. The distance is measured with the metric the database was created with; see [Distance Metric](#distance-metric).

## How It Works

`n.embedding` is the node's vector, the one stored with `set_vector`. A node has
one vector, and `embedding` is the only property name that reads it; `set_vector`
accepts `"embedding"` (or an empty key) and rejects any other key as unsupported.
Under the cosine metric, once committed, the vector reads back normalised to unit
length, because the index stores only the normalised copy: a stored `[3, 4]`
returns as `[0.6, 0.8]`. Under the Euclidean and dot-product metrics vectors are
stored, and read back, as given.

A query vector must have the database's dimensions and finite components. A
vector containing NaN or infinity fails the query, as it fails `set_vector`.

When the whole `WHERE` clause is a `<=>` predicate (`n.embedding <=> q < t`,
`<= t`, or `n.embedding <=> q` on its own), with `q` a parameter or a list literal
and `t` a number or a parameter, the planner answers it with the HNSW index. The
rows the `MATCH` pattern produces are the candidates, and the search looks only
among their nodes:

- **With a threshold**: every candidate within it. `<` excludes a distance of
  exactly `t` and `<=` includes it. A null threshold matches nothing.
- **Under a `RETURN` with a `LIMIT`**, and no `DISTINCT` or aggregation in
  between: the `SKIP` + `LIMIT` nearest candidate nodes. An `ORDER BY` in between
  keeps this bound only when it sorts nearest first by the same distance, written
  as in the `WHERE` (`ORDER BY n.embedding <=> $q`, ascending) or as the alias of a
  `RETURN` item that is that distance, since the search returns its results in that
  order already. Any other `ORDER BY` makes the search find every candidate before
  sorting.
- **With neither**: the 100 nearest candidate nodes.

So the label narrows the search (`MATCH (c:Chunk)` returns Chunk nodes even when
other nodes are nearer), and a threshold query returns every match, however many.
When the candidates are few next to the index (fewer than about
`sqrt(ef_search × 32 × vectors)`: some 14,000 of 100,000), each one is measured,
which is exact. A larger set is searched through the HNSW graph with the set as
a filter; that stays approximate, so a candidate within the threshold can
occasionally be missed. Candidates the graph search could not reach are measured
directly when it runs out. When the pattern is a single labelled node,
`MATCH (n:Label) WHERE n.embedding <=> ...`, the label's index is that filter:
the search reads only as much of the label as it takes to tell that it is
large, so a label holding half of 100,000 nodes answers `LIMIT 10` in about
0.5 ms, and one holding 100 of them in 10 to 20 µs.

Any other form is evaluated row by row, exactly, by computing each candidate's
distance: a threshold that is an expression (`< 0.25 * 2`), or a `<=>` predicate
combined with others through `AND`. `<=>` has the same precedence as the
comparison operators and groups left to right, so write the distance first, or in
parentheses: `0.5 > n.embedding <=> $q` compares `0.5 > n.embedding` first.

The `WHERE` of an `OPTIONAL MATCH`, of the short form of `EXISTS { ... }` or
`COUNT { ... }` (a pattern and a `WHERE`), of a pattern comprehension and of a
`WITH` is always checked row by row, so a `<=>` there needs a bound
(`WHERE n.embedding <=> $q < 0.5`). A bare `n.embedding <=> $q` there is refused
("A WHERE of only `<=>` ranks the rows of a MATCH by distance, which OPTIONAL
MATCH cannot do; give it a bound instead"). Each part of a `UNION` is planned as
a query of its own, so a `<=>` with a `LIMIT` there is the search it would be
alone; so is one under a named path (`MATCH p = (n:Chunk) WHERE n.embedding <=>
$q`), and the `MATCH` of a `CALL { }` subquery or of a whole query inside
`EXISTS`, `COUNT` or `COLLECT`, which searches once for each incoming row (see
[Subqueries](./with.md#subqueries-call--)).

`<=>` is also an ordinary expression. Outside such a search, in a `RETURN`, a
`WITH`, an `ORDER BY` or a combined `WHERE`, it computes the distance between its
two vectors with the database's metric (cosine in a database without a vector
index), and either side can be a property, a parameter or a list literal:

```cypher
RETURN [1.0, 0.0] <=> [0.0, 2.0] AS d   // 1.0 under cosine
```

For the nearest `k` across the whole database, whatever the labels, use the
`vector_search` API (`db.vector_search(vector, k=...)`). A `k` larger
than `ef_search` widens the search to `k`, so asking for 500 neighbours returns 500
when the database holds that many.

## Returning the Distance

Writing the search's own distance in the `RETURN`, the same way as in the `WHERE`
(same variable, property, and parameter or literal), returns the distance the search
already measured instead of computing it again:

```cypher
MATCH (d:Doc)
WHERE d.embedding <=> $q < 3
RETURN d.name, d.embedding <=> $q AS dist
ORDER BY dist
LIMIT 10
```

The measured distance stays with its node: `WITH d` carries it on, while under
another name (`WITH d AS e`) it is computed again. It does not survive an
aggregation.

## Distance Metric

Each database's vector index has one metric, chosen when the index is created and
fixed afterwards (`lattice create --vector-metric=cosine|euclidean|dot_product`, or the
`vector_metric` open option in the bindings). Search, `<=>` and the scoring of
vectors not yet committed all use it. Smaller is always nearer.

| Metric | `<=>` returns | Range | `[1, 0] <=> [0, 2]` |
|--------|---------------|-------|---------------------|
| `cosine` (default) | 1 - cosine similarity | 0 (same direction) to 2 (opposite) | 1.0 |
| `euclidean` | the L2 distance, square root included | 0 and up | 2.236 (√5) |
| `dot_product` | the negated dot product | any number | 0.0 |

Under cosine, vectors are normalised when stored, so their length does not matter.
Under Euclidean and dot product they are stored as given, and their length counts.
With the dot-product metric a distance can be negative, so pick thresholds for the
metric in use: `< 0.5` means something different under each.

To change a database's metric, or its `m` and `ef_construction`, export it and
import it into a database created with the new settings.

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
