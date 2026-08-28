# Full-Text Search

LatticeDB includes a BM25-scored inverted index for full-text search. This guide covers indexing, searching, and fuzzy matching.

## How It Works

LatticeDB's full-text search uses:

- **Tokenization** — text is split into terms
- **Stemming** — terms are reduced to their root form
- **Inverted index** — maps terms to the nodes containing them
- **BM25 scoring** — ranks results by relevance considering term frequency, document frequency, and document length

## Declaring an Index

An index covers one label and one property, and the property is where the text
lives. Declare it once; writing that property keeps the index current, the same
way a property index stays current.

Declaring an index reads the property from every node already carrying the label,
so adding one to a database full of documents makes them searchable immediately.

If you are coming from an earlier version, see
[Migrating to Per-Property FTS](./migrating-to-per-property-fts.md).

```python
db.create_node_fts_index("Document", "text")

with db.write() as txn:
    node = txn.create_node(labels=["Document"], properties={
        "title": "My Doc",
        "text": "The quick brown fox jumps over the lazy dog",
    })
    txn.commit()
```

```typescript
await db.write(async (txn) => {
  const node = await txn.createNode({
    labels: ["Document"],
    properties: {
      title: "My Doc",
      text: "The quick brown fox jumps over the lazy dog",
    },
  });
});
```

## Searching

### Programmatic API

```python
results = db.fts_search("Document", "text", "quick fox", limit=10)
for r in results:
    print(f"Node {r.node_id}: score={r.score:.4f}")
```

```typescript
const results = await db.ftsSearch("Document", "text", "quick fox", { limit: 10 });
for (const r of results) {
  console.log(`Node ${r.nodeId}: score=${r.score.toFixed(4)}`);
}
```

### Cypher

```cypher
MATCH (d:Document)
WHERE d.text @@ "quick fox"
RETURN d.title
```

The property name on the left of `@@` is not actually used. The index holds one
document per node rather than one per property, so this reads as "does this
node's indexed text match" no matter which property you name. `d.text`,
`d.title`, and `d.spelled_wrong` all behave the same way.

Name the property that holds the text you indexed anyway, because it tells the
next person what you meant. Just do not expect a mistake in it to be caught.

## Fuzzy Search

Fuzzy search tolerates typos using Levenshtein edit distance:

```python
# Finds "machine learning" despite typos
results = db.fts_search_fuzzy("Document", "text", "machin lerning", limit=10)
```

### Controlling Sensitivity

```python
results = db.fts_search_fuzzy(
    "machne",
    limit=10,
    max_distance=2,      # Max edit distance (default: 2)
    min_term_length=4,   # Min term length for fuzzy matching (default: 4)
)
```

```typescript
const results = await db.ftsSearchFuzzy("Document", "text", "machne", {
  limit: 10,
  maxDistance: 2,
  minTermLength: 4,
});
```

- **max_distance** — maximum Levenshtein edit distance. Higher values find more matches but may include irrelevant results.
- **min_term_length** — minimum term length to apply fuzzy matching. Short terms (like "a", "the") are matched exactly.

## Combining with Vector Search

Use both search modes in a single Cypher query for hybrid retrieval:

```cypher
MATCH (chunk:Chunk)
WHERE chunk.embedding <=> $query < 0.5
  AND chunk.text @@ "neural networks"
RETURN chunk.text
ORDER BY chunk.embedding <=> $query
LIMIT 10
```

## Performance

Full-text search in LatticeDB is fast:

| Operation | Latency |
|-----------|---------|
| FTS search (100 docs) | 19 us |

This is ~300x faster than SQLite FTS5 and competitive with Tantivy, a dedicated Rust search library. See [Benchmarks](../performance/benchmarks.md) for details.

### Write the query so it can use the index

A `@@` predicate reads the index directly when it is the whole `WHERE` clause or a
branch of an `AND`. The query never looks at documents the index did not name, so
a selective search costs about the same on eight thousand documents as on five
hundred:

```cypher
MATCH (d:Document) WHERE d.body @@ "sourdough" RETURN d.title
MATCH (d:Document) WHERE d.body @@ "sourdough" AND d.year > 2020 RETURN d.title
```

Under an `OR` it cannot. The other branch may match documents the index never
names, so the query has to look at every node carrying the label:

```cypher
MATCH (d:Document) WHERE d.body @@ "sourdough" OR d.year > 2020 RETURN d.title
```

That is correct, just more work. Two `@@` predicates joined by `OR` on the same
variable are fine — those are planned as one pass over both indexes:

```cypher
MATCH (d:Document) WHERE d.title @@ "sourdough" OR d.body @@ "sourdough" RETURN d.title
```

If an `OR` against a non-text condition is slow on a large label, the usual fix is
two queries and a `UNION`, so each side can use the index that suits it.

### Rare terms cost less than common ones

Scoring reads the posting list for each term, so a term appearing in most
documents costs proportionally more than one appearing in a handful. That is
inherent to the index rather than something to tune around, and it is why a
search for a distinctive word is far quicker than one for a word your whole
corpus shares.
