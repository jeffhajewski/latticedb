# Full-Text Search (`@@`)

The `@@` operator runs a BM25-scored full-text search against an index you have
declared over the property holding the text.

```cypher
MATCH (d:Document)
WHERE d.content @@ "neural networks"
RETURN d.title
```

That searches the index declared for `Document.content`, and nothing else.

## Declaring an index

A full-text index covers one label and one property. Declaring it reads that
property from every node already carrying the label, so an index you add later
still finds the documents you wrote earlier.

**Python:**
```python
db.create_node_fts_index("Document", "content")
```

**TypeScript:**
```typescript
await db.createNodeFtsIndex("Document", "content");
```

**Go:**
```go
err := db.CreateNodeFTSIndex("Document", "content")
```

**Java:**
```java
db.createNodeFtsIndex("Document", "content");
```

After that, writing the property is all it takes. Creating a node with the
property, changing it, and deleting the node each keep the index current, the
same way a property index stays current. There is no separate indexing call.

Only string properties are indexed. A number or a list is not text, and quietly
turning one into a string form of itself would make results hard to explain.

## The property name means what it says

`d.title @@ "..."` searches the index declared for `Document.title`. It does not
search `d.content`, and it does not search some pooled text belonging to the node.

If no index is declared for the property you named, the query fails and says so:

```
No full-text index is declared for Document.title. Declare one before searching it.
```

That is deliberately not an empty result. Returning no rows would make a mistyped
property name look exactly like a search that found nothing, which is how such a
mistake survives for months.

The pattern also has to carry a label, because two labels can each declare an
index on `title` and the property alone does not say which you mean:

```cypher
MATCH (d) WHERE d.title @@ "bread" RETURN d
```

```
`d.title @@ ...` needs a label to say which full-text index it means.
Write it in the pattern, as in (d:Label).
```

## Searching several properties

`OR` searches each index and returns the union:

```cypher
MATCH (d:Document)
WHERE d.title @@ "bread" OR d.body @@ "bread"
RETURN d.title
```

A document matching both sides is returned once, scored by whichever side scored
it higher. Two properties matching is not evidence that either matched twice as
well, so the scores are not added; a document matching both weakly should not
outrank one matching a single property strongly.

If you want several fields treated as one document with one merged score, store
the combined text in a property and declare an index over that:

```python
doc["search_text"] = f"{title} {body}"
db.create_node_fts_index("Document", "search_text")
```

That keeps the searchable text visible in the database, where you can read it,
rebuild it, and correct it.

## How it works

When the planner sees `@@`, it resolves the label and property to a declared
index and scans it, rather than reading every node. Results are scored with BM25,
which weighs term frequency, inverse document frequency, and document length —
which is why a title mentioning a term beats a passing mention buried in a page
of text.

A query with no `LIMIT` returns every match. Writing `LIMIT` still limits.

## String queries

The query is a space-separated list of terms, and all of them must match:

```cypher
-- Both "neural" and "networks" must appear
MATCH (d:Document) WHERE d.text @@ "neural networks" RETURN d
```

## Using parameters

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

## Combining with graph traversal

```cypher
MATCH (chunk:Chunk)-[:PART_OF]->(doc:Document)-[:AUTHORED_BY]->(author:Person)
WHERE chunk.text @@ "transformer attention"
RETURN doc.title, author.name
```

## Combining with vector search

```cypher
MATCH (chunk:Chunk)
WHERE chunk.embedding <=> $query < 0.5
  AND chunk.text @@ "transformer"
RETURN chunk.text
ORDER BY chunk.embedding <=> $query
```

## Searching without Cypher

The same indexes are reachable directly, which is useful when you want scores
rather than rows.

**Python:**
```python
results = db.fts_search("Document", "content", "machine learning", limit=10)

# Typo-tolerant
results = db.fts_search_fuzzy("Document", "content", "machin lerning", limit=10)
```

**TypeScript:**
```typescript
const results = await db.ftsSearch("Document", "content", "machine learning", { limit: 10 });

const fuzzy = await db.ftsSearchFuzzy("Document", "content", "machin lerning", { limit: 10 });
```

Inside a write transaction, these see that transaction's own uncommitted writes.
Fuzzy matching is the one exception: pending text is matched by term presence
rather than edit distance, so a typo will not find a document the transaction has
only just written.

## Dropping an index

```python
db.drop_node_fts_index("Document", "content")
```

That removes everything the index stored. The property itself is untouched, so
declaring the index again rebuilds it from the text still in the database.
