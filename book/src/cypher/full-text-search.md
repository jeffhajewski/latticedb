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

## Relationships

A relationship property works the same way, with the relationship type standing
in for the label:

```python
db.create_edge_fts_index("REVIEWED", "note")
```

```cypher
MATCH (a:Person)-[x:REVIEWED]->(p:Paper)
WHERE x.note @@ "thorough"
RETURN p.title
```

The pattern has to name the type, for the same reason a node pattern has to name
a label — two relationship types can each declare an index on `note`:

```
`x.note @@ ...` needs a relationship type to say which full-text index it means.
Write it in the pattern, as in -[x:TYPE]->.
```

Labels and relationship types share one namespace, so a node index and a
relationship index cannot both use the same name and property: with
`(:Review).text` declared, declaring `[:Review].text` fails with "already exists",
and the other way round. Their entries would land on the same storage keys. A
database that declared both before this check existed keeps sharing them, and
dropping either clears both; drop both and declare again the one you keep.

A single `@@` predicate searches either nodes or relationships, not both at once.
`WHERE d.title @@ "x" OR x.note @@ "x"` is not a union of two index scans, because
one scan filters one variable; that query still works, through the row filter,
but it is not planned as a single scan.

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

When the planner sees `@@`, it resolves the label and property to a declared index
and reads the matching entities straight out of it. It does not look at anything
the index did not name, so a search that matches one document out of eight
thousand costs about what it costs out of five hundred.

That holds when `@@` is the whole `WHERE` clause or a branch of an `AND`. Under an
`OR` beside a non-text condition it cannot: the other branch may match entities
the index never names, so the query examines every entity carrying the label. The
result is the same; the work is not. See the
[performance notes](../guides/full-text-search.md#performance) if that matters for
your data.

Results are scored with BM25, which weighs term frequency, inverse document
frequency, and document length — which is why a title mentioning a term beats a
passing mention buried in a page of text.

A query with no `LIMIT` returns every match. Writing `LIMIT` still limits.
`score(d)` returns the BM25 score the search gave `d`. When `@@` is the whole
`WHERE` and the `RETURN` has a `LIMIT` with no `ORDER BY`, or with
`ORDER BY score(d) DESC`, the search keeps only the best `SKIP + LIMIT` matches
instead of producing every match and sorting them:

```cypher
MATCH (d:Document) WHERE d.text @@ "neural networks"
RETURN d.title, score(d) AS s ORDER BY s DESC LIMIT 10
```

Any other `ORDER BY`, a `DISTINCT` or aggregating `RETURN`, a longer pattern or
another condition beside the `@@` makes the search find every match first, as
before; the answer is the same either way.

When the `WHERE` is exactly the search the `MATCH` already answered from the index
(same variable, index, and query text or parameter), the search runs once; it is not
repeated as a filter over the rows it produced.

## Reading the score

`score(d)` returns the BM25 score of the `@@` search that matched `d`, the same
number the direct search calls return. Higher is better:

```cypher
MATCH (d:Document)
WHERE d.text @@ 'starter'
RETURN d.title, score(d) AS s
ORDER BY s DESC
```

For an `OR` of searches, it is the best score among the searches that matched,
also when the `OR` stands beside other conditions:

```cypher
MATCH (d:Document)
WHERE d.year >= 2020 AND (d.title @@ 'sourdough' OR d.body @@ 'sourdough')
RETURN d.title, score(d) AS s
ORDER BY s DESC
```

`score(d)` is null when no single search scored `d`: the query has no `@@` on `d`,
the `MATCH` searched several indexes and chose between them as it ran, or the
`@@` was checked row by row. That is the case for an `@@` in the `WHERE` of a
`WITH`, of an `EXISTS` or `COUNT` subquery or a pattern comprehension (which match
correctly but give no score), and of an `OPTIONAL MATCH` that reaches `d` through
a relationship; an `OPTIONAL MATCH (d:Label) WHERE d.text @@ ...` reads the index
and scores. Each part of a `UNION` is scored, and bounded by its own
`ORDER BY score(d) DESC LIMIT`, as it would be alone. It is
carried through `WITH d`, but not under another name (`WITH d AS e`) and not
through an aggregation. Its argument must be a variable; `score(1)` fails with
"score() takes a variable, as in score(d) after WHERE d.text @@ 'query'".

`ORDER BY score(d) LIMIT k`, ascending, sorts every match and keeps the `k`
weakest; only `ORDER BY score(d) DESC` lets the `LIMIT` bound the search (see
[How it works](#how-it-works)).

## String queries

Words written side by side must all match:

```cypher
// Both "neural" and "networks" must appear
MATCH (d:Document) WHERE d.text @@ "neural networks" RETURN d
```

Words are split and normalised the same way the indexed text is: words in any
script, lowercased, accents folded, English stop words dropped, and punctuation
inside a word treated as a separator. A word that no document contains means no
rows. `OR`, `-word`, `"a phrase"`, `word*` and `word~` are operators; see the
[query syntax](../guides/full-text-search.md#query-syntax). The [full-text search guide](../guides/full-text-search.md#how-it-works)
describes the tokenizer, and what changes for indexes built by 0.16.0.

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

Fuzzy search matches documents containing any of the query words, or a near
spelling of one, rather than all of them.

Inside a write transaction, `@@` and the transaction's search methods see that
transaction's own uncommitted writes. Uncommitted text is matched by the same
query, split and normalised as the index would, fuzzy words included, and scored
with the same BM25 and the index's statistics, so it ranks where it will once
committed, give or take the statistics its own commit changes.

The query syntax (`OR`, `-word`, `"phrase"`, `word*`, `word~`) is described in the
[full-text search guide](../guides/full-text-search.md#query-syntax). It applies
to `"some text" @@ "query"` on a plain string as well.

## Dropping an index

```python
db.drop_node_fts_index("Document", "content")
```

That removes everything the index stored. The property itself is untouched, so
declaring the index again rebuilds it from the text still in the database.
