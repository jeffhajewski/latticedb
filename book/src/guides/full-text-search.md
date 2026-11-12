# Full-Text Search

LatticeDB includes a BM25-scored inverted index for full-text search. This guide covers indexing, searching, and fuzzy matching.

## How It Works

LatticeDB's full-text search uses:

- **Tokenization** — text is split into words: runs of letters, digits, combining
  marks and `_`, in any script. Spaces, punctuation, symbols and emoji separate
  words, and so do bytes that are not valid UTF-8. Chinese (Han) characters and
  hiragana are a word each, because those scripts do not put spaces between
  words. Each word is lowercased (Latin, Greek, Cyrillic and Armenian letters)
  and its accents are folded, so "Café", "cafe" and "CAFÉ" are one term and
  "Straße" is stored as "strasse". Single ASCII characters, words longer than 64
  bytes and English stop words ("the", "and", …) are dropped. A query goes
  through the same steps, so `"Müller"` and `"muller"` find the same documents.
- **No stemming** — "run" does not find "running". The engine has an English
  stemmer, but the C API, the bindings and Cypher cannot turn it on.
- **Inverted index** — maps terms to the nodes containing them
- **BM25 scoring** — ranks results by relevance considering term frequency, document frequency, and document length

### Indexes built by 0.16.0 or earlier

The tokenizer above applies to indexes that are new, or empty, in this release.
An index that already held documents keeps the tokenizer it was built with,
because its stored terms came from it: only ASCII letters, digits and `_` are
word characters, so "Müller" was stored as "ller" (and matches "Zöller"), "café"
as "caf", and Cyrillic or CJK text produced no terms at all. Accents are not
folded in such an index. Searches against it keep working as they did.

Each index records how it analyses text when its first document goes in, and an
old index has no record. It switches to the new tokenizer only when every
document has left it. There is no reindex command; to rebuild an old index with
the new tokenizer, drop it and declare it again, which reads the property from
every node afresh:

```python
db.drop_node_fts_index("Document", "text")
db.create_node_fts_index("Document", "text")
```

Opening a database whose log has to be replayed after a crash also rebuilds
every full-text index, and a rebuilt index takes the new tokenizer.

Going back to 0.16.0 works for a database whose full-text indexes do not store
word positions (indexes 0.16.0 built, and indexes this release has only declared),
but 0.16.0 splits non-ASCII words its old way, so it will not find words with
non-ASCII letters that this release indexed. ASCII text is unaffected. Once an
index that stores positions (every index this release fills) holds a document,
the file asks for this release or later and 0.16.0 refuses to open it: 0.16.0
would append entries without their positions to pages whose entries carry them,
and damage the index.

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
await db.createNodeFtsIndex("Document", "text");

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

`@@` searches the index declared for that label and property — here
`Document.text`. The variable needs a label in the pattern, and if no index is
declared for the label and property you name, the query fails with "No full-text
index is declared for Document.title" rather than returning nothing.

`score(d)` returns the BM25 score the search gave `d`, the same number
`fts_search` reports, so a query can rank by it or return it:

```cypher
MATCH (d:Document)
WHERE d.text @@ "quick fox"
RETURN d.title, score(d) AS score
ORDER BY score DESC
LIMIT 10
```

It is null for a node no `@@` search scored. See
[Reading the score](../cypher/full-text-search.md#reading-the-score).

A query with several words matches documents containing every word, so a word no
document contains gives no results: `"apple zebra"` finds nothing if no document
has "zebra". Every match is scored, and the best `limit` are returned.

### Query syntax

The same syntax works in `@@`, `fts_search` and `fts_search_fuzzy`:

| Query | Matches |
|---|---|
| `quick fox` | both words |
| `quick OR fox` | either; `OR` must be in capitals. `a b OR c` is `(a b) OR c` |
| `fox -dog` | "fox" and not "dog"; `-"lazy dog"` excludes a phrase |
| `"quick brown"` | the words next to each other, in that order |
| `data*` | any word starting with "data" (at least two characters before the `*`) |
| `databse~`, `databse~1` | words within two edits, or one (`~0`: exactly; more than two counts as two); words under four characters match exactly |
| `\-fox`, `fox\*`, `fox\~`, `\"` | a backslash makes the next character literal; inside quotes nothing is an operator |

Words go through the same tokenizer as the indexed text, so punctuation separates
words (`well-known` asks for "well" and "known"), and stop words ask for nothing. A query
made only of exclusions (`-dog`) is an error rather than "everything except dog":
in Cypher write `NOT d.text @@ "dog"`. There are no parentheses.

A Cypher string reads a backslash as the start of an escape, so to pass one
through to the search write it twice: `d.text @@ '\\-fox'` searches for the
word "fox" written `-fox`, where `'\-fox'` arrives as `-fox`, an exclusion.

Phrases need word positions, which indexes created in 0.17.0 or later store; they
cost a few percent more space (about 1% at 100,000 documents). An index created
earlier has none, and a phrase query against it is an error rather than a search
for the words in any order ("Full-text query '"quick brown"' has a phrase, and the
index on Document.text stores no word positions to match one"); drop and
re-create the index to store them.

Releases before 0.17.0 treated quotes, `-`, `*` and `~` as punctuation and `OR` as
a stop word, so `fox -quick` asked for both words and `cats OR dogs` for both.

## Fuzzy Search

Fuzzy search tolerates typos using Levenshtein edit distance:

```python
# Finds "machine learning" despite typos
results = db.fts_search_fuzzy("Document", "text", "machin lerning", limit=10)
```

### Controlling Sensitivity

```python
results = db.fts_search_fuzzy(
    "Document",
    "text",
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
- **min_term_length** — minimum term length to apply fuzzy matching. Shorter terms are matched exactly.

Unlike plain search, fuzzy search matches a document that contains **any** of the
query words or a near spelling of one. Each query word scores a document by its
best-matching spelling in that document, and the words' scores add up, so a
document matching more of the query ranks higher. A spelling further from the
query word keeps less of its score: with a maximum of two edits, one edit keeps
89% and two edits 56%.

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

See [Benchmarks](../performance/benchmarks.md) for details.

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
to run two queries and merge the results in your application, so each side can
use the index that suits it. (LatticeDB's Cypher has no `UNION`.)

### Asking for the best few

When `@@` is the whole `WHERE` and the `RETURN` has a `LIMIT`, with no `ORDER BY`
or with `ORDER BY score(d) DESC`, the search keeps only the best `SKIP + LIMIT`
matches as it scores them instead of producing every match and sorting it:

```cypher
MATCH (d:Document) WHERE d.text @@ "quick fox"
RETURN d.title, score(d) AS s ORDER BY s DESC LIMIT 10
```

With a word every one of 50,000 documents contains, `LIMIT 10` this way takes
about 6 ms, where sorting every match took about 19 ms. Another `ORDER BY`,
`DISTINCT`, an aggregation, or another condition beside the `@@` brings back the
full sort; the answer is the same either way.

### Rare terms cost less than common ones

Scoring reads the posting list for each term, so a term appearing in most
documents costs proportionally more than one appearing in a handful. That is
inherent to the index rather than something to tune around, and it is why a
search for a distinctive word is far quicker than one for a word your whole
corpus shares.
