# LatticeDB 0.15.0 Release Notes

## Summary

Full-text search is now per-property. You declare an index over the property that
holds the text, and `d.title @@ "bread"` searches titles. Previously every
property name searched the same pooled text, so `d.title`, `d.body`, and a
misspelled property name all returned identical rows.

**This release is breaking.** `ftsIndexDocument` and its equivalent in every
binding are gone, and every search takes a label and property. Most people have a
short migration; some have work to do first. Read the next section before
upgrading.

## Migrating

Full detail is in
[Migrating to Per-Property FTS](https://docs.latticedb.org/guides/migrating-to-per-property-fts.html).
The short version depends on where your searchable text lives.

**If the text is already in a property**, declare an index over it and delete the
indexing call. Declaring reads the property from every node already carrying the
label, so your existing data becomes searchable immediately — there is nothing to
rewrite.

```python
# before
with db.write() as txn:
    doc = txn.create_node(labels=["Document"], properties={"text": body})
    txn.fts_index(doc.id, body)
    txn.commit()
results = db.fts_search("bread")

# after
db.create_node_fts_index("Document", "text")
with db.write() as txn:
    doc = txn.create_node(labels=["Document"], properties={"text": body})
    txn.commit()
results = db.fts_search("Document", "text", "bread")
```

**If the text is not stored in any property, the database cannot rebuild it.** It
never held that text anywhere else. This is the case worth checking for before you
upgrade, and it is more common than it sounds — anything that assembled a
searchable string out of several fields falls into it:

```python
searchable = f"{title} {body} {' '.join(tags)}"
txn.fts_index(node.id, searchable)   # this string existed only inside the index
```

Store the assembled text in a property, backfill it for existing rows, then
declare an index over it. Only you know how the string was built, which is why
this part cannot be automatic.

We found this pattern in three places in our own repository while doing the work,
so it is worth grepping for rather than assuming you are clear.

## What you get

**The property name means what it says.** Searching `title` searches titles. A
property with no declared index is an error naming what is missing, rather than
zero rows — because zero rows is indistinguishable from a search that legitimately
found nothing, which is how a typo survives for months.

**Relationship text is searchable**, which it never was before:

```cypher
MATCH (a:Person)-[x:REVIEWED]->(p:Paper)
WHERE x.note @@ "thorough"
RETURN p.title
```

**Searching several properties** unions the indexes, scoring each document by its
best side rather than the sum — matching two properties weakly should not outrank
matching one strongly:

```cypher
MATCH (d:Document)
WHERE d.title @@ "bread" OR d.body @@ "bread"
RETURN d.title
```

## Performance

A `@@` predicate now reads the index directly instead of scanning every node
carrying the label and keeping the ones the index named. Measured on eight
thousand documents:

| query | before | after |
|---|---:|---:|
| `@@ 'rare'` | 72 ms | 105 µs |
| `@@ 'rare' AND …` | 216 ms | 105 µs |
| `@@ 'common' AND …` | 21.3 s | 320 ms |

Selective searches also stopped growing with the corpus: 65 µs, 80 µs, 105 µs
across five hundred to eight thousand documents, against 3.9 ms, 17 ms, 72 ms
before.

This is a direct consequence of per-property indexing. An index covering one label
holds only that label's nodes, so reading it does the label scan's job as well as
answering the text question. The old single index spanned every node and could
only ever filter a scan somebody else produced.

It applies when `@@` is the whole `WHERE` clause or a branch of an `AND`. Under an
`OR` beside a non-text condition the query still examines every node with the
label, because the other branch may match documents the index never names.

## Fixes

- **A search with no `LIMIT` returns every match.** It used to stop at a hundred
  rows, silently, whether or not you asked for a limit — and the same predicate
  inside an `OR` returned all of them, so the answer depended on where you wrote
  it.
- **Fuzzy search inside a transaction is actually fuzzy.** It accepted
  `max_distance` and `min_term_length`, discarded both, and ran an exact search.
- **Corpus statistics survive a reopen.** Document count and average length are
  stored, so scores no longer depend on whether the session happened to index
  anything before searching.
- **Relevance scoring no longer reads freed memory** in a case where the scorer
  outlived the document store it was built from.
- **Property indexes are maintained reliably.** The code that walks index
  definitions on every write was reading a stack frame that had already been
  returned from. It worked by luck of stack layout; a change to any caller could
  have turned it into indexes that silently stopped being updated.

## Known limits

- Fuzzy matching does not reach text your transaction has written but not
  committed; that text is matched by term presence instead.
- One property per index. Searching several fields as one document means storing
  the combined text in a property and indexing that.
- With two `@@` predicates joined by `AND`, the planner seeks whichever it meets
  first rather than the more selective one.

## Upgrading

```bash
pip install --upgrade latticedb
npm install @hajewski/latticedb@0.15.0
go get github.com/jeffhajewski/latticedb/bindings/go@v0.15.0
```

Java artifacts are published at `io.latticedb:latticedb:0.15.0`.
