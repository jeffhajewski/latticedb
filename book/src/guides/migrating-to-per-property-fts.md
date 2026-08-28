# Migrating to Per-Property Full-Text Search

Version 0.15.0 changes how full-text search works. This page is what you need to
do about it.

## What changed

Before, a node had one indexed document. You handed text to `fts_index()`, and
`@@` searched that text whatever property you named on the left of it. So
`d.title @@ "bread"` and `d.body @@ "bread"` returned exactly the same rows, and
so did `d.nonexistent @@ "bread"`.

Now an index covers one label and one property, you declare it once, and writing
that property keeps it current. `d.title @@ "bread"` searches titles.

## The straightforward case

If the text you indexed is already stored in a property — the common case, and
what the syntax always implied — declare an index over that property and delete
the indexing call.

**Before:**
```python
with db.write() as txn:
    doc = txn.create_node(labels=["Document"], properties={"text": body})
    txn.fts_index(doc.id, body)
    txn.commit()

results = db.fts_search("bread")
```

**After:**
```python
db.create_node_fts_index("Document", "text")

with db.write() as txn:
    doc = txn.create_node(labels=["Document"], properties={"text": body})
    txn.commit()

results = db.fts_search("Document", "text", "bread")
```

Declaring the index reads the property from every node already carrying the
label, so an existing database is searchable as soon as you declare it. You do
not need to rewrite your data.

## The case that needs work first

If you indexed text that is **not stored in any property**, the database cannot
rebuild it. It never held that text anywhere else, so there is nothing to read it
back from.

This is more common than it sounds. Anything that assembled a searchable string
from several fields falls into it:

```python
# The old way: this string existed only inside the index
searchable = f"{title} {body} {' '.join(tags)}"
txn.fts_index(node.id, searchable)
```

Store the assembled text in a property, then declare an index over it:

```python
db.create_node_fts_index("Document", "search_text")

with db.write() as txn:
    txn.set_property(node.id, "search_text", f"{title} {body} {' '.join(tags)}")
    txn.commit()
```

You have to backfill that property for existing rows, because only you know how
the string was built. Read each node, assemble the text the way you used to, and
write it; declaring the index afterwards picks it all up.

The upside is that the searchable text is now visible in the database. You can
read it, correct it, and rebuild the index from it — none of which was possible
when it lived only inside the index.

## Relationship properties

Relationships work the same way, with the type standing in for the label:

```python
db.create_edge_fts_index("REVIEWED", "note")
```

```cypher
MATCH (a:Person)-[x:REVIEWED]->(p:Paper)
WHERE x.note @@ "thorough"
RETURN p.title
```

This is new rather than changed: there was no way to search relationship text
before, because the old index held one document per node and relationships had no
place in it. Nothing to migrate — but if you worked around the gap by copying
relationship text onto one of its endpoints, you can stop.

The pattern has to name the type, for the reason a node pattern has to name a
label. A single `@@` searches nodes or relationships, not both, so
`d.title @@ "x" OR x.note @@ "x"` is answered row by row rather than as one
index scan. It returns the right rows either way.

## Searching several properties at once

`OR` searches each index and returns the union, scoring each document by its best
side:

```cypher
MATCH (d:Document)
WHERE d.title @@ "bread" OR d.body @@ "bread"
RETURN d.title
```

That is usually what people wanted from the old merged document. If you need one
combined score rather than a union, use the assembled-property approach above.

## Errors you will see

**"No full-text index is declared for Document.title."** You searched a property
with no index. Declare one. This is an error rather than an empty result because
no rows is indistinguishable from a search that legitimately found nothing, which
is how a typo in a property name goes unnoticed for months.

**"`d.title @@ ...` needs a label to say which full-text index it means."** Your
pattern was `MATCH (d)` with no label. Two labels can each declare an index on
`title`, so the property alone does not say which you mean. Write `MATCH
(d:Document)`.

Note that a `WITH` starts a new scope: what comes through it is an alias rather
than a node written with a label, so `@@` after a `WITH` needs the label written
again in a later pattern.

## API changes by language

Every search now takes the label and property first.

| Language | Removed | Use instead |
|---|---|---|
| Python | `txn.fts_index(id, text)` | `db.create_node_fts_index(label, prop)` |
| Python | `db.fts_search(q)` | `db.fts_search(label, prop, q)` |
| TypeScript | `txn.ftsIndex(id, text)` | `db.createNodeFtsIndex(label, prop)` |
| TypeScript | `db.ftsSearch(q)` | `db.ftsSearch(label, prop, q)` |
| Go | `tx.FTSIndex(id, text)` | `db.CreateNodeFTSIndex(label, prop)` |
| Go | `db.FTSSearch(q, opts)` | `db.FTSSearch(label, prop, q, opts)` |
| Java | `txn.ftsIndex(id, text)` | `db.createNodeFtsIndex(label, prop)` |
| Java | `db.ftsSearch(q, opts)` | `db.ftsSearch(label, prop, q, opts)` |
| C | `lattice_fts_index(...)` | `lattice_node_fts_index_create(db, label, prop)` |
| C | `lattice_fts_search(db, q, ...)` | `lattice_fts_search(db, label, prop, q, ...)` |

Every language also gained the relationship equivalent of the declaration call —
`create_edge_fts_index`, `createEdgeFtsIndex`, `CreateEdgeFTSIndex`, and
`lattice_edge_fts_index_create` — along with `drop` and `has` forms of both.

The fuzzy variants changed the same way.

## Two fixes that came with it

**A search with no `LIMIT` now returns every match.** It used to stop at a hundred
rows, silently, whether or not you asked for a limit — and the same predicate
inside an `OR` returned all of them, so the answer depended on where you wrote it.

**Fuzzy search inside a transaction is now actually fuzzy.** It accepted
`max_distance` and `min_term_length`, discarded both, and ran an exact search.
One caveat remains and is deliberate: text your transaction has written but not
committed is matched by term presence rather than edit distance, so a typo will
not find a document you have only just written.

## Only string properties are indexed

A number, a list, or a missing property contributes nothing. Turning `42` into
`"42"` so a text search could match it would be a different feature with
different rules, and doing half of it quietly would make results hard to explain.
