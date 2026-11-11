# Per-Property Full-Text Search

> **Status: built in 0.15.0 and archived.** Two parts of the scope below changed: edge full-text
> indexes were built, and `OR` returns a best-score union rather than two scores. `compact` does not
> reclaim an old index's pages in the middle of the file; it only truncates free pages at the end.
> See the 0.15.0 release notes and the book's Full-Text Search pages.

## The problem

`@@` looks like it searches a property and does not:

```cypher
WHERE d.content @@ "neural networks"
```

The index holds one document per node, so this asks whether the node's indexed
text matches. The property name is read by the planner and then discarded.
`d.title`, `d.content`, and `d.spelled_wrong` all behave identically, and a node
matches on text that has nothing to do with the property named.

It is worth being precise about what the current behaviour actually is, because it
is not "everything gets indexed". Nothing is indexed automatically. Text is handed
over explicitly:

```zig
try db.ftsIndexDocument(node_id, "whatever text you like");
```

That text need not be a property of the node, or resemble one. The index is a
mapping from node to an arbitrary document, and the Cypher syntax describes
something else entirely.

Two things follow. Somebody who stores text in a property and searches it gets
nothing back, with no indication why. And somebody who indexes one property's text
and searches by naming a different property gets matches, which looks like the
feature working.

## What it should be

The property that holds the text is the thing to index:

```zig
try db.createNodeFtsIndex("Document", "content");
```

After that, writing to `content` on a `:Document` indexes it, deleting the node
removes it, and `d.content @@ "..."` searches that index and nothing else.

This is what the syntax has always promised, and it is how the engine's other
indexes already work.

## Following the property index

There is no need to invent any of this. Explicit property indexes already do the
same job for equality lookups, and the shape is worth copying rather than
paralleling:

- **A catalog.** Definitions live in `property_index_catalog_tree`, keyed by
  entity kind, scope, and property. `createNodePropertyIndex(label, property)`
  adds one and populates it from existing data.
- **Automatic maintenance.** The write path calls `index.indexNode` and
  `index.removeNode` on every create, update, and delete, so an index cannot drift
  from the data.
- **One tree, many definitions.** Entry keys are prefixed with the scope and
  property symbol ids, so a single B-tree serves every declared index without new
  slots in the file header.

Full-text search can use all three. The prefix trick matters most: `FtsIndex`
holds a dictionary, a lengths tree, and a reverse tree, and prefixing their keys
with `(label_id, property_id)` lets one set of trees carry every declared
full-text index. No new header trees, no format-wide restructuring.

## Scoring

The question that makes this a versioned change rather than a patch: when one node
has several indexed properties, is that one document or several?

**Several, one per property, with per-index corpus statistics.** Each declared
index keeps its own document count, average document length, and term
frequencies.

The alternative — one document per node, merging every indexed property — is worse
in a way that shows up immediately. BM25 normalises by document length, so a title
merged with a body is a long document, and matching a term in the title scores as
though the term were buried in a page of text. Keeping them separate means a title
is compared against other titles.

It also happens to be simpler. Within one index each node appears at most once, so
the document identifier stays the node id and nothing about the posting format
changes. Only the key prefix is new.

## Decisions worth taking deliberately

### `ftsIndexDocument` goes away

*Decided: remove it.* Text is indexed because a declared index says to, and there
is one concept rather than two.

This breaks every database currently using full-text search, and one case breaks
worse than the others. Text that was indexed but is **not stored in a property**
cannot be rebuilt, because the database never held it anywhere else. Declaring an
index populates it from property values, and there is nothing to populate from.

So the migration is: store the text in a property, then declare an index on that
property. For anyone who indexed a property's value — the common case, and the one
the syntax always implied — that is a rename away. For anyone who indexed
something derived or assembled, it means keeping the derived text somewhere the
database can see.

That is a real cost and the release notes have to lead with it rather than bury
it in an upgrade section.

`ftsSearch` becomes scoped to an index, since there is no longer a single index to
search:

```zig
const hits = try db.ftsSearch("Document", "content", "neural networks", 10);
```

### Searching a property with no index

Today this silently returns nothing, which is the trap being fixed.

*Decided: refuse it.* `d.content @@ "..."` where nothing declares an index on
`Document.content` is an error naming the missing index, not an empty result. An
empty result is indistinguishable from "nothing matched", which is exactly how the
current behaviour goes unnoticed.

This is a behaviour change for anyone whose query never worked, which is the
population it is meant to reach.

### Migrating existing indexes

Old entries are keyed without a scope prefix, so nothing written by the new code
will find them. They become inert rather than wrong, which is the safe direction:
a query returns an error about a missing index instead of quietly matching against
stale data.

They are not cleared on open. The old index may hold text that exists nowhere else
in the database, and deleting it during an upgrade would destroy the only copy.
Reclaiming that space is what `compact` is for, once the user has migrated and
knows they no longer want it.

Declaring an index populates it from the property values already stored, exactly
as `createNodePropertyIndex` does.

## What had to be fixed first

Groundwork turned up two bugs in scoring that per-index statistics would have
inherited, once per index rather than once.

**The scorer read freed memory.** `FtsIndex.init` built a `DocLengthStore` as a
local, copied it into the returned struct, and handed the scorer the address of
the local — which died when the constructor returned. A workaround at one call
site rebuilt the scorer with the correct address after indexing a document, which
is why the bug stayed hidden: indexing then searching worked, and searching
without indexing first did not. That is most sessions in production.

**Corpus statistics were never persisted.** Document lengths were written to a
tree; the document count and average length that BM25 needs to interpret them were
kept in memory, started at zero, and never written down. Scores therefore changed
on every restart. On one test corpus a document's score moved 65 per cent.

Both are fixed. Statistics live under a key that cannot collide with a document
id, and the scorer holds no reference at all — it takes the statistics it needs as
an argument, which removes the possibility rather than repairing one instance of
it.

This matters for the design below: per-index statistics are the whole point of
keeping properties in separate documents, and building them on a store whose
totals evaporate on restart would have been building on sand.

## Plan

Six stages, each of which leaves the tree building and passing.

**1. The catalog.** FTS definitions join the existing index catalog under new kind
discriminators. The catalog key is `[kind, scope_id, property_id]`, so adding
`node_fts` and `edge_fts` beside `node` and `edge` needs no format change at all.
`createNodeFtsIndex`, `dropNodeFtsIndex`, and `hasNodeFtsIndex` follow the shape of
their property-index equivalents.

**2. Scoped storage.** The dictionary, lengths, and reverse trees take a
`(scope_id, property_id)` key prefix, so one set of trees carries every declared
index. Each index gets its own corpus statistics for free, since the statistics
record is itself prefixed.

**3. Population and maintenance.** Declaring an index reads the property from
every matching node and indexes it. The write path indexes on create and update
and removes on delete, next to the property index calls that already do this.

**4. Query resolution.** `@@` resolves the named property to a declared index and
searches that one. No declared index is an error naming what is missing.

**5. Removing the old path.** `ftsIndexDocument` goes, and `ftsSearch` becomes
scoped to an index.

**6. Surfaces.** C API, Python, TypeScript, Go, and the documentation.

Stages 1 to 4 are the feature. Stage 5 is what makes it a breaking release, and it
is worth doing in its own commit so the diff shows exactly what a user has to
change.

## Release shape

This wants a release of its own rather than riding along with other work, because
the migration is not automatic and the notes have to lead with it.

The breaking part is narrow but real: text that was indexed and is **not stored in
a property** cannot be rebuilt, because the database never held it anywhere else.
Someone who indexed a property's value — the common case, and what the syntax
always implied — declares an index and carries on. Someone who indexed assembled
or derived text has to store that text in a property first.

The version should be 0.15.0. There is no compatibility promise below 1.0, but a
removed API and a changed query behaviour deserve a minor bump and a migration
section rather than a patch.

## Scope

In:

- `createNodeFtsIndex(label, property)` and the edge equivalent, plus `drop` and
  `has`
- Catalog entries, key prefixing, and population from existing data
- Automatic maintenance on create, update, and delete
- `@@` resolving to the declared index for the property named
- An error when no such index exists
- The C API and all four bindings

Out, for now:

- **An index covering several properties.** This was considered and rejected, and
  the reason is the query syntax rather than the storage.

  Storing several properties as one document is easy. Asking for it is not. If an
  index merges `title` and `body`, then `WHERE d.title @@ "x"` matches documents
  whose *body* contains the term, and the property name is lying again — which is
  the exact bug this whole design exists to fix. Reusing property access requires
  one property per index.

  Naming the index instead, as in `fts(d, "doc_search") @@ "x"`, avoids that at
  the cost of syntax Cypher does not have and a second namespace to manage. Not
  worth it for this release.

  The migration for somebody searching several fields today is better than it
  sounds. They store the combined text in a property and index that:

  ```python
  doc["search_text"] = title + " " + body
  db.create_node_fts_index("Document", "search_text")
  ```

  Same single document, same merged score, and the searchable text is visible in
  the database instead of hidden inside an index where nothing can rebuild it.
  That last part is what made the old API impossible to migrate away from.

- **Merged ranking across separately indexed properties.** `WHERE d.title @@ "x"
  OR d.body @@ "x"` filters correctly but produces two scores rather than one
  ranking. That is a real limitation and a distinct feature: it needs a way to
  express "score these fields together", which is a syntax question, not a storage
  one.
- Full-text indexes on edges, unless it falls out for free.
- Changing the tokenizer, the analyzer, or anything about how terms are produced.
- Cypher syntax for declaring an index. There is none for property indexes either,
  so adding it for one index type and not the other would be the inconsistency
  rather than the fix. If index DDL arrives it should cover both at once.

## Open questions

- **Does the tokenizer configuration belong per index?** Different properties want
  different treatment — a title and a body plausibly want the same analyzer, a
  product code does not. Starting with one shared configuration is smaller, and
  the catalog entry is the natural place to put a per-index one later.
- **Should declaring an index be transactional?** `createNodePropertyIndex`
  refuses while a write transaction is open. Following that is the consistent
  choice and it is worth confirming it is also the right one for an index that may
  take a while to populate.
