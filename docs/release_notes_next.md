# LatticeDB Next Release Notes

Draft for the next release after `0.16.0`.

## Summary

A database that was killed, or lost power, could reopen missing committed data,
or not reopen at all. Most of this release closes those holes: pages reach the
database file only in whole flushes through a new journal, checkpoints save
everything they depend on, a commit is durable before it becomes visible, and
the log survives a torn write at its end.

Cypher queries now give the answers Cypher specifies in many places where they
did not: null follows three-valued logic, `DELETE` refuses a node that still has
relationships, `MERGE` matches every label and property, a `MATCH` never uses one
relationship twice, variable-length patterns return every path, `sum` of integers
is an integer, and aggregates skip nulls. `=~`, relationship type alternation and
`count(*)` work. These and the other behaviour changes are listed under *Breaking
changes*, among them integer overflow as an error, a 1012-byte key cap, opaque C
API handles, JSON export format version 2, Unicode full-text tokenization and
stricter `set_vector` keys. The page layout of the database file is unchanged.

## Durability

**Checkpoints save what they depend on.** A truncating checkpoint flushed the
page cache and emptied the log, but left the B+Tree root page numbers and the
vector index's node map in memory; only `sync()` and `close()` saved them. A
process killed after an automatic checkpoint reopened onto old roots and an empty
log. With the default threshold, 3,000 committed single-node transactions
followed by a kill left 96 nodes. Every checkpoint now saves both before the log
is emptied.

**Pages reach the file only in whole flushes.** Between checkpoints the page
cache wrote a changed page to the database file whenever it needed the frame,
and freeing a page wrote to it on the spot. Neither write is in the log, so after
a checkpoint had emptied it, a kill could leave a tree half rewritten with
nothing to rebuild it from. Any database larger than its page cache was exposed
with default settings: 4,000 small transactions against a 48-page cache lost
nodes at the first kill. Changed pages now go to the file only in a flush of
every changed page together, at a checkpoint, at close, on `sync()`, or early
when half the cache holds changes. The cache grows rather than write a changed
page on its own, and gives the extra frames back at the next flush.

**A flush journal makes each flush all or nothing.** Before a flush writes pages
in place, it copies them to `<database>-journal` and syncs that. If the process
or machine dies part way through, the next open finishes the flush from the
journal, so the file always holds one whole image. The journal exists while the
database is open read-write and is deleted at a clean close. Leave it in place
after a crash: it is what the next open needs. A read-only open refuses a
database with a complete journal waiting; open it read-write once.

**A commit is durable before anyone sees it.** Commit applied a transaction's
changes and then wrote and synced the commit record, so a failure in between
left changes visible, and checkpointed, that the log said never happened. The
order is now: write the records, write and sync the commit record, then apply.
If applying fails, or the sync fails and the outcome is unknown, the handle marks
itself failed. It refuses further transactions, `sync()` and checkpoints, and
writes nothing more to the database file. Close it and reopen; recovery gives
exactly the state the log describes.

**A torn log opens.** A power cut could leave the log's header counting a frame
the file did not fully hold, a torn last frame that later frames were appended
behind, or a half-written header. Each of these made the database unopenable or
reported corruption. Opening now cuts a torn tail back to the last good frame,
and rebuilds a header whose checksum fails but whose identity fields match.
Damage with good frames after it is still reported as corruption: recovery now
checks every later frame to decide, where it looked only ten frames ahead and
could take a longer damaged run for a tail, dropping the commits behind it.

**Open and close empty the log.** Recovery replayed the log and left it in
place, and closing through the Zig API or the CLI never checkpointed, so every
open replayed everything since the last truncation and rebuilt every index. Open
now checkpoints after recovery, and close checkpoints when the log holds changes
since its last checkpoint.
Transaction ids continue above the highest id in the log, so records from two
sessions can no longer be read as one transaction.

**Syncs cover what they claim.** `sync()` synced the log but not the database
file, and with the log off nothing synced the file at all; it now does. A newly
created database's directory is synced, so a power cut right after creation
cannot leave the files missing from it, and so is the directory entry of each
session's flush journal, before the first page is written in place (neither on
Windows).

**Full fsync on macOS.** On macOS `fsync` does not ask the drive to flush its own
cache, so a commit there survives a crash but not necessarily a power cut. The
new `full_fsync` option, or `LATTICE_FULL_FSYNC=1` from the C API, the bindings
and the CLI, uses `F_FULLFSYNC`. It is off by default: it makes each commit
several milliseconds slower.

**A sync mode for throughput.** `sync_mode = .normal`, or
`LATTICE_SYNC_MODE=normal`, returns from a commit without waiting for the disk. A
returned commit survives the process being killed; a power cut or OS crash can
lose the latest commits, from the end only, and the database stays consistent.
The default stays `.full`.

**Other recovery fixes.**

- Deleting a node logged a vector removal whether or not the node had a vector.
  On a database that had never stored a vector, one delete followed by a kill, or
  a close through the Zig API or the CLI, made every later open fail with an I/O
  error. Logs written by 0.16.0 that hold such records now open.
- A flush that ran before a checkpoint wrote vector pages without the index's
  node map, and replay re-linked every vector it saw, which could leave most of
  the graph unreachable by search (186 of 298 vectors in one test). Both are
  fixed.
- Opening an existing database for writing fails if the node tree, the symbol
  table or the edge id index cannot be read. The id counters used to restart,
  so new nodes, names or edges would have taken ids already in use.
- Declaring or dropping a property index, and dropping a full-text index, reach
  the file before the call returns. A kill straight afterwards lost the
  declaration or brought the dropped index back.
- A B+Tree insert whose split fails part way, a delete whose overflow chain
  cannot be freed, and a damaged freelist head no longer leave pages with two
  owners or entries pointing at freed pages.
- `sync()` on a handle whose commit failed is refused. It could write a
  half-applied transaction into the file, and the C API's close calls it.

## Breaking changes

### Cypher

- **Null follows three-valued logic.** A comparison, arithmetic or string
  operation with a null operand is null, and `NOT`, `AND` and `OR` are Kleene:
  `null = null` is null, `NOT null` is null, `false AND null` is false, `true OR
  null` is true, `2 IN [1, null]` is null. A `WHERE` keeps a row only when its
  predicate is true, so `WHERE n.x <> 5` no longer keeps nodes with no `x`, and
  `MATCH (n {x: null})` matches nothing. `IS NULL` and `IS NOT NULL` are
  unchanged. Grouping, `DISTINCT` and `ORDER BY` still treat nulls as equal.
- **`%` takes the sign of the dividend.** `-7 % 2` is `-1` and `7 % -2` is `1`
  (they were `1` and `-1`). `%` now works on floats (`7.5 % 2` is `1.5`), where it
  returned null, and a zero divisor is an error for floats as for integers.
- **`count(*)` is accepted**, and its column is named `count(*)`. A query may
  start with `RETURN` or `WITH`: `RETURN 1 + 2` returns one row, and `RETURN
  count(*)` returns 1.
- **Integer overflow is an error.** `+`, `-`, `*`, `/`, unary minus and `abs()`
  on integers fail the query when the result does not fit in 64 bits.
  `9223372036854775807 + 1` used to panic in debug builds and was undefined in
  release builds. Float arithmetic is unchanged. `toInteger()` of a float outside
  the 64-bit range, NaN or an infinity also fails, as does a float argument out
  of range where `substring` or `range` expect an integer; all were undefined
  behaviour. `range()` of more than ten million elements fails with
  `RangeTooLarge`; it tried to allocate until memory ran out.
- **`RETURN` and `WITH` take at most 16 items.** More fails to plan with
  `TooManyVariables`; it used to read past the end of the row.
- **`DELETE` refuses a node that still has relationships.** The query fails with
  `NodeHasRelationships` and deletes nothing; it used to delete the node and
  leave its relationships pointing at nothing. Use `DETACH DELETE`, or delete the
  relationships in the same statement (`DELETE r, n`). `DETACH DELETE` now
  removes relationships by id, so parallel relationships and self-loops go too,
  and deleting one relationship twice in a statement no longer fails. `DELETE` of
  a value such as `n.name` is rejected, where it did nothing.
- **`MERGE` matches every label and every property, and returns every match.**
  `MERGE (n:A:B {k: 1})` matched a node with only `:A`; `MERGE (n {email: 'x'})`,
  with no label, created a node every time; only the first match was returned.
  Now a node matches when it has all the pattern's labels and properties, a
  pattern without a label considers every node, each match is a row with
  `ON MATCH` applied to it, and a node is created only when nothing matches.
  Values compare as `=` does, so an integer matches the equal float and lists
  match by value. A null pattern property fails the query, where it created a
  node no later `MERGE` could find.
- **A `MATCH` uses each relationship once.**
  `MATCH (a {name: 'Alice'})-[:KNOWS]-(b)-[:KNOWS]-(c)` returned Alice as `c` by
  walking back over the relationship it had just used, and comma-separated patterns could bind one relationship
  twice. An undirected match returns a self-loop once, not twice. Separate
  `MATCH` clauses may still reuse a relationship.
- **Variable-length patterns return every path.** Each path uses a relationship
  at most once, and nodes may repeat. `MATCH (s)-[:R*1..2]->(x)` over a->b, b->c,
  a->c, c->d returned b and c, and now returns b, c, c and d: a node reached by
  two paths comes back twice, and a path can return to its start. The silent
  100-hop limit on `*` is gone; the number of paths can grow quickly on dense
  graphs.
- **`sum` of integers is an integer**, computed exactly: `sum` of 10 and 25 is 35,
  not 35.0, and a sum that does not fit in 64 bits fails the query. Any float
  makes the sum a float.
- **Aggregates skip nulls.** `avg`, `collect`, `min` and `max` skip nulls, as
  `count(x)` already did; `avg` of 10, null and 30 is 20, not 13.33. `sum` or
  `avg` of a value that is not a number fails with a type error, where it was
  ignored.
- **Unaliased columns are named after their text.** `RETURN null = null` was a
  column named after whichever variable shared its slot (`y` in one case);
  `coalesce(a, b)` was named `coalesce`. Columns are now `null = null`,
  `coalesce(a, b)`, `y + 1`. Columns that had names keep them.
- **`ORDER BY` has a total order.** Values of different types sort in a fixed
  order (maps, nodes, relationships, lists, vectors, bytes, strings, booleans,
  numbers, null), where a mixed sort came back in an order that depended on the
  sort itself. Integers and floats compare exactly (2^53 + 1 no longer ties with
  2^53), NaN sorts above all other numbers, booleans can be ordered, and a sort
  key that fails to evaluate fails the query (`ORDER BY 10 / x` with `x = 0` used
  to succeed).
- **`UNWIND` of null gives no rows**, where it gave one null row. An `UNWIND`
  whose expression fails, including a missing parameter, fails the query; it used
  to skip the row silently.
- **A `<=>` predicate searches among the pattern's candidates.** It took the 100
  nearest vectors in the whole database and kept the pattern's nodes among them,
  so a label whose nodes were not in that 100 matched nothing and a threshold
  query returned at most 100 nodes. The search now widens until every candidate
  within the threshold is found, or the `LIMIT` is met. A threshold passed as a
  parameter is honoured (it was ignored), any other threshold expression is
  evaluated exactly, and `<` no longer includes a distance equal to the
  threshold.
- **Storage errors fail the query.** A read error in a property access or a
  label check read as null, or as "no such label"; it now fails the query with
  `StorageError`.
- **Function names are not case-sensitive** (`TOLOWER` works), and
  `toString(null)` is null, where it was the string `'null'`.
- **`CREATE` cannot add labels or properties to a bound variable.**
  `MATCH (n) CREATE (n:Extra {p: 1})` silently dropped them; it is now rejected.

### Storage, API and formats

- **Keys are capped at 1012 bytes** (for 4 KB pages). Labels, relationship types
  and property names are stored as keys, so a name of more than about a thousand
  bytes is refused with a storage error. A long name used to be able to overflow
  a B+Tree page in memory. Longer keys already in a file can still be read and
  deleted. Property values are not affected.
- **C API handles are opaque tokens**, not addresses, and are never reused, so a
  stale handle is rejected instead of reaching freed memory. A transaction handle
  used after commit or rollback returns `LATTICE_ERROR_TXN_ABORTED`, also after
  `lattice_close`, where it returned `LATTICE_ERROR_INVALID_ARG`; other stale
  handles return `LATTICE_ERROR_INVALID_ARG`. Enum arguments are checked: an
  unknown `lattice_begin` mode is `LATTICE_ERROR_INVALID_ARG` (any non-zero mode
  used to mean read-write). An empty batch-insert label means no label.
- **JSON export format version 2.** Exports start with `"version": 2`, write
  bytes as `{"$bytes": base64}`, vector properties as `{"$vector": [...]}`, floats
  always with a decimal point or exponent, and non-finite floats as
  `{"$float": "NaN"}`. Import reads version 2, and still reads 0.16.0 files (no
  version field), except that strings made of hex digits, such as `"cafe"`, stay
  strings; they used to come back as bytes. A file with a newer version is
  refused. 0.16.0 cannot read version 2 files. CSV export now writes properties,
  in `<key>:json` columns.
- **A `<database>-journal` file** sits beside a database open read-write (see
  *Durability*). Backup, sync or file-management tooling that copies a database
  by its file names should leave it alone; use `backup` to copy an open
  database.
- **Full-text search splits words by Unicode.** Words are runs of letters,
  digits and marks in any script, lowercased, with accents folded: "Café",
  "cafe" and "CAFÉ" are one term. Han characters and hiragana are a term each.
  Indexes that already hold documents keep 0.16.0's ASCII tokenizer until they
  are emptied; there is no reindex command, so drop and recreate an index to
  switch it. A crash-recovery replay also rebuilds it.
- **A node and a relationship full-text index on the same name and property are
  refused** with `ALREADY_EXISTS`; they would share storage. Databases that
  already have both keep working as before.
- **Vectors with NaN or infinite components are rejected** with
  `INVALID_ARG`, when stored and when used as a query.
- **`set_vector` refuses keys other than `"embedding"`**, `""` and `NULL`, with
  `UNSUPPORTED`. A node holds one vector, and another key silently replaced it.

## Performance

<!-- numbers to be re-measured on a quiet machine before release -->

The C API used Zig's debug allocator in every build, with one global lock per
allocation. Release builds now use `smp_allocator`. Label scans read the label
index instead of decoding every node, and changefeed records no longer make a
large commit quadratic. Through the C API, release build, 20,000 nodes:

| | before | after |
|---|---:|---:|
| read one property | 9.4 µs | 0.37 µs |
| `MATCH (n:Rare) … LIMIT 1` | 190 ms | 3.7 µs |
| `MATCH (n:Person) WHERE n.age > 60` | 478 ms | about 9 ms |
| read-only transaction, no work | 44 µs | 0.08 µs |
| commit 20,000 nodes in one transaction | 9.5 s | about 0.3–0.6 s |

A read-only transaction no longer writes to the log at all; it used to write two
records and an fsync.

Other changes, measured by their commits on a busy machine:

- Full-text search scores posting lists in batches and keeps only the best
  results: a common term over 100,000 documents went from 35 ms to 6.7 ms.
- Saving the vector index writes only the entries that changed: after one insert
  into 10,000 vectors, 58 ms became 0.05 ms. Storing a vector at 100,000 vectors
  went from 801 µs to 3.1 µs, and removing one from 1.96 ms to 0.15 ms.
- Page checksums use the CPU's CRC instruction where there is one (7.2 µs to
  0.39 µs per 4 KB page). They are bit-identical to before.
- Writes no longer invalidate the query cache, and B+Tree lookups and point
  reads avoid copies.
- A second thread waiting on a database lock sleeps instead of spinning on a
  core.

The second table compares the query engine before and after its own fixes in
this release, so its "before" already includes the gains above. Cypher queries
read one property at a time instead of copying the whole node, evaluate
`ORDER BY` keys once per row and keep only the top `k` under a `LIMIT`, stream
label scans in read-only queries so a `LIMIT` stops them early, and parse a
prepared query once instead of once for `lattice_query_writes` and again to run
it. Through the C API, release build, 20,000 nodes, median of three runs on a
busy machine (±15%):

| query | before | after |
|---|---:|---:|
| `MATCH (n:Person) RETURN n.id LIMIT 1`, 20,000 `Person` nodes | 99.7 µs | 1.39 µs |
| `RETURN` 4 properties | 31.2 ms | 12.4 ms |
| `WHERE age = 33` | 6.9 ms | 4.3 ms |
| `ORDER BY age DESC, id LIMIT 10` | 288 ms | 4.9 ms |
| `ORDER BY name`, all rows | 82 ms | 16.1 ms |
| `count` + `avg` + `sum` | 12.5 ms | 4.7 ms |
| 1-hop expand | 21.9 ms | 12.3 ms |
| `*1..3` from one node | 7.0 ms | 4.4 ms |
| `<=>` with `LIMIT 10` | 131 µs | 96 µs |
| `UNWIND` of a tiny list | 0.57 µs | 0.60 µs |

Preparing, running and freeing a tiny read query went from 0.79 µs to 0.60 µs.
One lookup is slower: an indexed integer equality also probes the equal float,
about 0.3 µs more.

## Fixes

**Cypher**

- `=~` works, with a regular-expression engine whose time grows linearly with the
  text, whatever the pattern. It was a parse error. Backreferences and
  lookaround fail the query.
- `-[:A|B]->` matches a relationship of either type, also in variable-length
  patterns. It was rejected. `-[:A:B]->` is now a parse error.
- A node variable can be named again, in a later `MATCH`, after `WITH`, or later
  in the same pattern, and means the same node. It was rejected as already
  defined.
- `MATCH (a)-->(b) WITH b RETURN b.name` returned `a`'s name, and a relationship
  passed through `WITH` was treated as a node, so `WITH r DELETE r` failed.
- A variable holding null reads as null. `UNWIND [1, null] AS v RETURN v` and
  `WITH n.x AS x RETURN x` over a node without `x` failed the query.
- An index lookup for `1` finds a node stored with `1.0`, as a scan does; `MERGE`
  through the index used to create a duplicate.
- Lists and maps compare by value: `[1, 2] = [1, 2]` is true.
- `-9223372036854775808` is a valid literal, and keywords can name properties,
  labels and map keys (`n.order`, `:Set`, `{limit: 1}`).
- A query that failed while its operators were opening leaked memory, and reading
  `n.embedding` leaked a copy of the vector on every row.
- Decoding a damaged node or edge record no longer frees memory twice or trusts
  a length field before allocating.
- `RETURN DISTINCT x LIMIT n` applies `SKIP` and `LIMIT` to the distinct rows.
  They were applied first, so `RETURN DISTINCT x LIMIT 2` over 1, 1, 1, 2
  returned one row.
- `min` and `max` order values as `ORDER BY` does. Over values of different
  types they returned whichever came first, and they could not tell integers
  apart beyond 2^53 when a float was among them.
- `size`, `abs`, `id` and `range` return null for a null argument; they failed
  the query. `{a: 1}.a` is 1; property access on a map was always null.

**Full-text search**

- AND search could miss matching documents after updates or multi-document
  transactions.
- A query term no document contains gives no results; it was ignored.
- All matches are ranked before the limit is applied; only the first 2 × limit
  were considered.
- Fuzzy search scores each word by its best spelling. Uncommitted text is
  matched by whole terms, so "cat" no longer matches "concatenate".
- Zig API: phrase alignment, stop words inside phrases, prefix words counted
  twice, exclusions applied after ranking, and `searchWithMode` looping forever
  on punctuation or accented queries are fixed. `use_stemming` now stems.
- Long posting entries no longer overrun their buffers.
- A term whose first posting page had been emptied by deletes matched nothing,
  however many documents on later pages still contained it.
- A query of more than 32 words dropped the words after the 32nd, which made an
  AND search match more than it should.
- Fuzzy search inside a transaction sees uncommitted edges and fills its limit.
- Declaring an index again after a declaration failed part way no longer
  indexes some documents twice.
- Deleting a document with no indexable words no longer walks the whole
  dictionary (about 325 ms down to 12 µs at 20,000 documents).
- Zig API: every phrase of a multi-phrase query is required, a phrase marked
  with `-` is excluded instead of required, and highlight snippets no longer
  cut a UTF-8 character in half.

**Vector search**

- `vector_search` with `k` above `ef_search` returns `k` results; it returned
  `ef_search` of them.
- HNSW inserts and removes are all or nothing. `m` and `m_max0` are capped at 64.
- Pending vectors are scored on the same scale as committed ones. Under cosine,
  committed vectors read back normalised (documented; unchanged).
- Replacing a node's vector could leave the node with no neighbours, where no
  search would reach it (113 of 600 replacements in one test).
- Rebuilding the index at open, when the saved graph cannot be loaded, frees
  the pages the old graph used instead of leaking them.
- A neighbour whose vector cannot be read fails the insert instead of being
  linked unchecked.

**CLI**

- Every command that opens a database takes `--enable-vector` and
  `--vector-dims`, and an open that fails because the log holds vector changes
  says which options to retry with.
- Export, JSONL export and dump include node vectors, and import restores them.
- `export --query` exports what the query returns.
- CSV import follows RFC 4180, finds columns by header, resolves edges through
  `_id`, and keeps edge properties and value types.
- An import that fails names the record and the reason. Each record is imported
  whole or not at all, and `--on-error=skip` lists what it left out.
- JSONL files can be imported.
- `lattice ... >> file` appends; it overwrote the start of the file.
- A failed export removes its partial file.
- `replicate --follow` opens the database for each pass instead of holding it,
  and its lock, for the whole run, so other processes can write between passes;
  a pass that finds the database open elsewhere is skipped. Its output is
  flushed after every pass.
- A `replicate` pass with nothing changed ships nothing. Each run was taking a
  full snapshot.
- The result table no longer fails on the smallest integer, on floats wider
  than ten characters, or on large node ids.

**C API and bindings**

- Database-level C calls take the database lock, and close wakes a blocked stream
  reader instead of freeing memory under it.
- A failed `lattice_commit` leaves the transaction open for rollback. Python
  `query()` and Go `Update` and `Query` roll back after a failed commit; they held
  the writer slot.
- Edge types are read by length (TypeScript returned trailing bytes), and C also
  NUL-terminates them.
- The embedding client copies its configuration strings; TypeScript requests
  could read freed memory.
- Python: integers outside the 64-bit range raise `OverflowError` instead of
  wrapping, and unlabeled nodes no longer leak.
- TypeScript: `getProperty` returns `null` for a missing property, and a timed
  `readStream` waits on a worker thread instead of blocking the event loop.
- Go: buffers of 2 GiB or more are copied whole; they panicked or were
  truncated.
- Java: `close()` refused while work is open keeps the handle, `QueryException`
  carries the real error code, `BigDecimal` is stored as a float, and characters
  outside the BMP survive error text and column names.
- `LATTICE_PREFIX` on Windows looks for the DLL under `bin/`, then `lib/`.
- The embedding client applies its timeout (`timeout_ms`, 30 s by default) to the
  wait for the response to begin; it was stored and never used, so a stalled
  server kept a request waiting indefinitely. Connecting and reading the rest of
  a response are not bounded, and Windows is unchanged.

## Known limits

- **Reopen a crashed vector database with vectors enabled.** If the log holds
  committed vector writes, the next open must enable vectors with the dimensions
  the writer used. Every CLI command now accepts the options, and the error says
  so.
- **A read-only open does not read the log.** After a crash it shows the
  database file as of its last flush, which can be behind the commits the log
  holds. Open the database read-write once to recover them.
- **`lattice replicate` takes a snapshot after any process that wrote has
  closed the database.** Closing now folds the log into the file and resets it,
  which begins a new replication generation. That was already so for the
  language bindings; it is new for the CLI and for Zig callers, whose writes
  0.16.0 left in the log to be shipped as frames. A pass with nothing changed
  still ships nothing, and a process that stays open and calls `replicateTo`
  on its own handle ships frames as before.
- **`sync_mode` and `full_fsync` are set through the environment** outside Zig;
  the C API's open options do not include them.
- **Some Cypher is not implemented.** `OPTIONAL MATCH`, `CASE`, `UNION`, `CALL`,
  list indexing, list comprehensions and named paths fail with a parse error.
  The message names the clauses the parser expected, not the missing feature.
- **One relationship-uniqueness gap.** A single-hop relationship written after a
  variable-length one in the same `MATCH` can reuse a relationship of that path.
- **Strings are measured in bytes.** `size()` and `substring()` count bytes, and
  `toLower()` and `toUpper()` change ASCII letters only. An unknown function name
  is reported when it is first evaluated, not when the query is parsed.

## Upgrade notes

- The page layout and the log format are unchanged. Full-text indexes record how
  they analyse text when their first document goes in, under a new key.
- Close a database cleanly with this release before opening it with 0.16.0.
  0.16.0 does not know about the journal, so after a crash it would ignore an
  unfinished flush.
- Check application code against the Cypher changes above, in particular plain
  `DELETE` of nodes that may have relationships, code that reads results by
  the name of an unaliased column, code that expects `sum` of integers to be a
  float, and `MERGE` patterns without a label or with several labels.
- 0.16.0 can open a database whose full-text indexes this release wrote, but it
  splits non-ASCII words the old way and will not find words with non-ASCII
  letters indexed by this release.

## Upgrading

```bash
pip install --upgrade latticedb
npm install @hajewski/latticedb@<version>
go get github.com/jeffhajewski/latticedb/bindings/go@v<version>
```

The Java binding (`io.latticedb:latticedb`) is not published to a Maven repository; build it from `bindings/java` (see its README).
