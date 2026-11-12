# LatticeDB Next Release Notes

Draft for the next release after `0.16.0`.

## Summary

A database that was killed, or lost power, could reopen missing committed data,
or not reopen at all. Most of this release closes those holes: pages reach the
database file only in whole flushes through a new journal, checkpoints save
everything they depend on, a commit is durable before it becomes visible, the
log tells writes a power cut left unfinished from damage to synced data, and
replay applies exactly what was committed or refuses to open. A simulated disk
that cuts the power at every sync point now tests this, and a read-only open
recovers a crashed database in memory instead of showing a stale one.

The file now records its vector index and its node id counter, so a database
with vectors opens without being told its dimensions, after a crash included,
and a deleted node's id is never handed out again. The vector metric can be
cosine, Euclidean or dot product, chosen at creation. Every API refuses to
delete a node that still has relationships and to create an edge to a missing
node, and `lattice_open_v5` sets the sync mode and vector options per open.

Cypher queries now give the answers Cypher specifies in many places where they
did not: null follows three-valued logic, `DELETE` refuses a node that still has
relationships, `MERGE` matches every label and property, a `MATCH` never uses one
relationship twice, variable-length patterns return every path, `sum` of integers
is an integer, and aggregates skip nulls. `=~`, relationship type alternation and
`count(*)` work. A failed query says what failed and where, strings are counted
in characters, and integers and floats compare exactly. These and the other
behaviour changes are listed under *Breaking changes*, among them integer
overflow as an error, a 1012-byte key cap, opaque C API handles, JSON export
format version 2, Unicode full-text tokenization and stricter `set_vector` keys.
The file format is version 4 and the log format version 3; *Upgrade notes* says
exactly what 0.16.0 can still open.

Much more of Cypher runs: `OPTIONAL MATCH`, `UNION`, `CASE`, `CALL { }`
subqueries, `FOREACH`, named paths, `shortestPath`, list indexing and slicing,
list and pattern comprehensions, `EXISTS`, `COUNT` and `COLLECT` subqueries,
map projection, label expressions and `RETURN *`, among others listed under
*Cypher*. What is still missing fails with an error that names it.

Property indexes keep their values in order, so the planner reads ranges,
`STARTS WITH` prefixes and `ORDER BY ... LIMIT` in either direction from them,
not only equalities; a file with such an index needs this release to open.

`lattice import` and `lattice export` read and write RDF in Turtle and
N-Triples, so vocabularies and data sets kept as RDF can be queried with Cypher,
full-text and vector search, and written back out.

Database files are much smaller: the built-in changefeed, which made up over 90%
of a typical file, is stored about twelve times more compactly, and a retention
policy can bound it or turn it off. Keeping every event is still the default.

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
after a crash: it is what the next open needs.

**A commit is durable before anyone sees it.** Commit applied a transaction's
changes and then wrote and synced the commit record, so a failure in between
left changes visible, and checkpointed, that the log said never happened. The
order is now: write the records, write and sync the commit record, then apply.
If applying fails, or the sync fails and the outcome is unknown, the handle marks
itself failed. It refuses further transactions, `sync()` and checkpoints, and
writes nothing more to the database file. Close it and reopen; recovery gives
exactly the state the log describes.

**What a power cut leaves, the log can tell apart from damage.** After a power
cut the frames written since the last sync can be in any state, some whole and
some torn or never written, in any order. 0.16.0 called a bad frame with a good
one after it corruption, so an ordinary power failure during a commit could
leave a database that would not open; it looked only ten frames ahead, so a
longer damaged run could instead be taken for a torn tail, dropping the commits
behind it. A frame that never reached the disk reads back as zeroes, which
recovery accepted as an empty frame, so a transaction spread over several
frames could be replayed without the records of the lost one: a commit applied
in part. The log is now format version 3. Each frame records how many frames had
been synced when it was written, and its checksum covers that claim. At open, a
bad frame that a later frame vouches for as synced is damage, left as found and
refused (`InvalidDatabase`); any other bad frame starts an unsynced tail, which
is cut off. A frame verifies only if it holds records, in every reader of the
log. A header whose checksum fails but whose identity fields match is rebuilt.
No write or sync is added to a commit.

**The guarantee, stated and tested.** In the default sync mode, a transaction
whose commit returned survives a power cut, and one whose commit had not
returned is wholly present or wholly absent; after recovery the database holds
exactly the transactions up to some point at or after the last acknowledged
one, with every index consistent. In `normal` mode it holds a whole number of
transactions up to some point no earlier than the last log sync. A crash during
recovery changes nothing. This assumes a device that honours `fsync` (and
`F_FULLFSYNC` on macOS with `full_fsync`). A new power-loss simulation tests it:
an in-memory disk records every write and sync and cuts the power before and
after every sync and at random points, losing, reordering and tearing unsynced
writes and file names, and the crash suite runs a random workload through it and
checks every result, including a second cut during recovery. It found five of
the defects listed here.

**Replay is exact.** Replay re-applied logged changes loosely: replaying a node
insert added its labels to the label index again, and replaying a label removal
did nothing if the node already lacked the label, index entry included, so a
node could come back listed under a label it did not have. Every logged change
is now applied as an assignment of the state it records. A committed record
that cannot be replayed, one whose payload does not decode, of a type no release
writes, a page image, or a change to a node that is missing and not deleted
later, fails the open with `InvalidDatabase`; it used to be skipped, and the
database opened without an acknowledged change.

**A read-only open recovers in memory.** A read-only open never read the log, so
after a crash it showed the file as of its last flush, missing any number of
committed transactions, and with a flush the crash had interrupted it failed
with an I/O error. It now runs the same recovery as a read-write open on an
in-memory overlay of the files: every committed transaction is visible, and
nothing is written to disk. The memory it may use is 64 MB plus four times the
size of the log and journal; a recovery that needs more fails with
`BufferPoolFull` (`LATTICE_ERROR_FULL`), and a read-write open recovers it.

**A large value cut short no longer breaks the log.** A process killed while a
value too large for one log frame was being logged left the first fragments
behind. With automatic checkpoints off, or the open's own checkpoint
interrupted, later commits were logged after them, and every later open called
the log corrupt and refused the database. The unfinished fragments are now
skipped when another transaction's records follow them.

**A zeroed page header is caught.** A page whose stored checksum was zero passed
verification whatever it held, so damage that zeroed a page's first sector,
checksum included, was read as a valid page. A zero checksum now passes only for
a page exactly as allocation writes it.

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
Windows). A log created for an existing database, as when a backup is put back
without its log, is synced with its directory entry when it is created; before,
a power cut could lose the new log's name and up to every commit in it.

**Full fsync on macOS.** On macOS `fsync` does not ask the drive to flush its own
cache, so a commit there survives a crash but not necessarily a power cut. The
new `full_fsync` option uses `F_FULLFSYNC`. It is off by default: it makes each
commit several milliseconds slower.

**A sync mode for throughput.** `sync_mode = normal` returns from a commit
without waiting for the disk. A returned commit survives the process being
killed; a power cut or OS crash can lose the commits since the last log sync,
from the end only, and the database stays consistent. The default stays `full`.

Both are open options in every API: `sync_mode` and `full_fsync` in Zig and
Python, `syncMode` and `fullFsync` in TypeScript and Java, `SyncMode` and
`FullFsync` in Go, and fields of `lattice_open_options_v5` in C. The
`LATTICE_SYNC_MODE` and `LATTICE_FULL_FSYNC` environment variables apply when a
program does not set them, and are how the CLI sets them.

**Other recovery fixes.**

- A database whose log held vector changes could only be reopened with vectors
  enabled at the dimensions it was written with; any other open failed with an
  I/O error. The file now records its vector index, so it opens with it, after a
  crash included, whatever the open's options say.
- Deleting a node logged a vector removal whether or not the node had a vector.
  On a database that had never stored a vector, one delete followed by a kill, or
  a close through the Zig API or the CLI, made every later open fail with an I/O
  error. Logs written by 0.16.0 that hold such records now open.
- A flush that ran before a checkpoint wrote vector pages without the index's
  node map, and replay re-linked every vector it saw, which could leave most of
  the graph unreachable by search (186 of 298 vectors in one test). Both are
  fixed.
- Opening an existing database for writing fails if the symbol table or the
  edge id index cannot be read, or, for a file that does not yet record its node
  id counter, the node tree. The id counters used to restart, so new nodes,
  names or edges would have taken ids already in use.
- Declaring or dropping a property index, and dropping a full-text index, reach
  the file before the call returns. A kill straight afterwards lost the
  declaration or brought the dropped index back.
- A B+Tree insert whose split fails part way, a delete whose overflow chain
  cannot be freed, and a damaged freelist head no longer leave pages with two
  owners or entries pointing at freed pages.
- `sync()` on a handle whose commit failed is refused. It could write a
  half-applied transaction into the file, and the C API's close calls it.

## RDF import and export

`lattice import` reads Turtle (`.ttl`) and N-Triples (`.nt`), including
Turtle-star and N-Triples-star, and passes the W3C RDF 1.1 Turtle and
N-Triples suites (383 tests). Each IRI becomes a node labelled `Resource` with
an `iri` property, found through a `Resource(iri)` property index the import
declares, so later files add to the same nodes. `rdf:type` becomes labels (or
edges, with more than 1,000 classes or `--rdf-types=edges`), literals typed
properties, other objects edges, and annotations edge properties. Names are
`<prefix>_<local>` (`rdfs_label`, `skos_prefLabel`), strings in other languages
than `--rdf-lang` get properties of their own (`rdfs_label_de`), and
`--rdf-fts` declares full-text indexes on the result. A statement is one record
for `--on-error=skip`; an integer that does not fit in 64 bits fails its
statement.

`lattice export` writes Turtle, grouped by subject and abbreviated with the
prefixes import stored, or N-Triples, streaming in both cases. Edge properties
are RDF-star annotations, or `rdf:Statement`s with
`--rdf-edge-properties=reify`. A graph that never came from RDF exports too:
its names under `urn:lattice:` (or `--base`), nodes as blank nodes or, with
`--base`, as IRIs. Turtle to a database and back gives the same RDF graph;
[RDF: Turtle and N-Triples](../book/src/guides/rdf.md) lists what changes in
each direction. JSON-LD, RDF/XML, TriG, N-Quads, SPARQL and inference are not
supported.

## The changefeed

**Changefeed events are stored compactly.** Every committed change writes an
event to the changefeed (`__lattice_changes`), and in 0.16.0 each took about
300 bytes on disk: a 26-byte key and a value spelling out its kind and payload
map in full. Events now take about 24: a 3- to 5-byte key and a binary value
that refers to labels, types and property names by their symbol ids and
carries the commit time once per transaction. What `changes()` and
`lattice_stream_read` return is unchanged, byte for byte, apart from the order
of events within a transaction (below). Loading 200,000
nodes with a label and two properties each:

| | 0.16.0 | this release |
|---|---:|---:|
| file size | 256.4 MB | 35.7 MB |
| bytes per node | 1,282 | 178.5 |
| changefeed | 238.0 MB | 19.0 MB |
| load time, 1,000 nodes per transaction | 2.8 s | 1.0 s |
| one 20,000-node transaction, per node | 19–38 µs | 4.7–5.4 µs |
| reopen after a crash, 999 transactions in the log | 12–15 ms | 6.3–7.9 ms |

**Retention policies.** How much of the changefeed to keep is now a policy the
file records: `all` (the default: every event, forever), `last:<N>` (the newest
N events), `age:<duration>` (events committed within, say, `age:7d`) or `off`
(no events; reading the changefeed is an error). Old events are dropped at
checkpoints, a bounded number each time, or all at once with a trim. With
`off`, the same file is 16.7 MB; with `last:10000`, 18.1 MB.

- Set it with `changefeed_retention` (Python), `changefeedRetention`
  (TypeScript), `ChangefeedRetention` (Go), `changefeedRetention` (Java),
  `lattice_open_options_v5.changefeed_retention` and
  `changefeed_retention_value` (C), `--changefeed=<policy>` on any writing CLI
  command, or `LATTICE_CHANGEFEED_RETENTION` for a database being created. A
  policy given when opening for writing replaces the file's; a read-only open
  never changes it.
- Inspect and apply it with `changefeed_status()` / `trim_changefeed()`
  (Python), `changefeedStatus()` / `trimChangefeed()` (TypeScript, Java),
  `ChangefeedStatus()` / `TrimChangefeed()` (Go), `lattice_changefeed_status` /
  `lattice_changefeed_trim` (C), and `lattice changefeed <db> [--trim]`.
  `lattice info` shows the policy and the range of events held.
- A reader asking for events after one that has been dropped gets an error
  (`LATTICE_ERROR_NOT_FOUND`; `LatticeChangesTrimmedError` in Python and
  TypeScript, `ErrChangesTrimmed` in Go, `ChangesTrimmedException` in Java)
  instead of a silent gap. Sequence numbers are never reused.

**Events within a transaction come in an order a consumer can replay.** In
0.16.0 and until now, a transaction's events came all nodes first, then all
edges, each in an order that depended on a hash map's layout: two nodes created
in that order could appear the other way round, and a node deleted with its
relationships had its `node.delete` before the `edge.delete` of each of its
edges, so a consumer applying events in sequence deleted a node that still had
relationships. Events now come in a fixed order: the edges the transaction
deletes, the nodes that exist after it, the edges that exist after it, then
the nodes it deletes, each group by ascending id. An edge is created after both
its nodes and deleted before either, every change to a node or edge follows its
creation, and a node's deletion is the last event that names it. As before, the
events are the transaction's net effect: a property set several times in one
transaction is one event, from its value before the transaction to its value
after. Commit cost is unchanged (the ids are sorted once and the sort is reused
to apply the transaction).

The guide, with when to choose each policy, the order of events and the stored
format, is `book/src/guides/changefeed.md`.

## Cypher

Queries can use much more of the language. Each of these was a parse error,
or an unknown function, in 0.16.0:

- **`OPTIONAL MATCH`**, with its own `WHERE`: a row with no match is kept, with
  the pattern's new variables null.
- **`UNION` and `UNION ALL`.** The queries run in the order written.
- **`CASE`**, in both the simple and the searched form, anywhere an expression
  goes. Only the chosen branch is evaluated.
- **`CALL { }` subqueries** run once per row, importing variables with a leading
  `WITH`; aggregation, `ORDER BY` and `LIMIT` inside apply to each run, which
  gives per-row top-k and counts. A subquery without `RETURN` runs for its writes.
- **`CALL db.labels()`**, `db.relationshipTypes()`, `db.propertyKeys()` and
  `db.indexes()` list the catalog, with `YIELD` to name their columns.
- **`FOREACH (x IN list | ...)`** runs `CREATE`, `MERGE`, `SET`, `REMOVE`,
  `DELETE` and nested `FOREACH` for each element.
- **Named paths**, `p = (a)-[:R*]->(b)`, with `nodes(p)`, `relationships(p)` and
  `length(p)`. The variable of a variable-length relationship, `-[r:R*]->`, is the
  list of the relationships it matched, and a property map on it,
  `-[:R* {since: 2020}]->`, applies to every relationship.
- **`shortestPath` and `allShortestPaths`**, searched breadth-first from both
  ends: about 0.35 ms between two random nodes of a 100,000-node,
  1,000,000-relationship graph.
- **Lists:** indexing (`list[0]`, `list[-1]`), slicing (`list[1..3]`), access by
  key (`map['k']`, `n[$prop]`), comprehensions (`[x IN list WHERE x > 0 | x * 2]`),
  `all`, `any`, `none`, `single` and `reduce`.
- **Patterns and subqueries in expressions:** pattern predicates
  (`WHERE (n)-[:KNOWS]->()`), pattern comprehensions (`[(n)-->(m) | m.name]`),
  and `EXISTS { }`, `COUNT { }` and `COLLECT { }` holding a pattern or a whole
  query: `COLLECT { MATCH (p)-[:WROTE]->(d) RETURN d.title ORDER BY d.year DESC
  LIMIT 3 }`. The query sees the variables around it, runs once per row, and
  cannot write.
- **Map projection**, `n {.name, .age, total: x}` and `n {.*}`, and `properties()`
  of a map.
- **`RETURN *` and `WITH *`**, every variable in scope.
- **Aggregates inside larger expressions:** `count(n) + 1`, `sum(x) / count(x)`,
  an aggregate inside `CASE`, and grouping keys that are expressions.
- **Label expressions:** `(n:A|B)`, `(n:A&!B)`, `(n:%)` and parentheses, in
  patterns and as tests in expressions (`WHERE n:Person|Robot`). On a
  relationship the test is of its type: `r:KNOWS`.
- **Smaller additions:** `exists(n.prop)`, `ORDER BY ... NULLS FIRST | NULLS LAST`, backtick-quoted
  names (`` n.`first name` ``), hexadecimal integers (`0x1F`), octal integers (`0o17`) and `.5`.

What is not implemented fails with an error that names the construct, where
0.16.0 reported the clauses it expected: quantified path patterns, `LOAD CSV`, `CALL { } IN TRANSACTIONS`, `OPTIONAL CALL`,
type predicates, `||`, schema and administration statements, and
the GQL-only clauses. `book/src/cypher/overview.md` lists them.

## Breaking changes

### Cypher

- **`CASE` is a reserved word.** It cannot name a variable, since it now starts
  an expression. It still works as a property name, label, relationship type or
  map key, and `WHEN`, `THEN`, `ELSE`, `END` and `UNION` stay usable as names.

- **Query errors say what failed and where.** A failed query reports its stage
  (parse, semantic, plan or execution), a diagnostic code, a message naming the
  value or operation, and the line and column: `RETURN 1/0` fails with "Division
  by zero: 1 / 0" at 1:9, where it said "query failed". The CLI prints the query
  with a caret under the place. Unknown functions and wrong argument counts are
  semantic errors found before the query runs, so `lenght(n)` fails over an empty
  graph too; it used to succeed until a row reached it.
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
  `NodeHasRelationships` (`LATTICE_ERROR_HAS_RELATIONSHIPS`, -17, through the C
  API, the same code as `lattice_node_delete`) and deletes nothing; it used to delete the node and
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
- **Every ordering comparison with NaN is false.** `>` and `>=` were computed as
  `NOT <=` and `NOT <`, so `NaN > 1` and `NaN >= 1` were true while `NaN < 1`
  was false. Now `<`, `<=`, `>`, `>=` and `=` with a NaN operand are false and
  `<>` is true, as openCypher has it, so `WHERE n.x > 0` no longer keeps nodes
  whose `x` is NaN. `ORDER BY` still sorts NaN after every number.
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
- **Storage errors fail the query.** A failure to read or write while a query
  runs fails it with a message naming what was being read or written. Before, a
  property read, `labels()`, `type()`, `properties()`, a relationship property,
  a vector for `<=>` or a `@@` filter read as null or false, and a `WHERE`
  quietly dropped the row. Null now means only a missing property, a deleted
  entity or a property name never used. Through the C API, a storage failure
  under a query returns `LATTICE_ERROR_IO` and running out of memory
  `LATTICE_ERROR_OUT_OF_MEMORY`, where both were `LATTICE_ERROR`; the bindings'
  query errors carry the code.
- **A relationship `CREATE` to a missing node fails with `LATTICE_ERROR_NOT_FOUND`**
  (-4), the code `lattice_edge_create` returns, and names both nodes: "Cannot
  complete creating a relationship from node 2 to node 3: a node or relationship
  it needs does not exist, or was deleted earlier in the query". It was
  `LATTICE_ERROR` with "Storage error while creating a relationship: NotFound".
- **Strings are counted in characters.** `size('Müller')` is 6, not 7.
  `substring` takes offsets in characters and no longer cuts one in half, which
  produced invalid UTF-8 that a binding then failed to decode. `toLower` and
  `toUpper` map Latin, Greek, Cyrillic, Armenian and fullwidth letters, not ASCII
  only. `split(s, '')` splits into characters; it crashed the process. A null
  argument to `substring`, `replace` or `split` gives null, where a wrong type in
  another argument made it a type error.
- **Integers and floats compare exactly everywhere.**
  `9007199254740993 = 9007199254740992.0` is false and `>` is true, as `ORDER BY`
  already had it. `DISTINCT` and grouping treat `1` and `1.0` as one value, and
  `0` and `-0.0` as one. `MERGE (a)-[:R {w: 1}]->(b)` finds a relationship whose
  `w` is `1.0` instead of creating a second.
- **A `MATCH` never reuses a variable-length path's relationships.**
  `MATCH (a)-[:R*1..2]->(b)-[:R]->(c)` over a two-relationship cycle returned 4
  rows that walked one relationship twice, and now returns 2.
- **Function names are not case-sensitive** (`TOLOWER` works), and
  `toString(null)` is null, where it was the string `'null'`.
- **`CREATE` cannot add labels or properties to a bound variable.**
  `MATCH (n) CREATE (n:Extra {p: 1})` silently dropped them; it is now rejected.
- **Two `RETURN` columns cannot share a name.** `RETURN n.v, n.v` returned two
  columns called `n.v`, which no caller could tell apart by name; it now fails
  with "Column `n.v` is returned twice; give one of them another name with AS".
  `WITH` already refused a name twice.
- **An integer cannot start with 0.** `012` was read as the decimal 12, where
  openCypher 9 reads it as the octal 10; it now fails to parse with "An integer
  cannot start with 0, as in 012: write 12 for the decimal number, or 0o12 for
  the octal one". Octal integers are written `0o12`. `0`, floats such as `01.5`
  and hexadecimal `0x0F` are unchanged.
- **A query without `RETURN` returns no columns and no rows.** A bare `CREATE`,
  `MERGE`, `SET`, `DELETE` or `FOREACH` returned the engine's working slots as
  columns named after its variables or `col0`, `col1`, ..., with a row for each
  row it wrote: `CREATE (a)-[:R]->(b)` returned `a | b`, `UNWIND [1, 2] AS x
  MERGE (:M {x: x})` returned `x | col1`, and a `FOREACH` after an aggregating
  `WITH` returned a column of nulls. Add a `RETURN` to get values back. The CLI
  prints "Query executed" in a table, `{"columns":[],"rows":[],"count":0}` in
  JSON and nothing in CSV.
- **`MATCH (n)` returns nodes in id order.** It returned them in the order of
  the node tree's little-endian keys (1, 257, 513, ...). A pattern that starts
  from a relationship type, `MATCH (a)-[r:KNOWS]->(b)`, now returns its rows in
  the edge-ID index's order rather than grouped by `a`. Neither order was
  promised; add `ORDER BY` where order matters.

### Storage, API and formats

- **Creating a property index makes the file unreadable by 0.16.0.** Indexes now
  keep values in an order-preserving encoding that 0.16.0 would misread, so
  declaring one raises the header's minimum reader version to 4 and 0.16.0
  refuses the file (with an I/O error). A file without one stays readable by
  0.16.0, and indexes created by 0.16.0 keep working for equality; see
  *Upgrade notes*.
- **Deleting a node that has relationships is refused in every API.**
  `lattice_node_delete`, and so `delete_node` in Python, `deleteNode` in
  TypeScript and Java and `DeleteNode` in Go, removed the node and left its
  relationships in place, pointing at nothing and still returned from their other
  end. It now returns `LATTICE_ERROR_HAS_RELATIONSHIPS` (-17) and deletes nothing.
  `lattice_node_detach_delete` deletes the node with its relationships:
  `delete_node(id, detach=True)` in Python, `deleteNode(id, { detach: true })` in
  TypeScript, `Tx.DetachDeleteNode` in Go, `Transaction.detachDeleteNode` in Java.
  Deleting a node that does not exist is still not an error.
- **An edge needs both of its ends.** `lattice_edge_create` returns
  `LATTICE_ERROR_NOT_FOUND` (-4) unless both nodes exist as the transaction sees
  them; it created edges to nodes that did not exist.
- **A database opens with the vector index it has.** The file records the index's
  dimensions, metric and build settings, and a database that has an index opens
  with it whatever the options say; `enable_vector` now means "create an index if
  there is none". Options that contradict the file are refused with the new
  `LATTICE_ERROR_CONFIG_MISMATCH` (-18), where wrong dimensions were refused as
  an invalid argument, or misread the pages. Python and TypeScript accept
  `vector_dimensions=0` (`vectorDimensions: 0`) for "the file's"; Go and Java
  cannot express that, so leave vectors off when reopening there.
- **`<=>` and vector search use the database's metric.** New indexes can use
  Euclidean (L2) distance or the negated dot product as well as cosine, and other
  `m` and `ef_construction`: `vector_metric`, `vector_m` and
  `vector_ef_construction` in every binding and in `lattice_open_options_v5`, and
  `lattice create --vector-metric=cosine|euclidean|dot --vector-m=<n>
  --vector-ef-construction=<n>`. They are fixed once the index exists; export and
  import to change them. Under Euclidean and dot product vectors are stored as
  given; cosine still normalises them.
- **A node keeps its vector when its last label is removed.** Removing the only
  label a node had also deleted its vector, in a transaction and outside one,
  and adding a label back did not restore it. Nothing about having no labels
  puts a vector out of reach: a node created without labels can hold one, vector
  search returns it and `MATCH (n) WHERE n.embedding <=> ...` finds it. A vector
  now goes only when it is cleared or its node is deleted. If you relied on a
  label removal to take a node out of vector search, clear its vector or delete
  the node instead.
- **A node's id is never handed out again.** Every open found the next node id
  by scanning for the highest one, so deleting the newest node and reopening gave
  its id to the next node created. The counter is now stored in the file. Ids
  handed out by work that rolled back, or that had not committed before a crash,
  can still be reused.
- **An explicit sync setting wins over the environment.** `LATTICE_SYNC_MODE` and
  `LATTICE_FULL_FSYNC` overrode whatever a program set; they now apply only when
  the program leaves the option unset.
- **A log that cannot be replayed refuses to open** (see *Replay is exact*).
  Store failures during replay are `IoError` and running out of memory
  `OutOfMemory`; every recovery failure used to be `IoError`.
- **New error codes.** `LATTICE_ERROR_HAS_RELATIONSHIPS` (-17),
  `LATTICE_ERROR_CONFIG_MISMATCH` (-18) and `LATTICE_ERROR_NEEDS_NEWER_WRITER`
  (-19, a writable open of a file whose header asks for a newer writer; a
  read-only open works). Python raises `LatticeHasRelationshipsError`,
  `LatticeConfigMismatchError` and `LatticeNeedsNewerWriterError`; TypeScript
  uses `LatticeErrorCode.HasRelationships`, `ConfigMismatch` and
  `NeedsNewerWriter`; Go `ErrorHasRelationships`, `ErrorConfigMismatch` and
  `ErrorNeedsNewerWriter`; Java `HAS_RELATIONSHIPS`, `CONFIG_MISMATCH` and
  `NEEDS_NEWER_WRITER`.
- **A commit that fails with `LATTICE_ERROR_TXN_ABORTED` ends the transaction**
  in Python, TypeScript, Go and Java, and rolling it back afterwards does
  nothing. Leaving a `with` block, `Database.write` or try-with-resources raised
  the error a second time, and Go's `IsActive` stayed true.
- **The Go binding needs a library with `lattice_open_v5`**, since it links the
  library rather than looking for the function. Python and TypeScript fall back
  to the older open functions, and raise their Unsupported error if one of the
  new options is set against an older library.

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
- **Full-text search has one query language**, the same in `@@`, `fts_search`,
  `fts_search_fuzzy`, the C API and on uncommitted text: words side by side are
  all required, `a OR b` either (in capitals), `-word` and `-"a phrase"`
  exclude, `"a phrase"` needs the words adjacent and in order, `word*` is a
  prefix, `word~` and `word~1` are within two or one edits, and a backslash
  makes the next character literal. Before, `OR` was a stop word, so `cats OR
  dogs` asked for both words, and quotes, `-`, `*` and `~` were punctuation, so
  `fox -quick` asked for both. A query that only excludes (`-dog`) is an error
  rather than a search; in Cypher write `NOT d.text @@ 'dog'`. `"text" @@
  "query"` on a plain string uses the same language with whole words, where it
  was a substring test of each word (`'databases' @@ 'data'` was true; it is
  false, and `'data*'` is true).
- **Full-text indexes store word positions**, for phrases, from their first
  document; a phrase against an index 0.16.0 built, which has none, is an
  error (`LATTICE_ERROR_INVALID_ARG`) instead of a search for the words in any
  order. Drop and declare the index again to store them. Positions take about
  1% more space. A file holding such an index cannot be opened by 0.16.0 (see
  *Upgrade notes*).
- **A fuzzy match at the full edit distance scores above zero.** A word two
  edits away under `word~` scored 0, ranking with documents that did not match
  that word at all. Fuzzy scores keep 89% at one edit and 56% at two (they kept
  75% and 0%), so fuzzy scores and rankings change.
- **Vectors with NaN or infinite components are rejected** with
  `INVALID_ARG`, when stored and when used as a query.
- **`set_vector` refuses keys other than `"embedding"`**, `""` and `NULL`, with
  `UNSUPPORTED`. A node holds one vector, and another key silently replaced it.

## Performance

**Property indexes answer ranges, prefixes and sorting.** `<`, `<=`, `>`, `>=`
(several on one property joined by `AND` become one range) and `STARTS WITH` on
an indexed property read only the matching entries, and `ORDER BY n.p [SKIP s]
LIMIT k` on an indexed property, ascending or descending, over one pattern,
reads the index in order and stops after the first `s + k` rows, with the same
rows, ties and nulls included, as the full sort. A label's index holds the
nodes without the property too, which costs a node created without it about
0.4 µs per such index. 200,000 `:Person` nodes, release
build, median of seven runs on a busy machine (12 November 2026), the same
release without and with the index:

| query | label scan | index |
|---|---:|---:|
| `WHERE n.age > 97`, 4,000 rows | 65 ms | 1.7 ms |
| `WHERE n.score >= 500 AND n.score < 505`, 978 rows | 76 ms | 0.62 ms |
| `WHERE n.name STARTS WITH 'Jo'`, 293 rows | 66 ms | 0.11 ms |
| `ORDER BY n.score LIMIT 10` | 64 ms | 0.010 ms |
| `ORDER BY n.score DESC LIMIT 10` | 58 ms | 0.016 ms |
| `WHERE n.age >= 50 RETURN count(n)`, 100,066 nodes | 64 ms | 3.7 ms |
| `WHERE n.age >= 0`, every node | 146 ms | 120 ms |

Without statistics, the planner always reads a range from the index; the last
row shows that even a range that holds every node is no slower.

**Relationship property indexes answer Cypher.** They were used only by the
`find_edges_by_type_property` calls. A pattern that starts from one
relationship type, `MATCH (a)-[r:KNOWS]->(b)`, now reads the type's index for
`=`, ranges and `STARTS WITH` on an indexed property, in the `WHERE` or the
inline map. A relationship read through the index costs about four times a
step of the walk over every relationship, so a range holding a fifth of them or
more walks them instead. 1,000,000 `:KNOWS` relationships, `w` spread over
0–9,999, release build:

| query | walk | index |
|---|---:|---:|
| `WHERE r.w = 1234`, 115 rows | 173 ms | 0.09 ms |
| `WHERE r.w > 9990`, 944 rows | 174 ms | 0.75 ms |
| `WHERE r.w >= 100 AND r.w < 200`, about 10,000 rows | 221 ms | 8.5 ms |
| `WHERE r.name STARTS WITH 'Jo'`, 1,447 rows | 180 ms | 1.2 ms |
| `WHERE r.w >= 5000`, half of them | 191 ms | 215 ms |

<!-- Measured on 11 November 2026 on an Apple M5 in ordinary use, not a quiet
     machine: v0.16.0 and this release built ReleaseFast and run alternately,
     medians of three runs for the first table and single runs for the second.
     Re-measure on a quiet machine before quoting these elsewhere. -->

The C API used Zig's debug allocator in every build, with one global lock per
allocation. Release builds now use `smp_allocator`. Label scans read the label
index instead of decoding every node, and changefeed records no longer make a
large commit quadratic. A read-only transaction no longer writes to the log at
all; it used to write two records and an fsync. Through the C API, release
build, 20,000 nodes:

| | 0.16.0 | this release |
|---|---:|---:|
| read one property | 10.2 µs | 0.26 µs |
| read-only transaction, no work | 43.7 µs | 0.08 µs |
| write transaction: one node, one property, commit | 96 µs | 54 µs |
| commit 20,000 nodes in one transaction | 10.7 s | 0.23 s |
| `UNWIND [1] AS x RETURN x` | 72 µs | 0.70 µs |
| `MATCH (n:Rare) RETURN n.name LIMIT 1`, 10 of 22,000 nodes | 208 ms | 0.9 µs |
| `MATCH (n:Person) WHERE n.age > 60 RETURN n.name`, 3,600 rows | 560 ms | 5.5 ms |

The `LIMIT 1` figure is the steady state; the first call after a burst of
writes took between 40 µs and 2 ms.

Cypher queries read one property at a time instead of copying the whole node,
evaluate `ORDER BY` keys once per row and keep only the top `k` under a
`LIMIT`, stream label scans in read-only queries so a `LIMIT` stops them early,
and parse a prepared query once instead of once for `lattice_query_writes` and
again to run it. Same setup:

| query | 0.16.0 | this release |
|---|---:|---:|
| return one property of all 20,000 nodes | 654 ms | 6.4 ms |
| return four properties of all 20,000 nodes | 1.88 s | 11.5 ms |
| `WHERE n.age = 33`, 400 rows | 613 ms | 4.3 ms |
| `ORDER BY n.age DESC, n.id LIMIT 10` | 19.7 s | 4.3 ms |
| `ORDER BY n.name`, all 20,000 rows | 10.5 s | 15.6 ms |
| `count` and `avg` over 20,000 nodes | 1.18 s | 4.5 ms |
| one-hop expand, 2,000 rows | 1.19 s | 10.7 ms |
| `*1..3` from one node | 646 ms | 4.0 ms |

One lookup is slower than it would otherwise be: an indexed integer equality
also probes the equal float, about 0.3 µs more.

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
- Opening a database reads the node id counter from the file instead of
  scanning every node: 200,000 nodes, 2.5–2.8 ms to 0.5 ms.
- A process that had just built a large database read it back many times more
  slowly than after reopening it, because the buffer pool's page table filled
  with tombstones. In the process that built a million-node database: a full
  scan 28.9 µs to about 1 µs a node, a random node read 51.1 µs to about 1.7 µs,
  declaring a property index 16.7 s to 2.2–3.1 s, and the build itself 52.7 s to
  about 21 s.
- Expanding a node inside a write transaction that holds 20,000 new
  relationships looks only at that node's pending ones: 265 µs to 4.9 µs a hop.
- `r.prop`, `type(r)` and `labels(n)` decode only what they need: `RETURN r.w,
  r.note` over 20,000 relationships, 15.3 ms to 10.2 ms.
- A `WHERE` that is the same `@@` search the `MATCH` already ran no longer runs
  it again: 366 µs to 235 µs at 663 hits of 2,000 documents.
- `@@` with `ORDER BY score(d) DESC LIMIT k` (or a `LIMIT` alone) keeps only the
  best `SKIP + LIMIT` matches instead of sorting every one: 18.8 ms to 6.2 ms
  for `LIMIT 10` over 50,000 documents that all match.
- Adding a document to a full-text index appends to each posting list at its
  tail instead of rewriting it: 25.6 µs to 10.6 µs a document at 100,000
  documents that share a word. Removing one closes its gap in place, and pages
  and dictionary terms nothing uses any more are given back: deleting 50,000
  documents took 3.5 s and now 1.2 s, and an index filled, emptied and filled
  again with new words stays at about 26,500 pages, where it grew to 66,900.
- `MATCH (n:Label) WHERE n.embedding <=> $q ... LIMIT k` searches the HNSW graph
  with the label index as the filter, reading only as much of the label as it
  needs. At 100,000 128-dimensional vectors and `LIMIT 10`: a label on half the
  nodes 2.3 ms to 0.5 ms, on 1% 4.3–5.4 ms to 53–71 µs, on 0.1% 51 ms to
  9–21 µs; recall against the exact nearest of the label was 97.6–99.9% and is
  99.8–100%.
- `ORDER BY d.embedding <=> $q LIMIT 10` keeps the vector search bounded, as
  `LIMIT` alone did: 598 µs to 227 µs at 2,000 vectors.
- A B+Tree leaf that fills while keys arrive in order splits next to the new
  key instead of in the middle, so trees written in key order (the changefeed,
  the label index, ids) fill their pages instead of leaving them half empty.
  200,000 in-order inserts went from about 440 to about 290 ns each; random
  inserts are unchanged within measurement noise (about 575 ns).
- Changing a relationship's property rewrites only its record, not its two
  traversal keys as well: 6.6 µs to 3.6 µs an update (20,000 relationships, one
  transaction, commit included). Changing one property no longer copies the
  whole node or relationship twice first (50 properties: about 9.4 µs to 7.9 µs).
- A pattern that starts from a relationship type with an unlabelled start node,
  `MATCH (a)-[:KNOWS]->(b)`, walks the relationships once instead of expanding
  every node: a count over 1,000,000 relationships 66 ms to 20 ms, two types
  128 ms to 38 ms.
- `MATCH (n)` no longer builds a hash set of every node id: `LIMIT 5` over
  200,000 nodes 9–11 ms to 5 ms.
- An index lookup inside a transaction finds the transaction's own staged nodes
  by value instead of checking every one: with 100,000 staged, 857 µs to about
  1 µs a lookup after a first lookup of 15 ms. Index declarations are kept in
  memory rather than read from the catalog on each write.

Two checks cost a little. Creating an edge checks that both ends exist, two key
probes: 100,000 edge creates in one transaction take 21–26 ms instead of 6–7 ms,
which is within the noise once the commit is counted. Refusing to delete a node
with relationships is one range probe, under 1 µs.

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
- `score(d)` returns the BM25 score of the `@@` search that matched `d`, the best
  term's for an `OR`. It is null when no single search scored `d`.
- Writing a search's distance in the `RETURN` (`d.embedding <=> $q AS dist`)
  returns the distance the search measured instead of computing it again, and a
  distance no longer outlives its node when a later clause reuses its slot or
  `WITH` renames it.
- `ORDER BY` on the search's own `<=>`, or on the alias of a returned one, keeps
  `LIMIT` and `SKIP` bounding the vector search, as `LIMIT` alone did. The
  answer is HNSW's approximate top k; before, every match under the threshold was
  sorted.
- `ORDER BY` after an aggregating `RETURN` takes expressions over the returned
  columns (`RETURN p, count(f) AS c ORDER BY p.name, c * -1`), and an alias
  works inside an expression (`RETURN n.name AS nm ORDER BY toUpper(nm)`, which
  failed with "Variable `nm` is not defined here"). Ordering by what the `RETURN`
  does not keep fails before the query runs and says which item.
- A `COUNT { }`, `EXISTS { }`, `COLLECT { }` or pattern comprehension beside an
  aggregate reads the grouping keys; one that reads a variable that is not a key
  fails and names it.
- An aggregate in `WHERE`, `SET`, `UNWIND`, `FOREACH`, `MATCH` properties, `SKIP`
  or `LIMIT` fails before the query runs with "collect() aggregates rows, which
  FOREACH cannot do; aggregate in a WITH first". It failed as an unknown function
  when a row reached it, or not at all over no rows. `count(1, 2)` and `sum()`
  fail before the query runs too.
- A pattern used as a number, as in `(n)-->() - 1`, fails with a message that
  points to `COUNT { }`, where it said "Operator not valid for these types".
- `CALL db.labels() YIELD * RETURN *` fails with "YIELD * is only allowed when
  CALL db.labels() is the whole query", where it said no variables were in scope.
- An aggregate over distinct values is named for it: `count(DISTINCT x)`, where
  the column was `count(x)` and could not be told from `count(x)` beside it.
- A `DELETE` refused because the node still has relationships gives the line
  and column of the variable in the `DELETE`, as other execution errors do; it
  gave none.

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
- Uncommitted text in a transaction is scored with BM25 and the index's
  statistics. It scored the number of query words it held, so it ranked above
  committed documents until the commit, then moved.
- An `OR` of `@@` searches beside another condition
  (`d.year >= 2020 AND (d.title @@ 'x' OR d.body @@ 'x')`) scores what it
  matches; `score(d)` was null unless a property index had answered the other
  condition.
- A search a vector search ran over kept its score when the score was exactly
  0; `score(d)` came back null.
- An index refusing a query says why ("Full-text query '-dog' only excludes;
  it needs a word to match"), where it said only that an argument was not
  valid.
- A full-text index whose declaration failed to reach the disk is no longer
  reported as existing.
- `score(d)` survives a join: with two patterns in one `MATCH`
  (`MATCH (p:Person), (d:Doc) WHERE d.text @@ 'x'`) it was null.
- A `LIMIT` under `ORDER BY score(d) DESC` bounds the search under a named path
  (`MATCH p = (d:Doc) WHERE ...`) too.

**Graph storage**

- A relationship record holding a value tag no release writes was read as a
  null property, and the rest of the record from the wrong offset; it is
  refused as invalid data, as a node record already was.
- `enable_adjacency_cache` (which only the in-process traversal benchmarks
  read; queries and the bindings never did) was cleared when a transaction
  staged an edge but not when the commit applied it, so a read during the
  transaction left it stale for good. The commit now clears it. The bindings'
  documentation no longer says the option speeds anything up.

**Vector search**

- A `WHERE` of only `n.embedding <=> $q` after `WITH` (and in the new
  `OPTIONAL MATCH`, subqueries and pattern comprehensions, where a `WHERE` is a
  row filter) is refused with a message asking for a bound. It read the
  distance as a condition: every row passed except an exact match, in no order.
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
- The search operator measured vectors not yet in the index with cosine on
  unnormalised vectors; it uses the database's metric.

**CLI**

- Every command opens a database's vector index as it was created, without
  vector options, after a crash included. `--enable-vector --vector-dims` add an
  index to a database that has none; a `--vector-dims` that contradicts the file
  is refused with a message saying to leave it out. `create` takes
  `--vector-metric`, `--vector-m` and `--vector-ef-construction`.
- A failed query prints its stage, code, message, line and column, and the query
  with a caret under the place.
- The read-only commands (`info`, `count`, `labels`, `types`, `schema`, `export`,
  `dump`) recover a crashed database in memory and show every committed
  transaction. `check` still reads pages as the crash left them.
- A file that needs a newer writer is refused by the commands that write, with a
  message naming the commands that still read it.
- A CSV file that cannot be read as CSV, such as one with an unclosed quote,
  names the line and the reason; it said "Import failed: ParseError". It says to
  fix the file, since `--on-error=skip` cannot read past such a fault and is no
  help once given.
- The result table prints floats of 1e16 and above, or below 1e-6, in size in
  the shortest scientific form (`1e300`, `1e-300`), where `1e300` took 301
  digits and `1e-300` printed as `0.000000`.
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
- The result table no longer fails on the smallest integer, on floats wider
  than ten characters, or on large node ids.
- A query's CSV quotes lists and maps as RFC 4180 says: the quotes of the JSON
  inside are doubled, where `{"k":"v"}` was written between quotes as it was and
  ended the field at its first quote. Column names are quoted when they hold a
  comma or a quote; a backticked `` `a,b` `` made two columns of the header. The
  JSON output escapes column names and map keys, and control characters in
  strings, which it wrote as they were.

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
- `lattice_open_v5` and `lattice_open_options_v5` add `sync_mode`, `full_fsync`,
  `vector_metric`, `vector_m`, `vector_ef_construction`, `changefeed_retention`
  and `changefeed_retention_value`; 0 means "not set",
  and an out-of-range value is `LATTICE_ERROR_INVALID_ARG`. The older open
  functions behave as v5 with those fields at 0. `lattice_deserialize` still
  takes the v4 options.
- A second fetch of a page another thread was still reading from disk could see
  the frame's previous contents, another page's or zeroes; it now waits for the
  read.
- The embedding client applies its timeout (`timeout_ms`, 30 s by default) to the
  wait for the response to begin; it was stored and never used, so a stalled
  server kept a request waiting indefinitely. Connecting and reading the rest of
  a response are not bounded, and Windows is unchanged.

## Known limits

- **The automatic checkpoint runs inside the commit that triggers it.** About
  every thousandth small commit takes 5 to 12 ms, up to 27 ms, against about
  50 µs for the others (C API, macOS, a busy machine). There is no background
  checkpointer; from Zig, turn the automatic checkpoint off and run checkpoints
  at moments you choose.
- **Damage in the newest sync batch reads as a torn tail.** Only a frame written
  after a later sync can vouch that a frame was synced, so damage to the last
  frames before the most recent sync, with nothing written after them, is cut
  off, and the commits in them are lost without an error.
- **A page that is entirely zero still passes its checksum**, since nothing
  tells it apart from a page allocated and never written; a pointer to such a
  page reads it as an empty free page.
- **A read-only recovery is bounded by memory**, 64 MB plus four times the size
  of the log and journal; beyond that the read-only open fails with
  `LATTICE_ERROR_FULL`. `lattice check` does not recover first.
- **Indexes created by 0.16.0 answer equality only** until rebuilt with
  `lattice compact --upgrade-indexes` (or `Database.upgradePropertyIndexes`
  from Zig, or dropped and created again).
- **There is no index from relationship type to relationship**; a pattern that
  starts from a type walks every relationship once.
- **The vector metric, `m` and `ef_construction` cannot be changed** once the
  index exists; export and import into a new database to change them.
- **`lattice replicate` takes a snapshot after any process that wrote has
  closed the database.** Closing now folds the log into the file and resets it,
  which begins a new replication generation. That was already so for the
  language bindings; it is new for the CLI and for Zig callers, whose writes
  0.16.0 left in the log to be shipped as frames. A pass with nothing changed
  still ships nothing, and a process that stays open and calls `replicateTo`
  on its own handle ships frames as before.
- **Files that need a newer writer.** `exec`, `query`, `import`, `backup`,
  `compact` and `checkpoint` open the database for writing, so the CLI cannot run
  them, not even a read-only query through `exec`, on a file whose header asks
  for a newer writer. 0.16.0 does not know that gate and writes such a file
  anyway.
- **Some Cypher is not implemented**, each part with an error that names it:
  quantified path patterns (`((a)-->(b)){1,3}`, `(x) ((a)-->(b))+`) and
  relationships (`-[:R]->{1,3}`), `&`, `!` and `%` in a
  relationship pattern's types (`-[:!KNOWS]->`), `WHERE` inside a pattern,
  writing inside `EXISTS { }`, `COUNT { }` or `COLLECT { }`, `CALL { } IN
  TRANSACTIONS`, `OPTIONAL CALL`, procedures other than the four catalog ones,
  `LOAD CSV`, type predicates (`IS :: INTEGER`), `||`, temporal and spatial
  types, and schema and administration statements (`CREATE INDEX`, `SHOW`,
  `EXPLAIN`).
- **Plan errors have no line and column.** Their message names the part of the
  query instead (`ORDER BY item 2`, ``RETURN uses `p` in COUNT { ... }``).
  Parse, semantic and execution errors carry both.
- **A label alternation reads every node.** `(n:A|B)`, `(n:!A)` and `(n:%)` are
  checked on each node; `(n:A&B)` and `(n:A:B)` still start from a label scan.
  A property map on a variable-length relationship (`-[:R*1..3 {k: 1}]->`)
  filters the paths after the walk and does not shorten it.
- **`shortestPath` takes one relationship pattern** with a lower bound of 0 or 1
  and no property map, only in `MATCH`, and a `WHERE` that reads the path is
  refused, since the search would not look further for a longer path that passes.
- **Case mapping is one to one.** `toUpper` and `toLower` use the simple
  Unicode mappings: `ß` stays `ß`, a final `Σ` lowercases to `σ`, and there is
  no language-specific casing such as Turkish. `trim` removes ASCII whitespace
  only.
- **Search distances and scores do not survive an aggregation.**
- **A full-text search scores every match**, also under `LIMIT`; the limit only
  saves the sort. Skipping documents that cannot reach the top `k` needs posting
  lists ordered by document, a format change. Full-text queries have no
  parentheses.

## Upgrade notes

- **The file format is version 4.** The header gains fields past the 200 bytes
  0.16.0 uses, behind a magic number: the node id counter, the vector index's
  dimensions, metric and HNSW settings, and a minimum writer version. Data pages
  are laid out as before. Full-text indexes record how they analyse text when
  their first document goes in, under a new key.
- **Opening a 0.16.0 file with this release.** The first read-write open
  upgrades the header to format 4: it finds the node id counter with one scan,
  reads the vector dimensions from the vector pages (0.16.0 could only build
  cosine indexes with `m` 16 and `ef_construction` 200) and records both. A
  read-only open changes nothing. If 0.16.0 had itself deleted the highest node,
  that id is handed out once more, as 0.16.0 would have done; from then on no
  deleted id is reused. A 0.16.0 vector database opens with no vector options;
  opened with different dimensions it is refused with -18.
- **Property indexes.** Indexes 0.16.0 created keep their encoding and answer
  equality as before; they are not used for ranges, prefixes or sorting until
  rebuilt. `lattice info` lists every index with its encoding (`digest` is
  0.16.0's), as does the `encoding` column of `CALL db.indexes()`, and
  `lattice compact --upgrade-indexes` rebuilds the old ones in place. Creating
  or rebuilding an index with this release raises the header's minimum reader
  version to 4, so 0.16.0 refuses that file from then on, also after the index
  is dropped. Files without one are unaffected.
- **Opening a file this release wrote with 0.16.0.** 0.16.0 opens and writes a
  file whose vector index, if any, is cosine with `m` 16, whatever its
  `ef_construction`. It writes zeros over the format-4 fields, and this release
  derives them again at its next open, as for a 0.16.0 file. 0.16.0 refuses a
  file whose index is Euclidean or dot product or has another `m`, reporting an
  I/O error, and leaves the file untouched. It does not know the minimum writer
  version: it writes a file that asks for a newer writer, and zeros the field.
- **The log.** A log that holds frames is format version 3, which 0.16.0 refuses
  to open. A clean close or a truncating checkpoint leaves an empty log, written
  as version 2, which 0.16.0 opens. So close a database cleanly with this
  release before opening it with 0.16.0; after a crash, open it once with this
  release first. 0.16.0 does not know about the journal either, so after a crash
  it would ignore an unfinished flush. This release opens and replays logs
  0.16.0 wrote.
- Code that deletes nodes through `lattice_node_delete` or a binding's delete
  now gets -17 for a node with relationships, and must delete them first or use
  the detach variant.
- A program that relied on `LATTICE_SYNC_MODE` or `LATTICE_FULL_FSYNC` to
  override what it set in code now gets what it set.
- **Changefeed events written by 0.16.0 stay in the old format** until the
  retention policy drops them; nothing rewrites them, and `lattice compact`
  does not convert them. They are read alongside new events, in order.
  `lattice changefeed <db>` counts them. To shed them while keeping the graph,
  export it and import it into a new database.
- Check application code against the Cypher changes above, in particular plain
  `DELETE` of nodes that may have relationships, code that reads results by
  the name of an unaliased column, code that expects `sum` of integers to be a
  float, and `MERGE` patterns without a label or with several labels.
- **Full-text indexes and 0.16.0.** Once a full-text index this release fills
  holds a document, it stores word positions, and 0.16.0 would damage it by
  appending entries without them; the file then asks for this release or later,
  and 0.16.0 refuses to open it with an I/O error. A file whose full-text
  indexes 0.16.0 built, or that this release has only declared, still opens in
  0.16.0, which splits non-ASCII words the old way and will not find words with
  non-ASCII letters indexed by this release.

## Upgrading

```bash
pip install --upgrade latticedb
npm install @hajewski/latticedb@<version>
go get github.com/jeffhajewski/latticedb/bindings/go@v<version>
```

The Java binding (`io.latticedb:latticedb`) is not published to a Maven repository; build it from `bindings/java` (see its README).
