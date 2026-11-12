# The Changefeed

Every write transaction that changes the graph leaves a record of what it
changed: a node created, a label added, a property set from one value to
another, an edge removed. Those records are the **changefeed**, a stream named
`__lattice_changes` that lives inside the database file. This page covers what
the changefeed holds, what it is for, what it costs, how to decide how much of
it to keep, how to configure that in each language, and the format it is stored
in.

## What it does

When a transaction commits, LatticeDB compares every node and edge the
transaction touched with how it was before, and writes one event per
elementary change:

| Event kind | Written when | Payload keys |
|---|---|---|
| `node.insert` / `node.delete` | a node is created or deleted | `entity`, `op`, `node_id` |
| `node.label_add` / `node.label_remove` | a label is added or removed | ... `label` |
| `node.property_set` / `node.property_remove` | a property gets a value or loses one | ... `key`, `old_value`, `new_value` |
| `edge.insert` / `edge.delete` | an edge is created or deleted | `entity`, `op`, `edge_id`, `source_id`, `target_id`, `type` |
| `edge.property_set` / `edge.property_remove` | an edge property changes | ... `key`, `old_value`, `new_value` |

The events are written in the same transaction as the change, so they appear
exactly when it commits and never when it rolls back, and a crash can never
leave a change without its event or an event without its change. Each event has
a **sequence number**: they start at 1, rise by one per event, a transaction's
events are consecutive, and a number is never reused, not even after old events
are dropped.

A property change records both the old and the new value. A value larger than
1 MB is replaced in the event by a summary map
(`{__lattice_value_omitted: true, type, encoded_bytes}`), so one huge value
does not double the cost of the write that stored it.

Vectors are not part of the changefeed: setting or replacing a node's vector
writes no event.

## Why it is there

LatticeDB is built to sit underneath tools that edit structured knowledge:
ontology editors, knowledge-graph curation tools, applications whose users
change a model and expect to see what changed and take it back. Those tools
need the history of the graph, and the history is hard to reconstruct after
the fact. The changefeed records it at the one place that sees every change,
in the same transaction as the change.

What it is used for:

- **Audit.** Who changed this class's name, and from what? The event carries
  the old value and the new one, and the transaction's commit time.
- **Undo.** Because each property event keeps the old value, reversing a
  change is writing that value back.
- **Keeping something else in step.** A search index, a cache, a second
  database, a UI that shows live edits: read the events after the last one you
  handled, apply them, remember how far you got.
- **Reviewing a session.** Everything that happened between two points is a
  range of sequence numbers.

Reading the changefeed and undoing the last change, in Python:

```python
import latticedb

with latticedb.Database("ontology.lattice", create=True) as db:
    db.query("CREATE (:Class {name: 'Person'})")
    db.query("MATCH (c:Class {name: 'Person'}) SET c.name = 'Human'")

    for change in db.changes(after_sequence=0, limit=100):
        print(change.sequence, change.kind, change.payload)

    # Undo: put the old value of the last property change back.
    last = db.changes(after_sequence=0, limit=100)[-1]
    if last.kind == "node.property_set":
        p = last.payload
        with db.write() as txn:
            txn.set_property(p["node_id"], p["key"], p["old_value"])
            txn.commit()
```

```text
1 node.insert {'entity': 'node', 'op': 'insert', 'node_id': 1}
2 node.label_add {'entity': 'node', 'op': 'label_add', 'node_id': 1, 'label': 'Class'}
3 node.property_set {'entity': 'node', 'op': 'property_set', 'node_id': 1, 'key': 'name', 'old_value': None, 'new_value': 'Person'}
4 node.property_set {'entity': 'node', 'op': 'property_set', 'node_id': 1, 'key': 'name', 'old_value': 'Person', 'new_value': 'Human'}
```

The undo is itself a change, so it appears as event 5.

A consumer that keeps something in step, in TypeScript. It stores its position
as a consumer offset, in the same transaction as any other bookkeeping, so a
restart picks up where it stopped:

```typescript
import { Database, LatticeChangesTrimmedError } from '@hajewski/latticedb';

const db = new Database('ontology.lattice', { create: true });
await db.open();

let after = (await db.getStreamOffset('__lattice_changes', 'indexer')) ?? 0n;
try {
  const changes = await db.changes({ afterSequence: after, limit: 100, timeoutMs: 1000 });
  for (const change of changes) {
    // ... update the search index from change.kind and change.payload ...
    after = change.sequence;
  }
} catch (err) {
  if (err instanceof LatticeChangesTrimmedError) {
    // Events after `after` were dropped by retention: rebuild from the graph,
    // then continue from the oldest event still held (afterSequence 0n).
  } else {
    throw err;
  }
}
await db.write(async (txn) => {
  txn.setStreamOffset('__lattice_changes', 'indexer', after);
});
await db.close();
```

`changes()` with `timeoutMs` waits for the next commit in the same process when
there is nothing new, so a loop around it reacts to writes without polling.

## What it costs

Every change writes an event, so the changefeed grows with the number of
changes ever made, not with the size of the graph. A database that is edited
for years holds years of events. This release stores them in a compact binary
form (described [below](#the-stored-format)); the numbers are from loading
200,000 nodes, each with one label and two properties (four events per node), in
transactions of 1,000 nodes, on an Apple-silicon laptop:

| | 0.16.0 format | this release, `all` | `last:10000` | `off` |
|---|---|---|---|---|
| File size | 256.4 MB | 35.7 MB | 18.1 MB | 16.7 MB |
| Bytes per node | 1,282 | 178.5 | 90.6 | 83.5 |
| Of which changefeed | 238.0 MB (93%) | 19.0 MB (53%) | 0.25 MB | none |
| Bytes per event: key and value | 143 | 17.4 | 17.8 | |
| Bytes per event: on disk, pages included | 297 | 23.8 | 25.0 | |
| Loading the 200,000 nodes | 2.8 s | 1.0 s | 1.5 s | 0.6 s |
| One 20,000-node transaction, per node | 19–38 µs | 4.7–5.4 µs | 8.1–8.6 µs | 2.5–3.1 µs |
| A one-node transaction, commit included | 79–84 µs | 62–70 µs | 62–69 µs | 60–66 µs |
| Reopening after a crash with 999 small transactions in the log | 12–15 ms | 6.3–7.9 ms | 6.4–8.0 ms | 4.9–6.1 ms |

(MB here is 1,000,000 bytes.) So with the default of keeping everything, events
are about half the file and
a large part of the work of a big write; small transactions are dominated by the
commit's sync to disk, and the changefeed adds little to them. A typical event
takes 3 to 5 bytes of key and 4 to 20 bytes of value: a label event is 4 bytes,
a node creation 8 (the first event of a transaction carries its commit time),
a property change 4 plus the size of its two values.

`last:10000` costs time while it trims: the large-transaction row is slower
than `all` because each checkpoint deletes the events that fell out of the
window.

## Retention policies

How much of the changefeed to keep is a **retention policy**, recorded in the
database file. There are four.

### `all` (the default)

Keep every event, forever. Nothing is ever dropped unless you change the
policy. This is the default because history is the raw material for audit and
undo: a tool that has thrown history away cannot get it back, while one that
kept it can always decide to drop it later.

### `last:<N>`

Keep the newest N events; drop older ones. N counts events, not transactions,
so the oldest transaction still held may have lost its first events. The file
stays bounded however long the database lives: N events take roughly
N × 25 bytes on disk.

### `age:<duration>`

Keep the events of transactions committed within the duration; drop older
ones. The duration is a whole number with a unit: `ms`, `s`, `m`, `h` or `d`
(`age:500ms`, `age:90s`, `age:30m`, `age:12h`, `age:7d`). Whole transactions
go together. The age is measured against the system clock at the time of each
commit and of each trim, so a clock set far forward or back keeps events
longer or drops them sooner than intended. Events written by 0.16.0 carry no
commit time; they go only once a later, dated event is old enough to go.

### `off`

Write no events at all, and drop the ones there are. Reading the changefeed
returns an error that says it is off (`LATTICE_ERROR_UNSUPPORTED`).
Sequence numbers do not move while it is off. Turning it back on skips one
sequence number, so that a reader that stopped before the feed went off is
told it missed changes rather than silently reading past them.

Nothing inside LatticeDB reads the changefeed, so `off` breaks nothing but
your own readers: replication ships the log rather than events, backups copy
the files, and your own durable streams are separate and keep working.

## Choosing a policy

| Setting | Policy | Why |
|---|---|---|
| An editor with undo, an audit trail, a curated knowledge graph | `all` | The history is the product. Size grows with edits, not with the graph: at about 25 bytes an event, a million changes take about 25 MB. |
| Undo or review over a recent window only | `age:30d` (or whatever the window is) | Bounded by time, which matches how people think about "recent". |
| A consumer that keeps a search index, cache or other store in step | `last:<N>` with N well above what it can fall behind by, or `age:` longer than it can be down | It needs only what it has not yet handled. If it falls further behind it is told so and can rebuild. |
| No one reads the changefeed: a cache, a derived dataset, a test fixture | `off` | Smallest file and fastest writes. |
| Loading a large dataset into a new database | `off` for the load, then `all` (or your policy) | The load's events describe "everything was created", which the graph itself already says. Switch the policy when the load is done; history starts from there. |
| Embedded on a device with little storage | `last:<N>` or `age:` | A firm bound on the file. |

Some things to weigh:

- **A policy only ever drops.** Changing `off` to `all` does not bring events
  back; changing `last:1000` to `all` keeps what is left of the last 1,000
  and everything after.
- **Dropping is not instant.** Old events go at checkpoints, a bounded number
  each time (4,096 plus the number written since the last one), so a stricter
  policy on a large changefeed takes several checkpoints to take effect and
  never stalls one. Call [trim](#trimming-now) to apply it in full at once.
- **Freed space is reused, not returned.** Pages freed by dropping events are
  reused by later writes; the file does not shrink. `lattice compact` gives back
  only free pages at the end of the file.
- **A reader behind the window is told.** It gets an error, not a gap.

## Configuring it

The policy is written as text in every language and on the command line:
`all`, `off`, `last:<N>` or `age:<duration>`.

### Python

```python
db = latticedb.Database("ontology.lattice", create=True, changefeed_retention="age:30d")
db.open()

status = db.changefeed_status()
# ChangefeedStatus(retention='age:30d', oldest_sequence=1, last_sequence=1204)
removed = db.trim_changefeed()
```

A malformed policy raises `ValueError` before anything is opened. A reader
whose events were dropped gets `LatticeChangesTrimmedError`, a subclass of
`LatticeNotFoundError`.

### TypeScript

```typescript
const db = new Database('ontology.lattice', { create: true, changefeedRetention: 'last:100000' });
await db.open();

const status = await db.changefeedStatus();
// { retention: 'last:100000', oldestSequence: 1n, lastSequence: 1204n }
const removed = await db.trimChangefeed();
```

`changefeedRetention` is typed (`ChangefeedRetention`); a malformed value is a
`TypeError` at open. Dropped events surface as `LatticeChangesTrimmedError`.

### Go

```go
db, err := latticedb.Open("ontology.lattice", latticedb.OpenOptions{
    Create:              true,
    ChangefeedRetention: "age:7d",
})

status, err := db.ChangefeedStatus() // Retention, OldestSequence (0 if none), LastSequence
removed, err := db.TrimChangefeed()

if _, err := db.Changes(after, 100, 0); errors.Is(err, latticedb.ErrChangesTrimmed) {
    // rebuild, then read from 0
}
```

### Java

```java
try (Database db = Database.open("ontology.lattice",
        OpenOptions.defaults().create(true).changefeedRetention("last:100000"))) {
    ChangefeedStatus status = db.changefeedStatus(); // retention(), oldestSequence(), lastSequence()
    long removed = db.trimChangefeed();
}
```

A reader whose events were dropped gets `ChangesTrimmedException` (error code
`NOT_FOUND`).

### C

The policy is two fields of `lattice_open_options_v5`:

```c
lattice_open_options_v5 options = LATTICE_OPEN_OPTIONS_V5_DEFAULT;
options.create = true;
options.changefeed_retention = LATTICE_CHANGEFEED_RETENTION_AGE;
options.changefeed_retention_value = 7ull * 24 * 60 * 60 * 1000; /* ms */

lattice_database* db;
lattice_open_v5("ontology.lattice", &options, &db);

uint8_t mode;
uint64_t value, oldest, last;
lattice_changefeed_status(db, &mode, &value, &oldest, &last);

uint64_t removed;
lattice_changefeed_trim(db, &removed);
```

`changefeed_retention` takes `LATTICE_CHANGEFEED_RETENTION_DEFAULT` (0, keep
the file's policy), `_ALL`, `_LAST` (value: events), `_AGE` (value:
milliseconds) or `_OFF`. `oldest` is 0 when the changefeed holds no events.
`lattice_stream_read` on `__lattice_changes` returns `LATTICE_ERROR_NOT_FOUND`
when the events after `after_sequence` were dropped, and
`LATTICE_ERROR_UNSUPPORTED` when the feed is off.

### Command line

Any command that opens a database for writing takes `--changefeed=<policy>`,
`create` included. `lattice changefeed` shows the policy and what is held, and
sets and trims it:

```text
$ lattice changefeed ontology.lattice
Changefeed of ontology.lattice
  Retention:      all
  Events held:    5
  Bytes:          105
  Sequences:      1..5

$ lattice changefeed ontology.lattice --changefeed=last:2 --trim
Changefeed of ontology.lattice
  Retention:      last:2
  Events held:    2
  Bytes:          68
  Sequences:      4..5
  Trimmed:        3 events
```

`--format=json` and `--format=csv` give the same fields. `lattice info` shows
the policy and range on one line (`Changefeed:   keep last:2, events 4..5`).
Events still in the 0.16.0 format are counted separately
(`Events held: 1200 (800 in the 0.16 format)`).

### Environment

`LATTICE_CHANGEFEED_RETENTION=<policy>` sets the policy of a database **when it
is created** and no option names one. It never changes an existing database,
which keeps a stray environment variable from discarding history.

### Which setting wins

1. A policy passed when opening for writing replaces the one in the file, and
   is recorded at once.
2. Otherwise the file's policy stands. A read-only open never changes it,
   whatever it is given.
3. A new database with no policy given takes `LATTICE_CHANGEFEED_RETENTION`.
4. Otherwise `all`.

Opening with a new policy drops nothing by itself; the drop happens at the
next checkpoint or trim. Note that opening a database whose log still holds
transactions (after a crash, or a process that did not close) folds that log
in with a checkpoint, which is such a point.

### Trimming now

`trim_changefeed()` / `trimChangefeed()` / `TrimChangefeed()` /
`lattice_changefeed_trim` / `lattice changefeed --trim` apply the policy in
full and make the result durable before returning, and return how many events
were dropped. It is refused while a write transaction is open on the handle
(`LATTICE_ERROR_LOCK_TIMEOUT`). Under `all` it drops nothing.

Otherwise, events are dropped at checkpoints: automatically once the log
passes its threshold (1,000 log frames by default), on `lattice checkpoint`,
and when the database is closed.

You can also drop events through a sequence yourself, in a write transaction,
with the stream trim every stream has (`txn.trim_stream("__lattice_changes", s)`,
`txn.trimStream(...)`, `lattice_stream_trim`), whatever the policy. That is the
way to discard history up to a point you choose, such as one every consumer has
passed, while keeping `all` for everything after it.

## Reading at the edge of the window

`changes(after_sequence=s)` returns the events from `s + 1` on.

- `s = 0` always reads from the oldest event held.
- If event `s + 1` has been dropped, the read fails with the trimmed error
  (`LATTICE_ERROR_NOT_FOUND` underneath). The reader has missed changes; it
  should rebuild whatever it maintains from the graph and continue from 0, or
  from `changefeed_status().last_sequence` if it rebuilt from a snapshot taken
  at that point.
- A reader that is exactly at the edge (it handled event `oldest - 1`) reads
  on without an error.

## The stored format

Events are stored in the database's stream-events B+Tree, next to user streams.
This section is the reference for that encoding, for anyone writing a tool
that reads the file directly. Applications read events through `changes()` and
never see it: what `changes()` returns is identical whichever form an event is
stored in.

Conventions: keys are compared bytewise and their numbers are big-endian;
**varints** are unsigned LEB128 (seven bits per byte, low bits first, the high
bit set on every byte but the last, at most 10 bytes); engine values are
little-endian.

### Key

```text
00  n  sequence (n bytes, big-endian)
```

`n` (1 to 8) is the fewest bytes that hold the sequence, so keys are 3 bytes up
to sequence 255, 4 up to 65,535 and 5 up to 16,777,215, and they sort in
sequence order (a longer number is a larger one). User stream keys begin with
the stream name's length, 1 to 255, so a key beginning with `00` is always the
changefeed's.

### Value

| Field | Size | Present |
|---|---|---|
| head byte `vvv t oooo` | 1 | always. `vvv` = encoding version (1), `t` = a transaction header follows, `oooo` = operation |
| commit time | varint, milliseconds since 1970-01-01 UTC | when `t` is set: the first event of every transaction |
| id | varint | always: the node id, or the edge id for edge operations |
| source id, target id | varint, varint | `edge.insert`, `edge.delete` |
| symbol | varint, sometimes followed by a name | label operations (the label), property operations (the key), `edge.insert`/`edge.delete` (the edge type) |
| old value, new value | engine value, twice | property operations; a missing value is `00` (null) |

Operations: 1 `node.insert`, 2 `node.delete`, 3 `node.label_add`,
4 `node.label_remove`, 5 `node.property_set`, 6 `node.property_remove`,
7 `edge.insert`, 8 `edge.delete`, 9 `edge.property_set`,
10 `edge.property_remove`. Head bytes are therefore `21`–`2a` without a
header and `31`–`3a` with one.

**Symbol.** A varint `s`. Even: `s / 2` is an id in the database's symbol
table, the table that maps label, type and key names to numbers (ids are 16-bit,
user names start at 1000, and an id is never removed or reused). Odd:
`(s - 1) / 2` bytes of UTF-8 name follow. This release always writes ids; the
name form is reserved.

**Engine value.** A type tag, then the value: `00` null; `01` bool (1 byte);
`02` int (8 bytes, signed); `03` float (8 bytes, IEEE 754); `04` string
(4-byte length, UTF-8); `05` bytes (4-byte length, bytes); `06` vector (4-byte
count, 4-byte floats); `07` list (4-byte count, values); `08` map (4-byte
count, then per entry a 4-byte key length, the key and a value).

**What `changes()` builds from it.** The kind is the operation's name; the
payload is a map with, in order, `entity` (`"node"` or `"edge"`), `op`
(`"insert"`, `"delete"`, `"label_add"`, `"label_remove"`, `"property_set"`,
`"property_remove"`), `node_id` or `edge_id`, then `source_id`, `target_id`
and `type` for edge insert and delete, `label` for label operations, and
`key`, `old_value`, `new_value` for property operations.

### An example

Two transactions, `CREATE (:Person {name: 'Ada'})` and then a second person
with a `KNOWS` edge to the first, store these entries:

```text
key 00 01 01      sequence 1
val 31            version 1, transaction header, op 1 node.insert
    cb a8 dc 84 9d 34   commit time 1794500924491 (2026-11-12 16:28:44.491 UTC)
    01            node 1
key 00 01 02      sequence 2
val 23 01 d0 0f   op 3 label_add, node 1, symbol 2000 = id 1000 ("Person")
key 00 01 03      sequence 3
val 25 01 d2 0f   op 5 property_set, node 1, symbol id 1001 ("name")
    00            old value: null
    04 03 00 00 00 41 64 61   new value: string of 3 bytes, "Ada"
key 00 01 04      sequence 4
val 31 cd a8 dc 84 9d 34 02   node.insert, header (2 ms later), node 2
key 00 01 05      sequence 5
val 23 02 d0 0f   label_add, node 2, "Person"
key 00 01 06      sequence 6
val 27 01 01 02 d4 0f   op 7 edge.insert, edge 1, from 1 to 2, type id 1002 ("KNOWS")
```

In the 0.16.0 format the first three of these took 26-byte keys and values of
107 to 177 bytes each.

### Other state

The stream-meta B+Tree holds the changefeed's bookkeeping:

| Key | Value |
|---|---|
| `11` `"__lattice_changes"` | the last sequence handed out (8 bytes, little-endian) |
| `00` `"changefeed.floor"` | every event below this sequence has been dropped (8 bytes, little-endian); missing means 0 |
| `00` `"changefeed.retention"` | the policy: mode (1 byte: 1 all, 2 last, 3 age, 4 off), value (8 bytes, little-endian: events or milliseconds); missing means all |

### In the log

A transaction's events are written to the write-ahead log as records of payload
type `0x23`, each holding up to about 3,800 bytes of events:

```text
23  01  first-sequence(varint)  count(varint)
    symbol-count(varint)  { id(varint) name-length(varint) name }*
    { event-length(varint) event-bytes }*
```

The event bytes are exactly what is stored in the tree. Each record names the
symbols its events use, because the symbol table is not itself logged: after a
crash, recovery may give a name a different id than the event carries, and
replay rewrites the event to the id the name has now.

### Events from 0.16.0

Events written by 0.16.0 (and 0.17.0 development builds before this format)
keep their old form, about 300 bytes each on disk: key `11 "__lattice_changes"` followed by the sequence as
8 bytes big-endian, value the kind's length, the kind, a 4-byte payload length
and the payload map as an engine value. Their first value byte is 11 to 20
(the kind's length); a compact value's is `21` to `3a`, and the key already
tells them apart. Both forms are read side by side, in sequence order.

Old events are not rewritten: they stay in the old form until the retention
policy drops them (`lattice compact` does not convert them). To shed them while
keeping everything else, export the graph and import it into a new database;
the new database's changefeed starts with the import, and the old history stays
behind in the old file.

### Compatibility promise

Every form described here stays readable by every later release. A future
encoding will use a new version number in the head byte; a reader that meets a
version it does not know refuses the record rather than guess at it.
