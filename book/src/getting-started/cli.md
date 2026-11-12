# The `lattice` Command

Installing LatticeDB gives you a `lattice` command. It is the fastest way to create a
database, poke around in one, and move data in and out — no code required.

Every command follows the same shape:

```bash
lattice <command> [options] <path>
```

The path is the database file. Almost every command needs one; `version`, `help`, and
`update` are the exceptions.

If you ever forget what something does, ask it:

```bash
lattice help            # everything at a glance
lattice export --help   # detail for one command
```

## A quick tour

Here is the whole thing in five commands. Create a database, put something in it, and
look at what you have.

```bash
lattice create social.lattice
lattice exec social.lattice --query="CREATE (a:Person {name: 'Alice', age: 30})"
lattice exec social.lattice --query="CREATE (b:Person {name: 'Bob', age: 41})"
lattice exec social.lattice --query="MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}) CREATE (a)-[:KNOWS {since: 2020}]->(b)"
lattice count social.lattice
```

That last command prints:

```text
┌───────────┬────────────┐
│ Type      │ Count      │
├───────────┼────────────┤
│ Nodes     │          2 │
│ Edges     │          1 │
│ Labels    │          1 │
│ EdgeTypes │          1 │
└───────────┴────────────┘
```

## Creating a database

`create` makes a new database file. On its own it gives you a graph with full-text
search turned on:

```bash
lattice create social.lattice
```

```text
Created database: social.lattice
  Full-text search enabled
```

If you plan to store embeddings, turn the vector index on at creation time and say how
many dimensions your vectors have. This is decided up front, because the index is
built into the file and the file records how it was built:

```bash
lattice create embeddings.lattice --enable-vector --vector-dims=1536
```

```text
Created database: embeddings.lattice
  Vector index enabled (dimensions: 1536, metric: cosine)
  Full-text search enabled
```

| Option | What it does |
|--------|--------------|
| `--enable-vector` | Turn on the vector index for similarity search |
| `--vector-dims=<n>` | How many numbers are in each vector, 1 to 4096 (default 128) |
| `--vector-metric=<m>` | How distance is measured: `cosine` (default), `euclidean`, or `dot` (the negative dot product) |
| `--vector-m=<n>` | HNSW connections per node, 2 to 32 (default 16) |
| `--vector-ef-construction=<n>` | HNSW candidate list size while building, 1 to 4096 (default 200). These four options need `--enable-vector`: without it the command is refused rather than creating a database with no index |
| `--enable-fts` | Turn on full-text search — already the default |
| `--no-fts` | Leave full-text search out |
| `--cache-size=<mb>` | How much memory to keep pages in, in MB (default 64) |
| `--page-size=<bytes>` | Page size, 4096 to 65535 (default 4096) |

Pick `--vector-dims` to match whatever produces your embeddings. OpenAI's
`text-embedding-3-small` gives you 1536 numbers per vector, so that is the number you
would use. The metric and the two HNSW settings cannot be changed later either; to
change them, export the data and import it into a new database.

### Opening a database that stores vectors

The database file records its vector index: dimensions, metric and build settings.
Every command opens the index as it was created, with no vector options, including
`export`, `dump` and `import`, which carry node vectors, and the first command after
a crash, which replays the vector changes in the log.

A `--vector-dims` that contradicts the file is refused:

```bash
lattice count embeddings.lattice --enable-vector --vector-dims=4
```

```text
Error: Failed to open database embeddings.lattice: its vector index has a different number of dimensions than --vector-dims gives. A database with a vector index opens with it as it was created; leave out --vector-dims.
```

The vector options of commands other than `create` are needed in two cases only.
`--enable-vector --vector-dims=<n>` on a database that has no vector index creates
one, for example to import a file that carries vectors into a database created
without them. And a database last written by LatticeDB 0.16.0 records no vector
settings: its dimensions are read from its vector pages, and in the rare case that
fails, a command that has to replay vector changes from its log stops and asks for
`--enable-vector --vector-dims=<n>` with the dimensions it was created with.

## Running queries

There are two ways to run Cypher. Use `exec` for one query, and `query` when you want
to sit and explore.

### One query at a time

```bash
lattice exec social.lattice --query="MATCH (p:Person) RETURN p.name, p.age"
```

You can keep a longer query in a file instead of fighting with shell quoting:

```bash
lattice exec social.lattice --file=report.cypher
```

When a query fails, the error says at which stage it failed, what went wrong and
where, with a caret under the part of the query it is about, and the command exits
with status 1:

```bash
lattice exec social.lattice --query="MATCH (p:Person) RETURN p.name, p.age / 0"
```

```text
Error: Execution error (DivisionByZero) at 1:39: Division by zero: 30 / 0
  1 | MATCH (p:Person) RETURN p.name, p.age / 0
    |                                       ^
```

A misspelt function is found before the query runs, whether or not any rows would
reach it:

```text
Error: Semantic error (unknown_function) at 1:25: Unknown function 'toUppr'
  1 | MATCH (p:Person) RETURN toUppr(p.name)
    |                         ^^^^^^
```

### An interactive shell

```bash
lattice query social.lattice
```

This drops you into a prompt where you can type queries and see results immediately.
It also understands a few commands of its own, all starting with a dot:

| Command | What it does |
|---------|--------------|
| `.help` | List these commands |
| `.labels` | List every node label |
| `.types` | List every edge type |
| `.schema` | Show what the data looks like |
| `.format table\|json\|csv` | Change how results are printed |
| `.timing on\|off` | Show how long each query took |
| `.exit` or `.quit` | Leave |

## Looking at what is in there

Four commands answer "what is actually in this database?".

`count` gives you the totals, as shown in the tour above.

`labels` and `types` list the node labels and edge types, with how many of each:

```bash
lattice labels social.lattice
```

```text
┌────────┬────────────┐
│ Label  │ Count      │
├────────┼────────────┤
│ Person │          2 │
└────────┴────────────┘
1 label(s)
```

`schema` puts both together. LatticeDB does not make you declare a schema up front, so
this is worked out by looking at the data that is actually there:

```bash
lattice schema social.lattice
```

```text
Schema
══════

Node Labels:
  (:Person) - 2 node(s)

Edge Types:
  [:KNOWS] - 1 edge(s)

Total: 1 label(s), 1 edge type(s)
```

`info` describes the file rather than its contents — size, counts, and which features
it was built with:

```bash
lattice info social.lattice
```

```text
Database: social.lattice
─────────────────────────────
Size:         64 KB
Nodes:        2
Edges:        1
Format:       v4
Vector:       disabled
FTS:          enabled
Changefeed:   keep all, events 1..7
```

The last line is the [changefeed](../guides/changefeed.md): the record of every
change, which the database keeps by default. Here the two people and the edge
between them took seven events. A database with
[property indexes](../guides/property-indexes.md) lists each on a line of its
own, with how its keys are encoded:

```text
Index:        :Person(age)  ordered_dense
Index:        [:KNOWS](since)  ordered
```

An index created by LatticeDB 0.16.0 shows `digest`: it answers equality only
until `compact --upgrade-indexes` rebuilds it.

## Moving data in and out

### Importing

`import` reads JSON, JSONL or CSV, chosen by the file extension. It also reads
RDF in Turtle (`.ttl`) and N-Triples (`.nt`), which
[RDF: Turtle and N-Triples](../guides/rdf.md) describes:

```bash
lattice import social.lattice --file=people.json
```

```text
Importing people.json into social.lattice...
Import complete
  Nodes imported: 2
  Edges imported: 1
```

JSON should have a `nodes` array and an `edges` array:

```json
{
  "nodes": [
    {"id": "n1", "labels": ["Person"], "properties": {"name": "Carol", "age": 29}},
    {"id": "n2", "labels": ["Person"], "properties": {"name": "Dan", "age": 35}}
  ],
  "edges": [
    {"source": "n1", "target": "n2", "type": "KNOWS", "properties": {"since": 2021}}
  ]
}
```

In JSON, `id` is a name local to the file: edges refer to nodes by it, and each
node gets a fresh ID in the database. A node can also carry a `"vector"` array,
which is stored as its embedding; that needs a database with a vector index. Into a
database without one, the import stops and asks for `--enable-vector
--vector-dims=<n>`, which creates the index. Files that `export` writes
also have a `"version"` field and write some values in a tagged form; see
[Data Export](../guides/data-export.md) for the format.

JSONL is what `export` writes to a `.jsonl` file: one record per line, each a
node (`"kind":"node"`) or an edge (`"kind":"edge"`), with edges naming nodes
from earlier lines.

CSV files are read as RFC 4180 describes: a quoted field can hold commas, line
breaks and doubled quotes (`""`). Columns are found by name in the header. A
nodes file has an `_id` column, `_labels` with the labels separated by `;`, an
optional `_vector`, and a column per property:

```text
_id,_labels,name,age
n1,"Person;Employee","Smith, Jr",30
n2,Person,"Ann ""The Hat"" Lee",41
```

An edges file has `_source`, `_target` and `_type`, and a column per property:

```text
_source,_target,_type,since
n1,n2,KNOWS,2020
```

A property column holds plain text, read as an integer, a float, or else a
string. A column named `<key>:json` holds a value in the JSON export encoding
instead, which is how a list, a map, bytes or a string that looks like a number
gets through CSV; `export` writes every property column that way.

How an edges file names its nodes depends on how you import it. Give `import`
the `--file` value an export was given, and it imports `<base>_nodes.csv` and
`<base>_edges.csv` together, matching `_source` and `_target` against the nodes
file's `_id` values:

```bash
lattice import social.lattice --file=people.csv
```

```text
Importing people_nodes.csv and people_edges.csv into social.lattice...
Import complete
  Nodes imported: 2
  Edges imported: 1
```

An edges file imported on its own names nodes by their numeric database IDs, and
an ID that is not a node in the database is an error.

#### When a record is wrong

Without further options, `import` stops at the first record it cannot import and
says which one it was and why:

```text
Error: Import failed at nodes[1]: its "labels" is not an array
Nothing was imported. Use --on-error=skip to import the records that are valid.
```

Records are named by where they are: `nodes[1]` and `edges[0]` in JSON, `line 12`
in JSONL and CSV, `edges file line 4` when a CSV pair is imported together. A
record is imported whole or not at all: a property that cannot be read or
stored, a label or property of the wrong JSON type, or an edge whose endpoint
names no node fails the record.

A CSV file that cannot be read as CSV at all, such as one with a quote that is
never closed or text after a field's closing quote, stops the import at the line
where the problem starts:

```text
Error: Import failed at line 2: a quoted field that starts here has no closing quote
Nothing was imported. Fix the file and import it again.
```

`--on-error=skip` cannot skip past such a fault, since the reader cannot tell where
the next record begins, so it stops the import too, and the message does not
suggest it.

Two options are worth knowing about for large imports:

| Option | What it does |
|--------|--------------|
| `--batch-size=<n>` | Commit every N records (default 1000) |
| `--on-error=skip` | Leave out records that fail instead of stopping |

When an import stops, batches committed before the failing one stay in the
database, and the message says how many records they held. With
`--on-error=skip`, each record is committed on its own, so a bad one leaves
nothing behind, and every record left out is listed (up to 20, then a count):

```text
Skipped nodes[1]: its "labels" is not an array
Skipped edges[0]: target "n9" is not the id of any node in the file
Importing bad.json into bad.lattice...
Import complete
  Nodes imported: 1
  Nodes failed:   1
  Edges imported: 0
  Edges failed:   1
```

Committing record by record is slower, but one bad row in a large file does not
cost you the import.

### Exporting

`export` writes JSON, JSONL, CSV, or DOT, or RDF as Turtle or N-Triples. It
picks the format from the file extension:

```bash
lattice export social.lattice --file=backup.json
lattice export social.lattice --file=graph.jsonl
lattice export social.lattice --file=people.csv --labels=Person
lattice export social.lattice --file=graph.dot
```

JSON, JSONL and CSV exports import back with `import`. Every node gets a new ID
when they do.

CSV export writes two files named after the one you give:
`--file=people.csv` produces `people_nodes.csv` and `people_edges.csv`:

```text
_id,_labels,age:json,name:json
1,"Person",29,"""Carol"""
2,"Person",35,"""Dan"""
```

```text
_source,_target,_type,since:json
1,2,KNOWS,2021
```

DOT is the format Graphviz reads, so you can turn a small graph into a picture:

```text
digraph G {
  n1 [label="1 : Person"];
  n2 [label="2 : Person"];
  n1 -> n2 [label="KNOWS"];
}
```

You can narrow what gets exported in one of two ways. `--labels=Person,Company`
exports the nodes with any of those labels and the edges between them.
`--query` exports what a query returns: the nodes and relationships in its rows,
plus the endpoints of every relationship, so the file imports on its own:

```bash
lattice export social.lattice --file=knows.json --query="MATCH (a:Person)-[k:KNOWS]->(b:Person) RETURN k"
```

The query has to return nodes or relationships, not just their properties
(`RETURN p`, not `RETURN p.name`); otherwise the export fails and says so.
`--labels` and `--query` cannot be combined.

Node vectors are included whenever the database has a vector index. The one
exception is a database last written by 0.16.0 whose dimensions cannot be read
from its vector pages: its export works, with a warning that the vectors are left
out unless you pass `--enable-vector --vector-dims=<n>`.

An export that fails removes the file it was writing rather than leave a
partial one behind.

### Dumping

`dump` writes the whole database to standard output as JSON, in a stable order:

```bash
lattice dump social.lattice > snapshot.json
```

Because the ordering is fixed, two dumps of the same data produce identical files. That
makes `dump` useful in tests and for spotting what changed between two databases with a
plain `diff`. It uses the same format as a JSON export, with each edge's ID added,
and it takes `--labels`, `--query` and the vector options the same way `export`
does. The summary line (`Dumped 2 nodes, 1 edges`) goes to standard error, so
it does not end up in the file.

## Looking after a database

### Checking for damage

`check` opens the file read-only and verifies the checksum stored on every page:

```bash
lattice check social.lattice
```

```text
Database file checks passed
  Pages checked: 15
  Note: sibling WAL file exists but was not validated
```

One thing to be aware of: this checks the main database file only. If a write-ahead log
file is sitting next to it, `check` will tell you it exists but will not look inside it.
The write-ahead log is the file LatticeDB appends changes to before applying them, so
that a crash cannot leave the database half-written.

### Backing up

`backup` copies a database to another file without closing it:

```bash
lattice backup social.lattice --file=/backups/social-2026-08-25.lattice
```

```text
Backed up social.lattice to /backups/social-2026-08-25.lattice
  Bytes copied:    65536
  Pages:           16
  Pages flushed:   0
  Duration:        1 ms
```

Pending writes are folded into the file before the copy starts, so what you get
is a complete database on its own — you can open it directly, and it needs no
write-ahead log beside it.

The copy is written next to the destination and renamed into place only once it
is finished. An interrupted backup leaves nothing that looks usable, rather than
a truncated file you discover is bad when you need it.

Two things to know. The backup captures the database as of the moment it starts,
so writes that land during it are not included. And it will refuse to run while
a transaction is open, because a copy taken while writes land underneath it is
torn in ways no later check would catch.

**Do not back up by copying the file yourself while the database is open.** The
write-ahead log holds committed data that is not in the main file yet, so
copying just the main file gives you a database that opens and then fails on
query, and copying both files catches them at different instants and fails on
open. Neither announces itself at copy time — you find out at restore. Use
`backup`, or stop the process first.

### Shipping changes somewhere else

A backup taken by hand is only as good as the last time you remembered to take
one. `replicate` copies a database into a directory you can restore from later:

```bash
lattice replicate social.lattice --to=social-backup
```

```text
Started generation 1 for social.lattice in social-backup
  Snapshot bytes:  65536
  Generation:      1
  Frames shipped:  0
  Bytes shipped:   0
  Duration:        4 ms
```

The first pass starts a **generation**: a full snapshot of the database, which
later passes extend with the write-ahead log frames written after it. Run it
again with nothing changed and it ships nothing:

```text
Nothing new to ship for social.lattice
  Generation:      1
  Frames shipped:  0
  Bytes shipped:   0
  Duration:        2 ms
```

That is normal and not an error: the command exits successfully, so you can put
it on a timer without filling your logs with things that look like failures.

Frames can only be shipped while the log still holds them. A process that writes
to the database and then closes it, as `lattice exec` or an application does,
folds the log into the file and resets it, and a reset log starts a new
generation. So when the writers are separate processes, as they are from the
command line, every pass after a change takes a fresh snapshot:

```bash
lattice exec social.lattice --query="CREATE (:Person {name: 'Carol'})"
lattice replicate social.lattice --to=social-backup
```

```text
Started generation 2 for social.lattice in social-backup
  Snapshot bytes:  65536
  Generation:      2
  Frames shipped:  0
  Bytes shipped:   0
  Duration:        5 ms
```

The destination then holds a manifest and one directory per generation:

```text
social-backup/manifest.json
social-backup/gen-0000000001/snapshot.lattice
social-backup/gen-0000000002/snapshot.lattice
```

Older generations are left alone, because restoring to a moment inside one still
needs them. Delete a `gen-` directory to reclaim its space, at the cost of
restoring to any moment inside it.

Passes that ship frames rather than snapshots come from a process that keeps the
database open while it writes and replicates through the same handle: the Zig
API's `replicateTo`. Its log is reset only by a checkpoint (automatically after
about a thousand frames, by default) or when the database closes, and between
resets each pass copies only the new frames, into a `frames/` directory inside
the generation. The C API and the
language bindings do not expose replication.

This command opens the database for writing, so it takes the file lock. It
cannot run against a database another process has open; it stops with the error
described under [One process at a time](#one-process-at-a-time). `--follow`
keeps it running and replicating on an interval:

```bash
lattice replicate social.lattice --to=social-backup --follow --interval=30
```

Each pass opens the database and closes it again, so other processes can use
the database between passes. A pass that finds the database open elsewhere is
skipped until the next interval; a process that opens the database while a pass
is running gets the same error and can retry. To replicate a database your
application keeps open, call `replicateTo` from the application.

### Getting a database back

`restore` turns a replication directory back into a database:

```bash
lattice restore social-backup --output=recovered.lattice
```

```text
Restored social-backup to recovered.lattice
  Generation:      2
  Segments:        0
  Frames replayed: 0
  Bytes written:   65536
  Restored to:     2026-11-12T00:17:49Z
  Duration:        500 ms
```

It copies the snapshot, replays the frames shipped after it (none here, since
every pass in this generation came from the command line), and folds the
result into one file. You can open that file, copy it, or move it, with nothing
else beside it that has to be kept together with it.

To go back to an earlier moment, say when:

```bash
lattice restore social-backup --output=recovered.lattice --at="2026-11-12T00:18:00Z"
```

Times are read as UTC, so a restore means the same thing wherever you run it. A
bare date works too and means midnight.

There is a limit worth understanding here. What you get is the state as of the
last replication pass at or before the moment you asked for, not the state at
that exact instant. If you replicate every thirty seconds, you can rewind to
within thirty seconds. That is why the output tells you the moment it actually
restored to, rather than repeating the one you asked for.

`restore` will not write over a file that already exists unless you pass
`--force`, because the thing most likely to be sitting at the output path is a
database somebody still wants.

### Flushing pending writes

Changes are written to a write-ahead log first, and folded into the database file
later. `checkpoint` does that folding on demand and then resets the log:

```bash
lattice checkpoint social.lattice
```

```text
Checkpointed database: social.lattice
  Pages flushed:   0
  Checkpoint LSN:  2
  WAL truncated:   yes
  Duration:        0 ms
```

You mostly do not need this. A database checkpoints itself as the log grows, and
again when it closes. It is worth running by hand in two situations: before
copying a database file, so the copy is complete on its own, and on a database
that has been open a very long time under heavy writes, if you want to pick the
moment the flush happens rather than let it land mid-request.

This is not the same as `compact`. Checkpointing shrinks the log, not the
database file.

After a crash nothing has to be run first. The commands that only read
(`info`, `count`, `labels`, `types`, `schema`, `export` and `dump`) open the file
read-only and recover it in memory, so they show every committed transaction
without changing the files; the first command that writes recovers the files
themselves. `check` is the exception: it reads pages directly and does not
recover first, so after a crash it checks the file as the crash left it, and
reports a flush the crash interrupted as an error. Run a writing command such as
`checkpoint` before it.

### Reclaiming space

Deleting a lot of data leaves free pages behind, and the file stays the same size
because those pages are kept for reuse. `compact` hands the unused ones at the end of
the file back to the operating system:

```bash
lattice compact social.lattice
```

```text
Compacted database: social.lattice
  Pages before:    16
  Pages after:     16
  Pages removed:   0
  Bytes reclaimed: 0
```

Nothing was reclaimed here because nothing had been deleted. A few things are worth
knowing before you run it on real data:

- It only truncates free pages at the *end* of the file. Free pages in the middle stay
  where they are and get reused normally, so a file with holes in the middle will not
  shrink much.
- Live pages are never moved, which is what makes it safe to interrupt.
- It will refuse to run on a read-only database, or while any transaction is open.

With `--upgrade-indexes` it first rebuilds every property index still in an older
encoding, so that one created by 0.16.0 answers ranges and sorting too, and
reports how many it rebuilt as `Indexes rebuilt:`.

### Keeping the changefeed in check

Every change to the graph is recorded as an event in the changefeed, and by
default every event is kept, so a database that is edited a lot grows with its
history. `changefeed` shows how much there is and how much the file keeps:

```bash
lattice changefeed social.lattice
```

```text
Changefeed of social.lattice
  Retention:      all
  Events held:    7
  Bytes:          73
  Sequences:      1..7
```

`--changefeed=<policy>` sets how much to keep: `all`, `off`, `last:<events>`,
or `age:<duration>` such as `age:30d`. The file records it, and old events are
dropped as the database checkpoints; `--trim` drops them now:

```bash
lattice changefeed social.lattice --changefeed=last:2 --trim
```

```text
Changefeed of social.lattice
  Retention:      last:2
  Events held:    2
  Bytes:          31
  Sequences:      6..7
  Trimmed:        5 events
```

Every command that writes takes `--changefeed` too, so
`lattice create big.lattice --changefeed=off` makes a database that keeps no
history at all. [The Changefeed](../guides/changefeed.md) explains what the
history is for and how to choose.

### Updating

`update` replaces your installed copy with the latest release:

```bash
lattice update
```

## Working without a file

Pass `:memory:` instead of a path and the database lives only in memory:

```bash
lattice exec :memory: --query="CREATE (n:Note {t: 'scratch'}) RETURN n"
```

Nothing is written to disk and nothing survives the command, which makes it handy
for trying a query out or checking what some Cypher does. There is no file to
lock, so `--no-lock` means nothing here.

## One process at a time

A database can only be open in one process at a time. If you run a command against
a database your application already has open, it stops before touching anything:

```text
Error: social.lattice is open in another process. Close it first, or pass
--no-lock if you are certain nothing is writing to it.
```

A command that writes needs the database to itself, and a command that only reads
is also refused while something else is writing. That second part surprises
people, so it is worth saying why: a reader in another process cannot see the
writer's pending changes, and would be reading a file that is being rewritten
underneath it. The answer it gave you would be stale at best.

Any number of read-only commands can run at once against a database nobody is
writing.

Every command accepts `--no-lock`, which skips the check. It exists for
filesystems where locking does not work, such as some network filesystems. It
does not make concurrent access safe — it removes the thing that was going to
tell you it was not.

## Files from a newer release

A database file can say that releases older than some version may read it but not
write it. A command that writes stops on such a file and names the ones that
still work:

```text
Error: future.lattice was written by a newer LatticeDB that does not let this version write to it. info, count, labels, types, schema, export and dump still read it; use that newer version for anything else.
```

`exec` and `query` open the database for writing, so they cannot run even a
read-only query against such a file.

## Output formats

Most commands can print in a different format, which is what you want when something
else is going to read the output:

```bash
lattice count social.lattice --format=json
lattice count social.lattice --format=csv
```

```text
type,count
nodes,2
edges,1
labels,1
edge_types,1
```

`table` is the default and is meant for reading. It prints floats with six
decimals, and in the shortest scientific form that reads back as the same number
when they are 1e16 or more, or below 1e-6, in size (`1e300`, `1e-300`), so a very
small number never shows as `0.000000`. `json` and `csv` are meant for piping
somewhere else, and print floats in full, as plain decimals:

```bash
lattice exec social.lattice --query="MATCH (p:Person) RETURN p.name" --format=json | jq
```

A query's CSV follows RFC 4180, with one line per row ending in a newline: a
column name or value that holds a comma, a quote or a line break is put in
quotes, with each quote inside doubled. A list or map is written as its JSON, in
quotes, so `{k: 'v'}` becomes `"{""k"":""v""}"`. A query without `RETURN` prints
nothing in CSV, an empty `columns` list in JSON and "Query executed" in a table.

Output redirected to a file can be appended to with `>>`, for example to keep a
log of counts taken over time:

```bash
lattice count social.lattice --format=csv >> counts.csv
```

## Every command at a glance

| Command | What it does |
|---------|--------------|
| `create <path>` | Make a new database |
| `info <path>` | Show file size, counts, and enabled features |
| `compact <path>` | Give free pages at the end of the file back to the OS |
| `checkpoint <path>` | Flush pending writes into the file and reset the log |
| `changefeed <path>` | Show the changefeed, set how much of it to keep, trim it |
| `backup <path>` | Copy to another file while the database stays open |
| `replicate <path>` | Keep a directory up to date with the database |
| `restore <dir>` | Rebuild a database from a replication directory |
| `check <path>` | Verify page checksums in the main file |
| `query <path>` | Open an interactive Cypher shell |
| `exec <path>` | Run one query and exit |
| `import <path>` | Load data from JSON, JSONL, or CSV |
| `export <path>` | Write data to JSON, JSONL, CSV, or DOT |
| `dump <path>` | Print the whole database as canonical JSON |
| `labels <path>` | List node labels and their counts |
| `types <path>` | List edge types and their counts |
| `schema <path>` | Show the shape of the data |
| `count <path>` | Show node and edge totals |
| `update` | Update your LatticeDB installation |
| `version` | Print the version and file format version |
| `help` | Show usage |

## Where to go next

- [Quick Start](./quickstart.md) for the same ideas from Python or TypeScript
- [Core Concepts](./concepts.md) for what nodes, edges, and properties actually are
- [Cypher Overview](../cypher/overview.md) for the query language itself
