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
many dimensions your vectors have. This has to be decided up front, because the index
is built into the file:

```bash
lattice create embeddings.lattice --enable-vector --vector-dims=1536
```

| Option | What it does |
|--------|--------------|
| `--enable-vector` | Turn on the vector index for similarity search |
| `--vector-dims=<n>` | How many numbers are in each vector, 1 to 4096 (default 128) |
| `--enable-fts` | Turn on full-text search — already the default |
| `--no-fts` | Leave full-text search out |
| `--cache-size=<mb>` | How much memory to keep pages in, in MB (default 64) |
| `--page-size=<bytes>` | Page size, 4096 to 65535 (default 4096) |

Pick `--vector-dims` to match whatever produces your embeddings. OpenAI's
`text-embedding-3-small` gives you 1536 numbers per vector, so that is the number you
would use.

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
Size:         68 KB
Nodes:        2
Edges:        1
Format:       v3
Vector:       disabled
FTS:          enabled
```

## Moving data in and out

### Importing

`import` reads JSON or CSV:

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

CSV needs two files, one for nodes and one for edges. Columns starting with an
underscore have special meaning; everything else becomes a property:

```text
_id,_labels,name,age
n1,"Person;Employee",Alice,30
```

```text
_source,_target,_type
1,2,KNOWS
```

Two options are worth knowing about for large imports:

| Option | What it does |
|--------|--------------|
| `--batch-size=<n>` | Commit every N items instead of all at once (default 1000) |
| `--on-error=skip` | Skip records that fail instead of giving up on the whole import |

`--on-error=skip` applies records one at a time, so the good rows still land. That is
slower, but it means one bad row in a large file does not cost you the import.

### Exporting

`export` writes JSON, JSONL, CSV, or DOT. It picks the format from the file extension:

```bash
lattice export social.lattice --file=backup.json
lattice export social.lattice --file=graph.jsonl
lattice export social.lattice --file=people.csv --labels=Person
lattice export social.lattice --file=graph.dot
```

DOT is the format Graphviz reads, so you can turn a small graph into a picture:

```text
digraph G {
  n1 [label="1 : Person"];
  n2 [label="2 : Person"];
  n1 -> n2 [label="KNOWS"];
}
```

You can narrow what gets exported with `--labels=Person,Company`, or export the result
of a query instead of the whole graph with `--query="..."`.

### Dumping

`dump` writes the whole database to standard output as JSON, in a stable order:

```bash
lattice dump social.lattice > snapshot.json
```

Because the ordering is fixed, two dumps of the same data produce identical files. That
makes `dump` useful in tests and for spotting what changed between two databases with a
plain `diff`.

## Looking after a database

### Checking for damage

`check` opens the file read-only and verifies the checksum stored on every page:

```bash
lattice check social.lattice
```

```text
Database file checks passed
  Pages checked: 16
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
one. `replicate` keeps a directory up to date with a database, so a failed disk
costs you the last few seconds of writes rather than everything since your last
copy:

```bash
lattice replicate social.lattice --to=/mnt/backup/social
```

```text
Started generation 1 for social.lattice in /mnt/backup/social
  Snapshot bytes:  73728
  Generation:      1
  Frames shipped:  0
  Bytes shipped:   0
  Duration:        2 ms
```

The first pass copies the whole database. Every pass after that copies only the
changes since the last one, which is why running it often is cheap:

```text
Shipped social.lattice to /mnt/backup/social
  Generation:      1
  Frames shipped:  5
  Bytes shipped:   20480
  Duration:        2 ms
```

A pass with nothing to ship is normal and is not an error. It tells you so and
exits successfully, which means you can put this on a timer without your logs
filling up with things that look like failures.

Add `--follow` to leave it running instead:

```bash
lattice replicate social.lattice --to=/mnt/backup/social --follow --interval=30
```

Inside the destination you will find a manifest, a snapshot, and the changes
that have arrived since:

```text
/mnt/backup/social/manifest.json
/mnt/backup/social/gen-0000000001/snapshot.lattice
/mnt/backup/social/gen-0000000001/frames/0000000012-0000000016.frames
```

Every so often LatticeDB folds pending changes into the database file and starts
the log over. When that happens, replication starts a **generation**: a fresh
snapshot, followed by the changes that came after it. Older generations are left
alone, because restoring to a moment inside one still needs them.

There is one important limit. This command opens the database, and LatticeDB does
not lock a database across processes, so you must not point it at a database
another process has open. If you want to replicate a database while your
application is using it, call `replicateTo` on the handle your application
already has, and use this command for the case where nothing else is running.

### Getting a database back

`restore` turns a replication directory back into a database:

```bash
lattice restore /mnt/backup/social --output=recovered.lattice
```

```text
Restored /mnt/backup/social to recovered.lattice
  Generation:      1
  Segments:        2
  Frames replayed: 20
  Bytes written:   73728
  Restored to:     2026-08-25T22:44:18Z
  Duration:        468 ms
```

It copies the snapshot, replays the changes shipped after it, and folds the
result into one file. You can open that file, copy it, or move it, with nothing
else beside it that has to be kept together with it.

To go back to an earlier moment, say when:

```bash
lattice restore /mnt/backup/social --output=recovered.lattice --at="2026-08-25T14:00:00Z"
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

### Reclaiming space

Deleting a lot of data leaves free pages behind, and the file stays the same size
because those pages are kept for reuse. `compact` hands the unused ones at the end of
the file back to the operating system:

```bash
lattice compact social.lattice
```

```text
Compacted database: social.lattice
  Pages before:    17
  Pages after:     17
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

`table` is the default and is meant for reading. `json` and `csv` are meant for piping
somewhere else:

```bash
lattice exec social.lattice --query="MATCH (p:Person) RETURN p.name" --format=json | jq
```

## Every command at a glance

| Command | What it does |
|---------|--------------|
| `create <path>` | Make a new database |
| `info <path>` | Show file size, counts, and enabled features |
| `compact <path>` | Give free pages at the end of the file back to the OS |
| `checkpoint <path>` | Flush pending writes into the file and reset the log |
| `backup <path>` | Copy to another file while the database stays open |
| `replicate <path>` | Keep a directory up to date with the database |
| `restore <dir>` | Rebuild a database from a replication directory |
| `check <path>` | Verify page checksums in the main file |
| `query <path>` | Open an interactive Cypher shell |
| `exec <path>` | Run one query and exit |
| `import <path>` | Load data from JSON or CSV |
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
