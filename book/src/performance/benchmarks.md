# Benchmarks

Every number on this page was measured through the public C API
(`include/lattice.h` against a ReleaseFast `liblattice`), the way an application or
a binding meets the database, with the options a user gets by default unless a row
says otherwise. The program is `tests/benchmark/capi_bench.c`; the commands that
produce these tables are under [Reproducing](#reproducing).

| | |
|---|---|
| Version | 0.17.0 (`release/0.17.0` at 75de895), ReleaseFast. The two tables marked **format 5** are the build after 0.18.1 (`main` at 9f087a04) |
| Machine | Apple M5, 10 cores, 24 GB, internal SSD, macOS 26.5 |
| Date | 2026-11-13; the format-5 tables 2026-11-20 and 2026-11-22 |
| Defaults | write-ahead log on; sync mode `full` (the log is fsynced on every commit; plain `fsync`, not `F_FULLFSYNC`); 100 MB page cache; changefeed keeps every event; automatic checkpoint every 1,000 log frames (since format 5, every 3,200 with this cache) |
| Load | one thread; nothing else measuring or building, desktop applications open (load average about 2; about 4 on 2026-11-22) |

Each row ran several times (five unless it says otherwise), each run a fresh
process on a fresh database. A figure is the **median of the runs**; the range in
brackets is the lowest and highest run. Within a run, p50 and p99 are taken over
every timed operation. Times are wall-clock.

**What was measured again, and what was not.** The release after 0.18.1 changes
how ids are stored (file format 5) and how a commit files new relationships, and
both show in batched writes and bulk loads. Those were measured again on that
build and have their own tables below. Everything else on this page is still
0.17.0's run.

The other rows were checked on the new build on 2026-11-22, with 0.18.1 run
beside it: the median commit, the Cypher lookups, the traversals and vector
search at 100,000 came out level between the two builds. Two things moved.
`lattice_node_get_property` took 0.24 us where 0.18.1 took 0.22, in each of
three alternating runs. And a full-text index is smaller in a format-5 file: 31
MB and 141 MB for the two sizes below, where the table says 36 and 172, with
search times unchanged.

That day's figures are not in the tables. The machine was in use, and both
builds measured behind this page: by a few percent on the small workloads, and by
15 to 30% on traversal and vector search at 100,000, the two that do not fit the
processor's cache. The builds agreed with each other, so the tables keep the
quieter 0.17.0 run. To be sure it was the day and not the releases, 0.17.0 itself
was run in rotation with 0.18.1 and the new build that afternoon: all three
measured the same on those two workloads, about 9.0 us for two hops and 130 to
140 us for a vector search.

## Writes

A commit here is everything from `lattice_begin` to the return of
`lattice_commit`: the log record is written and fsynced, so the transaction
survives a crash once the call returns. A node is `lattice_node_create` with a label
and two properties (an integer and a short string).

| Operation | p50 | p99 | p99.9 | Throughput |
|-----------|----:|----:|------:|-----------:|
| Create one node, one commit | 44 us [44–45] | 70 us [61–74] | 1.4 ms [0.2–1.9] | 18,900/s [15,600–21,600] |
| Create one edge (100K nodes present), one commit | 44 us [42–46] | 67 us [58–78] | 1.1 ms [0.2–3.0] | 15,400/s [8,200–22,600] |

20,000 commits per run. About one commit in a few hundred also runs an automatic
checkpoint (every 1,000 log frames), writing the pages the log changed into the
database file; those, and the disk's own occasional stalls, are the p99.9. The
slowest single commit in a run ranged from 2 ms to over 200 ms, from run to run
and in 0.16.0 as well, so it is not given as a figure; throughput, which is the
mean, moves with it.

Batched: N nodes or edges per transaction, the rate counting every commit (the
time to close the database is not included).

| Transaction size | Nodes/s | Time per transaction | Edges/s (100K nodes) |
|-----------------:|--------:|---------------------:|---------------------:|
| 10 | 130,000 [117,000–144,000] | 63 us | |
| 100 | 284,000 [274,000–295,000] | 0.28 ms | |
| 1,000 | 332,000 [320,000–337,000] | 2.4 ms | 247,000 [245,000–248,000] |
| 10,000 | 351,000 [345,000–353,000] | 27 ms | 252,000 [244,000–254,000] |

**Format 5.** The same two workloads on the build after 0.18.1, with 0.18.1 run
beside it on the same day (2026-11-22, the machine in use):

| Transaction size | Nodes/s | Time per transaction | 0.18.1 | Edges/s (100K nodes) | 0.18.1 |
|-----------------:|--------:|---------------------:|-------:|---------------------:|-------:|
| 1,000 | 408,000 [386,000–420,000] | 2.1 ms | 319,000 | 320,000 [314,000–325,000] | 219,000 |
| 10,000 | 416,000 [409,000–425,000] | 21 ms | 338,000 | 372,000 [359,000–376,000] | 220,000 |

Nodes are a quarter faster because a new id now goes to the right edge of the
node tree instead of a different page each time
([Id Key Order](../architecture/graph-storage.md#id-key-order)). Relationships
gain more, and more the larger the transaction, because a commit that creates 64
or more of them sorts their tree entries and inserts them in order
([Bulk loading](../configuration/durability.md#bulk-loading)). 0.18.1's figures
that day are 4 to 13% below the 0.17.0 rows above, so read the new rows as a
little low too. Transactions of 10 and 100 rows are not in this table: an fsync
per transaction decides them, the disk stalled often that day, and the two
builds' runs overlapped.

### Other settings

These rows change one setting from the defaults, and are labelled so.

| Operation | Setting | p50 | p99 | Throughput |
|-----------|---------|----:|----:|-----------:|
| Create one node, one commit | `sync_mode = normal` | 6.4 us [6.3–6.5] | 14 us [14–15] | 102,000/s [86,000–116,000] |
| Nodes in transactions of 1,000 | `sync_mode = normal` | | | 339,000/s [328,000–341,000] |

`normal` hands each commit to the operating system without waiting for the disk:
it survives the process being killed, and a power cut can lose the last commits
(see [Durability](../configuration/durability.md)). Once a transaction holds a
thousand rows the fsync no longer shows. (On the format-5 build the second row
measured 423,000/s [406,000–444,000] on 2026-11-22, beside 408,000/s with `full`.)

### Bulk load

1,000,000 nodes, then 2,000,000 edges between random pairs of them, then close
(three runs).

**Format 5**, the build after 0.18.1, measured 2026-11-20 on a quiet machine:

| Settings | Rows per transaction | Total | Nodes | Edges | File | Peak memory |
|----------|---------------------:|------:|------:|------:|-----:|------------:|
| Defaults | 10,000 | 23.4 s [22.4–23.5] | 2.3 s | 20.9 s | 451 MB | 130 MB |
| Defaults | 100,000 | 11.3 s [11.1–11.8] | 2.7 s | 8.6 s | 461 MB | 190 MB |
| Defaults | 1,000,000 | 8.7 s [8.7–9.0] | 2.7 s | 6.1 s | 458 MB | 680 MB |
| No log, changefeed off, 1 GB cache ([the bulk-load recipe](../configuration/durability.md#bulk-loading)) | 10,000 | 6.4 s [6.3–6.5] | 0.8 s | 5.2 s | 315 MB | 1.1 GB |

Before format 5, in transactions of 10,000 (0.17.0; 0.18.1 took 35.9 s and 8.2 s
for these two rows on 2026-11-20):

| Settings | Total | Nodes | Edges | File | Peak memory |
|----------|------:|------:|------:|-----:|------------:|
| Defaults | 34.4 s [34.3–35.0] | 3.5 s | 30.9 s | 500 MB | 175 MB |
| No log, changefeed off, 1 GB cache | 7.9 s [7.8–8.0] | 1.1 s | 6.3 s | 364 MB | 1.1 GB |

The recipe gives up crash safety for the load (an interrupted load is started
again from its source) and the changefeed's record of it; the file is smaller
because the changefeed holds no events.

Before format 5 the size of the transaction made almost no difference to a
load: 0.18.1 took 35.9 s at 10,000 rows and 33.6 s at 100,000. Now it is the
first thing to change, and
[Bulk loading](../configuration/durability.md#bulk-loading) has the full table,
with what each setting writes to disk.

The format-5 rows are from the branch before it was merged. They were run again
on the merged commit on 2026-11-22, with the machine in use: every configuration
wrote the same amount to disk and the same size of file as on the 20th, and took
6 to 18% longer, as 0.18.1 did beside it.

## Point lookups

100,000 nodes, each with an indexed integer `uid`. The database was written,
closed and reopened, and every node read once before timing, so the pages are in
the cache: these are warm reads. Uniformly random nodes.

| Operation | p50 | p99 |
|-----------|----:|----:|
| `lattice_node_get_property` inside an open read transaction | 0.22 us | 0.24 us |
| Begin a read transaction, read one property, commit | 0.34 us | 0.36 us |
| Cypher `MATCH (n:Person {uid: $uid}) RETURN n.name` (prepared once) | 2.3 us | 2.5 us |
| The same, prepared and freed on every call | 2.4 us | 2.7 us |
| Cypher `MATCH (n) WHERE id(n) = $id RETURN n.name` (prepared once) | 1.1 us | 1.3 us |

A Cypher row is what a binding's `query` call does: bind the parameter, begin a
read transaction, execute, read every row, free the result, commit. The two
property-read rows are timed 100 and 10 calls at a time, so their percentiles are
of those groups' means. Run-to-run ranges were within 5% for every row.

## Graph traversal

A graph of Person nodes with 5 outgoing `KNOWS` edges each to uniformly random
other nodes. Each query starts at a random node found through the `uid` index and
returns a count; the prepared query runs as in the lookups above. "Paths" is the
mean count returned, which is the work the query did.

| Query | 10K nodes, 50K edges: p50 | p99 | 100K nodes, 500K edges: p50 | p99 | Paths |
|-------|----:|----:|----:|----:|----:|
| `(a {uid: $uid})-[:KNOWS]->(b) RETURN count(b)` | 2.5 us | 5.4 us | 4.8 us | 7.2 us | 5 |
| two hops, `count(c)` | 4.4 us | 6.6 us | 8.8 us | 12 us | 25 |
| two hops, `count(DISTINCT c)` | 5.3 us | 5.7 us | 6.7 us | 7.3 us | 25 |
| three hops, `count(d)` | 14 us | 16 us | 21 us | 30 us | 125 |
| `-[:KNOWS*1..5]->(b) RETURN count(b)` | 472 us | 499 us | 600 us | 661 us | 3,904 |
| `lattice_edge_get_outgoing` (C API, read transaction included) | 0.63 us | 0.75 us | 0.75 us | 1.0 us | 5 |

A variable-length pattern enumerates paths, one at a time, so its cost follows the
number of paths (here 5 + 25 + 125 + 625 + 3,125), not the number of nodes reached:
about 0.12 to 0.15 us per path. See [Variable-Length Paths](../cypher/variable-length-paths.md).

## Full-text search

Documents of 50 words drawn from a 5,000-word vocabulary with a Zipf distribution
(a few words very common, most rare), indexed by a declared full-text index on
`Doc.text` as they were written (transactions of 1,000). Searches ask
`lattice_fts_search` for the top 10 by BM25, with query words chosen uniformly
among the 20th to the 2,000th most frequent, so a query word appears in between
about 0.3% and 27% of the documents.

| | 10,000 documents | 100,000 documents |
|---|---:|---:|
| One word: p50 / p99 | 8.5 us / 103 us | 104 us / 1.2 ms |
| Two words (both required): p50 / p99 | 9.0 us / 70 us | 78 us / 0.70 ms |
| Indexing rate | 29,800 docs/s | 28,200 docs/s |
| File size | 36 MB | 172 MB |

Ranking reads every posting of the query's words, so a search costs in proportion
to how many documents contain them: the p99 is the common words, which at 100,000
documents match tens of thousands. The file holds the text, the index (with word
positions, for phrase queries), and the changefeed's record of every write.

## Vector search

128-dimensional vectors, cosine distance, the default index (`m` 16,
`ef_construction` 200, `ef_search` 64), top 10. The vectors are clustered (one
centre per thousand vectors, at least ten, plus Gaussian noise of 0.1 per
dimension, normalised); each query is a stored vector plus noise of 0.05 per
dimension. Recall@10 is against an exact search over 200 queries. Vectors were
inserted with `lattice_batch_insert`, 1,000 per transaction; the searches ran in a
fresh process after reopening, 1,000 timed searches after 100 warm-up ones (three
runs per scale, one at 1M).

| Vectors | Insert rate | Search p50 | p99 | Recall@10 | File | Memory |
|--------:|------------:|-----------:|----:|----------:|-----:|-------:|
| 1,000 | 3,800/s | 35 us | 42 us | 100% | 1.2 MB | 3 MB |
| 10,000 | 2,800/s | 66 us | 73 us | 98.6% | 11 MB | 13 MB |
| 100,000 | 1,830/s | 122 us | 192 us | 98.3% | 108 MB | 108 MB |
| 1,000,000 | 224/s | 1.34 ms | 3.1 ms | 95.6% | 1,074 MB | 185 MB |

"Memory" is what opening the database and running the searches added to the
process's footprint (the figure Activity Monitor shows). It follows the file until
the file outgrows the 100 MB page cache, which bounds it. Run-to-run ranges were
within 5% at 1K to 100K; the 1M row is one run.

**The default cache is the limit at a million vectors.** Once the file outgrows
the 100 MB cache (at about 100,000 vectors), searches read pages back from the
operating system and inserts slow down steeply: building 250,000 vectors, the rate
fell from 1,500/s over the first 90,000 to 400/s by 230,000, where with a 2 GB
cache it held at about 1,450/s throughout. With the cache sized to the file
(one run each):

| Vectors | Cache | Build | Insert rate | Search p50 | p99 | Recall@10 | Memory |
|--------:|------:|------:|------------:|-----------:|----:|----------:|-------:|
| 250,000 | 100 MB (default) | 6.0 min | 693/s | | | | |
| 250,000 | 2 GB | 2.9 min | 1,425/s | | | | |
| 1,000,000 | 100 MB (default) | 75 min | 224/s | 1.34 ms | 3.1 ms | 95.6% | 185 MB |
| 1,000,000 | 2 GB, searching the default build | | | 0.21 ms | 0.46 ms | 95.6% | 1.1 GB |
| 1,000,000 | 2 GB, built and searched | 17.7 min | 939/s | 0.22 ms | 0.58 ms | 92.6% | 1.1 GB |

The two 1M builds hold the same vectors but are different graphs (an HNSW node's
level is random), and their recall at the default `ef_search` differs by three
points: at this scale, treat recall@10 at `ef_search` 64 as about 93–96% on this
data, and use 128 where the last points matter. Opening the 1M database took 4.0 s
with the default cache and 1.2 s with the larger one.

### ef_search

Mean latency and recall@10 for other `ef_search` values (`lattice_vector_search`'s
argument; 0 means the default, 64).

| ef_search | 10,000: mean | recall | 100,000: mean | recall | 1,000,000: mean | recall |
|----------:|----:|----:|----:|----:|----:|----:|
| 16 | 28 us | 92.0% | 55 us | 90.7% | 0.71 ms | 72.5% |
| 32 | 44 us | 97.0% | 86 us | 96.3% | 1.05 ms | 87.7% |
| 64 (default) | 67 us | 98.6% | 123 us | 98.3% | 1.54 ms | 95.6% |
| 128 | 100 us | 98.9% | 180 us | 98.5% | 2.0 ms | 98.4% |
| 256 | 154 us | 98.9% | 266 us | 98.5% | 2.5 ms | 98.5% |

The 1M column is with the default 100 MB cache. Recall stops rising at about
98.5–99% on this data: past `ef_search` 128, a larger search list finds nothing
more.

## Developer benchmarks

`zig build benchmark`, `vector-benchmark`, `graph-benchmark`, `sqlite-benchmark`,
`storage-benchmark` and `fts-benchmark` remain for working on the engine. They call
internal Zig paths directly, some with the log off, the graph ones over an
in-memory adjacency cache that queries, the C API and the bindings never read, and
the vector one against the HNSW index without the database around it. Their
figures are not comparable to the tables on this page and are not published.

Earlier versions of this page quoted those programs (a 0.13 us node lookup, a
0.65 us node creation, 0.83 ms vector search at 1M, an optimisation history of the
vector index). Those were measured on an Apple M1, on older releases, by that
method; they are withdrawn rather than carried forward.

## Reproducing

From the repository root, on an idle machine (Python 3 and the system SQLite are
needed; nothing else):

```bash
zig build capi-benchmark                         # every table above but the 1M rows (~15 min)
zig build capi-benchmark -- --suite vector-1m    # the 1M-vector row (~80 min; a 1 GB file, then a 2 GB cache)
zig build capi-benchmark -- --suite vector-cache # the 250K builds and the 1M build with a 2 GB cache (~30 min)
zig build capi-benchmark -- --list               # what each suite runs
zig build capi-benchmark -- --suite core --json now.json   # keep every run's numbers
```

Each row prints its median and range; `--json` keeps every run. One workload can be
run by hand, for example
`zig-out/bin/capi-bench commit_node --db /tmp/x.db --ops 5000`; it prints one
`RESULT` line. The bulk-load rows with 100,000 and 1,000,000 rows per transaction
were run that way, since the suite loads in transactions of 10,000:
`zig-out/bin/capi-bench bulk --db /tmp/x.db --n 1000000 --degree 2 --batch 100000`.

To compare two versions with the same program, build it against the other
checkout and compare the saved runs:

```bash
python3 tests/benchmark/capi_bench.py build-against ../latticedb-0.16.0 --out /tmp/cb-0160
python3 tests/benchmark/capi_bench.py --bin /tmp/cb-0160 --suite core --json old.json
zig build capi-benchmark -- --suite core --json new.json
python3 tests/benchmark/capi_bench.py compare old.json new.json
```
