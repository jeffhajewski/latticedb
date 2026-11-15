# LatticeDB Java Bindings

Java 21+ bindings for LatticeDB via JNI, mirroring the Go binding's API surface.

## Build & Test

Prerequisites: JDK 21+, Maven, `zig` (to build `liblattice`), a C compiler (`cc`).

```bash
# from the repository root: build the native library once
zig build -Doptimize=ReleaseSafe

# build + test the Java bindings (compiles the JNI bridge into target/native)
cd bindings/java
mvn test
```

The build compiles `native/lattice_jni.c` against `zig-out/lib/liblattice.dylib|so`,
stages both libraries into `target/native`, and tests load them from there
(override with `-Dlattice.lib.dir=` or `-Dlatticedb.native.dir=`).

The Java binding is not published to Maven Central; build it from source as above.
CI builds and tests it on Linux and macOS only; it has not been tested on Windows.

## Usage

```java
import io.latticedb.*;
import java.util.List;
import java.util.Map;

try (Database db = Database.open("knowledge.db",
        OpenOptions.defaults().create(true).enableVectors(true).vectorDimensions(128))) {

    // Full-text search reads a declared index over the property holding the text.
    db.createNodeFtsIndex("Person", "bio");

    try (Transaction txn = db.beginWrite()) {
        Node alice = txn.createNode(List.of("Person"), Map.of("name", "Alice"));
        Node bob = txn.createNode(List.of("Person"),
                Map.of("name", "Bob", "bio", "some document text"));
        txn.createEdge(alice.id(), bob.id(), "KNOWS");
        txn.setVector(alice.id(), "embedding", Embedding.hashEmbed("text", 128));
        txn.commit();
    }

    // Auto-selects read/write transaction mode from the query itself.
    QueryResult rows = db.query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.name AS name",
            Map.of());
    rows.rows().forEach(row -> System.out.println(row.get("name")));

    List<FTSSearchResult> hits = db.ftsSearch("Person", "bio", "document", FTSSearchOptions.defaults());
    List<VectorSearchResult> near = db.vectorSearch(Embedding.hashEmbed("text", 128),
            VectorSearchOptions.defaults().k(5));
}

// A snapshot can be stored anywhere and reopened without a file path.
byte[] snapshot;
try (Database db = Database.open("knowledge.db")) {
    snapshot = db.serialize();
}
try (Database restored = Database.deserialize(snapshot)) {
    // use the independent in-memory database
}
```

Run the complete knowledge-graph example after building the shared library:

```bash
mvn -q compile exec:java@run-example -Dexec.args=/tmp/knowledge.db
```

## API overview

- `Database`: open/close, serialize/deserialize, `beginRead`/`beginWrite`, `read`/`write` (auto-managed
  transactions), `query`, `vectorSearch`, full-text index create/drop/exists for node and
  edge properties, `ftsSearch`/`ftsSearchFuzzy`,
  `getNodesByLabel`, property-index create/drop, durable streams
  (`readStream`, `getLastSequence`, `getStreamOffset`, `changes`), changefeed
  retention (`changefeedStatus`, `trimChangefeed`),
  query-cache stats/clear, `checkpoint`/`setAutoCheckpoint`.
- `Transaction`: node/edge CRUD with properties (nested `List`/`Map` values,
  `byte[]`, `float[]` supported), labels, vectors (`setVector`, `getVector`,
  `removeVector`), equality-index
  lookups, batch vector insert, stream publish/offsets/trim, Cypher queries,
  transaction-scoped search (`queryVector`, `ftsSearch`).
- `Embedding`: deterministic `hashEmbed` plus native HTTP embedding `Client`
  (Ollama/OpenAI wire formats).
- Errors: unchecked `LatticeException` (with `ErrorCode`) and `QueryException`
  carrying stage/diagnostic-code/location diagnostics.
- `OpenOptions` also takes `syncMode(SyncMode.FULL | NORMAL)`, `fullFsync(Boolean)`,
  `vectorMetric(VectorMetric.COSINE | EUCLIDEAN | DOT_PRODUCT)`, `vectorM(int)`,
  `vectorEfConstruction(int)` and `changefeedRetention(String)`, all unset by
  default (`null` or 0), passed through `lattice_open_v5`. `SyncMode.NORMAL` returns from a commit once the operating
  system has it: it survives the process being killed, but a power cut or OS crash
  can lose the most recent commits. Unset sync settings defer to the
  `LATTICE_SYNC_MODE` and `LATTICE_FULL_FSYNC` environment variables, else a full
  sync without `F_FULLFSYNC`. `Database.deserialize(bytes, options)` takes the
  same options through `lattice_deserialize_v5`, with the same meaning, apart from
  `create` and `readOnly`, which it ignores: bytes that hold a vector index open
  with the configuration they record, any vector option that is set must match it
  (`ErrorCode.CONFIG_MISMATCH`), and `enableVectors(true)` with the vector options
  creates and configures an index in bytes that have none.
- A database file records its vector index (dimensions, metric, M,
  ef_construction), and a database that has one opens with it whether or not
  `enableVectors` is set, so reopening needs no vector options. Any vector option
  that is set, and `vectorDimensions` when `enableVectors(true)` is set (it
  defaults to 128), must match the file or opening throws
  `ErrorCode.CONFIG_MISMATCH`. `vectorDimensions(0)` with `enableVectors(true)`
  means the file's: it opens a database that has an index whatever its size, and
  throws `ErrorCode.INVALID_ARG` where an index would have to be created. A file that a newer release has marked as not
  writable by this one opens with `readOnly(true)`; a writable open throws
  `ErrorCode.NEEDS_NEWER_WRITER` and writes nothing. A file in a format too new
  for this release to read at all throws `ErrorCode.VERSION_MISMATCH`, read-only
  or not. Cosine stores vectors at unit length and returns
  cosine distance; Euclidean returns the L2 distance and dot product the negative
  dot product, both over vectors stored as given; results come smallest first.
- A commit is durable once it is in the log (the `-wal` file beside the
  database); a checkpoint writes the pages the log's commits changed into the
  database file. `Database.checkpoint(CheckpointMode)` does it now and returns
  `CheckpointStats` (`pagesFlushed`, `durationNs`, `checkpointLsn`,
  `walTruncated`); `checkpoint()` means `TRUNCATE`. `PASSIVE` writes the changed
  pages unless one is in use (then nothing, 0 pages) and leaves the log in place;
  `FULL` writes every changed page and leaves the log in place; `TRUNCATE` does
  what `FULL` does and then empties the log, the only mode that bounds its size.
  A read-only database throws `ErrorCode.READ_ONLY`, and a transaction that
  already has records in the log `ErrorCode.LOCK_TIMEOUT`. By default a commit
  checkpoints once the log holds 1000 frames (one frame per page, 4096 bytes);
  the `LATTICE_AUTO_CHECKPOINT_FRAMES` environment variable sets another default
  (`0` for none), and `setAutoCheckpoint(maxWalFrames, minIntervalMs)` wins over
  it from then on: a commit checkpoints once the log holds `maxWalFrames` frames
  and no sooner than `minIntervalMs` after the last automatic one; 0 frames
  switches automatic checkpoints off, and `close` still folds the log into the
  file. It has no effect on a read-only database, and a negative argument throws
  `IllegalArgumentException`. A larger interval means fewer checkpoints, each a
  longer pause for the commit that runs it, a longer `-wal` file and more to
  replay after a crash; a smaller one means shorter pauses, more often.
- `cacheSizeMB` unset (0) leaves the cache size to the `LATTICE_BUFFER_POOL_MB`
  environment variable if it is set, else 100 MB; a size set here wins.
  `enableWal(false)` logs nothing: transactions, rollback, streams and the
  changefeed work as with the log, but a commit reaches the file at the next
  flush (`close()`, or when half the page cache holds changes) rather than when
  it returns. `enableAdjacencyCache` is accepted for compatibility and changes
  neither the speed nor the results of queries.
- `changefeedRetention` sets how much of the changefeed the file keeps: `"all"`
  (the default), `"off"`, `"last:N"` or `"age:7d"`; unset keeps the file's
  policy. `db.changefeedStatus()` reports the policy and the range of events
  held, and `db.trimChangefeed()` applies the policy in full now. `changes(...)`
  throws `ChangesTrimmedException` (`NOT_FOUND`) when retention has dropped the
  events after the sequence it is given, and `ErrorCode.UNSUPPORTED` when the
  policy is `"off"`. A transaction's events are its net effect (a property set
  several times is one event), in an order that can be replayed as it comes: the
  edges it deletes, the nodes that exist after it, the edges that exist after
  it, then the nodes it deletes, each group by ascending id.
- `Transaction.deleteNode` refuses a node that still has relationships with
  `ErrorCode.HAS_RELATIONSHIPS` and deletes nothing, as Cypher's `DELETE` does;
  `detachDeleteNode` deletes the node with all its relationships, as
  `DETACH DELETE` does. `createEdge` throws `ErrorCode.NOT_FOUND` if either end
  does not exist as the transaction sees it.
- A `commit()` that throws `ErrorCode.TXN_ABORTED` has ended the transaction;
  closing or rolling it back afterwards does nothing. Any other commit failure
  leaves it open, and closing it rolls it back.
- `readOnly(true)` on a database that was not closed cleanly recovers it in
  memory: the handle sees every committed transaction and nothing is written to
  the files. A recovery that needs more than 64 MB plus four times the size of
  the log and journal throws `ErrorCode.FULL`; open the database read-write once
  to recover it on disk.
- A failed query throws `QueryException` with the code the call returned, the
  stage, the diagnostic code and the location: `RETURN 1/0` gives
  `Division by zero: 1 / 0`, `ERROR`, `EXECUTION`, `DivisionByZero`, line 1,
  column 9. Parse and semantic failures have `INVALID_ARG`; a `DELETE` of a node
  with relationships has `HAS_RELATIONSHIPS` and a relationship created to a
  node deleted earlier in the query `NOT_FOUND`; a storage failure under a query
  has `IO` and an allocation failure `OUT_OF_MEMORY`.

Property values map to Java types as follows: null ↔ `null`, bool ↔ `Boolean`,
int ↔ `Long`, float ↔ `Double`, string ↔ `String`, bytes ↔ `byte[]`,
vector ↔ `float[]`, list ↔ `List<Object>`, map ↔ `Map<String,Object>`
(any `Number`, `List`, or `Map` is accepted on input; the fixed-width integer
types and a `BigInteger` that fits in 64 bits are stored as int, every other
`Number`, `BigDecimal` included, as float).
