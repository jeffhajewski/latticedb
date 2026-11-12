# Java

The Java binding (`io.latticedb:latticedb`, package `io.latticedb`) wraps the C
API through a small JNI bridge, `bindings/java/native/lattice_jni.c`. Its surface
mirrors the [Go binding](./go.md). It needs Java 21 or newer.

## Installing

The binding is not published to a Maven repository; build it from a checkout.
You need JDK 21+, Maven, Zig, and a C compiler for the JNI bridge:

```bash
# from the repository root: build the native library
zig build -Doptimize=ReleaseSafe

# build and test the binding; this compiles the JNI bridge into target/native
cd bindings/java
mvn test
```

The build stages `liblattice` and the JNI library into `target/native`. Point a
different build at them with `-Dlattice.lib.dir=` or `-Dlatticedb.native.dir=`.
CI builds and tests the Java binding on Linux and macOS; it has not been tested on
Windows.

## Opening a database

```java
import io.latticedb.*;

try (Database db = Database.open("graph.lattice",
        OpenOptions.defaults().create(true).enableVectors(true).vectorDimensions(128))) {
    // ...
}
```

`OpenOptions` has `create`, `readOnly`, `cacheSizeMB` (default 100), `pageSize`
(default 4096), `enableVectors`, `vectorDimensions` (default 128), `enableWal`
(default true), `enableAdjacencyCache` and `lock` (default true), and, passed
through `lattice_open_v5`, `syncMode(SyncMode.FULL | NORMAL)`,
`fullFsync(Boolean)`, `vectorMetric(VectorMetric.COSINE | EUCLIDEAN |
DOT_PRODUCT)`, `vectorM(int)`, `vectorEfConstruction(int)` and
`changefeedRetention(String)`, all unset by default (`null` or 0). `:memory:` opens an in-memory database, and
`Database.deserialize(bytes)` opens one from the bytes `db.serialize()`
returned; it takes the older options and ignores the v5 ones.

```java
try (Database db = Database.open("vectors.lattice",
        OpenOptions.defaults().create(true).enableVectors(true).vectorDimensions(2)
                .vectorMetric(VectorMetric.EUCLIDEAN).syncMode(SyncMode.NORMAL))) {
    // ...
}
```

A database file records its vector index: the dimensions, the metric, M and
ef_construction. A database that has one opens with it whether or not
`enableVectors` is set, so reopening needs no vector options, also after a
crash. `vectorMetric`, `vectorM` and `vectorEfConstruction` configure a new
index and are fixed once it exists; any of them that is set, and
`vectorDimensions` when `enableVectors(true)` is set (it defaults to 128), must
match the file, or opening throws `ErrorCode.CONFIG_MISMATCH`. `vectorDimensions(0)`
means the file's dimensions: it opens a database that has an index whatever its
size, and throws `ErrorCode.INVALID_ARG` where an index would have to be created.
Cosine stores vectors at unit length and returns the cosine distance; Euclidean
returns the L2 distance and dot product the negative dot product, both over
vectors stored as given; results come smallest first.

`SyncMode.NORMAL` returns from a commit once the operating system has it: it
survives the process being killed, but a power cut or OS crash can lose the most
recent commits. `fullFsync(true)` makes each sync on macOS ask the drive to
flush its cache. Unset, they defer to the `LATTICE_SYNC_MODE` and
`LATTICE_FULL_FSYNC` environment variables, else a full sync without
`F_FULLFSYNC`; a value you set wins over them.

`readOnly(true)` on a database that was not closed cleanly recovers it in
memory: the handle sees every committed transaction and nothing is written to
the files. A recovery that needs more than 64 MB plus four times the size of the
log and journal throws `ErrorCode.FULL`; open the database read-write once to
recover it on disk. A file that a newer release has marked as not writable by
this one opens only with `readOnly(true)`; a writable open throws
`ErrorCode.NEEDS_NEWER_WRITER` and writes nothing.

`changefeedRetention` sets how much of the changefeed the file keeps: `"all"`,
`"off"`, `"last:N"` or `"age:7d"`; unset keeps the file's policy (see
[The Changefeed](../guides/changefeed.md)). `db.changefeedStatus()` reports the
policy and the range of events held, `db.trimChangefeed()` applies the policy
in full now, and `db.changes(...)` throws `ChangesTrimmedException` (error code
`NOT_FOUND`) when retention has dropped the events after the sequence it is
given.

`close()` is refused with `ErrorCode.INVALID_ARG` while a transaction or result
from the database is still open. The `Database` then keeps its handle, so you can
finish what is open and call `close()` again. A close that fails with
`ErrorCode.IO` has still closed the database.

`db.checkpoint(CheckpointMode.TRUNCATE)` (or `checkpoint()`) folds the log into
the database file now and returns `CheckpointStats` (`pagesFlushed`,
`durationNs`, `checkpointLsn`, `walTruncated`); `PASSIVE` gives way if a changed
page is in use, `FULL` leaves the log in place. `db.setAutoCheckpoint(maxWalFrames,
minIntervalMs)` sets when commits checkpoint from then on, 0 frames for never.
Without the setter, the `LATTICE_AUTO_CHECKPOINT_FRAMES` environment variable sets the frame count (default 1000). A larger interval means fewer pauses, each longer, a longer `-wal` file and more to replay after a crash; see [Checkpointing](../configuration/durability.md#checkpointing).

## Transactions

`read` and `write` run a function in a transaction; `write` commits when the
function returns and rolls back if it throws:

```java
long aliceId = db.write(txn -> {
    Node alice = txn.createNode(List.of("Person"), Map.of("name", "Alice"));
    Node bob = txn.createNode(List.of("Person"), Map.of("name", "Bob"));
    txn.createEdge(alice.id(), bob.id(), "KNOWS");
    return alice.id();
});
```

For manual control, `beginRead()` and `beginWrite()` return a `Transaction`,
which is `AutoCloseable`: closing it without `commit()` rolls it back. Only one
write transaction can be open at a time; beginning a second one fails at once with
`ErrorCode.LOCK_TIMEOUT`. A `commit()` that throws `ErrorCode.TXN_ABORTED` has
ended the transaction, and closing or rolling it back afterwards does nothing;
after any other failure it stays open, and closing it rolls it back.

`Transaction.deleteNode` refuses a node that still has relationships with
`ErrorCode.HAS_RELATIONSHIPS` and deletes nothing, as Cypher's `DELETE` does;
`detachDeleteNode` deletes the node with all its relationships, as
`DETACH DELETE` does. `createEdge` throws `ErrorCode.NOT_FOUND` if either end
does not exist as the transaction sees it.

## Queries and search

```java
QueryResult rows = db.query(
        "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.name AS name",
        Map.of());
rows.rows().forEach(row -> System.out.println(row.get("name")));

db.createNodeFtsIndex("Person", "bio");
List<FTSSearchResult> hits =
        db.ftsSearch("Person", "bio", "graph retrieval", FTSSearchOptions.defaults().limit(5));

List<VectorSearchResult> near =
        db.vectorSearch(Embedding.hashEmbed("graph retrieval", 128),
                VectorSearchOptions.defaults().k(5));
```

`Database.query` picks a read or write transaction from the query itself.
`Transaction` has its own `query`, `ftsSearch`, `ftsSearchFuzzy` and `queryVector`
for searching inside a transaction, including its uncommitted writes.

A node has one vector, which Cypher reads as `n.embedding`. `setVector(nodeId,
key, vector)` takes a key that must be `"embedding"` (or `""`); any other key
fails with `ErrorCode.UNSUPPORTED`, and a vector with NaN or infinite components
with `ErrorCode.INVALID_ARG`.

## Values

| LatticeDB | Java |
|-----------|------|
| null | `null` |
| bool | `Boolean` |
| int | `Long` |
| float | `Double` |
| string | `String` |
| bytes | `byte[]` |
| vector | `float[]` |
| list | `List<Object>` |
| map | `Map<String, Object>` |

Any `Number`, `List` or `Map` is accepted on input. `Long`, `Integer`, `Short`,
`Byte`, `AtomicInteger`, `AtomicLong` and a `BigInteger` that fits in 64 bits
are stored as int (a larger `BigInteger` throws `ArithmeticException`); every
other `Number`, `BigDecimal` included, is stored as float.

## Errors

Errors are unchecked. `LatticeException` carries an `ErrorCode` (`getErrorCode()`)
and the native code (`getNativeCode()`). A failed query throws `QueryException`
(also unchecked) with the error code the call returned, the stage, the
diagnostic code and the source location; when the query has no diagnostic
message, the message is that code's. `DATABASE_LOCKED` means
another process holds the file; `LOCK_TIMEOUT` means another write transaction is
open in this one.

For `RETURN 1/0`, `getMessage()` is `Division by zero: 1 / 0`, `getCode()`
`ERROR`, `getStage()` `EXECUTION`, `getDiagnosticCode()` `DivisionByZero` and the
location line 1, column 9, length 1. Parse and semantic failures have
`INVALID_ARG` and execution failures `ERROR`, except that a `DELETE` of a node
with relationships has `HAS_RELATIONSHIPS` and a relationship created to a node
deleted earlier in the query `NOT_FOUND`, as `deleteNode` and `createEdge`
report them; a storage failure under a query has `IO` and an allocation failure
`OUT_OF_MEMORY`. `hasLocation()` is false when the failure does not belong to
one place in the query.

## More

[bindings/java/README.md](https://github.com/jeffhajewski/latticedb/blob/main/bindings/java/README.md)
has the full API overview, and
`bindings/java/src/main/java/io/latticedb/examples` a runnable knowledge-graph
example (`mvn -q compile exec:java@run-example -Dexec.args=/tmp/knowledge.db`).
