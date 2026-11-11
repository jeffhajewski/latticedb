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
(default true), `enableAdjacencyCache` and `lock` (default true). `:memory:` opens
an in-memory database, and `Database.deserialize(bytes)` opens one from the bytes
`db.serialize()` returned.

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
`ErrorCode.LOCK_TIMEOUT`.

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

A node has one vector: `setVector(nodeId, key, vector)` accepts a key but does not
use it, and Cypher reads the vector as `n.embedding`.

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

Any `Number`, `List` or `Map` is accepted on input.

## Errors

Errors are unchecked. `LatticeException` carries an `ErrorCode` (`getErrorCode()`)
and the native code (`getNativeCode()`). A failed query throws `QueryException`
(also unchecked) with the error code, the stage, the diagnostic code and the
source location. `DATABASE_LOCKED` means
another process holds the file; `LOCK_TIMEOUT` means another write transaction is
open in this one.

## More

[bindings/java/README.md](https://github.com/jeffhajewski/latticedb/blob/main/bindings/java/README.md)
has the full API overview, and
`bindings/java/src/main/java/io/latticedb/examples` a runnable knowledge-graph
example (`mvn -q compile exec:java@run-example -Dexec.args=/tmp/knowledge.db`).
