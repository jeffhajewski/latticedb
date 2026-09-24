# Java

The Java binding is a Java 21+ wrapper over LatticeDB's C API through JNI.
`Database`, `Transaction`, and the embedding client implement `AutoCloseable`,
so use them with try-with-resources. Native failures are unchecked
`LatticeException`s, and query failures are `QueryException`s with structured
diagnostics.

## Installing

The Java binding currently builds from the repository checkout. It requires JDK
21 or newer, Maven, Zig, and a C compiler. Build the LatticeDB shared library
first, then let Maven compile the JNI bridge and stage both native libraries:

```bash
# From the repository root
zig build -Doptimize=ReleaseSafe

cd bindings/java
mvn test
```

Maven places the JNI bridge and `liblattice` together in `target/native`, which
the test suite loads automatically. Set `-Dlattice.lib.dir=...` to use a
different LatticeDB library directory, or `-Dlatticedb.native.dir=...` to use a
different staged native-library directory.

## Opening a database

```java
import io.latticedb.Database;
import io.latticedb.OpenOptions;

try (Database db = Database.open("graph.lattice",
        OpenOptions.defaults().create(true))) {
    // use db
}
```

`OpenOptions.defaults()` returns a builder with the engine defaults. Its options
are:

| Method | Default | What it does |
|--------|---------|--------------|
| `create(boolean)` | `false` | Create the file when it does not exist |
| `readOnly(boolean)` | `false` | Open without the ability to write |
| `cacheSizeMB(int)` | `100` | Memory budget for cached pages |
| `pageSize(int)` | `4096` | Page size in bytes |
| `enableVectors(boolean)` | `false` | Enable vector storage and indexing |
| `vectorDimensions(int)` | `128` | Number of values in each vector (1–4096) |
| `enableWal(boolean)` | `true` | Enable write-ahead logging |
| `enableAdjacencyCache(boolean)` | `false` | Keep an in-memory graph adjacency cache |
| `lock(boolean)` | `true` | Take a filesystem lock on the database |

To create a vector-enabled database, set both vector options before opening it:

```java
try (Database db = Database.open("knowledge.lattice",
        OpenOptions.defaults()
                .create(true)
                .enableVectors(true)
                .vectorDimensions(384))) {
    // store and search 384-dimensional vectors
}
```

### In-memory databases and snapshots

Pass `":memory:"` to open a database that exists only for the lifetime of the
handle:

```java
try (Database db = Database.open(":memory:", OpenOptions.defaults())) {
    // no database file is created
}
```

`serialize()` returns a complete snapshot, including pending WAL data. Restore
one with `deserialize`; the result is an independent in-memory database, and
the input byte array may be reused or discarded after the call.

```java
byte[] snapshot;
try (Database db = Database.open("graph.lattice")) {
    snapshot = db.serialize();
}

try (Database restored = Database.deserialize(snapshot)) {
    // restored.getPath() is "<deserialized>"
}
```

Serialization fails with `ErrorCode.LOCK_TIMEOUT` while a transaction is open.

A database file can have one open writer process. Opening takes a filesystem
lock, so a conflicting process receives a `LatticeException` with
`ErrorCode.DATABASE_LOCKED`. A read-only handle shares the lock with other
readers, but cannot open while a writer holds it. Set `lock(false)` only on a
filesystem where locking is unavailable; it removes protection and does not
make concurrent access safe.

## Writing

`write` manages a write transaction: it commits when the function returns and
rolls back when it throws. The function must return a value; return `null` when
there is no result to return.

```java
import io.latticedb.Node;
import java.util.List;
import java.util.Map;

db.write(tx -> {
    Node alice = tx.createNode(List.of("Person"),
            Map.of("name", "Alice", "email", "alice@example.com"));
    Node bob = tx.createNode(List.of("Person"), Map.of("name", "Bob"));

    tx.createEdge(alice.id(), bob.id(), "KNOWS", Map.of("since", 2020L));
    return null;
});
```

For explicit control, use a write transaction. An uncommitted transaction rolls
back when closed, so this remains safe when an exception leaves the block:

```java
try (Transaction tx = db.beginWrite()) {
    tx.setProperty(nodeId, "name", "Alicia");
    tx.commit();
}
```

Only one write transaction may be active at a time. A second writer fails
immediately with `ErrorCode.LOCK_TIMEOUT`; coordinate writers in the
application. See [One writer at a time](../guides/transactions.md#one-writer-at-a-time).

## Reading

`read` opens and closes a read-only transaction for the supplied function:

```java
db.read(tx -> {
    tx.getProperty(nodeId, "name").ifPresent(System.out::println);
    return null;
});
```

For manual lifetime control, use `beginRead()`. `getProperty` and
`getEdgeProperty` return `Optional<Object>`; an empty optional means that the
property is absent or has a stored `null` value. `getNode` similarly returns an
empty `Optional` when the node does not exist. The returned node contains its ID
and labels, but not a populated property map—read properties individually.

Properties use ordinary Java objects. Supported values are `Boolean`, `Long`,
`Double`, `String`, `byte[]`, `float[]`, nested `List` values, and nested
`Map<String, Object>` values, plus `null`. Any `Number` is accepted on input;
integer values are returned as `Long` and floating-point values as `Double`.

Within a transaction, you can create/delete nodes, add or remove labels, set
node or edge properties, and delete edges. `Node` and `Edge` are records; use
accessors such as `node.id()` and `edge.type()`.

## Queries

```java
import io.latticedb.QueryResult;
import java.util.Map;

QueryResult result = db.query(
        "MATCH (p:Person) WHERE p.email = $email RETURN p.name AS name",
        Map.of("email", "bob@example.com"));

result.rows().forEach(row -> System.out.println(row.get("name")));
```

`Database.query` automatically uses a write transaction for a write query and a
read transaction for every other query. Parameters are safer than interpolating
values into query text; `float[]` parameters are bound as vectors. A
`QueryResult` contains `columns()` and a `rows()` list of maps keyed by column
name.

Use `Transaction.query` when a query must see the transaction's uncommitted
changes:

```java
try (Transaction tx = db.beginWrite()) {
    tx.createNode(List.of("Person"), Map.of("name", "Cara"));
    QueryResult result = tx.query("MATCH (p:Person) RETURN p.name AS name");
    tx.commit();
}
```

## Traversal and property indexes

```java
try (Transaction tx = db.beginRead()) {
    var outgoing = tx.getOutgoingEdges(nodeId);
    var incoming = tx.getIncomingEdges(nodeId);
    var knows = tx.getOutgoingEdgesByType(nodeId, "KNOWS", 100);
}
```

`getIncomingEdgesByType` is the incoming counterpart. Use `scanEdges(type,
limit)` for administrative or rebuild work; pass `null` for every edge type.

Equality lookups require an explicit index. Create it on the database, then
look up IDs inside a transaction:

```java
db.createNodePropertyIndex("Person", "email");

try (Transaction tx = db.beginRead()) {
    var ids = tx.findNodesByLabelProperty(
            "Person", "email", "alice@example.com", 10);
}
```

The limit must be positive. A lookup whose index does not exist raises a
`LatticeException` with `ErrorCode.UNSUPPORTED` rather than silently scanning.
Use `createEdgePropertyIndex` and `findEdgesByTypeProperty` for edge
properties. `dropNodePropertyIndex` and `dropEdgePropertyIndex` remove the
corresponding indexes.

## Vector search

```java
import io.latticedb.Embedding;
import io.latticedb.VectorSearchOptions;

float[] query = Embedding.hashEmbed("graph database", 128);
var results = db.vectorSearch(query,
        VectorSearchOptions.defaults().k(10).efSearch(64));
```

Store a vector with `tx.setVector(nodeId, "embedding", vector)`. `k` controls
how many neighbours are returned; higher `efSearch` values search more of the
HNSW index, trading speed for recall. An `efSearch` of `0` uses the engine
default. Each `VectorSearchResult` exposes `nodeId()` and `distance()`.

For a transaction's own snapshot, call `tx.queryVector(vector, k, efSearch)`.
`tx.batchInsertVectors(label, vectors)` creates several vector-bearing nodes
with one shared label.

`Embedding.hashEmbed` is deterministic and useful for tests or examples, but
it does not make semantically similar text nearby. For a real embedding service,
use an `Embedding.Client` with an `Embedding.Config` and close the client when
finished. The client supports Ollama and OpenAI wire formats.

## Full-text search

```java
import io.latticedb.FTSSearchOptions;

var results = db.ftsSearch("graph database",
        FTSSearchOptions.defaults().limit(20));

var fuzzy = db.ftsSearchFuzzy("databse",
        FTSSearchOptions.defaults().limit(20).maxDistance(2).minTermLength(4));
```

Index text before searching it with `tx.ftsIndex(nodeId, text)`. Results are
BM25-scored `FTSSearchResult` records with `nodeId()` and `score()`.

Fuzzy search tolerates misspellings. `maxDistance` is the permitted Levenshtein
edit distance, while `minTermLength` prevents loose matching of short terms. A
value of `0` for either uses the engine default (2 and 4 respectively). The
transaction-scoped equivalents are `tx.ftsSearch` and `tx.ftsSearchFuzzy`.

## Durable streams

```java
db.write(tx -> {
    tx.publishStream("events", "signup", "alice joined");
    return null;
});

var records = db.readStream("events", 0, 10, 0);
for (var record : records) {
    System.out.println(record.sequence() + " " + record.kind()
            + " " + record.payload());
}
```

`readStream` accepts the stream name, the last sequence processed, a result
limit, and a timeout in milliseconds. It does not save a consumer position;
save the offset after handling the records in the transaction that handled them:

```java
db.write(tx -> {
    // handle records
    tx.setStreamOffset("events", "billing-worker", lastSequence);
    return null;
});

var offset = db.getStreamOffset("events", "billing-worker");
```

`getStreamOffset` returns `OptionalLong`; `getLastSequence` returns zero for an
empty stream. `publishStreamGetSequence` returns the sequence assigned to a new
record. `changes(afterSequence, limit, timeoutMs)` reads LatticeDB's built-in
graph-change stream. `trimStream` removes records through a sequence number;
nothing trims automatically.

## Query cache and errors

`cacheStats()` returns a `QueryCacheStats` record with `entries()`, `hits()`,
and `misses()`. Use `cacheClear()` to clear cached query plans.

Native operation failures throw `LatticeException`; inspect
`getErrorCode()` for its `ErrorCode` and `getNativeCode()` when preserving an
unrecognized native value matters:

```java
try {
    db.beginWrite();
} catch (LatticeException e) {
    if (e.getErrorCode() == ErrorCode.LOCK_TIMEOUT) {
        // another writer is active
    }
}
```

Cypher preparation or execution failures throw `QueryException`. Besides the
error code and message, it supplies `getStage()`, an optional
`getDiagnosticCode()`, and, when `hasLocation()` is true, a one-based line,
column, and source-span length.

## Where to go next

- [Quick Start](../getting-started/quickstart.md) for the same ideas in other languages
- [Cypher Overview](../cypher/overview.md) for the query language
- [Transactions and Durability](../guides/transactions.md) for the one-writer rule
