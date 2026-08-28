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
  transactions), `query`, `vectorSearch`, `ftsSearch`/`ftsSearchFuzzy`,
  `getNodesByLabel`, property-index create/drop, durable streams
  (`readStream`, `getLastSequence`, `getStreamOffset`, `changes`),
  query-cache stats/clear.
- `Transaction`: node/edge CRUD with properties (nested `List`/`Map` values,
  `byte[]`, `float[]` supported), labels, vectors, FTS indexing, equality-index
  lookups, batch vector insert, stream publish/offsets/trim, Cypher queries,
  transaction-scoped search (`queryVector`, `ftsSearch`).
- `Embedding`: deterministic `hashEmbed` plus native HTTP embedding `Client`
  (Ollama/OpenAI wire formats).
- Errors: unchecked `LatticeException` (with `ErrorCode`) and `QueryException`
  carrying stage/diagnostic-code/location diagnostics.

Property values map to Java types as follows: null ↔ `null`, bool ↔ `Boolean`,
int ↔ `Long`, float ↔ `Double`, string ↔ `String`, bytes ↔ `byte[]`,
vector ↔ `float[]`, list ↔ `List<Object>`, map ↔ `Map<String,Object>`
(any `Number`, `List`, or `Map` is accepted on input).
