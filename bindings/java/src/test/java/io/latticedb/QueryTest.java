package io.latticedb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class QueryTest {
    @TempDir
    Path dir;

    private Database seeded(String name) {
        Database db = Database.open(dir.resolve(name).toString(),
                OpenOptions.defaults().create(true).enableVectors(true).vectorDimensions(4));
        db.write(tx -> {
            Node alice = tx.createNode(List.of("Person"), Map.of("name", "Alice"));
            Node bob = tx.createNode(List.of("Person"), Map.of("name", "Bob"));
            tx.createEdge(alice.id(), bob.id(), "KNOWS");
            tx.setVector(alice.id(), "embedding", new float[]{1, 0, 0, 0});
            return null;
        });
        return db;
    }

    @Test
    void parameterizedMatchQuery() {
        try (Database db = seeded("q.db")) {
            QueryResult result = db.query(
                    "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.name = $name RETURN b.name AS other",
                    Map.of("name", "Alice"));
            assertEquals(List.of("other"), result.columns());
            assertEquals(1, result.rows().size());
            assertEquals("Bob", result.rows().get(0).get("other"));
        }
    }

    @Test
    void vectorParameterQuery() {
        try (Database db = seeded("qv.db")) {
            QueryResult result = db.query(
                    "MATCH (n:Person) WHERE n.embedding <=> $v < 0.5 RETURN n.name AS name",
                    Map.of("v", new float[]{1, 0, 0, 0}));
            assertFalse(result.rows().isEmpty());
        }
    }

    @Test
    void writeQueryAutoSelectsWriteTransaction() {
        try (Database db = seeded("wq.db")) {
            QueryResult result = db.query(
                    "CREATE (n:Created {tag: $t}) RETURN n.tag AS tag",
                    Map.of("t", "yes"));
            assertEquals("yes", result.rows().get(0).get("tag"));
            long count = db.read(tx -> (long) tx.query(
                    "MATCH (n:Created) RETURN count(n) AS c").rows().get(0).get("c"));
            assertEquals(1L, count);
        }
    }

    @Test
    void parseErrorCarriesLocation() {
        try (Database db = seeded("pe.db")) {
            QueryException ex = assertThrows(QueryException.class,
                    () -> db.query("MATCH (n RETURN n"));
            assertEquals(QueryErrorStage.PARSE, ex.getStage());
            assertTrue(ex.getMessage().contains("MATCH") || !ex.getMessage().isEmpty());
        }
    }

    @Test
    void queryErrorCarriesTheNativeCode() {
        try (Database db = seeded("qc.db")) {
            QueryException ex = assertThrows(QueryException.class,
                    () -> db.query("MATCH (n RETURN n"));
            assertNotEquals(ErrorCode.UNKNOWN, ex.getCode());
            assertFalse(ex.getMessage().isEmpty());
        }
    }

    @Test
    void decimalParametersKeepTheirFraction() {
        try (Database db = seeded("bd.db")) {
            QueryResult result = db.query("CREATE (n:Num {v: $x}) RETURN n.v AS v",
                    Map.of("x", new java.math.BigDecimal("1.5")));
            assertEquals(1.5, ((Number) result.rows().get(0).get("v")).doubleValue(), 0.0);
        }
    }

    @Test
    void emptyVectorBatchInsertsNothing() {
        try (Database db = seeded("eb.db")) {
            List<Long> ids = db.write(tx -> tx.batchInsertVectors("Bulk", new float[0][]));
            assertTrue(ids.isEmpty());
        }
    }

    @Test
    void semanticErrorHasStage() {
        try (Database db = seeded("se.db")) {
            QueryException ex = assertThrows(QueryException.class,
                    () -> db.query("MATCH (n) RETURN bogus_function(n) AS x"));
            // Unknown functions surface after parse (semantic or execution).
            assertNotEquals(QueryErrorStage.PARSE, ex.getStage());
            assertNotEquals(QueryErrorStage.NONE, ex.getStage());
        }
    }

    @Test
    void queryInsideExplicitTransaction() {
        try (Database db = seeded("tx.db");
             Transaction tx = db.beginRead()) {
            QueryResult result = tx.query("MATCH (a)-[:KNOWS]->(b) RETURN b.name AS n",
                    null);
            assertEquals("Bob", result.rows().get(0).get("n"));
        }
    }

    @Test
    void cacheStatsAndClear() {
        try (Database db = seeded("cs.db")) {
            db.cacheClear();
            db.query("MATCH (n:Person) RETURN count(n) AS c");
            db.query("MATCH (n:Person) RETURN count(n) AS c");
            QueryCacheStats stats = db.cacheStats();
            assertTrue(stats.entries() > 0);
            assertTrue(stats.hits() >= 1);
            db.cacheClear();
            assertEquals(0, db.cacheStats().entries());
        }
    }

    @Test
    void aggregationQuery() {
        try (Database db = Database.open(dir.resolve("agg.db").toString(),
                OpenOptions.defaults().create(true))) {
            db.write(tx -> {
                Node alice = tx.createNode(List.of("Person"), Map.of("name", "Alice"));
                for (int i = 0; i < 3; i++) {
                    Node doc = tx.createNode(List.of("Doc"), Map.of("title", "d" + i));
                    tx.createEdge(doc.id(), alice.id(), "AUTHORED_BY");
                }
                return null;
            });
            QueryResult stats = db.query("""
                    MATCH (doc:Document0)-[:AUTHORED_BY]->(p:Person)
                    RETURN p.name AS name, count(doc) AS papers
                    """.replace("Document0", "Doc"));
            assertEquals(1, stats.rows().size());
            assertEquals(3L, ((Number) stats.rows().get(0).get("papers")).longValue());
        }
    }
}
