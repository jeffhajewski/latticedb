package io.latticedb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class GraphOperationsTest {
    @TempDir
    Path dir;

    @Test
    void nodeCrudLabelsAndProperties() {
        try (Database db = Database.open(dir.resolve("g.db").toString(),
                OpenOptions.defaults().create(true))) {
            try (Transaction tx = db.beginWrite()) {
                Node alice = tx.createNode(List.of("Person"), Map.of("name", "Alice"));
                tx.createEdge(alice.id(), alice.id(), "SELF");

                // labels
                tx.addLabel(alice.id(), "Employee");
                assertEquals(List.of("Person", "Employee"),
                        tx.getNode(alice.id()).orElseThrow().labels());
                tx.removeLabel(alice.id(), "Employee");

                // property round-trip of every scalar type plus nesting
                Map<String, Object> props = Map.of(
                        "b", true,
                        "i", 42L,
                        "d", 3.14d,
                        "s", "hello",
                        "bytes", new byte[]{1, 2, 3},
                        "list", List.of(1L, "two", false),
                        "map", Map.of("nested", List.of("deep")));
                props.forEach((k, v) -> tx.setProperty(alice.id(), k, v));

                assertEquals(Boolean.TRUE, tx.getProperty(alice.id(), "b").orElseThrow());
                assertEquals(42L, ((Number) tx.getProperty(alice.id(), "i").orElseThrow()).longValue());
                assertEquals(3.14d, ((Number) tx.getProperty(alice.id(), "d").orElseThrow()).doubleValue(), 1e-9);
                assertEquals("hello", tx.getProperty(alice.id(), "s").orElseThrow());
                assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) tx.getProperty(alice.id(), "bytes").orElseThrow());
                Object list = tx.getProperty(alice.id(), "list").orElseThrow();
                assertEquals(List.of(1L, "two", false), list);
                Object map = tx.getProperty(alice.id(), "map").orElseThrow();
                assertEquals(Map.of("nested", List.of("deep")), map);
                assertTrue(tx.getProperty(alice.id(), "missing").isEmpty());

                tx.commit();

                // delete after commit boundary in a fresh write txn. Alice has
                // a SELF edge, so a plain delete is refused and leaves her be;
                // a detach delete takes the edge with her.
                try (Transaction tx2 = db.beginWrite()) {
                    LatticeException refused = assertThrows(LatticeException.class,
                            () -> tx2.deleteNode(alice.id()));
                    assertEquals(ErrorCode.HAS_RELATIONSHIPS, refused.getErrorCode());
                    assertTrue(tx2.nodeExists(alice.id()));
                    tx2.detachDeleteNode(alice.id());
                    tx2.commit();
                }
                try (Transaction tx3 = db.beginRead()) {
                    assertFalse(tx3.nodeExists(alice.id()));
                    assertTrue(tx3.getNode(alice.id()).isEmpty());
                }
            }
        }
    }

    @Test
    void largeCollectionsAndInvalidMapKeysAreHandledSafely() {
        try (Database db = Database.open(dir.resolve("collections.db").toString(),
                OpenOptions.defaults().create(true));
             Transaction tx = db.beginWrite()) {
            Node node = tx.createNode(List.of("Collection"));
            List<Integer> input = java.util.stream.IntStream.range(0, 2_000)
                    .boxed().toList();
            tx.setProperty(node.id(), "items", input);

            List<?> output = (List<?>) tx.getProperty(node.id(), "items").orElseThrow();
            assertEquals(2_000, output.size());
            assertEquals(0L, output.get(0));
            assertEquals(1_999L, output.get(1_999));

            LatticeException ex = assertThrows(LatticeException.class,
                    () -> tx.setProperty(node.id(), "invalid", Map.of(1, "value")));
            assertEquals(ErrorCode.INVALID_ARG, ex.getErrorCode());
        }
    }

    @Test
    void edgesTraversalAndProperties() {
        try (Database db = Database.open(dir.resolve("e.db").toString(),
                OpenOptions.defaults().create(true))) {
            try (Transaction tx = db.beginWrite()) {
                Node a = tx.createNode(List.of("N"), Map.of("name", "a"));
                Node b = tx.createNode(List.of("N"), Map.of("name", "b"));
                Node c = tx.createNode(List.of("N"), Map.of("name", "c"));

                Edge e1 = tx.createEdge(a.id(), b.id(), "LINKS", Map.of("weight", 5));
                tx.createEdge(b.id(), c.id(), "LINKS");
                Edge typed = tx.createEdge(a.id(), c.id(), "LIKES");
                tx.setEdgeProperty(typed.id(), "stars", 4);

                assertEquals(e1.sourceId(), a.id());
                assertEquals(e1.targetId(), b.id());

                List<Edge> out = tx.getOutgoingEdges(a.id());
                assertEquals(2, out.size());
                List<Edge> outLinks = tx.getOutgoingEdgesByType(a.id(), "LINKS", 0);
                assertEquals(1, outLinks.size());
                assertEquals("LINKS", outLinks.get(0).type());
                List<Edge> limited = tx.getOutgoingEdgesByType(a.id(), "LINKS", 0);
                assertEquals(1, limited.size());

                List<Edge> inc = tx.getIncomingEdges(b.id());
                assertEquals(1, inc.size());
                assertEquals(a.id(), inc.get(0).sourceId());

                assertEquals(5L, ((Number) tx.getEdgeProperty(e1.id(), "weight")
                        .orElseThrow()).longValue());
                tx.removeEdgeProperty(e1.id(), "weight");
                assertTrue(tx.getEdgeProperty(e1.id(), "weight").isEmpty());

                tx.deleteEdge(a.id(), b.id(), "LINKS");
                assertEquals(1, tx.getOutgoingEdges(a.id()).size());

                // scan
                assertTrue(tx.scanEdges(null, 0).size() >= 2);

                tx.commit();
            }
        }
    }

    @Test
    void propertyIndexesAndLookups() {
        try (Database db = Database.open(dir.resolve("idx.db").toString(),
                OpenOptions.defaults().create(true))) {
            try (Transaction tx = db.beginWrite()) {
                for (int i = 0; i < 5; i++) {
                    tx.createNode(List.of("Item"), Map.of("kind", i % 2 == 0 ? "even" : "odd"));
                }
                Edge template = tx.createEdge(
                        tx.createNode(List.of("X")).id(),
                        tx.createNode(List.of("X")).id(), "REL");
                tx.setEdgeProperty(template.id(), "weight", 7);
                tx.commit();
            }
            db.createNodePropertyIndex("Item", "kind");
            db.createEdgePropertyIndex("REL", "weight");
            try (Transaction tx = db.beginRead()) {
                List<Long> evens = tx.findNodesByLabelProperty("Item", "kind", "even", 100);
                assertEquals(3, evens.size());
                assertEquals(1,
                        tx.findEdgesByTypeProperty("REL", "weight", 7, 100).size());
            }
            db.dropNodePropertyIndex("Item", "kind");
            try (Transaction tx = db.beginRead()) {
                // Index dropped: lookup must now report unsupported.
                LatticeException ex = assertThrows(LatticeException.class,
                        () -> tx.findNodesByLabelProperty("Item", "kind", "even", 100));
                assertEquals(ErrorCode.UNSUPPORTED, ex.getErrorCode());
            }
            db.dropEdgePropertyIndex("REL", "weight");
        }
    }

    @Test
    void getNodesByLabelAndAllNodes() {
        try (Database db = Database.open(dir.resolve("l.db").toString(),
                OpenOptions.defaults().create(true))) {
            db.write(tx -> {
                tx.createNode(List.of("A"));
                tx.createNode(List.of("B"));
                tx.createNode(List.of("A"));
                return null;
            });
            assertEquals(2, db.getNodesByLabel("A").size());
            assertEquals(0, db.getNodesByLabel("Nope").size());
            assertEquals(1, db.getNodesByLabel("B").size());
        }
    }

    @Test
    void readOnlyTransactionRejectsWrites() {
        try (Database db = Database.open(dir.resolve("ro.db").toString(),
                OpenOptions.defaults().create(true))) {
            try (Transaction tx = db.beginRead()) {
                LatticeException ex = assertThrows(LatticeException.class,
                        () -> tx.createNode(List.of("P")));
                assertEquals(ErrorCode.READ_ONLY, ex.getErrorCode());
                ex = assertThrows(LatticeException.class, () -> tx.setProperty(1, "k", 1));
                assertEquals(ErrorCode.READ_ONLY, ex.getErrorCode());
                // reads still fine
                assertFalse(tx.nodeExists(9999));
            }
        }
    }

    @Test
    void inactiveTransactionRejected() {
        try (Database db = Database.open(dir.resolve("in.db").toString(),
                OpenOptions.defaults().create(true));
             Transaction tx = db.beginWrite()) {
            tx.commit();
            LatticeException ex = assertThrows(LatticeException.class,
                    () -> tx.createNode(List.of("P")));
            assertEquals(ErrorCode.TXN_ABORTED, ex.getErrorCode());
        }
    }

    @Test
    void rollbackDiscardsChanges() {
        try (Database db = Database.open(dir.resolve("rb.db").toString(),
                OpenOptions.defaults().create(true))) {
            try (Transaction tx = db.beginWrite()) {
                tx.createNode(List.of("T"));
                tx.rollback();
            }
            long count = db.read(t ->
                    (long) t.query("MATCH (n:T) RETURN count(n) AS c")
                            .rows().get(0).get("c"));
            assertEquals(0L, count);
        }
    }

    @Test
    void optionalImportUsedConsistently() {
        // sanity: getNode returns Optional
        try (Database db = Database.open(dir.resolve("o.db").toString(),
                OpenOptions.defaults().create(true))) {
            try (Transaction tx = db.beginRead()) {
                Optional<Node> empty = tx.getNode(12345);
                assertTrue(empty.isEmpty());
            }
        }
    }
}
