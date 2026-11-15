package io.latticedb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SearchTest {
    @TempDir
    Path dir;

    @Test
    void vectorSearchFindsExactMatch() {
        try (Database db = Database.open(dir.resolve("v.db").toString(),
                OpenOptions.defaults().create(true).enableVectors(true).vectorDimensions(4))) {
            long target = db.write(tx -> tx.batchInsertVectors("Vec", new float[][]{
                    {1, 0, 0, 0},
                    {0, 1, 0, 0},
                    {0, 0, 1, 0},
            })).get(0);

            List<VectorSearchResult> results = db.vectorSearch(
                    new float[]{1, 0, 0, 0}, VectorSearchOptions.defaults().k(2));
            assertFalse(results.isEmpty());
            assertEquals(target, results.get(0).nodeId());
            assertTrue(Math.abs(results.get(0).distance()) < 1e-3);
        }
    }

    @Test
    void getVectorAndRemoveVector() {
        String path = dir.resolve("crud.db").toString();
        long[] ids = new long[2];
        try (Database db = Database.open(path,
                OpenOptions.defaults().create(true).enableVectors(true).vectorDimensions(2))) {
            db.write(tx -> {
                Node node = tx.createNode(List.of("Doc"));
                Node other = tx.createNode(List.of("Doc"));
                ids[0] = node.id();
                ids[1] = other.id();
                assertTrue(tx.getVector(node.id(), "embedding").isEmpty());
                tx.setVector(node.id(), "embedding", new float[]{3, 4});
                tx.setVector(other.id(), "embedding", new float[]{0, 1});
                // Uncommitted: as written.
                assertArrayEquals(new float[]{3, 4}, tx.getVector(node.id(), "").orElseThrow());
                return null;
            });
            db.read(tx -> {
                // Committed, under the default cosine metric: normalised.
                assertArrayEquals(new float[]{0.6f, 0.8f},
                        tx.getVector(ids[0], "embedding").orElseThrow(), 1e-6f);
                LatticeException missing = assertThrows(LatticeException.class,
                        () -> tx.getVector(ids[0] + 1000, "embedding"));
                assertEquals(ErrorCode.NOT_FOUND, missing.getErrorCode());
                LatticeException otherKey = assertThrows(LatticeException.class,
                        () -> tx.getVector(ids[0], "title_vec"));
                assertEquals(ErrorCode.UNSUPPORTED, otherKey.getErrorCode());
                LatticeException readOnly = assertThrows(LatticeException.class,
                        () -> tx.removeVector(ids[0], "embedding"));
                assertEquals(ErrorCode.READ_ONLY, readOnly.getErrorCode());
                return null;
            });
            db.write(tx -> {
                tx.removeVector(ids[0], "embedding");
                assertTrue(tx.getVector(ids[0], "embedding").isEmpty());
                // Removing a vector the node does not have does nothing.
                tx.removeVector(ids[0], "");
                LatticeException missing = assertThrows(LatticeException.class,
                        () -> tx.removeVector(ids[0] + 1000, "embedding"));
                assertEquals(ErrorCode.NOT_FOUND, missing.getErrorCode());
                return null;
            });
            List<VectorSearchResult> results = db.vectorSearch(
                    new float[]{3, 4}, VectorSearchOptions.defaults().k(5));
            assertEquals(1, results.size());
            assertEquals(ids[1], results.get(0).nodeId());
        }

        // After reopening, the removal holds and the node can take a vector again.
        try (Database db = Database.open(path, OpenOptions.defaults())) {
            db.write(tx -> {
                assertTrue(tx.getVector(ids[0], "embedding").isEmpty());
                assertTrue(tx.nodeExists(ids[0]));
                tx.setVector(ids[0], "embedding", new float[]{1, 0});
                return null;
            });
            db.read(tx -> {
                assertArrayEquals(new float[]{1, 0}, tx.getVector(ids[0], "embedding").orElseThrow());
                return null;
            });
        }
    }

    @Test
    void vectorSearchInTransaction() {
        try (Database db = Database.open(dir.resolve("vt.db").toString(),
                OpenOptions.defaults().create(true).enableVectors(true).vectorDimensions(2))) {
            db.write(tx -> {
                Node n = tx.createNode(List.of("V"));
                tx.setVector(n.id(), "embedding", new float[]{0.5f, 0.5f});
                List<VectorSearchResult> hits =
                        tx.queryVector(new float[]{0.5f, 0.5f}, 1, 0);
                assertEquals(1, hits.size());
                assertEquals(n.id(), hits.get(0).nodeId());
                return null;
            });
        }
    }

    @Test
    void batchInsertVectors() {
        try (Database db = Database.open(dir.resolve("b.db").toString(),
                OpenOptions.defaults().create(true).enableVectors(true).vectorDimensions(3))) {
            List<Long> ids = db.write(tx ->
                    tx.batchInsertVectors("Bulk", new float[][]{{1, 1, 1}, {2, 2, 2}, {3, 3, 3}}));
            assertEquals(3, ids.size());
            assertEquals(3, db.getNodesByLabel("Bulk").size());
        }
    }

    @Test
    void edgeFtsSearchThroughCypher() {
        try (Database db = Database.open(dir.resolve("edgefts.db").toString(),
                OpenOptions.defaults().create(true))) {
            db.createEdgeFtsIndex("REVIEWED", "note");
            assertTrue(db.hasEdgeFtsIndex("REVIEWED", "note"));

            db.write(tx -> {
                Node alice = tx.createNode(List.of("Person"), Map.of("name", "Alice"));
                Node paper = tx.createNode(List.of("Paper"), Map.of("title", "Attention"));
                Node other = tx.createNode(List.of("Paper"), Map.of("title", "Sourdough"));
                Edge good = tx.createEdge(alice.id(), paper.id(), "REVIEWED");
                Edge weak = tx.createEdge(alice.id(), other.id(), "REVIEWED");
                tx.setEdgeProperty(good.id(), "note", "thorough and well argued");
                tx.setEdgeProperty(weak.id(), "note", "mostly about bread");
                return null;
            });

            QueryResult rows = db.query(
                    "MATCH (a:Person)-[x:REVIEWED]->(p:Paper) "
                            + "WHERE x.note @@ 'thorough' RETURN p.title AS t",
                    Map.of());
            assertEquals(1, rows.rows().size());
            assertEquals("Attention", rows.rows().get(0).get("t"));
        }
    }

    @Test
    void ftsSearchAndFuzzy() {
        try (Database db = Database.open(dir.resolve("f.db").toString(),
                OpenOptions.defaults().create(true))) {
            db.createNodeFtsIndex("Doc", "text");
            assertTrue(db.hasNodeFtsIndex("Doc", "text"));

            db.write(tx -> {
                Node a = tx.createNode(List.of("Doc"));
                tx.setProperty(a.id(), "text",
                        "The quick brown fox jumps over the lazy dog");
                Node b = tx.createNode(List.of("Doc"));
                tx.setProperty(b.id(), "text", "Completely different words appear here");
                return null;
            });

            List<FTSSearchResult> hits = db.ftsSearch("Doc", "text", "quick fox",
                    FTSSearchOptions.defaults());
            assertEquals(1, hits.size());
            assertEquals(1, hits.get(0).nodeId());
            assertTrue(hits.get(0).score() > 0);

            // typo tolerance: "quck" should match via fuzzy search
            List<FTSSearchResult> fuzzy = db.ftsSearchFuzzy("Doc", "text", "quck fox",
                    FTSSearchOptions.defaults());
            assertFalse(fuzzy.isEmpty());

            // A null options value consistently selects binding defaults.
            assertFalse(db.ftsSearchFuzzy("Doc", "text", "quck fox", null).isEmpty());
            try (Transaction tx = db.beginRead()) {
                assertFalse(tx.ftsSearchFuzzy("Doc", "text", "quck fox", null).isEmpty());
            }

            List<FTSSearchResult> none = db.ftsSearch("Doc", "text", "zebra",
                    FTSSearchOptions.defaults());
            assertTrue(none.isEmpty());
        }
    }

    @Test
    void hashEmbedIsDeterministic() {
        float[] a = Embedding.hashEmbed("hello", 16);
        float[] b = Embedding.hashEmbed("hello", 16);
        assertEquals(16, a.length);
        assertArrayEquals(a, b);
        assertFalse(java.util.Arrays.equals(a, Embedding.hashEmbed("world", 16)),
                "different inputs must produce different embeddings");
    }
}
