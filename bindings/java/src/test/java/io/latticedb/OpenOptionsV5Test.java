package io.latticedb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class OpenOptionsV5Test {
    @TempDir
    Path dir;

    private static void assertCode(ErrorCode code, Runnable action) {
        LatticeException ex = assertThrows(LatticeException.class, action::run);
        assertEquals(code, ex.getErrorCode());
    }

    @Test
    void euclideanIndexReopensWithoutVectorOptions() {
        String path = dir.resolve("euclid.db").toString();
        long[] ids = new long[2];
        try (Database db = Database.open(path, OpenOptions.defaults().create(true)
                .enableVectors(true).vectorDimensions(3)
                .vectorMetric(VectorMetric.EUCLIDEAN).vectorM(8).vectorEfConstruction(64))) {
            db.write(tx -> {
                Node a = tx.createNode(List.of("P"));
                Node b = tx.createNode(List.of("P"));
                tx.setVector(a.id(), "embedding", new float[]{3, 0, 0});
                tx.setVector(b.id(), "embedding", new float[]{0, 4, 0});
                ids[0] = a.id();
                ids[1] = b.id();
                return null;
            });
        }

        // No vector options: the file's index comes with it.
        try (Database db = Database.open(path, OpenOptions.defaults())) {
            List<VectorSearchResult> results = db.vectorSearch(
                    new float[]{0, 0, 0}, VectorSearchOptions.defaults().k(2));
            assertEquals(2, results.size());
            assertEquals(ids[0], results.get(0).nodeId());
            assertEquals(ids[1], results.get(1).nodeId());
            // L2 distance of vectors stored as given, not normalised.
            assertEquals(3.0, results.get(0).distance(), 1e-4);
            assertEquals(4.0, results.get(1).distance(), 1e-4);
        }
    }

    @Test
    void optionsThatContradictTheFileAreConfigMismatch() {
        String path = dir.resolve("mismatch.db").toString();
        Database.open(path, OpenOptions.defaults().create(true)
                .enableVectors(true).vectorDimensions(3)).close();

        assertCode(ErrorCode.CONFIG_MISMATCH, () -> Database.open(path,
                OpenOptions.defaults().enableVectors(true).vectorDimensions(4)).close());
        assertCode(ErrorCode.CONFIG_MISMATCH, () -> Database.open(path,
                OpenOptions.defaults().vectorMetric(VectorMetric.DOT_PRODUCT)).close());
        assertCode(ErrorCode.CONFIG_MISMATCH, () -> Database.open(path,
                OpenOptions.defaults().vectorM(12)).close());
        assertThrows(IllegalArgumentException.class, () -> OpenOptions.defaults().vectorM(64));
    }

    private static void addOriginAndThreeFour(Database db) {
        db.write(tx -> {
            Node a = tx.createNode(List.of("P"));
            Node b = tx.createNode(List.of("P"));
            tx.setVector(a.id(), "embedding", new float[]{0, 0});
            tx.setVector(b.id(), "embedding", new float[]{3, 4});
            return null;
        });
    }

    private static void assertSecondDistanceFive(Database db) {
        List<VectorSearchResult> results = db.vectorSearch(
                new float[]{0, 0}, VectorSearchOptions.defaults().k(2));
        assertEquals(2, results.size());
        // L2 distance from the origin to (3, 4); cosine could not give 5.
        assertEquals(5.0, results.get(1).distance(), 1e-4);
    }

    private byte[] serializedEuclideanDatabase() {
        try (Database db = Database.open(dir.resolve("deser-euclid.db").toString(),
                OpenOptions.defaults().create(true).enableVectors(true)
                        .vectorDimensions(2).vectorMetric(VectorMetric.EUCLIDEAN))) {
            addOriginAndThreeFour(db);
            return db.serialize();
        }
    }

    @Test
    void deserializeTakesTheIndexTheBytesRecord() {
        try (Database db = Database.deserialize(serializedEuclideanDatabase(),
                OpenOptions.defaults())) {
            assertSecondDistanceFive(db);
        }
    }

    @Test
    void deserializeWithContradictingMetricIsConfigMismatch() {
        byte[] bytes = serializedEuclideanDatabase();
        assertCode(ErrorCode.CONFIG_MISMATCH, () -> Database.deserialize(bytes,
                OpenOptions.defaults().vectorMetric(VectorMetric.COSINE)).close());
    }

    @Test
    void deserializeEnableVectorsConfiguresANewIndex() {
        byte[] bytes;
        try (Database db = Database.open(dir.resolve("deser-plain.db").toString(),
                OpenOptions.defaults().create(true))) {
            bytes = db.serialize();
        }
        try (Database db = Database.deserialize(bytes, OpenOptions.defaults()
                .enableVectors(true).vectorDimensions(2)
                .vectorMetric(VectorMetric.EUCLIDEAN))) {
            addOriginAndThreeFour(db);
            assertSecondDistanceFive(db);
        }
    }

    @Test
    void vectorDimensionsZeroMeansTheFiles() {
        String path = dir.resolve("zero-dims.db").toString();
        long id;
        try (Database db = Database.open(path, OpenOptions.defaults().create(true)
                .enableVectors(true).vectorDimensions(3))) {
            id = db.write(tx -> {
                Node n = tx.createNode(List.of("P"));
                tx.setVector(n.id(), "embedding", new float[]{1, 0, 0});
                return n.id();
            });
        }

        // Vectors enabled with dimensions 0: the file's 3, not the default 128.
        try (Database db = Database.open(path, OpenOptions.defaults()
                .enableVectors(true).vectorDimensions(0))) {
            List<VectorSearchResult> results = db.vectorSearch(
                    new float[]{1, 0, 0}, VectorSearchOptions.defaults().k(1));
            assertEquals(1, results.size());
            assertEquals(id, results.get(0).nodeId());
        }
    }

    @Test
    void vectorDimensionsZeroCannotCreateAnIndex() {
        String path = dir.resolve("zero-dims-new.db").toString();
        assertCode(ErrorCode.INVALID_ARG, () -> Database.open(path, OpenOptions.defaults()
                .create(true).enableVectors(true).vectorDimensions(0)).close());
    }

    @Test
    void fileMarkedForNewerWritersOpensOnlyReadOnly() throws IOException {
        String path = dir.resolve("newer-writer.db").toString();
        try (Database db = Database.open(path, OpenOptions.defaults().create(true))) {
            db.write(tx -> tx.createNode(List.of("Old")));
        }

        // min_writer_version is a little-endian u16 at byte 222 of the header.
        try (RandomAccessFile file = new RandomAccessFile(path, "rw")) {
            file.seek(222);
            file.write(new byte[]{5, 0});
        }

        assertCode(ErrorCode.NEEDS_NEWER_WRITER,
                () -> Database.open(path, OpenOptions.defaults()).close());
        try (Database db = Database.open(path, OpenOptions.defaults().readOnly(true))) {
            Object c = db.query("MATCH (n:Old) RETURN count(n) AS c").rows().get(0).get("c");
            assertEquals(1L, ((Number) c).longValue());
        }
    }

    @Test
    void fileInANewerFormatIsAVersionMismatch() throws Exception {
        String path = dir.resolve("newer-reader.db").toString();
        try (Database db = Database.open(path, OpenOptions.defaults().create(true))) {
            db.write(tx -> tx.createNode(List.of("Old")));
        }

        // min_reader_version is a little-endian u16 at byte 6 of the header.
        try (RandomAccessFile file = new RandomAccessFile(path, "rw")) {
            file.seek(6);
            file.write(new byte[]{5, 0});
        }

        assertCode(ErrorCode.VERSION_MISMATCH,
                () -> Database.open(path, OpenOptions.defaults()).close());
        assertCode(ErrorCode.VERSION_MISMATCH,
                () -> Database.open(path, OpenOptions.defaults().readOnly(true)).close());
    }

    @Test
    void syncOptionsAreAccepted() {
        String path = dir.resolve("sync.db").toString();
        List<OpenOptions> settings = List.of(
                OpenOptions.defaults().create(true).syncMode(SyncMode.NORMAL).fullFsync(false),
                OpenOptions.defaults().create(true).syncMode(SyncMode.FULL).fullFsync(true),
                OpenOptions.defaults().create(true));
        for (OpenOptions opts : settings) {
            try (Database db = Database.open(path, opts)) {
                db.write(tx -> tx.createNode(List.of("S")));
            }
        }
        try (Database db = Database.open(path, OpenOptions.defaults())) {
            Object c = db.query("MATCH (n:S) RETURN count(n) AS c").rows().get(0).get("c");
            assertEquals(3L, ((Number) c).longValue());
        }
    }

    @Test
    void plainDeleteIsRefusedAndDetachDeleteRemovesRelationships() {
        try (Database db = Database.open(dir.resolve("delete.db").toString(),
                OpenOptions.defaults().create(true))) {
            long[] ids = db.write(tx -> {
                Node a = tx.createNode(List.of("N"));
                Node b = tx.createNode(List.of("N"));
                Node c = tx.createNode(List.of("N"));
                tx.createEdge(a.id(), b.id(), "R");
                tx.createEdge(c.id(), a.id(), "R");
                return new long[]{a.id(), b.id(), c.id()};
            });

            db.write(tx -> {
                assertCode(ErrorCode.HAS_RELATIONSHIPS, () -> tx.deleteNode(ids[0]));
                assertTrue(tx.nodeExists(ids[0]));
                return null;
            });
            db.write(tx -> {
                tx.detachDeleteNode(ids[0]);
                return null;
            });
            db.read(tx -> {
                assertFalse(tx.nodeExists(ids[0]));
                assertTrue(tx.getIncomingEdges(ids[1]).isEmpty());
                assertTrue(tx.getOutgoingEdges(ids[2]).isEmpty());
                return null;
            });
            db.write(tx -> {
                tx.deleteNode(ids[1]);
                tx.deleteNode(987654L);
                tx.detachDeleteNode(987654L);
                return null;
            });
        }
    }

    @Test
    void commitThatReportsTxnAbortedEndsTheTransaction() {
        try (Database db = Database.open(dir.resolve("aborted-commit.db").toString(),
                OpenOptions.defaults().create(true))) {
            Transaction tx = db.beginWrite();
            tx.createNode(List.of("N"));
            // Retire the native handle behind the wrapper's back, so the
            // wrapper's own commit gets TXN_ABORTED from the C side.
            Native.commit(tx.handle());

            assertCode(ErrorCode.TXN_ABORTED, tx::commit);
            assertFalse(tx.isActive());
            // Nothing is left to roll back; this must not reach the retired
            // handle and throw again.
            assertDoesNotThrow(tx::close);
        }
    }

    @Test
    void edgeToMissingNodeIsNotFound() {
        try (Database db = Database.open(dir.resolve("edge.db").toString(),
                OpenOptions.defaults().create(true))) {
            db.write(tx -> {
                Node n = tx.createNode(List.of("N"));
                assertCode(ErrorCode.NOT_FOUND, () -> tx.createEdge(n.id(), 987654L, "R"));
                assertCode(ErrorCode.NOT_FOUND, () -> tx.createEdge(987654L, n.id(), "R"));
                return null;
            });
        }
    }
}
