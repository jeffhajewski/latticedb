package io.latticedb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ChangefeedRetentionTest {
    @TempDir
    Path dir;

    /** Commits {@code count} transactions of one labelled node: two events each. */
    private static void commitNodes(Database db, int count) {
        for (int i = 0; i < count; i++) {
            db.write(tx -> tx.createNode(List.of("N")));
        }
    }

    private static long[] sequences(Database db, long after) {
        return db.changes(after, 100, 0).stream().mapToLong(StreamRecord::sequence).toArray();
    }

    @Test
    void policiesAreParsedBeforeAnythingOpens() {
        assertArrayEquals(new long[]{1, 0}, OpenOptions.parseChangefeedRetention("all"));
        assertArrayEquals(new long[]{4, 0}, OpenOptions.parseChangefeedRetention("OFF"));
        assertArrayEquals(new long[]{2, 1000}, OpenOptions.parseChangefeedRetention("last:1000"));
        assertArrayEquals(new long[]{3, 500}, OpenOptions.parseChangefeedRetention("age:500ms"));
        assertArrayEquals(new long[]{3, 90_000}, OpenOptions.parseChangefeedRetention("age:90s"));
        assertArrayEquals(new long[]{3, 7L * 86_400_000}, OpenOptions.parseChangefeedRetention("age:7d"));
        for (String bad : List.of("sometimes", "last:0", "last:", "last:-3", "age:7", "age:0s", "age:7w", "age:d")) {
            assertThrows(IllegalArgumentException.class,
                    () -> OpenOptions.defaults().changefeedRetention(bad), bad);
        }
        assertEquals("age:12h", OpenOptions.changefeedRetentionText(3, 12L * 3_600_000));
        assertEquals("last:5", OpenOptions.changefeedRetentionText(2, 5));
    }

    @Test
    void lastNKeepsTheNewestAndTellsAReaderThatFellBehind() {
        String path = dir.resolve("last.db").toString();
        try (Database db = Database.open(path,
                OpenOptions.defaults().create(true).changefeedRetention("last:4"))) {
            commitNodes(db, 5); // events 1..10
            assertEquals(6, db.trimChangefeed());
            assertEquals(new ChangefeedStatus("last:4", 7, 10), db.changefeedStatus());
            assertArrayEquals(new long[]{7, 8, 9, 10}, sequences(db, 0));
            assertArrayEquals(new long[]{7, 8, 9, 10}, sequences(db, 6));
            ChangesTrimmedException ex = assertThrows(ChangesTrimmedException.class,
                    () -> db.changes(3, 100, 0));
            assertEquals(ErrorCode.NOT_FOUND, ex.getErrorCode());
        }
    }

    @Test
    void theFileKeepsItsPolicyUntilAWritableOpenNamesAnother() {
        String path = dir.resolve("stored.db").toString();
        String[][] steps = {
                {"create", "age:7d", "age:7d"},
                {"", null, "age:7d"},
                {"readonly", "off", "age:7d"},
                {"", "all", "all"},
                {"", null, "all"},
        };
        for (String[] step : steps) {
            OpenOptions options = OpenOptions.defaults()
                    .create(step[0].equals("create"))
                    .readOnly(step[0].equals("readonly"))
                    .changefeedRetention(step[1]);
            try (Database db = Database.open(path, options)) {
                assertEquals(step[2], db.changefeedStatus().retention());
            }
        }
    }

    @Test
    void offWritesNothingAndSwitchingBackOnLeavesAGap() {
        String path = dir.resolve("off.db").toString();
        try (Database db = Database.open(path, OpenOptions.defaults().create(true))) {
            commitNodes(db, 2); // events 1..4
        }
        try (Database db = Database.open(path, OpenOptions.defaults().changefeedRetention("off"))) {
            commitNodes(db, 2);
            LatticeException ex = assertThrows(LatticeException.class, () -> db.changes(0, 100, 0));
            assertEquals(ErrorCode.UNSUPPORTED, ex.getErrorCode());
            db.trimChangefeed();
            assertEquals(new ChangefeedStatus("off", 0, 4), db.changefeedStatus());
        }
        try (Database db = Database.open(path, OpenOptions.defaults().changefeedRetention("all"))) {
            commitNodes(db, 1);
            assertThrows(ChangesTrimmedException.class, () -> db.changes(4, 100, 0));
            assertArrayEquals(new long[]{6, 7}, sequences(db, 0));
        }
    }

    @Test
    void trimIsRefusedWhileAWriteTransactionIsOpen() {
        String path = dir.resolve("busy.db").toString();
        try (Database db = Database.open(path,
                OpenOptions.defaults().create(true).changefeedRetention("last:1"))) {
            commitNodes(db, 2);
            try (Transaction txn = db.beginWrite()) {
                LatticeException ex = assertThrows(LatticeException.class, db::trimChangefeed);
                assertEquals(ErrorCode.LOCK_TIMEOUT, ex.getErrorCode());
                txn.rollback();
            }
        }
    }
}
