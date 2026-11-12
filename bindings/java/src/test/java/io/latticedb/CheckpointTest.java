package io.latticedb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CheckpointTest {
    private static final long PAGE_SIZE = 4096;

    @TempDir
    Path dir;

    private static long walSize(String dbPath) {
        Path wal = Path.of(dbPath + "-wal");
        try {
            return Files.exists(wal) ? Files.size(wal) : 0;
        } catch (IOException e) {
            throw new AssertionError("stat " + wal, e);
        }
    }

    private static void commitOne(Database db, long i) {
        db.write(tx -> tx.createNode(List.of("Item"), Map.of("i", i)));
    }

    private static void assertItems(String dbPath, int want) {
        try (Database db = Database.open(dbPath, OpenOptions.defaults())) {
            assertEquals(want, db.getNodesByLabel("Item").size());
        }
    }

    private static void assertCode(ErrorCode code, Runnable action) {
        LatticeException ex = assertThrows(LatticeException.class, action::run);
        assertEquals(code, ex.getErrorCode());
    }

    @Test
    void logGrowsWithAutomaticCheckpointsOffAndTruncateEmptiesIt() {
        String path = dir.resolve("off.db").toString();
        try (Database db = Database.open(path, OpenOptions.defaults().create(true))) {
            db.setAutoCheckpoint(0, 0);
            commitOne(db, 0);
            long first = walSize(path);
            for (long i = 1; i < 100; i++) {
                commitOne(db, i);
            }
            long grown = walSize(path);
            // One small commit is one 4096-byte frame, and nothing folds them.
            assertTrue(grown >= first + 99 * PAGE_SIZE,
                    "wal is " + grown + " bytes after 100 commits, started at " + first);

            CheckpointStats full = db.checkpoint(CheckpointMode.FULL);
            assertFalse(full.walTruncated());
            assertTrue(full.pagesFlushed() > 0);
            assertTrue(full.checkpointLsn() > 0);
            assertTrue(full.durationNs() >= 0);
            assertTrue(walSize(path) >= grown);

            CheckpointStats truncated = db.checkpoint(CheckpointMode.TRUNCATE);
            assertTrue(truncated.walTruncated());
            assertTrue(truncated.checkpointLsn() >= full.checkpointLsn());
            long after = walSize(path);
            assertTrue(after < grown && after <= 2 * PAGE_SIZE,
                    "wal is " + after + " bytes after truncate, was " + grown);

            // The no-argument form truncates.
            commitOne(db, 100);
            assertTrue(db.checkpoint().walTruncated());
        }
        assertItems(path, 101);
    }

    @Test
    void passiveLeavesTheLogInPlace() {
        String path = dir.resolve("passive.db").toString();
        try (Database db = Database.open(path, OpenOptions.defaults().create(true))) {
            db.setAutoCheckpoint(0, 0);
            for (long i = 0; i < 10; i++) {
                commitOne(db, i);
            }
            long before = walSize(path);
            CheckpointStats stats = db.checkpoint(CheckpointMode.PASSIVE);
            assertFalse(stats.walTruncated());
            assertTrue(walSize(path) >= before);
        }
    }

    @Test
    void smallIntervalKeepsTheLogShort() {
        String path = dir.resolve("four.db").toString();
        try (Database db = Database.open(path, OpenOptions.defaults().create(true))) {
            db.setAutoCheckpoint(4, 0);
            long largest = 0;
            for (long i = 0; i < 50; i++) {
                commitOne(db, i);
                largest = Math.max(largest, walSize(path));
            }
            assertTrue(largest > 0 && largest < 8 * PAGE_SIZE,
                    "largest wal was " + largest + " bytes");
        }
        assertItems(path, 50);
    }

    @Test
    void minimumIntervalHoldsAutomaticCheckpointsBack() {
        String path = dir.resolve("interval.db").toString();
        try (Database db = Database.open(path, OpenOptions.defaults().create(true))) {
            db.setAutoCheckpoint(4, 60_000);
            for (long i = 0; i < 20; i++) {
                commitOne(db, i);
            }
            // The first automatic checkpoint may run; a minute must pass
            // before another, so the log holds more than four frames.
            assertTrue(walSize(path) > 8 * PAGE_SIZE);
        }
    }

    @Test
    void readOnlyHandleRefusesCheckpointAndIgnoresTheSetter() {
        String path = dir.resolve("ro.db").toString();
        try (Database db = Database.open(path, OpenOptions.defaults().create(true))) {
            commitOne(db, 0);
        }
        try (Database db = Database.open(path, OpenOptions.defaults().readOnly(true))) {
            for (CheckpointMode mode : CheckpointMode.values()) {
                assertCode(ErrorCode.READ_ONLY, () -> db.checkpoint(mode));
            }
            assertCode(ErrorCode.READ_ONLY, db::checkpoint);
            db.setAutoCheckpoint(10, 0);
            assertEquals(1, db.getNodesByLabel("Item").size());
        }
    }

    @Test
    void badArgumentsAreRefusedBeforeTheLibrary() {
        String path = dir.resolve("args.db").toString();
        Database db = Database.open(path, OpenOptions.defaults().create(true));
        try {
            assertThrows(NullPointerException.class, () -> db.checkpoint(null));
            assertThrows(IllegalArgumentException.class, () -> db.setAutoCheckpoint(-1, 0));
            assertThrows(IllegalArgumentException.class, () -> db.setAutoCheckpoint(10, -5));
        } finally {
            db.close();
        }
        // Checked before the handle: a closed database still reports the
        // argument, not the closed handle.
        assertThrows(NullPointerException.class, () -> db.checkpoint(null));
        assertThrows(IllegalArgumentException.class, () -> db.setAutoCheckpoint(-1, 0));
        assertThrows(LatticeException.class, db::checkpoint);
        assertThrows(LatticeException.class, () -> db.setAutoCheckpoint(0, 0));
    }

    @Test
    void modeCodesMatchTheNativeEnum() {
        assertEquals(0, CheckpointMode.PASSIVE.code());
        assertEquals(1, CheckpointMode.FULL.code());
        assertEquals(2, CheckpointMode.TRUNCATE.code());
    }
}
