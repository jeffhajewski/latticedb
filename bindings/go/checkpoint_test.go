package latticedb

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const checkpointTestPageSize = 4096

func checkpointWALSize(t *testing.T, dbPath string) int64 {
	t.Helper()
	info, err := os.Stat(dbPath + "-wal")
	if errors.Is(err, os.ErrNotExist) {
		return 0
	}
	if err != nil {
		t.Fatalf("stat wal: %v", err)
	}
	return info.Size()
}

func commitOneItem(t *testing.T, db *DB, i int) {
	t.Helper()
	err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{
			Labels:     []string{"Item"},
			Properties: map[string]Value{"i": int64(i)},
		})
		return err
	})
	if err != nil {
		t.Fatalf("commit %d: %v", i, err)
	}
}

func countItems(t *testing.T, dbPath string, want int) {
	t.Helper()
	db, err := Open(dbPath, OpenOptions{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
	ids, err := db.GetNodesByLabel("Item")
	if err != nil {
		t.Fatalf("get nodes: %v", err)
	}
	if len(ids) != want {
		t.Fatalf("%d Item nodes after reopen, want %d", len(ids), want)
	}
}

func TestCheckpointLogGrowsWithAutomaticCheckpointsOffAndTruncateEmptiesIt(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "checkpoint-off.db")
	db, err := Open(dbPath, OpenOptions{Create: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := db.SetAutoCheckpoint(0, 0); err != nil {
		t.Fatalf("set auto checkpoint: %v", err)
	}
	commitOneItem(t, db, 0)
	first := checkpointWALSize(t, dbPath)
	for i := 1; i < 100; i++ {
		commitOneItem(t, db, i)
	}
	grown := checkpointWALSize(t, dbPath)
	// One small commit is one 4096-byte frame, and nothing folds them.
	if grown < first+99*checkpointTestPageSize {
		t.Fatalf("wal is %d bytes after 100 commits, started at %d", grown, first)
	}

	full, err := db.Checkpoint(CheckpointFull)
	if err != nil {
		t.Fatalf("full checkpoint: %v", err)
	}
	if full.WALTruncated {
		t.Fatalf("full checkpoint reported the log truncated: %+v", full)
	}
	if full.PagesFlushed == 0 || full.CheckpointLSN == 0 || full.Duration < 0 {
		t.Fatalf("full checkpoint stats = %+v", full)
	}
	if size := checkpointWALSize(t, dbPath); size < grown {
		t.Fatalf("full checkpoint shrank the wal from %d to %d bytes", grown, size)
	}

	truncated, err := db.Checkpoint(CheckpointTruncate)
	if err != nil {
		t.Fatalf("truncate checkpoint: %v", err)
	}
	if !truncated.WALTruncated {
		t.Fatalf("truncate checkpoint did not report the log truncated: %+v", truncated)
	}
	if truncated.CheckpointLSN < full.CheckpointLSN {
		t.Fatalf("lsn went backwards: %d after %d", truncated.CheckpointLSN, full.CheckpointLSN)
	}
	if size := checkpointWALSize(t, dbPath); size >= grown || size > 2*checkpointTestPageSize {
		t.Fatalf("wal is %d bytes after truncate, was %d", size, grown)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	countItems(t, dbPath, 100)
}

func TestCheckpointPassiveLeavesTheLogInPlace(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "checkpoint-passive.db")
	db, err := Open(dbPath, OpenOptions{Create: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := db.SetAutoCheckpoint(0, 0); err != nil {
		t.Fatalf("set auto checkpoint: %v", err)
	}
	for i := 0; i < 10; i++ {
		commitOneItem(t, db, i)
	}
	before := checkpointWALSize(t, dbPath)
	stats, err := db.Checkpoint(CheckpointPassive)
	if err != nil {
		t.Fatalf("passive checkpoint: %v", err)
	}
	if stats.WALTruncated {
		t.Fatalf("passive checkpoint reported the log truncated: %+v", stats)
	}
	if size := checkpointWALSize(t, dbPath); size < before {
		t.Fatalf("passive checkpoint shrank the wal from %d to %d bytes", before, size)
	}
}

func TestCheckpointSmallIntervalKeepsTheLogShort(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "checkpoint-four.db")
	db, err := Open(dbPath, OpenOptions{Create: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := db.SetAutoCheckpoint(4, 0); err != nil {
		t.Fatalf("set auto checkpoint: %v", err)
	}
	var largest int64
	for i := 0; i < 50; i++ {
		commitOneItem(t, db, i)
		if size := checkpointWALSize(t, dbPath); size > largest {
			largest = size
		}
	}
	if largest == 0 || largest >= 8*checkpointTestPageSize {
		t.Fatalf("largest wal was %d bytes, want under %d", largest, 8*checkpointTestPageSize)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	countItems(t, dbPath, 50)
}

func TestCheckpointMinimumIntervalHoldsAutomaticCheckpointsBack(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "checkpoint-interval.db")
	db, err := Open(dbPath, OpenOptions{Create: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := db.SetAutoCheckpoint(4, time.Minute); err != nil {
		t.Fatalf("set auto checkpoint: %v", err)
	}
	for i := 0; i < 20; i++ {
		commitOneItem(t, db, i)
	}
	// The first automatic checkpoint may run; a minute must pass before
	// another, so the log holds more than four frames by the end.
	if size := checkpointWALSize(t, dbPath); size <= 8*checkpointTestPageSize {
		t.Fatalf("wal is %d bytes; the minimum interval did not hold checkpoints back", size)
	}
}

func TestCheckpointReadOnlyHandle(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "checkpoint-ro.db")
	db, err := Open(dbPath, OpenOptions{Create: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	commitOneItem(t, db, 0)
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db, err = Open(dbPath, OpenOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("open read-only: %v", err)
	}
	defer db.Close()
	for _, mode := range []CheckpointMode{CheckpointPassive, CheckpointFull, CheckpointTruncate} {
		_, err := db.Checkpoint(mode)
		wantCode(t, "checkpoint "+mode.String()+" on a read-only handle", err, ErrorReadOnly)
	}
	// The setter has no effect on a read-only handle, and says nothing.
	if err := db.SetAutoCheckpoint(10, 0); err != nil {
		t.Fatalf("set auto checkpoint on a read-only handle: %v", err)
	}
}

func TestCheckpointBadArgumentsAreRefusedBeforeTheLibrary(t *testing.T) {
	// A closed database answers ErrDatabaseClosed once it reaches the library,
	// so ErrorInvalidArg here shows the argument was checked first.
	var closed *DB
	_, err := closed.Checkpoint(CheckpointMode(3))
	wantCode(t, "unknown mode", err, ErrorInvalidArg)
	_, err = closed.Checkpoint(CheckpointMode(-1))
	wantCode(t, "negative mode", err, ErrorInvalidArg)
	err = closed.SetAutoCheckpoint(4, -time.Millisecond)
	wantCode(t, "negative interval", err, ErrorInvalidArg)

	if _, err := closed.Checkpoint(CheckpointTruncate); err != ErrDatabaseClosed {
		t.Fatalf("checkpoint on a closed database = %v, want ErrDatabaseClosed", err)
	}
	if err := closed.SetAutoCheckpoint(0, 0); err != ErrDatabaseClosed {
		t.Fatalf("set auto checkpoint on a closed database = %v, want ErrDatabaseClosed", err)
	}

	dbPath := filepath.Join(t.TempDir(), "checkpoint-args.db")
	db, err := Open(dbPath, OpenOptions{Create: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	_, err = db.Checkpoint(CheckpointMode(7))
	wantCode(t, "unknown mode on an open database", err, ErrorInvalidArg)
	err = db.SetAutoCheckpoint(4, -time.Second)
	wantCode(t, "negative interval on an open database", err, ErrorInvalidArg)
}

func TestCheckpointIntervalRoundsUpToWholeMilliseconds(t *testing.T) {
	cases := []struct {
		in   time.Duration
		want uint64
	}{
		{0, 0},
		{time.Nanosecond, 1},
		{999 * time.Microsecond, 1},
		{time.Millisecond, 1},
		{time.Millisecond + time.Nanosecond, 2},
		{1500 * time.Microsecond, 2},
		{time.Second, 1000},
	}
	for _, c := range cases {
		if got := checkpointIntervalMS(c.in); got != c.want {
			t.Errorf("checkpointIntervalMS(%v) = %d, want %d", c.in, got, c.want)
		}
	}
}
