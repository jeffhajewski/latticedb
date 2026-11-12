package latticedb

import (
	"errors"
	"path/filepath"
	"testing"
)

// commitNodes commits count transactions of one labelled node: two events
// each.
func commitNodes(t *testing.T, db *DB, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		if err := db.Update(func(tx *Tx) error {
			_, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"N"}})
			return err
		}); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}
}

func changeSequences(t *testing.T, db *DB, after uint64) []uint64 {
	t.Helper()
	records, err := db.Changes(after, 100, 0)
	if err != nil {
		t.Fatalf("changes after %d: %v", after, err)
	}
	out := make([]uint64, 0, len(records))
	for _, r := range records {
		out = append(out, r.Sequence)
	}
	return out
}

func equalSequences(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func openForTest(t *testing.T, path string, opts OpenOptions) *DB {
	t.Helper()
	db, err := Open(path, opts)
	if err != nil {
		t.Fatalf("open %+v: %v", opts, err)
	}
	return db
}

func TestChangefeedRetentionPolicies(t *testing.T) {
	cases := map[string]struct {
		mode  uint8
		value uint64
	}{
		"":          {0, 0},
		"all":       {1, 0},
		"OFF":       {4, 0},
		"last:1000": {2, 1000},
		"age:500ms": {3, 500},
		"age:90s":   {3, 90_000},
		"age:7d":    {3, 7 * 86_400_000},
	}
	for policy, want := range cases {
		mode, value, err := changefeedRetentionCode(policy)
		if err != nil || mode != want.mode || value != want.value {
			t.Errorf("%q: got %d, %d, %v; want %d, %d", policy, mode, value, err, want.mode, want.value)
		}
	}
	for _, policy := range []string{"sometimes", "last:0", "last:", "last:-3", "age:7", "age:0s", "age:7w", "age:d"} {
		if _, _, err := changefeedRetentionCode(policy); !errors.Is(err, &Error{Code: ErrorInvalidArg}) {
			t.Errorf("%q: got %v, want ErrorInvalidArg", policy, err)
		}
	}
	if _, err := Open(filepath.Join(t.TempDir(), "bad.db"), OpenOptions{Create: true, ChangefeedRetention: "last:0"}); !errors.Is(err, &Error{Code: ErrorInvalidArg}) {
		t.Fatalf("open with a bad policy: %v", err)
	}
}

func TestChangefeedKeepLast(t *testing.T) {
	db := openForTest(t, filepath.Join(t.TempDir(), "last.db"), OpenOptions{Create: true, ChangefeedRetention: "last:4"})
	defer db.Close()
	commitNodes(t, db, 5) // events 1..10

	removed, err := db.TrimChangefeed()
	if err != nil || removed != 6 {
		t.Fatalf("trim: %d, %v", removed, err)
	}
	status, err := db.ChangefeedStatus()
	if err != nil || status != (ChangefeedStatus{Retention: "last:4", OldestSequence: 7, LastSequence: 10}) {
		t.Fatalf("status: %+v, %v", status, err)
	}
	if got := changeSequences(t, db, 0); !equalSequences(got, []uint64{7, 8, 9, 10}) {
		t.Fatalf("from the start: %v", got)
	}
	if got := changeSequences(t, db, 6); !equalSequences(got, []uint64{7, 8, 9, 10}) {
		t.Fatalf("after 6: %v", got)
	}
	if _, err := db.Changes(3, 100, 0); !errors.Is(err, ErrChangesTrimmed) {
		t.Fatalf("after a dropped event: %v", err)
	}
}

func TestChangefeedPolicyIsRecorded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stored.db")
	retention := func(opts OpenOptions) string {
		db := openForTest(t, path, opts)
		defer db.Close()
		status, err := db.ChangefeedStatus()
		if err != nil {
			t.Fatalf("status: %v", err)
		}
		return status.Retention
	}
	steps := []struct {
		opts OpenOptions
		want string
	}{
		{OpenOptions{Create: true, ChangefeedRetention: "age:7d"}, "age:7d"},
		{OpenOptions{}, "age:7d"},
		{OpenOptions{ReadOnly: true, ChangefeedRetention: "off"}, "age:7d"},
		{OpenOptions{ChangefeedRetention: "all"}, "all"},
		{OpenOptions{}, "all"},
	}
	for i, step := range steps {
		if got := retention(step.opts); got != step.want {
			t.Fatalf("step %d: %q, want %q", i, got, step.want)
		}
	}
}

func TestChangefeedOff(t *testing.T) {
	path := filepath.Join(t.TempDir(), "off.db")
	db := openForTest(t, path, OpenOptions{Create: true})
	commitNodes(t, db, 2) // events 1..4
	db.Close()

	db = openForTest(t, path, OpenOptions{ChangefeedRetention: "off"})
	commitNodes(t, db, 2)
	if _, err := db.Changes(0, 100, 0); !errors.Is(err, &Error{Code: ErrorUnsupported}) {
		t.Fatalf("reading a feed that is off: %v", err)
	}
	if _, err := db.TrimChangefeed(); err != nil {
		t.Fatalf("trim: %v", err)
	}
	status, err := db.ChangefeedStatus()
	if err != nil || status != (ChangefeedStatus{Retention: "off", OldestSequence: 0, LastSequence: 4}) {
		t.Fatalf("status: %+v, %v", status, err)
	}
	db.Close()

	db = openForTest(t, path, OpenOptions{ChangefeedRetention: "all"})
	defer db.Close()
	commitNodes(t, db, 1)
	if _, err := db.Changes(4, 100, 0); !errors.Is(err, ErrChangesTrimmed) {
		t.Fatalf("a reader from before the feed went off: %v", err)
	}
	if got := changeSequences(t, db, 0); !equalSequences(got, []uint64{6, 7}) {
		t.Fatalf("after switching back on: %v", got)
	}
}

func TestChangefeedTrimRefusedInWriteTransaction(t *testing.T) {
	db := openForTest(t, filepath.Join(t.TempDir(), "busy.db"), OpenOptions{Create: true, ChangefeedRetention: "last:1"})
	defer db.Close()
	commitNodes(t, db, 2)
	tx, err := db.BeginWrite()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback()
	if _, err := db.TrimChangefeed(); !errors.Is(err, &Error{Code: ErrorLockTimeout}) {
		t.Fatalf("trim inside a write transaction: %v", err)
	}
}
