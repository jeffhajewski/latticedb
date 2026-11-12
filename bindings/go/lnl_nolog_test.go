package latticedb

import (
	"os"
	"path/filepath"
	"testing"
)

// A database opened with DisableWAL has write transactions like any other;
// it used to refuse every one with ErrorUnsupported.
func TestDisableWALStillHasTransactions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nolog.db")
	db, err := Open(path, OpenOptions{Create: true, DisableWAL: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	count := func(cypher string) any {
		t.Helper()
		result, err := db.Query(cypher, nil)
		if err != nil {
			t.Fatalf("%s: %v", cypher, err)
		}
		return result.Rows[0]["c"]
	}

	if _, err := db.Query("CREATE (:D {i: 1})-[:R]->(:D {i: 2}), (:D {i: 3})", nil); err != nil {
		t.Fatalf("create: %v", err)
	}
	// Fails on the relationship after its DELETE has run; rolled back whole.
	if _, err := db.Query("MATCH (a:D {i: 3}), (b:D {i: 2}) DELETE a CREATE (b)-[:R]->(a)", nil); err == nil {
		t.Fatal("query that creates a relationship to a deleted node succeeded")
	}
	if got := count("MATCH (n:D) RETURN count(n) AS c"); got != int64(3) {
		t.Fatalf("D nodes after the failed query = %v, want 3", got)
	}

	tx, err := db.BeginWrite()
	if err != nil {
		t.Fatalf("begin write: %v", err)
	}
	if _, err := db.BeginWrite(); err == nil {
		t.Fatal("a second writer began while the first was open")
	} else {
		wantCode(t, "second writer", err, ErrorLockTimeout)
	}
	if _, err := tx.Query("CREATE (:D {i: 4})", nil); err != nil {
		t.Fatalf("create in transaction: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if got := count("MATCH (n:D) RETURN count(n) AS c"); got != int64(3) {
		t.Fatalf("D nodes after rollback = %v, want 3", got)
	}

	if _, err := os.Stat(path + "-wal"); !os.IsNotExist(err) {
		t.Fatalf("a database without a log has a log file (stat: %v)", err)
	}
}
