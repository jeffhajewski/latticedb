package latticedb

import (
	"errors"
	"path/filepath"
	"testing"
)

func wantCode(t *testing.T, what string, err error, code ErrorCode) {
	t.Helper()
	var latticeErr *Error
	if !errors.As(err, &latticeErr) || latticeErr.Code != code {
		t.Fatalf("%s = %v, want error code %d", what, err, code)
	}
}

func TestCommitThatReportsTxnAbortedEndsTheTransaction(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "v5-aborted-commit.db"), OpenOptions{Create: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	tx, err := db.BeginWrite()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	// Retire the native handle behind tx's back through a second wrapper of the
	// same handle, so tx's own commit gets TXN_ABORTED from the C side.
	alias := *tx.raw
	if err := alias.Commit(); err != nil {
		t.Fatalf("commit through alias: %v", err)
	}

	wantCode(t, "commit of a retired handle", tx.Commit(), ErrorTxnAborted)
	if tx.IsActive() {
		t.Fatal("transaction still active after its commit reported TXN_ABORTED")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback after TXN_ABORTED commit = %v, want nil", err)
	}
	if err := tx.Commit(); !errors.Is(err, ErrInactiveTx) {
		t.Fatalf("second commit = %v, want ErrInactiveTx", err)
	}
}
