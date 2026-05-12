package conformance

import (
	"encoding/binary"
	"fmt"
	"os"
	"testing"
)

type RecoveryHarness interface {
	CaptureCrashState(dbPath string) (RecoverySnapshot, error)
	SimulateCrash(dbPath string, snapshot RecoverySnapshot) error
}

var testRecoveryHarness RecoveryHarness = latticeRecoveryHarness{}

type RecoverySnapshot any

type latticeRecoveryHarness struct{}

type latticeRecoverySnapshot struct {
	wal []byte
}

const (
	headerPageSize          = 4096
	fileHeaderMinSize       = 200
	offsetNodeCount         = 16
	offsetEdgeCount         = 24
	offsetBTreeRootPage     = 32
	offsetVectorSegmentPage = 36
	offsetFTSSegmentPage    = 40
	offsetFreelistPage      = 44
	offsetSchemaPage        = 48
	offsetTreeRoots         = 52
	maxTreeRoots            = 16
	treeRootsByteLength     = maxTreeRoots * 4
)

func simulateCrashAfterClose(t *testing.T, dbPath string, db Database) {
	t.Helper()

	snapshot, err := testRecoveryHarness.CaptureCrashState(dbPath)
	if err != nil {
		t.Fatalf("capture crash state: %v", err)
	}

	closeDB(t, db)

	if err := testRecoveryHarness.SimulateCrash(dbPath, snapshot); err != nil {
		t.Fatalf("simulate crash: %v", err)
	}
}

func (latticeRecoveryHarness) CaptureCrashState(dbPath string) (RecoverySnapshot, error) {
	wal, err := os.ReadFile(dbPath + "-wal")
	if err != nil {
		return nil, fmt.Errorf("read wal file: %w", err)
	}
	if len(wal) < headerPageSize {
		return nil, fmt.Errorf("wal file too small: got %d bytes", len(wal))
	}
	return latticeRecoverySnapshot{wal: wal}, nil
}

func (latticeRecoveryHarness) SimulateCrash(dbPath string, snapshot RecoverySnapshot) error {
	latticeSnapshot, ok := snapshot.(latticeRecoverySnapshot)
	if !ok {
		return fmt.Errorf("unexpected recovery snapshot type %T", snapshot)
	}
	if len(latticeSnapshot.wal) < headerPageSize {
		return fmt.Errorf("wal snapshot too small: got %d bytes", len(latticeSnapshot.wal))
	}

	if err := os.WriteFile(dbPath+"-wal", latticeSnapshot.wal, 0o600); err != nil {
		return fmt.Errorf("restore wal snapshot: %w", err)
	}

	file, err := os.OpenFile(dbPath, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("open database file: %w", err)
	}
	defer file.Close()

	header := make([]byte, headerPageSize)
	n, err := file.ReadAt(header, 0)
	if err != nil {
		return fmt.Errorf("read database header: %w", err)
	}
	if n < fileHeaderMinSize {
		return fmt.Errorf("database header too small: got %d bytes", n)
	}

	binary.LittleEndian.PutUint64(header[offsetNodeCount:offsetNodeCount+8], 0)
	binary.LittleEndian.PutUint64(header[offsetEdgeCount:offsetEdgeCount+8], 0)
	binary.LittleEndian.PutUint32(header[offsetBTreeRootPage:offsetBTreeRootPage+4], 0)
	binary.LittleEndian.PutUint32(header[offsetVectorSegmentPage:offsetVectorSegmentPage+4], 0)
	binary.LittleEndian.PutUint32(header[offsetFTSSegmentPage:offsetFTSSegmentPage+4], 0)
	binary.LittleEndian.PutUint32(header[offsetFreelistPage:offsetFreelistPage+4], 0)
	binary.LittleEndian.PutUint32(header[offsetSchemaPage:offsetSchemaPage+4], 0)
	clear(header[offsetTreeRoots : offsetTreeRoots+treeRootsByteLength])

	if _, err := file.WriteAt(header, 0); err != nil {
		return fmt.Errorf("write simulated crash header: %w", err)
	}
	if err := file.Truncate(headerPageSize); err != nil {
		return fmt.Errorf("truncate database file: %w", err)
	}
	return nil
}
