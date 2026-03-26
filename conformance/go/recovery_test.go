package conformance

import (
	"encoding/binary"
	"fmt"
	"os"
)

type RecoveryHarness interface {
	SimulateCrash(dbPath string) error
}

var testRecoveryHarness RecoveryHarness = latticeRecoveryHarness{}

type latticeRecoveryHarness struct{}

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

func (latticeRecoveryHarness) SimulateCrash(dbPath string) error {
	if _, err := os.Stat(dbPath + "-wal"); err != nil {
		return fmt.Errorf("stat wal file: %w", err)
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
