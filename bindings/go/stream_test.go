package latticedb

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"
)

func TestStreamPublishAndReadRecord(t *testing.T) {
	db := openStreamTestDB(t, "publish-read.db")

	err := db.Update(func(tx *Tx) error {
		return tx.PublishStream("events", "created", map[string]Value{
			"id":     int64(1),
			"status": "queued",
		})
	})
	if err != nil {
		t.Fatalf("publish stream: %v", err)
	}

	records, err := db.ReadStream("events", 0, 100, 0)
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d: %#v", len(records), records)
	}

	record := records[0]
	if record.Sequence != 1 {
		t.Fatalf("unexpected sequence: %d", record.Sequence)
	}
	if record.Kind != "created" {
		t.Fatalf("unexpected kind: %q", record.Kind)
	}

	expectedPayload := map[string]any{
		"id":     int64(1),
		"status": "queued",
	}
	if !reflect.DeepEqual(record.Payload, expectedPayload) {
		t.Fatalf("unexpected payload: %#v", record.Payload)
	}

	afterRecords, err := db.ReadStream("events", record.Sequence, 100, 0)
	if err != nil {
		t.Fatalf("read after record: %v", err)
	}
	if len(afterRecords) != 0 {
		t.Fatalf("expected no records after sequence %d, got %#v", record.Sequence, afterRecords)
	}
}

func TestChangesFromNodeCreation(t *testing.T) {
	db := openStreamTestDB(t, "changes.db")

	var nodeID NodeID
	err := db.Update(func(tx *Tx) error {
		node, err := tx.CreateNode(CreateNodeOptions{
			Labels: []string{"Person"},
			Properties: map[string]Value{
				"name": "Ada",
			},
		})
		if err != nil {
			return err
		}
		nodeID = node.ID
		return nil
	})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	changes, err := db.Changes(0, 100, 0)
	if err != nil {
		t.Fatalf("read changes: %v", err)
	}
	if !hasChange(changes, "node.insert", nodeID, "") {
		t.Fatalf("missing node.insert change for %d: %#v", nodeID, changes)
	}
	if !hasChange(changes, "node.property_set", nodeID, "name") {
		t.Fatalf("missing node.property_set change for %d name: %#v", nodeID, changes)
	}
}

func TestStreamConsumerOffsets(t *testing.T) {
	db := openStreamTestDB(t, "offsets.db")

	offset, ok, err := db.GetStreamOffset("events", "worker-a")
	if err != nil {
		t.Fatalf("get missing offset: %v", err)
	}
	if ok {
		t.Fatalf("expected missing offset, got %d", offset)
	}

	err = db.Update(func(tx *Tx) error {
		return tx.SetStreamOffset("events", "worker-a", 42)
	})
	if err != nil {
		t.Fatalf("set offset: %v", err)
	}

	offset, ok, err = db.GetStreamOffset("events", "worker-a")
	if err != nil {
		t.Fatalf("get offset: %v", err)
	}
	if !ok || offset != 42 {
		t.Fatalf("expected offset 42, got offset=%d ok=%v", offset, ok)
	}
}

func TestStreamTrim(t *testing.T) {
	db := openStreamTestDB(t, "trim.db")

	err := db.Update(func(tx *Tx) error {
		if err := tx.PublishStream("events", "first", "one"); err != nil {
			return err
		}
		return tx.PublishStream("events", "second", "two")
	})
	if err != nil {
		t.Fatalf("publish stream records: %v", err)
	}

	err = db.Update(func(tx *Tx) error {
		return tx.TrimStream("events", 1)
	})
	if err != nil {
		t.Fatalf("trim stream: %v", err)
	}

	records, err := db.ReadStream("events", 0, 100, 0)
	if err != nil {
		t.Fatalf("read trimmed stream: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 remaining record, got %d: %#v", len(records), records)
	}
	if records[0].Sequence != 2 || records[0].Kind != "second" || records[0].Payload != "two" {
		t.Fatalf("unexpected remaining record: %#v", records[0])
	}
}

func TestStreamWritesRequireWAL(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "no-wal.db"), OpenOptions{
		Create:     true,
		DisableWAL: true,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := db.Close(); closeErr != nil {
			t.Fatalf("close db: %v", closeErr)
		}
	})

	err = db.Update(func(tx *Tx) error {
		return tx.PublishStream("events", "message", "hidden")
	})
	var latticeErr *Error
	if !errors.As(err, &latticeErr) || latticeErr.Code != ErrorUnsupported {
		t.Fatalf("expected unsupported error, got %v", err)
	}
}

func openStreamTestDB(t *testing.T, name string) *DB {
	t.Helper()

	db, err := Open(filepath.Join(t.TempDir(), name), OpenOptions{Create: true})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := db.Close(); closeErr != nil {
			t.Fatalf("close db: %v", closeErr)
		}
	})
	return db
}

func hasChange(records []StreamRecord, kind string, nodeID NodeID, key string) bool {
	for _, record := range records {
		if record.Kind != kind {
			continue
		}
		payload, ok := record.Payload.(map[string]any)
		if !ok {
			continue
		}
		if !valuesEqualUint64(payload["node_id"], uint64(nodeID)) {
			continue
		}
		if key == "" || payload["key"] == key {
			return true
		}
	}
	return false
}

func valuesEqualUint64(value any, expected uint64) bool {
	switch v := value.(type) {
	case int64:
		return v >= 0 && uint64(v) == expected
	case uint64:
		return v == expected
	case int:
		return v >= 0 && uint64(v) == expected
	default:
		return false
	}
}
