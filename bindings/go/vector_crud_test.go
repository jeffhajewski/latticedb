package latticedb

import (
	"math"
	"path/filepath"
	"testing"
)

func TestGetVectorAndRemoveVector(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "vector-crud.db")
	db, err := Open(dbPath, OpenOptions{Create: true, EnableVectors: true, VectorDimensions: 2})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	var node, other NodeID
	err = db.Update(func(tx *Tx) error {
		n, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Doc"}})
		if err != nil {
			return err
		}
		o, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Doc"}})
		if err != nil {
			return err
		}
		node, other = n.ID, o.ID
		if _, ok, err := tx.GetVector(node, "embedding"); err != nil || ok {
			t.Fatalf("vector before set: ok=%v err=%v, want none", ok, err)
		}
		if err := tx.SetVector(node, "embedding", []float32{3, 4}); err != nil {
			return err
		}
		if err := tx.SetVector(other, "embedding", []float32{0, 1}); err != nil {
			return err
		}
		// Uncommitted: as written.
		vector, ok, err := tx.GetVector(node, "")
		if err != nil || !ok || vector[0] != 3 || vector[1] != 4 {
			t.Fatalf("uncommitted vector = %v ok=%v err=%v, want [3 4]", vector, ok, err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("write: %v", err)
	}

	err = db.View(func(tx *Tx) error {
		// Committed, under the default cosine metric: normalised.
		vector, ok, err := tx.GetVector(node, "embedding")
		if err != nil || !ok || len(vector) != 2 ||
			math.Abs(float64(vector[0])-0.6) > 1e-6 || math.Abs(float64(vector[1])-0.8) > 1e-6 {
			t.Fatalf("committed vector = %v ok=%v err=%v, want [0.6 0.8]", vector, ok, err)
		}
		_, _, err = tx.GetVector(node+1000, "embedding")
		wantCode(t, "vector of a missing node", err, ErrorNotFound)
		_, _, err = tx.GetVector(node, "title_vec")
		wantCode(t, "vector under another key", err, ErrorUnsupported)
		if err := tx.RemoveVector(node, "embedding"); err == nil {
			t.Fatalf("remove in a read-only transaction succeeded")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	err = db.Update(func(tx *Tx) error {
		if err := tx.RemoveVector(node, "embedding"); err != nil {
			return err
		}
		if _, ok, err := tx.GetVector(node, "embedding"); err != nil || ok {
			t.Fatalf("vector after remove: ok=%v err=%v, want none", ok, err)
		}
		// Removing a vector the node does not have does nothing.
		if err := tx.RemoveVector(node, ""); err != nil {
			return err
		}
		wantCode(t, "remove from a missing node", tx.RemoveVector(node+1000, "embedding"), ErrorNotFound)
		return nil
	})
	if err != nil {
		t.Fatalf("remove: %v", err)
	}

	results, err := db.VectorSearch([]float32{3, 4}, VectorSearchOptions{K: 5})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(results) != 1 || results[0].NodeID != other {
		t.Fatalf("search = %+v, want only node %d", results, other)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// After reopening, the removal holds and the node can take a vector again.
	db, err = Open(dbPath, OpenOptions{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
	err = db.Update(func(tx *Tx) error {
		if _, ok, err := tx.GetVector(node, "embedding"); err != nil || ok {
			t.Fatalf("vector after reopen: ok=%v err=%v, want none", ok, err)
		}
		return tx.SetVector(node, "embedding", []float32{1, 0})
	})
	if err != nil {
		t.Fatalf("set again: %v", err)
	}
	err = db.View(func(tx *Tx) error {
		vector, ok, err := tx.GetVector(node, "embedding")
		if err != nil || !ok || vector[0] != 1 || vector[1] != 0 {
			t.Fatalf("vector set again = %v ok=%v err=%v, want [1 0]", vector, ok, err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("read again: %v", err)
	}
}
