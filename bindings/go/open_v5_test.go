package latticedb

import (
	"math"
	"path/filepath"
	"testing"
)

func TestOpenEuclideanIndexReopensWithoutVectorOptions(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "v5-euclid.db")
	db, err := Open(dbPath, OpenOptions{
		Create:               true,
		EnableVectors:        true,
		VectorDimensions:     3,
		VectorMetric:         VectorMetricEuclidean,
		VectorM:              8,
		VectorEfConstruction: 64,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	var a, b NodeID
	err = db.Update(func(tx *Tx) error {
		na, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"P"}})
		if err != nil {
			return err
		}
		nb, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"P"}})
		if err != nil {
			return err
		}
		a, b = na.ID, nb.ID
		if err := tx.SetVector(a, "embedding", []float32{3, 0, 0}); err != nil {
			return err
		}
		return tx.SetVector(b, "embedding", []float32{0, 4, 0})
	})
	if err != nil {
		t.Fatalf("write vectors: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// No vector options: the file's index comes with it.
	db, err = Open(dbPath, OpenOptions{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
	results, err := db.VectorSearch([]float32{0, 0, 0}, VectorSearchOptions{K: 2})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(results) != 2 || results[0].NodeID != a || results[1].NodeID != b {
		t.Fatalf("results = %+v, want %d then %d", results, a, b)
	}
	// L2 distance of vectors stored as given, not normalised.
	if math.Abs(float64(results[0].Distance)-3) > 1e-4 || math.Abs(float64(results[1].Distance)-4) > 1e-4 {
		t.Fatalf("distances = %v, %v, want 3 and 4", results[0].Distance, results[1].Distance)
	}
}

func TestOpenOptionsThatContradictTheFileAreConfigMismatch(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "v5-mismatch.db")
	db, err := Open(dbPath, OpenOptions{Create: true, EnableVectors: true, VectorDimensions: 3})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	for name, opts := range map[string]OpenOptions{
		"dimensions": {EnableVectors: true, VectorDimensions: 4},
		"metric":     {VectorMetric: VectorMetricDotProduct},
		"m":          {VectorM: 12},
	} {
		db, err := Open(dbPath, opts)
		if err == nil {
			db.Close()
		}
		wantCode(t, "open with different "+name, err, ErrorConfigMismatch)
	}

	db, err = Open(filepath.Join(t.TempDir(), "v5-bad-m.db"), OpenOptions{Create: true, EnableVectors: true, VectorM: 64})
	if err == nil {
		db.Close()
	}
	wantCode(t, "open with VectorM 64", err, ErrorInvalidArg)
}

func TestOpenAcceptsSyncOptions(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "v5-sync.db")
	for _, opts := range []OpenOptions{
		{Create: true, SyncMode: SyncModeNormal, FullFsync: FullFsyncOff},
		{Create: true, SyncMode: SyncModeFull, FullFsync: FullFsyncOn},
		{Create: true},
	} {
		db, err := Open(dbPath, opts)
		if err != nil {
			t.Fatalf("open %+v: %v", opts, err)
		}
		if err := db.Update(func(tx *Tx) error {
			_, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"S"}})
			return err
		}); err != nil {
			t.Fatalf("write: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}
	db, err := Open(dbPath, OpenOptions{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
	result, err := db.Query("MATCH (n:S) RETURN count(n) AS c", nil)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if got := result.Rows[0]["c"]; got != int64(3) {
		t.Fatalf("count = %v, want 3", got)
	}
}

func TestDeleteNodeWithRelationshipsIsRefusedAndDetachDeletes(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "v5-delete.db"), OpenOptions{Create: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	var a, b, c NodeID
	err = db.Update(func(tx *Tx) error {
		for _, id := range []*NodeID{&a, &b, &c} {
			n, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"N"}})
			if err != nil {
				return err
			}
			*id = n.ID
		}
		if _, err := tx.CreateEdge(a, b, "R", CreateEdgeOptions{}); err != nil {
			return err
		}
		_, err := tx.CreateEdge(c, a, "R", CreateEdgeOptions{})
		return err
	})
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	err = db.Update(func(tx *Tx) error {
		wantCode(t, "DeleteNode with relationships", tx.DeleteNode(a), ErrorHasRelationships)
		exists, err := tx.NodeExists(a)
		if err != nil || !exists {
			t.Fatalf("node after refused delete: exists=%v err=%v", exists, err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("refused delete transaction: %v", err)
	}

	if err := db.Update(func(tx *Tx) error { return tx.DetachDeleteNode(a) }); err != nil {
		t.Fatalf("DetachDeleteNode: %v", err)
	}
	err = db.View(func(tx *Tx) error {
		exists, err := tx.NodeExists(a)
		if err != nil || exists {
			t.Fatalf("node after detach delete: exists=%v err=%v", exists, err)
		}
		in, err := tx.GetIncomingEdges(b)
		if err != nil || len(in) != 0 {
			t.Fatalf("edges into b = %v, %v; want none", in, err)
		}
		out, err := tx.GetOutgoingEdges(c)
		if err != nil || len(out) != 0 {
			t.Fatalf("edges out of c = %v, %v; want none", out, err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("view: %v", err)
	}

	err = db.Update(func(tx *Tx) error {
		if err := tx.DeleteNode(b); err != nil {
			return err
		}
		if err := tx.DeleteNode(987654); err != nil {
			return err
		}
		return tx.DetachDeleteNode(987654)
	})
	if err != nil {
		t.Fatalf("plain deletes after detach: %v", err)
	}
}

func TestCreateEdgeToMissingNodeIsNotFound(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "v5-edge.db"), OpenOptions{Create: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	err = db.Update(func(tx *Tx) error {
		n, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"N"}})
		if err != nil {
			return err
		}
		_, err = tx.CreateEdge(n.ID, 987654, "R", CreateEdgeOptions{})
		wantCode(t, "edge to missing target", err, ErrorNotFound)
		_, err = tx.CreateEdge(987654, n.ID, "R", CreateEdgeOptions{})
		wantCode(t, "edge from missing source", err, ErrorNotFound)
		return nil
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
}
