package latticedb

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestNodePropertyRoundTripAndMissingVsNull(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	db, err := Open(dbPath, OpenOptions{Create: true})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			t.Fatalf("close db: %v", closeErr)
		}
	}()

	var nodeID NodeID
	err = db.Update(func(tx *Tx) error {
		node, err := tx.CreateNode(CreateNodeOptions{
			Labels: []string{"Person", "Employee"},
			Properties: map[string]Value{
				"name":    "Alice",
				"profile": map[string]Value{"active": true, "tags": []Value{"graph", int64(7)}},
				"payload": []byte{1, 2, 3},
				"vector":  []float32{1.0, 2.5, 3.0},
				"note":    nil,
			},
		})
		if err != nil {
			return err
		}
		nodeID = node.ID
		return nil
	})
	if err != nil {
		t.Fatalf("update db: %v", err)
	}

	if _, statErr := os.Stat(dbPath); statErr != nil {
		t.Fatalf("stat db file: %v", statErr)
	}

	err = db.View(func(tx *Tx) error {
		node, err := tx.GetNode(nodeID)
		if err != nil {
			return err
		}
		if node == nil {
			t.Fatalf("expected node to exist")
		}
		if !reflect.DeepEqual(node.Labels, []string{"Person", "Employee"}) {
			t.Fatalf("unexpected labels: %#v", node.Labels)
		}

		profile, ok, err := tx.GetProperty(nodeID, "profile")
		if err != nil {
			return err
		}
		if !ok {
			t.Fatalf("expected profile property")
		}
		expectedProfile := map[string]any{
			"active": true,
			"tags":   []any{"graph", int64(7)},
		}
		if !reflect.DeepEqual(profile, expectedProfile) {
			t.Fatalf("unexpected profile: %#v", profile)
		}

		note, ok, err := tx.GetProperty(nodeID, "note")
		if err != nil {
			return err
		}
		if !ok {
			t.Fatalf("expected note property to exist")
		}
		if note != nil {
			t.Fatalf("expected stored null, got %#v", note)
		}

		missing, ok, err := tx.GetProperty(nodeID, "missing")
		if err != nil {
			return err
		}
		if ok {
			t.Fatalf("expected missing property, got %#v", missing)
		}

		return nil
	})
	if err != nil {
		t.Fatalf("view db: %v", err)
	}
}

func TestEdgePropertiesAndQueryRoundTrip(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "query.db")

	db, err := Open(dbPath, OpenOptions{Create: true})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			t.Fatalf("close db: %v", closeErr)
		}
	}()

	var aliceID NodeID
	var edgeID EdgeID
	err = db.Update(func(tx *Tx) error {
		alice, err := tx.CreateNode(CreateNodeOptions{
			Labels: []string{"Person"},
			Properties: map[string]Value{
				"name": "Alice",
			},
		})
		if err != nil {
			return err
		}
		bob, err := tx.CreateNode(CreateNodeOptions{
			Labels: []string{"Person"},
			Properties: map[string]Value{
				"name": "Bob",
			},
		})
		if err != nil {
			return err
		}

		edge, err := tx.CreateEdge(alice.ID, bob.ID, "KNOWS", CreateEdgeOptions{
			Properties: map[string]Value{
				"meta": map[string]Value{"since": int64(2024), "weights": []Value{float64(0.5), float64(0.75)}},
			},
		})
		if err != nil {
			return err
		}

		aliceID = alice.ID
		edgeID = edge.ID
		return nil
	})
	if err != nil {
		t.Fatalf("seed graph: %v", err)
	}

	err = db.View(func(tx *Tx) error {
		meta, ok, err := tx.GetEdgeProperty(edgeID, "meta")
		if err != nil {
			return err
		}
		if !ok {
			t.Fatalf("expected edge property")
		}
		expectedMeta := map[string]any{
			"since":   int64(2024),
			"weights": []any{float64(0.5), float64(0.75)},
		}
		if !reflect.DeepEqual(meta, expectedMeta) {
			t.Fatalf("unexpected edge property: %#v", meta)
		}

		result, err := tx.Query(
			"MATCH (n:Person) WHERE n.name = $name RETURN $payload AS payload, n.name AS person",
			map[string]Value{
				"name": "Alice",
				"payload": map[string]Value{
					"labels": []Value{"Person", "Employee"},
					"state":  map[string]Value{"active": true},
				},
			},
		)
		if err != nil {
			return err
		}

		if !reflect.DeepEqual(result.Columns, []string{"payload", "person"}) {
			t.Fatalf("unexpected columns: %#v", result.Columns)
		}
		if len(result.Rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(result.Rows))
		}
		expectedPayload := map[string]any{
			"labels": []any{"Person", "Employee"},
			"state":  map[string]any{"active": true},
		}
		if !reflect.DeepEqual(result.Rows[0]["payload"], expectedPayload) {
			t.Fatalf("unexpected payload: %#v", result.Rows[0]["payload"])
		}
		if result.Rows[0]["person"] != "Alice" {
			t.Fatalf("unexpected person value: %#v", result.Rows[0]["person"])
		}

		outgoing, err := tx.GetOutgoingEdges(aliceID)
		if err != nil {
			return err
		}
		if len(outgoing) != 1 {
			t.Fatalf("expected 1 outgoing edge, got %d", len(outgoing))
		}
		if outgoing[0].ID != edgeID {
			t.Fatalf("unexpected outgoing edge id: %d", outgoing[0].ID)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("view db: %v", err)
	}
}
