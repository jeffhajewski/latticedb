package latticedb

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestRejectedCloseRetainsDatabaseHandle(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "close-active-transaction.db")
	db, err := Open(dbPath, OpenOptions{Create: true})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	tx, err := db.BeginRead()
	if err != nil {
		t.Fatalf("begin read: %v", err)
	}

	err = db.Close()
	var latticeErr *Error
	if !errors.As(err, &latticeErr) || latticeErr.Code != ErrorInvalidArg {
		t.Fatalf("close with active transaction = %v, want ErrorInvalidArg", err)
	}
	if !db.IsOpen() {
		t.Fatal("database should remain open after rejected close")
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback read: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db after rollback: %v", err)
	}
}

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

func TestExplicitPropertyIndexes(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "property-indexes.db")

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
	var bobID NodeID
	var edgeID EdgeID
	err = db.Update(func(tx *Tx) error {
		alice, err := tx.CreateNode(CreateNodeOptions{
			Labels:     []string{"Person"},
			Properties: map[string]Value{"email": "alice@example.com"},
		})
		if err != nil {
			return err
		}
		bob, err := tx.CreateNode(CreateNodeOptions{
			Labels:     []string{"Person"},
			Properties: map[string]Value{"email": "bob@example.com"},
		})
		if err != nil {
			return err
		}
		edge, err := tx.CreateEdge(alice.ID, bob.ID, "KNOWS", CreateEdgeOptions{
			Properties: map[string]Value{"since": int64(2024)},
		})
		if err != nil {
			return err
		}
		aliceID = alice.ID
		bobID = bob.ID
		edgeID = edge.ID
		return nil
	})
	if err != nil {
		t.Fatalf("seed graph: %v", err)
	}

	readTx, err := db.BeginRead()
	if err != nil {
		t.Fatalf("begin read before index creation: %v", err)
	}
	_, err = readTx.FindNodesByLabelProperty(
		"Person",
		"email",
		"alice@example.com",
		100,
	)
	var latticeErr *Error
	if !errors.As(err, &latticeErr) || latticeErr.Code != ErrorUnsupported {
		t.Fatalf("lookup without index = %v, want ErrorUnsupported", err)
	}
	if err := readTx.Rollback(); err != nil {
		t.Fatalf("rollback pre-index read: %v", err)
	}

	if err := db.CreateNodePropertyIndex("Person", "email"); err != nil {
		t.Fatalf("create node property index: %v", err)
	}
	if err := db.CreateEdgePropertyIndex("KNOWS", "since"); err != nil {
		t.Fatalf("create edge property index: %v", err)
	}

	err = db.View(func(tx *Tx) error {
		nodeIDs, err := tx.FindNodesByLabelProperty(
			"Person",
			"email",
			"alice@example.com",
			100,
		)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(nodeIDs, []NodeID{aliceID}) {
			t.Fatalf("node property lookup = %#v, want Alice", nodeIDs)
		}

		edgeIDs, err := tx.FindEdgesByTypeProperty("KNOWS", "since", int64(2024), 100)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(edgeIDs, []EdgeID{edgeID}) {
			t.Fatalf("edge property lookup = %#v, want edge %d", edgeIDs, edgeID)
		}

		_, err = tx.FindNodesByLabelProperty("Person", "email", "alice@example.com", 0)
		if !errors.As(err, &latticeErr) || latticeErr.Code != ErrorInvalidArg {
			t.Fatalf("zero-limit lookup = %v, want ErrorInvalidArg", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("read property indexes: %v", err)
	}

	err = db.Update(func(tx *Tx) error {
		if err := tx.SetProperty(bobID, "email", "alice@example.com"); err != nil {
			return err
		}
		nodeIDs, err := tx.FindNodesByLabelProperty(
			"Person",
			"email",
			"alice@example.com",
			100,
		)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(nodeIDs, []NodeID{aliceID, bobID}) {
			t.Fatalf("transactional node property lookup = %#v, want Alice and Bob", nodeIDs)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("update indexed property: %v", err)
	}

	if err := db.DropNodePropertyIndex("Person", "email"); err != nil {
		t.Fatalf("drop node property index: %v", err)
	}
	if err := db.DropEdgePropertyIndex("KNOWS", "since"); err != nil {
		t.Fatalf("drop edge property index: %v", err)
	}

	readTx, err = db.BeginRead()
	if err != nil {
		t.Fatalf("begin read after index drop: %v", err)
	}
	_, err = readTx.FindEdgesByTypeProperty("KNOWS", "since", int64(2024), 100)
	if !errors.As(err, &latticeErr) || latticeErr.Code != ErrorUnsupported {
		t.Fatalf("lookup after index drop = %v, want ErrorUnsupported", err)
	}
	if err := readTx.Rollback(); err != nil {
		t.Fatalf("rollback post-drop read: %v", err)
	}
}

func TestTypedEdgeTraversalWithLimit(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "typed-edges.db")

	db, err := Open(dbPath, OpenOptions{Create: true})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			t.Fatalf("close db: %v", closeErr)
		}
	}()

	var aliceID, bobID, charlieID NodeID
	err = db.Update(func(tx *Tx) error {
		alice, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Person"}})
		if err != nil {
			return err
		}
		bob, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Person"}})
		if err != nil {
			return err
		}
		charlie, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Person"}})
		if err != nil {
			return err
		}
		dana, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Person"}})
		if err != nil {
			return err
		}

		if _, err := tx.CreateEdge(alice.ID, bob.ID, "KNOWS", CreateEdgeOptions{}); err != nil {
			return err
		}
		if _, err := tx.CreateEdge(alice.ID, charlie.ID, "KNOWS", CreateEdgeOptions{}); err != nil {
			return err
		}
		if _, err := tx.CreateEdge(alice.ID, dana.ID, "LIKES", CreateEdgeOptions{}); err != nil {
			return err
		}
		if _, err := tx.CreateEdge(bob.ID, alice.ID, "KNOWS", CreateEdgeOptions{}); err != nil {
			return err
		}

		aliceID = alice.ID
		bobID = bob.ID
		charlieID = charlie.ID
		return nil
	})
	if err != nil {
		t.Fatalf("seed graph: %v", err)
	}

	err = db.View(func(tx *Tx) error {
		outgoing, err := tx.GetOutgoingEdgesByType(aliceID, "KNOWS", 0)
		if err != nil {
			return err
		}
		if len(outgoing) != 2 {
			t.Fatalf("expected 2 typed outgoing edges, got %d: %#v", len(outgoing), outgoing)
		}
		targets := map[NodeID]bool{
			outgoing[0].TargetID: true,
			outgoing[1].TargetID: true,
		}
		if !targets[bobID] || !targets[charlieID] {
			t.Fatalf("unexpected typed outgoing targets: %#v", outgoing)
		}

		limited, err := tx.GetOutgoingEdgesByType(aliceID, "KNOWS", 1)
		if err != nil {
			return err
		}
		if len(limited) != 1 {
			t.Fatalf("expected 1 limited edge, got %d: %#v", len(limited), limited)
		}

		incoming, err := tx.GetIncomingEdgesByType(aliceID, "KNOWS", 0)
		if err != nil {
			return err
		}
		if len(incoming) != 1 || incoming[0].SourceID != bobID {
			t.Fatalf("unexpected typed incoming edges: %#v", incoming)
		}

		missing, err := tx.GetOutgoingEdgesByType(aliceID, "MISSING", 0)
		if err != nil {
			return err
		}
		if len(missing) != 0 {
			t.Fatalf("expected no missing typed edges, got %#v", missing)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("view graph: %v", err)
	}
}

func TestBatchInsertVectorsVectorSearchAndFTS(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "search.db")

	db, err := Open(dbPath, OpenOptions{
		Create:           true,
		EnableVectors:    true,
		VectorDimensions: 4,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			t.Fatalf("close db: %v", closeErr)
		}
	}()

	if err := db.CreateNodeFTSIndex("Document", "text"); err != nil {
		t.Fatalf("declare fts index: %v", err)
	}

	var nodeIDs []NodeID
	err = db.Update(func(tx *Tx) error {
		ids, err := tx.BatchInsertVectors("Document", [][]float32{
			{1.0, 0.0, 0.0, 0.0},
			{0.0, 1.0, 0.0, 0.0},
			{0.9, 0.1, 0.0, 0.0},
		})
		if err != nil {
			return err
		}
		nodeIDs = ids

		if err := tx.SetProperty(ids[0], "title", "Attention Is All You Need"); err != nil {
			return err
		}
		if err := tx.SetProperty(ids[0], "text", "transformer self attention neural networks"); err != nil {
			return err
		}
		if err := tx.SetProperty(ids[1], "title", "LSM Trees"); err != nil {
			return err
		}
		if err := tx.SetProperty(ids[1], "text", "log structured merge trees storage engines"); err != nil {
			return err
		}

		node, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Document"}})
		if err != nil {
			return err
		}
		if err := tx.SetVector(node.ID, "embedding", []float32{0.0, 0.0, 1.0, 0.0}); err != nil {
			return err
		}
		if err := tx.SetProperty(node.ID, "text", "graph databases for ai agents"); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed search data: %v", err)
	}

	results, err := db.VectorSearch([]float32{1.0, 0.0, 0.0, 0.0}, VectorSearchOptions{K: 2})
	if err != nil {
		t.Fatalf("vector search: %v", err)
	}
	if len(results) < 2 {
		t.Fatalf("expected at least 2 vector results, got %d", len(results))
	}
	if results[0].NodeID != nodeIDs[0] {
		t.Fatalf("expected exact match first, got %d", results[0].NodeID)
	}

	ftsResults, err := db.FTSSearch("Document", "text", "transformer attention", FTSSearchOptions{Limit: 5})
	if err != nil {
		t.Fatalf("fts search: %v", err)
	}
	if len(ftsResults) == 0 {
		t.Fatalf("expected fts results")
	}
	if ftsResults[0].NodeID != nodeIDs[0] {
		t.Fatalf("expected transformer document first, got %d", ftsResults[0].NodeID)
	}

	fuzzyResults, err := db.FTSSearchFuzzy("Document", "text", "transfomer attentin", FTSSearchOptions{
		Limit:       5,
		MaxDistance: 2,
	})
	if err != nil {
		t.Fatalf("fts fuzzy search: %v", err)
	}
	if len(fuzzyResults) == 0 {
		t.Fatalf("expected fuzzy fts results")
	}
}

func TestBatchInsertCompatibilityAlias(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "compat.db")

	db, err := Open(dbPath, OpenOptions{
		Create:           true,
		EnableVectors:    true,
		VectorDimensions: 2,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			t.Fatalf("close db: %v", closeErr)
		}
	}()

	err = db.Update(func(tx *Tx) error {
		ids, err := tx.BatchInsert("Document", [][]float32{
			{1.0, 0.0},
			{0.0, 1.0},
		})
		if err != nil {
			return err
		}
		if len(ids) != 2 {
			t.Fatalf("expected 2 ids, got %d", len(ids))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("compat batch insert: %v", err)
	}
}

func TestOpenOptionsEnableVectorCompatibilityAlias(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "compat-open.db")

	db, err := Open(dbPath, OpenOptions{
		Create:           true,
		EnableVector:     true,
		VectorDimensions: 2,
	})
	if err != nil {
		t.Fatalf("open db with compatibility alias: %v", err)
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			t.Fatalf("close db: %v", closeErr)
		}
	}()

	err = db.Update(func(tx *Tx) error {
		node, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Doc"}})
		if err != nil {
			return err
		}
		return tx.SetVector(node.ID, "embedding", []float32{1.0, 0.0})
	})
	if err != nil {
		t.Fatalf("set vector with compatibility alias: %v", err)
	}
}

func TestOpenOptionsEnableAdjacencyCache(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "adjacency-cache.db")

	db, err := Open(dbPath, OpenOptions{
		Create:               true,
		EnableAdjacencyCache: true,
	})
	if err != nil {
		t.Fatalf("open db with adjacency cache: %v", err)
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			t.Fatalf("close db: %v", closeErr)
		}
	}()

	var aliceID, bobID NodeID
	err = db.Update(func(tx *Tx) error {
		alice, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Person"}})
		if err != nil {
			return err
		}
		bob, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Person"}})
		if err != nil {
			return err
		}
		if _, err := tx.CreateEdge(alice.ID, bob.ID, "KNOWS", CreateEdgeOptions{}); err != nil {
			return err
		}
		aliceID = alice.ID
		bobID = bob.ID
		return nil
	})
	if err != nil {
		t.Fatalf("seed graph: %v", err)
	}

	err = db.View(func(tx *Tx) error {
		edges, err := tx.GetOutgoingEdges(aliceID)
		if err != nil {
			return err
		}
		if len(edges) != 1 || edges[0].TargetID != bobID {
			t.Fatalf("unexpected outgoing edges: %#v", edges)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("view graph: %v", err)
	}
}

func TestQueryCache(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "cache.db")

	db, err := Open(dbPath, OpenOptions{Create: true})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			t.Fatalf("close db: %v", closeErr)
		}
	}()

	if err := db.CacheClear(); err != nil {
		t.Fatalf("cache clear on empty cache: %v", err)
	}

	stats, err := db.CacheStats()
	if err != nil {
		t.Fatalf("cache stats initially: %v", err)
	}
	if stats.Entries != 0 || stats.Hits != 0 || stats.Misses != 0 {
		t.Fatalf("unexpected initial cache stats: %#v", stats)
	}

	err = db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{
			Labels: []string{"Person"},
			Properties: map[string]Value{
				"name": "Alice",
			},
		})
		return err
	})
	if err != nil {
		t.Fatalf("seed cache test data: %v", err)
	}

	if _, err := db.Query("MATCH (n:Person) RETURN n", nil); err != nil {
		t.Fatalf("first query: %v", err)
	}
	statsAfterFirst, err := db.CacheStats()
	if err != nil {
		t.Fatalf("cache stats after first query: %v", err)
	}
	if statsAfterFirst.Misses < 1 {
		t.Fatalf("expected at least one cache miss, got %#v", statsAfterFirst)
	}

	if _, err := db.Query("MATCH (n:Person) RETURN n", nil); err != nil {
		t.Fatalf("second query: %v", err)
	}
	statsAfterSecond, err := db.CacheStats()
	if err != nil {
		t.Fatalf("cache stats after second query: %v", err)
	}
	if statsAfterSecond.Hits <= statsAfterFirst.Hits {
		t.Fatalf("expected cache hits to increase, got %#v then %#v", statsAfterFirst, statsAfterSecond)
	}

	if err := db.CacheClear(); err != nil {
		t.Fatalf("cache clear after queries: %v", err)
	}
	statsAfterClear, err := db.CacheStats()
	if err != nil {
		t.Fatalf("cache stats after clear: %v", err)
	}
	if statsAfterClear.Entries != 0 {
		t.Fatalf("expected cache entries to reset, got %#v", statsAfterClear)
	}
}

func TestBeginReadAndBeginWrite(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "tx-open.db")

	db, err := Open(dbPath, OpenOptions{Create: true})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			t.Fatalf("close db: %v", closeErr)
		}
	}()

	writeTx, err := db.BeginWrite()
	if err != nil {
		t.Fatalf("begin write: %v", err)
	}
	if writeTx.IsReadOnly() {
		t.Fatalf("begin write returned read-only transaction")
	}
	node, err := writeTx.CreateNode(CreateNodeOptions{Labels: []string{"Doc"}})
	if err != nil {
		t.Fatalf("create node in begin write: %v", err)
	}
	if err := writeTx.Commit(); err != nil {
		t.Fatalf("commit write tx: %v", err)
	}

	readTx, err := db.BeginRead()
	if err != nil {
		t.Fatalf("begin read: %v", err)
	}
	if !readTx.IsReadOnly() {
		t.Fatalf("begin read returned writable transaction")
	}
	readNode, err := readTx.GetNode(node.ID)
	if err != nil {
		t.Fatalf("get node in begin read: %v", err)
	}
	if readNode == nil {
		t.Fatalf("expected committed node to be visible in begin read")
	}
	if err := readTx.Rollback(); err != nil {
		t.Fatalf("rollback read tx: %v", err)
	}
}

func TestSecondWriterIsRejectedUntilFirstWriterEnds(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "single-writer.db")
	db, err := Open(dbPath, OpenOptions{Create: true})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			t.Fatalf("close db: %v", closeErr)
		}
	}()

	writer, err := db.BeginWrite()
	if err != nil {
		t.Fatalf("begin first writer: %v", err)
	}

	_, err = db.BeginWrite()
	var latticeErr *Error
	if !errors.As(err, &latticeErr) || latticeErr.Code != ErrorLockTimeout {
		t.Fatalf("expected lock-timeout error for second writer, got %v", err)
	}

	reader, err := db.BeginRead()
	if err != nil {
		t.Fatalf("begin reader while writer active: %v", err)
	}
	if err := reader.Rollback(); err != nil {
		t.Fatalf("rollback reader: %v", err)
	}
	if err := writer.Rollback(); err != nil {
		t.Fatalf("rollback first writer: %v", err)
	}

	nextWriter, err := db.BeginWrite()
	if err != nil {
		t.Fatalf("begin writer after rollback: %v", err)
	}
	if err := nextWriter.Rollback(); err != nil {
		t.Fatalf("rollback next writer: %v", err)
	}
}

func TestBeginCompatibilityAlias(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "begin-compat.db")

	db, err := Open(dbPath, OpenOptions{Create: true})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			t.Fatalf("close db: %v", closeErr)
		}
	}()

	readTx, err := db.Begin(true)
	if err != nil {
		t.Fatalf("begin read via compatibility alias: %v", err)
	}
	if !readTx.IsReadOnly() {
		t.Fatalf("expected compatibility begin(true) to return read-only transaction")
	}
	if err := readTx.Rollback(); err != nil {
		t.Fatalf("rollback compatibility read tx: %v", err)
	}

	writeTx, err := db.Begin(false)
	if err != nil {
		t.Fatalf("begin write via compatibility alias: %v", err)
	}
	if writeTx.IsReadOnly() {
		t.Fatalf("expected compatibility begin(false) to return writable transaction")
	}
	if err := writeTx.Rollback(); err != nil {
		t.Fatalf("rollback compatibility write tx: %v", err)
	}

	readOnlyDBPath := filepath.Join(t.TempDir(), "readonly.db")
	readOnlySeed, err := Open(readOnlyDBPath, OpenOptions{Create: true})
	if err != nil {
		t.Fatalf("seed read-only db: %v", err)
	}
	if err := readOnlySeed.Close(); err != nil {
		t.Fatalf("close seeded db: %v", err)
	}

	readOnlyDB, err := Open(readOnlyDBPath, OpenOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("open read-only db: %v", err)
	}
	defer func() {
		if closeErr := readOnlyDB.Close(); closeErr != nil {
			t.Fatalf("close read-only db: %v", closeErr)
		}
	}()

	if _, err := readOnlyDB.BeginWrite(); err != ErrReadOnlyDatabase {
		t.Fatalf("expected BeginWrite on read-only db to fail with ErrReadOnlyDatabase, got %v", err)
	}
	if _, err := readOnlyDB.Begin(false); err != ErrReadOnlyDatabase {
		t.Fatalf("expected Begin(false) on read-only db to fail with ErrReadOnlyDatabase, got %v", err)
	}
}

func TestEdgeFullTextSearch(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "edge-fts.db")
	db, err := Open(dbPath, OpenOptions{Create: true})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			t.Fatalf("close db: %v", closeErr)
		}
	}()

	if err := db.CreateEdgeFTSIndex("REVIEWED", "note"); err != nil {
		t.Fatalf("declare edge fts index: %v", err)
	}
	declared, err := db.HasEdgeFTSIndex("REVIEWED", "note")
	if err != nil || !declared {
		t.Fatalf("expected REVIEWED.note to be declared: %v", err)
	}

	err = db.Update(func(tx *Tx) error {
		alice, err := tx.CreateNode(CreateNodeOptions{
			Labels:     []string{"Person"},
			Properties: map[string]Value{"name": "Alice"},
		})
		if err != nil {
			return err
		}
		paper, err := tx.CreateNode(CreateNodeOptions{
			Labels:     []string{"Paper"},
			Properties: map[string]Value{"title": "Attention"},
		})
		if err != nil {
			return err
		}
		other, err := tx.CreateNode(CreateNodeOptions{
			Labels:     []string{"Paper"},
			Properties: map[string]Value{"title": "Sourdough"},
		})
		if err != nil {
			return err
		}
		good, err := tx.CreateEdge(alice.ID, paper.ID, "REVIEWED", CreateEdgeOptions{})
		if err != nil {
			return err
		}
		weak, err := tx.CreateEdge(alice.ID, other.ID, "REVIEWED", CreateEdgeOptions{})
		if err != nil {
			return err
		}
		if err := tx.SetEdgeProperty(good.ID, "note", "thorough and well argued"); err != nil {
			return err
		}
		return tx.SetEdgeProperty(weak.ID, "note", "mostly about bread")
	})
	if err != nil {
		t.Fatalf("seed reviews: %v", err)
	}

	result, err := db.Query(
		"MATCH (a:Person)-[x:REVIEWED]->(p:Paper) WHERE x.note @@ 'thorough' RETURN p.title AS t",
		nil,
	)
	if err != nil {
		t.Fatalf("edge fts query: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if got := result.Rows[0]["t"]; got != "Attention" {
		t.Fatalf("expected Attention, got %v", got)
	}
}
