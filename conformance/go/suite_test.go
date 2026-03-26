package conformance

import (
	"path/filepath"
	"reflect"
	"slices"
	"testing"
)

type Value = any

type OpenOptions struct {
	Create           bool
	ReadOnly         bool
	CacheSizeMB      uint32
	PageSize         uint32
	EnableVector     bool
	VectorDimensions uint16
}

type CreateNodeOptions struct {
	Labels     []string
	Properties map[string]Value
}

type CreateEdgeOptions struct {
	Properties map[string]Value
}

type Node struct {
	ID         uint64
	Labels     []string
	Properties map[string]Value
}

type Edge struct {
	ID         uint64
	SourceID   uint64
	TargetID   uint64
	Type       string
	Properties map[string]Value
}

type QueryResult struct {
	Columns []string
	Rows    []map[string]Value
}

type VectorSearchOptions struct {
	K        uint32
	EfSearch uint16
}

type FTSSearchOptions struct {
	Limit         uint32
	MaxDistance   uint32
	MinTermLength uint32
}

type QueryCacheStats struct {
	Entries uint32
	Hits    uint64
	Misses  uint64
}

type VectorSearchResult struct {
	NodeID   uint64
	Distance float32
}

type FTSSearchResult struct {
	NodeID uint64
	Score  float32
}

type Driver interface {
	Open(path string, opts OpenOptions) (Database, error)
}

type Database interface {
	Close() error
	Begin(readOnly bool) (Tx, error)
	View(func(Tx) error) error
	Update(func(Tx) error) error
	Query(cypher string, params map[string]Value) (QueryResult, error)
	VectorSearch(vector []float32, opts VectorSearchOptions) ([]VectorSearchResult, error)
	FTSSearch(query string, opts FTSSearchOptions) ([]FTSSearchResult, error)
	CacheClear() error
	CacheStats() (QueryCacheStats, error)
}

type Tx interface {
	Commit() error
	Rollback() error
	CreateNode(opts CreateNodeOptions) (Node, error)
	DeleteNode(nodeID uint64) error
	NodeExists(nodeID uint64) (bool, error)
	GetNode(nodeID uint64) (*Node, error)
	SetProperty(nodeID uint64, key string, value Value) error
	GetProperty(nodeID uint64, key string) (Value, bool, error)
	SetVector(nodeID uint64, key string, vector []float32) error
	FTSIndex(nodeID uint64, text string) error
	CreateEdge(sourceID, targetID uint64, edgeType string, opts CreateEdgeOptions) (Edge, error)
	GetEdgeProperty(edgeID uint64, key string) (Value, bool, error)
	SetEdgeProperty(edgeID uint64, key string, value Value) error
	RemoveEdgeProperty(edgeID uint64, key string) error
	GetOutgoingEdges(nodeID uint64) ([]Edge, error)
}

var testDriver Driver = latticeDriver{}

func TestConformancePersistenceAndStableEdgeIdentity(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "persist.ltdb")

	db := openDB(t, dbPath, OpenOptions{Create: true})

	var aliceID uint64
	var bobID uint64
	var edge1ID uint64
	var rolledBackEdgeID uint64
	var edge2ID uint64

	err := db.Update(func(tx Tx) error {
		alice, err := tx.CreateNode(CreateNodeOptions{
			Labels: []string{"Person", "Employee"},
			Properties: map[string]Value{
				"name": "Alice",
				"meta": map[string]Value{
					"team": "graph",
				},
			},
		})
		if err != nil {
			return err
		}
		bob, err := tx.CreateNode(CreateNodeOptions{
			Labels:     []string{"Person"},
			Properties: map[string]Value{"name": "Bob"},
		})
		if err != nil {
			return err
		}
		edge1, err := tx.CreateEdge(alice.ID, bob.ID, "KNOWS", CreateEdgeOptions{
			Properties: map[string]Value{"since": int64(2024)},
		})
		if err != nil {
			return err
		}

		aliceID = alice.ID
		bobID = bob.ID
		edge1ID = edge1.ID
		return nil
	})
	if err != nil {
		t.Fatalf("seed graph: %v", err)
	}

	rollbackTx, err := db.Begin(false)
	if err != nil {
		t.Fatalf("begin rollback tx: %v", err)
	}
	rolledBackEdge, err := rollbackTx.CreateEdge(aliceID, bobID, "KNOWS", CreateEdgeOptions{
		Properties: map[string]Value{"since": int64(2025)},
	})
	if err != nil {
		t.Fatalf("create rolled-back edge: %v", err)
	}
	rolledBackEdgeID = rolledBackEdge.ID
	if err := rollbackTx.Rollback(); err != nil {
		t.Fatalf("rollback tx: %v", err)
	}

	err = db.Update(func(tx Tx) error {
		edge2, err := tx.CreateEdge(aliceID, bobID, "KNOWS", CreateEdgeOptions{
			Properties: map[string]Value{"since": int64(2026)},
		})
		if err != nil {
			return err
		}
		edge2ID = edge2.ID
		return nil
	})
	if err != nil {
		t.Fatalf("create committed edge after rollback: %v", err)
	}

	if edge2ID <= rolledBackEdgeID {
		t.Fatalf("expected committed edge id %d to be greater than rolled-back edge id %d", edge2ID, rolledBackEdgeID)
	}

	closeDB(t, db)
	db = openDB(t, dbPath, OpenOptions{})
	defer closeDB(t, db)

	err = db.View(func(tx Tx) error {
		alice, err := tx.GetNode(aliceID)
		if err != nil {
			return err
		}
		if alice == nil {
			t.Fatalf("expected alice to exist")
		}
		if !reflect.DeepEqual(alice.Labels, []string{"Person", "Employee"}) {
			t.Fatalf("unexpected labels after reopen: %#v", alice.Labels)
		}

		meta, ok, err := tx.GetProperty(aliceID, "meta")
		if err != nil {
			return err
		}
		if !ok {
			t.Fatalf("expected meta property after reopen")
		}
		expectedMeta := map[string]Value{"team": "graph"}
		if !reflect.DeepEqual(meta, expectedMeta) {
			t.Fatalf("unexpected meta property: %#v", meta)
		}

		edge1Since, ok, err := tx.GetEdgeProperty(edge1ID, "since")
		if err != nil {
			return err
		}
		if !ok || edge1Since != int64(2024) {
			t.Fatalf("unexpected edge1 property: ok=%v value=%#v", ok, edge1Since)
		}

		edge2Since, ok, err := tx.GetEdgeProperty(edge2ID, "since")
		if err != nil {
			return err
		}
		if !ok || edge2Since != int64(2026) {
			t.Fatalf("unexpected edge2 property: ok=%v value=%#v", ok, edge2Since)
		}

		outgoing, err := tx.GetOutgoingEdges(aliceID)
		if err != nil {
			return err
		}
		if len(outgoing) != 2 {
			t.Fatalf("expected 2 persisted outgoing edges, got %d", len(outgoing))
		}
		ids := []uint64{outgoing[0].ID, outgoing[1].ID}
		slices.Sort(ids)
		if !reflect.DeepEqual(ids, []uint64{edge1ID, edge2ID}) {
			t.Fatalf("unexpected outgoing edge ids: %#v", ids)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("validate reopen state: %v", err)
	}

	var edge3ID uint64
	err = db.Update(func(tx Tx) error {
		edge3, err := tx.CreateEdge(aliceID, bobID, "KNOWS", CreateEdgeOptions{})
		if err != nil {
			return err
		}
		edge3ID = edge3.ID
		return nil
	})
	if err != nil {
		t.Fatalf("create edge after reopen: %v", err)
	}

	if edge3ID <= edge2ID {
		t.Fatalf("expected edge id after reopen %d to be greater than prior committed id %d", edge3ID, edge2ID)
	}
}

func TestConformanceMissingVsNullAndNestedRoundTrip(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "values.ltdb")
	db := openDB(t, dbPath, OpenOptions{Create: true})
	defer closeDB(t, db)

	var nodeID uint64
	err := db.Update(func(tx Tx) error {
		node, err := tx.CreateNode(CreateNodeOptions{
			Labels: []string{"Profile"},
			Properties: map[string]Value{
				"name":    "Alice",
				"note":    nil,
				"payload": []byte{1, 2, 3},
				"vector":  []float32{1.0, 2.5, 3.0},
				"profile": map[string]Value{
					"active": true,
					"tags":   []Value{"graph", int64(7)},
				},
			},
		})
		if err != nil {
			return err
		}
		nodeID = node.ID
		return nil
	})
	if err != nil {
		t.Fatalf("seed nested values: %v", err)
	}

	closeDB(t, db)
	db = openDB(t, dbPath, OpenOptions{})
	defer closeDB(t, db)

	err = db.View(func(tx Tx) error {
		note, ok, err := tx.GetProperty(nodeID, "note")
		if err != nil {
			return err
		}
		if !ok {
			t.Fatalf("expected stored null property")
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

		profile, ok, err := tx.GetProperty(nodeID, "profile")
		if err != nil {
			return err
		}
		if !ok {
			t.Fatalf("expected nested profile property")
		}
		expectedProfile := map[string]Value{
			"active": true,
			"tags":   []Value{"graph", int64(7)},
		}
		if !reflect.DeepEqual(profile, expectedProfile) {
			t.Fatalf("unexpected profile value: %#v", profile)
		}

		payload, ok, err := tx.GetProperty(nodeID, "payload")
		if err != nil {
			return err
		}
		if !ok || !reflect.DeepEqual(payload, []byte{1, 2, 3}) {
			t.Fatalf("unexpected bytes payload: ok=%v value=%#v", ok, payload)
		}

		vector, ok, err := tx.GetProperty(nodeID, "vector")
		if err != nil {
			return err
		}
		if !ok || !reflect.DeepEqual(vector, []float32{1.0, 2.5, 3.0}) {
			t.Fatalf("unexpected vector value: ok=%v value=%#v", ok, vector)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("validate nested values: %v", err)
	}
}

func TestConformanceTransactionOwnWritesCommitAndRollback(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "mvcc.ltdb")
	db := openDB(t, dbPath, OpenOptions{Create: true})
	defer closeDB(t, db)

	var aliceID uint64
	err := db.Update(func(tx Tx) error {
		alice, err := tx.CreateNode(CreateNodeOptions{
			Labels:     []string{"Person"},
			Properties: map[string]Value{"name": "Alice"},
		})
		if err != nil {
			return err
		}
		aliceID = alice.ID
		return nil
	})
	if err != nil {
		t.Fatalf("seed alice: %v", err)
	}

	readOnlyTx := beginTx(t, db, true)
	if _, err := readOnlyTx.CreateNode(CreateNodeOptions{Labels: []string{"ShouldFail"}}); err == nil {
		t.Fatalf("expected read-only transaction to reject writes")
	}
	rollbackTx(t, readOnlyTx)

	writer := beginTx(t, db, false)
	var tempID uint64
	tempNode, err := writer.CreateNode(CreateNodeOptions{
		Labels:     []string{"Temp"},
		Properties: map[string]Value{"state": "draft"},
	})
	if err != nil {
		t.Fatalf("create temp node in writer: %v", err)
	}
	tempID = tempNode.ID
	if err := writer.SetProperty(aliceID, "name", "Bob"); err != nil {
		t.Fatalf("writer set property: %v", err)
	}

	nameWithinWriter := requireStringProperty(t, writer, aliceID, "name")
	if nameWithinWriter != "Bob" {
		t.Fatalf("writer should see own change, got %q", nameWithinWriter)
	}
	requireNodeExists(t, writer, tempID, true)

	if err := writer.Commit(); err != nil {
		t.Fatalf("commit writer: %v", err)
	}

	reader3 := beginTx(t, db, true)
	defer rollbackTx(t, reader3)
	nameAfterCommit := requireStringProperty(t, reader3, aliceID, "name")
	if nameAfterCommit != "Bob" {
		t.Fatalf("new snapshot should see committed value, got %q", nameAfterCommit)
	}
	requireNodeExists(t, reader3, tempID, true)

	rollbackWriter := beginTx(t, db, false)
	rolledBackNode, err := rollbackWriter.CreateNode(CreateNodeOptions{
		Labels:     []string{"Temp"},
		Properties: map[string]Value{"state": "rollback"},
	})
	if err != nil {
		t.Fatalf("create rolled back node: %v", err)
	}
	requireNodeExists(t, rollbackWriter, rolledBackNode.ID, true)
	if err := rollbackWriter.Rollback(); err != nil {
		t.Fatalf("rollback writer: %v", err)
	}

	reader4 := beginTx(t, db, true)
	defer rollbackTx(t, reader4)
	requireNodeExists(t, reader4, rolledBackNode.ID, false)
}

func TestConformanceQueryMutationAtomicityAndParallelEdgeTargeting(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "query.ltdb")
	db := openDB(t, dbPath, OpenOptions{Create: true})
	defer closeDB(t, db)

	var aliceID uint64
	var edge1ID uint64
	var edge2ID uint64

	err := db.Update(func(tx Tx) error {
		alice, err := tx.CreateNode(CreateNodeOptions{
			Labels:     []string{"Person"},
			Properties: map[string]Value{"name": "Alice", "temp": "remove"},
		})
		if err != nil {
			return err
		}
		bob, err := tx.CreateNode(CreateNodeOptions{
			Labels:     []string{"Person"},
			Properties: map[string]Value{"name": "Bob"},
		})
		if err != nil {
			return err
		}
		edge1, err := tx.CreateEdge(alice.ID, bob.ID, "REL", CreateEdgeOptions{
			Properties: map[string]Value{"w": int64(1)},
		})
		if err != nil {
			return err
		}
		edge2, err := tx.CreateEdge(alice.ID, bob.ID, "REL", CreateEdgeOptions{
			Properties: map[string]Value{"w": int64(2)},
		})
		if err != nil {
			return err
		}

		aliceID = alice.ID
		edge1ID = edge1.ID
		edge2ID = edge2.ID
		return nil
	})
	if err != nil {
		t.Fatalf("seed query graph: %v", err)
	}

	if _, err := db.Query(
		"MATCH (a:Person {name: \"Alice\"}), (b:Person {name: \"Bob\"}) CREATE (a)-[:BAD {owner: a}]->(b)",
		nil,
	); err == nil {
		t.Fatalf("expected invalid mutation query to fail")
	}

	result, err := db.Query("MATCH (:Person)-[r:BAD]->(:Person) RETURN count(r) AS count", nil)
	if err != nil {
		t.Fatalf("query bad edge count: %v", err)
	}
	requireSingleIntResult(t, result, "count", 0)

	if _, err := db.Query("MATCH (a)-[r:REL]->(b) WHERE r.w = 1 SET r.tag = \"selected\"", nil); err != nil {
		t.Fatalf("selective edge mutation query: %v", err)
	}

	err = db.View(func(tx Tx) error {
		tag1, ok, err := tx.GetEdgeProperty(edge1ID, "tag")
		if err != nil {
			return err
		}
		if !ok || tag1 != "selected" {
			t.Fatalf("expected first parallel edge to be tagged, got ok=%v value=%#v", ok, tag1)
		}

		tag2, ok, err := tx.GetEdgeProperty(edge2ID, "tag")
		if err != nil {
			return err
		}
		if ok {
			t.Fatalf("expected second parallel edge to remain untagged, got %#v", tag2)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("validate parallel edge targeting: %v", err)
	}

	if _, err := db.Query("MATCH (n:Person {name: \"Alice\"}) SET n.temp = null", nil); err != nil {
		t.Fatalf("query null removal: %v", err)
	}

	err = db.View(func(tx Tx) error {
		_, ok, err := tx.GetProperty(aliceID, "temp")
		if err != nil {
			return err
		}
		if ok {
			t.Fatalf("expected query SET ... = null to remove property")
		}
		outgoing, err := tx.GetOutgoingEdges(aliceID)
		if err != nil {
			return err
		}
		if len(outgoing) != 2 {
			t.Fatalf("expected both parallel edges to remain, got %d", len(outgoing))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("validate query mutation semantics: %v", err)
	}
}

func TestConformanceSearchSemanticsAndQueryCache(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "search.ltdb")
	db := openDB(t, dbPath, OpenOptions{
		Create:           true,
		EnableVector:     true,
		VectorDimensions: 4,
	})
	defer closeDB(t, db)

	if err := db.CacheClear(); err != nil {
		t.Fatalf("clear cache before queries: %v", err)
	}
	initialStats, err := db.CacheStats()
	if err != nil {
		t.Fatalf("initial cache stats: %v", err)
	}
	if initialStats.Entries != 0 || initialStats.Hits != 0 || initialStats.Misses != 0 {
		t.Fatalf("unexpected initial cache stats: %#v", initialStats)
	}

	var nearDocID uint64
	var farDocID uint64
	err = db.Update(func(tx Tx) error {
		categoryDB, err := tx.CreateNode(CreateNodeOptions{
			Labels:     []string{"Category"},
			Properties: map[string]Value{"name": "Databases"},
		})
		if err != nil {
			return err
		}
		categoryMisc, err := tx.CreateNode(CreateNodeOptions{
			Labels:     []string{"Category"},
			Properties: map[string]Value{"name": "Misc"},
		})
		if err != nil {
			return err
		}

		docNear, err := tx.CreateNode(CreateNodeOptions{
			Labels:     []string{"Document"},
			Properties: map[string]Value{"name": "Doc Candidate"},
		})
		if err != nil {
			return err
		}
		if err := tx.SetVector(docNear.ID, "embedding", []float32{1.0, 0.0, 0.0, 0.0}); err != nil {
			return err
		}
		if err := tx.FTSIndex(docNear.ID, "graph databases and traversal"); err != nil {
			return err
		}
		nearDocID = docNear.ID

		docFar, err := tx.CreateNode(CreateNodeOptions{
			Labels:     []string{"Document"},
			Properties: map[string]Value{"name": "Doc Far"},
		})
		if err != nil {
			return err
		}
		if err := tx.SetVector(docFar.ID, "embedding", []float32{0.0, 1.0, 0.0, 0.0}); err != nil {
			return err
		}
		if err := tx.FTSIndex(docFar.ID, "cooking recipes and ingredients"); err != nil {
			return err
		}
		farDocID = docFar.ID

		if _, err := tx.CreateEdge(docNear.ID, categoryDB.ID, "TAGGED", CreateEdgeOptions{}); err != nil {
			return err
		}
		if _, err := tx.CreateEdge(docFar.ID, categoryMisc.ID, "TAGGED", CreateEdgeOptions{}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed search graph: %v", err)
	}

	vectorResults, err := db.VectorSearch([]float32{1.0, 0.0, 0.0, 0.0}, VectorSearchOptions{K: 2})
	if err != nil {
		t.Fatalf("direct vector search: %v", err)
	}
	if len(vectorResults) < 2 {
		t.Fatalf("expected at least 2 vector results, got %d", len(vectorResults))
	}
	if vectorResults[0].NodeID != nearDocID {
		t.Fatalf("expected nearest vector result %d first, got %d", nearDocID, vectorResults[0].NodeID)
	}
	if vectorResults[1].NodeID != farDocID {
		t.Fatalf("expected farther vector result %d second, got %d", farDocID, vectorResults[1].NodeID)
	}

	vectorQuery, err := db.Query(
		"MATCH (d:Document)-[:TAGGED]->(c:Category) WHERE d.embedding <=> $query RETURN c.name AS category, d.name AS document LIMIT 1",
		map[string]Value{"query": []float32{1.0, 0.0, 0.0, 0.0}},
	)
	if err != nil {
		t.Fatalf("vector query: %v", err)
	}
	requireSingleStringResult(t, vectorQuery, "category", "Databases")
	requireSingleStringResult(t, vectorQuery, "document", "Doc Candidate")

	ftsResults, err := db.FTSSearch("graph databases", FTSSearchOptions{Limit: 5})
	if err != nil {
		t.Fatalf("direct fts search: %v", err)
	}
	if len(ftsResults) == 0 {
		t.Fatalf("expected direct fts results")
	}
	if ftsResults[0].NodeID != nearDocID {
		t.Fatalf("expected graph-focused document %d to rank first, got %d", nearDocID, ftsResults[0].NodeID)
	}

	ftsQuery, err := db.Query(
		"MATCH (d:Document)-[:TAGGED]->(c:Category) WHERE d.text @@ \"graph\" RETURN c.name AS category, d.name AS document",
		nil,
	)
	if err != nil {
		t.Fatalf("fts query: %v", err)
	}
	requireSingleStringResult(t, ftsQuery, "category", "Databases")
	requireSingleStringResult(t, ftsQuery, "document", "Doc Candidate")

	statsAfterFirst, err := db.CacheStats()
	if err != nil {
		t.Fatalf("cache stats after first query batch: %v", err)
	}
	if statsAfterFirst.Misses < 1 {
		t.Fatalf("expected cache misses after first queries, got %#v", statsAfterFirst)
	}

	if _, err := db.Query(
		"MATCH (d:Document)-[:TAGGED]->(c:Category) WHERE d.embedding <=> $query RETURN c.name AS category, d.name AS document LIMIT 1",
		map[string]Value{"query": []float32{1.0, 0.0, 0.0, 0.0}},
	); err != nil {
		t.Fatalf("repeat vector query: %v", err)
	}

	statsAfterSecond, err := db.CacheStats()
	if err != nil {
		t.Fatalf("cache stats after repeat query: %v", err)
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
		t.Fatalf("expected cache entries to reset to zero, got %#v", statsAfterClear)
	}
}

func openDB(t *testing.T, path string, opts OpenOptions) Database {
	t.Helper()
	db, err := testDriver.Open(path, opts)
	if err != nil {
		t.Fatalf("open db %s: %v", path, err)
	}
	return db
}

func closeDB(t *testing.T, db Database) {
	t.Helper()
	if db == nil {
		return
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
}

func beginTx(t *testing.T, db Database, readOnly bool) Tx {
	t.Helper()
	tx, err := db.Begin(readOnly)
	if err != nil {
		t.Fatalf("begin tx (readOnly=%v): %v", readOnly, err)
	}
	return tx
}

func rollbackTx(t *testing.T, tx Tx) {
	t.Helper()
	if tx == nil {
		return
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback tx: %v", err)
	}
}

func requireNodeExists(t *testing.T, tx Tx, nodeID uint64, want bool) {
	t.Helper()
	exists, err := tx.NodeExists(nodeID)
	if err != nil {
		t.Fatalf("node exists %d: %v", nodeID, err)
	}
	if exists != want {
		t.Fatalf("node %d exists=%v, want %v", nodeID, exists, want)
	}
}

func requireStringProperty(t *testing.T, tx Tx, nodeID uint64, key string) string {
	t.Helper()
	value, ok, err := tx.GetProperty(nodeID, key)
	if err != nil {
		t.Fatalf("get property %q on node %d: %v", key, nodeID, err)
	}
	if !ok {
		t.Fatalf("missing property %q on node %d", key, nodeID)
	}
	text, ok := value.(string)
	if !ok {
		t.Fatalf("property %q on node %d is %T, want string", key, nodeID, value)
	}
	return text
}

func requireSingleIntResult(t *testing.T, result QueryResult, column string, want int64) {
	t.Helper()
	value := requireSingleValue(t, result, column)
	got, ok := value.(int64)
	if !ok {
		t.Fatalf("column %q is %T, want int64", column, value)
	}
	if got != want {
		t.Fatalf("column %q = %d, want %d", column, got, want)
	}
}

func requireSingleStringResult(t *testing.T, result QueryResult, column string, want string) {
	t.Helper()
	value := requireSingleValue(t, result, column)
	got, ok := value.(string)
	if !ok {
		t.Fatalf("column %q is %T, want string", column, value)
	}
	if got != want {
		t.Fatalf("column %q = %q, want %q", column, got, want)
	}
}

func requireSingleValue(t *testing.T, result QueryResult, column string) Value {
	t.Helper()
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	value, ok := result.Rows[0][column]
	if !ok {
		t.Fatalf("missing column %q in row %#v", column, result.Rows[0])
	}
	return value
}

func cloneValueMap(values map[string]Value) map[string]Value {
	if values == nil {
		return nil
	}
	out := make(map[string]Value, len(values))
	for key, value := range values {
		out[key] = cloneValue(value)
	}
	return out
}

func cloneValue(value Value) Value {
	switch v := value.(type) {
	case nil, bool, int64, float64, string:
		return v
	case []byte:
		return append([]byte(nil), v...)
	case []float32:
		return append([]float32(nil), v...)
	case []any:
		out := make([]Value, len(v))
		for i, item := range v {
			out[i] = cloneValue(item)
		}
		return out
	case map[string]any:
		out := make(map[string]Value, len(v))
		for key, item := range v {
			out[key] = cloneValue(item)
		}
		return out
	default:
		return v
	}
}
