package conformance

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func TestConformanceCrashRecoveryCommittedGraphAndAbortedTail(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "crash_graph.ltdb")
	db := openDB(t, dbPath, OpenOptions{Create: true})

	var aliceID uint64
	var bobID uint64
	var edge1ID uint64
	var edge2ID uint64
	var rolledBackNodeID uint64
	var rolledBackEdgeID uint64

	err := db.Update(func(tx Tx) error {
		alice, err := tx.CreateNode(CreateNodeOptions{
			Labels: []string{"Person"},
			Properties: map[string]Value{
				"name": "Alice",
				"team": "graph",
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
		edge1, err := tx.CreateEdge(alice.ID, bob.ID, "KNOWS", CreateEdgeOptions{})
		if err != nil {
			return err
		}
		edge2, err := tx.CreateEdge(alice.ID, bob.ID, "KNOWS", CreateEdgeOptions{})
		if err != nil {
			return err
		}

		aliceID = alice.ID
		bobID = bob.ID
		edge1ID = edge1.ID
		edge2ID = edge2.ID
		return nil
	})
	if err != nil {
		t.Fatalf("seed crash graph: %v", err)
	}

	rollback := beginTx(t, db, false)
	ghost, err := rollback.CreateNode(CreateNodeOptions{
		Labels:     []string{"Ghost"},
		Properties: map[string]Value{"name": "Transient"},
	})
	if err != nil {
		t.Fatalf("create rolled-back node: %v", err)
	}
	ghostEdge, err := rollback.CreateEdge(aliceID, bobID, "KNOWS", CreateEdgeOptions{})
	if err != nil {
		t.Fatalf("create rolled-back edge: %v", err)
	}
	rolledBackNodeID = ghost.ID
	rolledBackEdgeID = ghostEdge.ID
	rollbackTx(t, rollback)

	closeDB(t, db)

	if err := testRecoveryHarness.SimulateCrash(dbPath); err != nil {
		t.Fatalf("simulate crash: %v", err)
	}

	db = openDB(t, dbPath, OpenOptions{})
	defer closeDB(t, db)

	err = db.View(func(tx Tx) error {
		alice, err := tx.GetNode(aliceID)
		if err != nil {
			return err
		}
		if alice == nil {
			t.Fatalf("expected recovered alice node")
		}
		if !reflect.DeepEqual(alice.Labels, []string{"Person"}) {
			t.Fatalf("unexpected recovered labels: %#v", alice.Labels)
		}

		team, ok, err := tx.GetProperty(aliceID, "team")
		if err != nil {
			return err
		}
		if !ok || team != "graph" {
			t.Fatalf("unexpected recovered direct property: ok=%v value=%#v", ok, team)
		}

		requireNodeExists(t, tx, aliceID, true)
		requireNodeExists(t, tx, bobID, true)
		requireNodeExists(t, tx, rolledBackNodeID, false)

		outgoing, err := tx.GetOutgoingEdges(aliceID)
		if err != nil {
			return err
		}
		if len(outgoing) != 2 {
			t.Fatalf("expected 2 recovered committed outgoing edges, got %d", len(outgoing))
		}

		recoveredIDs := make([]uint64, 0, len(outgoing))
		for _, edge := range outgoing {
			recoveredIDs = append(recoveredIDs, edge.ID)
			if edge.ID == rolledBackEdgeID {
				t.Fatalf("unexpected rolled-back edge %d after recovery", rolledBackEdgeID)
			}
		}
		slices.Sort(recoveredIDs)

		expectedIDs := []uint64{edge1ID, edge2ID}
		slices.Sort(expectedIDs)
		if !reflect.DeepEqual(recoveredIDs, expectedIDs) {
			t.Fatalf("unexpected recovered edge ids: got %#v want %#v", recoveredIDs, expectedIDs)
		}

		return nil
	})
	if err != nil {
		t.Fatalf("validate recovered direct APIs: %v", err)
	}

	var newEdgeID uint64
	err = db.Update(func(tx Tx) error {
		edge, err := tx.CreateEdge(
			aliceID,
			bobID,
			"KNOWS",
			CreateEdgeOptions{Properties: map[string]Value{"since": int64(2026)}},
		)
		if err != nil {
			return err
		}
		newEdgeID = edge.ID
		return nil
	})
	if err != nil {
		t.Fatalf("create new edge after recovery: %v", err)
	}

	if newEdgeID <= edge2ID {
		t.Fatalf(
			"expected post-recovery edge id %d to be greater than highest committed id %d",
			newEdgeID,
			edge2ID,
		)
	}
}

func TestConformanceCrashRecoveryCommittedNodePropertyUpdateWinsOverAbortedUpdate(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "crash_node_property_update.ltdb")
	db := openDB(t, dbPath, OpenOptions{Create: true})

	var nodeID uint64

	err := db.Update(func(tx Tx) error {
		node, err := tx.CreateNode(CreateNodeOptions{
			Labels:     []string{"Metric"},
			Properties: map[string]Value{"score": int64(1)},
		})
		if err != nil {
			return err
		}
		nodeID = node.ID
		return nil
	})
	if err != nil {
		t.Fatalf("seed node property graph: %v", err)
	}

	err = db.Update(func(tx Tx) error {
		return tx.SetProperty(nodeID, "score", int64(7))
	})
	if err != nil {
		t.Fatalf("commit node property update: %v", err)
	}

	rollback := beginTx(t, db, false)
	if err := rollback.SetProperty(nodeID, "score", int64(9)); err != nil {
		t.Fatalf("set rolled-back node property update: %v", err)
	}
	rollbackTx(t, rollback)

	closeDB(t, db)

	if err := testRecoveryHarness.SimulateCrash(dbPath); err != nil {
		t.Fatalf("simulate crash: %v", err)
	}

	db = openDB(t, dbPath, OpenOptions{})
	defer closeDB(t, db)

	err = db.View(func(tx Tx) error {
		requireNodeExists(t, tx, nodeID, true)

		score, ok, err := tx.GetProperty(nodeID, "score")
		if err != nil {
			return err
		}
		if !ok || score != int64(7) {
			t.Fatalf("unexpected recovered node property: ok=%v value=%#v", ok, score)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("validate recovered node property: %v", err)
	}
}

func TestConformanceCrashRecoverySecondaryLabelsAndEdgeProperties(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "crash_secondary_labels_edge_props.ltdb")
	db := openDB(t, dbPath, OpenOptions{Create: true})

	var aliceID uint64
	var bobID uint64
	var edgeID uint64

	err := db.Update(func(tx Tx) error {
		alice, err := tx.CreateNode(CreateNodeOptions{
			Labels: []string{"Person", "Employee"},
			Properties: map[string]Value{
				"name": "Alice",
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
		edge, err := tx.CreateEdge(alice.ID, bob.ID, "KNOWS", CreateEdgeOptions{
			Properties: map[string]Value{
				"since": int64(2026),
				"note":  "stable",
			},
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
		t.Fatalf("seed crash graph with secondary labels and edge properties: %v", err)
	}
	closeDB(t, db)

	if err := testRecoveryHarness.SimulateCrash(dbPath); err != nil {
		t.Fatalf("simulate crash: %v", err)
	}

	db = openDB(t, dbPath, OpenOptions{})
	defer closeDB(t, db)

	err = db.View(func(tx Tx) error {
		requireNodeExists(t, tx, aliceID, true)
		requireNodeExists(t, tx, bobID, true)

		alice, err := tx.GetNode(aliceID)
		if err != nil {
			return err
		}
		if alice == nil {
			t.Fatalf("expected recovered alice node")
		}
		if !reflect.DeepEqual(alice.Labels, []string{"Person", "Employee"}) {
			t.Fatalf("unexpected recovered alice labels: %#v", alice.Labels)
		}

		since, ok, err := tx.GetEdgeProperty(edgeID, "since")
		if err != nil {
			return err
		}
		if !ok || since != int64(2026) {
			t.Fatalf("unexpected recovered edge property since: ok=%v value=%#v", ok, since)
		}

		note, ok, err := tx.GetEdgeProperty(edgeID, "note")
		if err != nil {
			return err
		}
		if !ok || note != "stable" {
			t.Fatalf("unexpected recovered edge property note: ok=%v value=%#v", ok, note)
		}

		return nil
	})
	if err != nil {
		t.Fatalf("validate recovered secondary labels and edge properties: %v", err)
	}

	labelQuery, err := db.Query(
		"MATCH (n:Employee) RETURN id(n) AS id",
		nil,
	)
	if err != nil {
		t.Fatalf("query recovered secondary label: %v", err)
	}
	if len(labelQuery.Rows) != 1 {
		t.Fatalf("expected 1 recovered Employee row, got %d", len(labelQuery.Rows))
	}
	if labelQuery.Rows[0]["id"] != int64(aliceID) {
		t.Fatalf("unexpected recovered Employee id: %#v", labelQuery.Rows[0]["id"])
	}

	edgeQuery, err := db.Query(
		"MATCH (:Person)-[r:KNOWS]->(:Person) RETURN r.since AS since, r.note AS note",
		nil,
	)
	if err != nil {
		t.Fatalf("query recovered edge properties: %v", err)
	}
	if len(edgeQuery.Rows) != 1 {
		t.Fatalf("expected 1 recovered edge row, got %d", len(edgeQuery.Rows))
	}
	if edgeQuery.Rows[0]["since"] != int64(2026) || edgeQuery.Rows[0]["note"] != "stable" {
		t.Fatalf("unexpected recovered edge query row: %#v", edgeQuery.Rows[0])
	}
}

func TestConformanceExportAndDumpInvariants(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "export.ltdb")
	db := openDB(t, dbPath, OpenOptions{Create: true})

	var aliceID uint64
	var bobID uint64
	err := db.Update(func(tx Tx) error {
		alice, err := tx.CreateNode(CreateNodeOptions{
			Labels: []string{"Person", "Employee"},
			Properties: map[string]Value{
				"name": "Alice",
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
		if _, err := tx.CreateEdge(alice.ID, bob.ID, "REL", CreateEdgeOptions{
			Properties: map[string]Value{"since": int64(2020), "status": "active"},
		}); err != nil {
			return err
		}
		if _, err := tx.CreateEdge(alice.ID, bob.ID, "REL", CreateEdgeOptions{
			Properties: map[string]Value{"since": int64(2021)},
		}); err != nil {
			return err
		}

		aliceID = alice.ID
		bobID = bob.ID
		return nil
	})
	if err != nil {
		t.Fatalf("seed export graph: %v", err)
	}
	closeDB(t, db)

	jsonPath := filepath.Join(t.TempDir(), "graph.json")
	if _, err := testExporter.Export(dbPath, ExportFormatJSON, jsonPath); err != nil {
		t.Fatalf("export json: %v", err)
	}
	jsonGraph := readJSONGraphFromFile(t, jsonPath)
	requireGraphCounts(t, jsonGraph, 2, 2)
	requireExportEdgeProperties(t, jsonGraph)
	requireSingleNodeID(t, jsonGraph, fmt.Sprintf("%d", aliceID))
	requireSingleNodeID(t, jsonGraph, fmt.Sprintf("%d", bobID))

	jsonlPath := filepath.Join(t.TempDir(), "graph.jsonl")
	if _, err := testExporter.Export(dbPath, ExportFormatJSONL, jsonlPath); err != nil {
		t.Fatalf("export jsonl: %v", err)
	}
	validateJSONLExport(t, jsonlPath)

	csvPath := filepath.Join(t.TempDir(), "graph.csv")
	if _, err := testExporter.Export(dbPath, ExportFormatCSV, csvPath); err != nil {
		t.Fatalf("export csv: %v", err)
	}
	nodesCSV := strings.TrimSuffix(csvPath, ".csv") + "_nodes.csv"
	edgesCSV := strings.TrimSuffix(csvPath, ".csv") + "_edges.csv"
	if lines := countNonEmptyLinesFile(t, nodesCSV); lines != 3 {
		t.Fatalf("unexpected node csv line count %d", lines)
	}
	if lines := countNonEmptyLinesFile(t, edgesCSV); lines != 3 {
		t.Fatalf("unexpected edge csv line count %d", lines)
	}

	dotPath := filepath.Join(t.TempDir(), "graph.dot")
	if _, err := testExporter.Export(dbPath, ExportFormatDOT, dotPath); err != nil {
		t.Fatalf("export dot: %v", err)
	}
	validateDOTExport(t, dotPath)

	dumpGraph := readJSONGraphBytes(t, mustDump(t, dbPath))
	requireGraphCounts(t, dumpGraph, 2, 2)
	requireExportEdgeProperties(t, dumpGraph)
}

type exportedGraph struct {
	Nodes []exportedNode `json:"nodes"`
	Edges []exportedEdge `json:"edges"`
}

type exportedNode struct {
	ID         string         `json:"id"`
	Labels     []string       `json:"labels"`
	Properties map[string]any `json:"properties"`
}

type exportedEdge struct {
	ID         string         `json:"id"`
	Source     string         `json:"source"`
	Target     string         `json:"target"`
	Type       string         `json:"type"`
	Properties map[string]any `json:"properties"`
}

func mustDump(t *testing.T, dbPath string) []byte {
	t.Helper()
	output, err := testExporter.Dump(dbPath)
	if err != nil {
		t.Fatalf("dump database: %v", err)
	}
	return output
}

func readJSONGraphFromFile(t *testing.T, path string) exportedGraph {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read json export %s: %v", path, err)
	}
	return readJSONGraphBytes(t, data)
}

func readJSONGraphBytes(t *testing.T, data []byte) exportedGraph {
	t.Helper()
	var graph exportedGraph
	if err := json.Unmarshal(data, &graph); err != nil {
		t.Fatalf("unmarshal graph export: %v\n%s", err, data)
	}
	return graph
}

func validateJSONLExport(t *testing.T, path string) {
	t.Helper()

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open jsonl export %s: %v", path, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	nodeIDs := map[string]struct{}{}
	nodeCount := 0
	edgeCount := 0
	found2020 := false
	found2021 := false

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("unmarshal jsonl line %q: %v", line, err)
		}

		switch record["kind"] {
		case "node":
			nodeCount++
			nodeIDs[fmt.Sprint(record["id"])] = struct{}{}
		case "edge":
			edgeCount++
			props, ok := record["properties"].(map[string]any)
			if !ok {
				t.Fatalf("jsonl edge missing properties object: %#v", record)
			}
			switch jsonIntValue(t, props["since"]) {
			case 2020:
				found2020 = true
			case 2021:
				found2021 = true
			}
		default:
			t.Fatalf("unexpected jsonl record kind: %#v", record["kind"])
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan jsonl export %s: %v", path, err)
	}

	if nodeCount != 2 || len(nodeIDs) != 2 {
		t.Fatalf("expected 2 unique jsonl node records, got count=%d unique=%d", nodeCount, len(nodeIDs))
	}
	if edgeCount != 2 {
		t.Fatalf("expected 2 jsonl edge records, got %d", edgeCount)
	}
	if !found2020 || !found2021 {
		t.Fatalf("expected jsonl export to preserve both parallel edges")
	}
}

func validateDOTExport(t *testing.T, path string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read dot export %s: %v", path, err)
	}

	output := string(data)
	if !strings.HasPrefix(output, "digraph G {\n") {
		t.Fatalf("dot export missing digraph header:\n%s", output)
	}
	if !strings.HasSuffix(output, "}\n") {
		t.Fatalf("dot export missing closing brace:\n%s", output)
	}

	nodeLines := 0
	edgeLines := 0
	for _, line := range strings.Split(output, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if strings.Contains(trimmed, " -> ") {
			edgeLines++
			continue
		}
		if strings.HasPrefix(trimmed, "n") && strings.HasSuffix(trimmed, "];") {
			nodeLines++
		}
	}
	if nodeLines != 2 {
		t.Fatalf("expected 2 dot node lines, got %d", nodeLines)
	}
	if edgeLines != 2 {
		t.Fatalf("expected 2 dot edge lines, got %d", edgeLines)
	}
}

func countNonEmptyLinesFile(t *testing.T, path string) int {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read csv export %s: %v", path, err)
	}

	count := 0
	for _, line := range strings.Split(string(data), "\n") {
		if strings.TrimSpace(line) != "" {
			count++
		}
	}
	return count
}

func requireGraphCounts(t *testing.T, graph exportedGraph, wantNodes int, wantEdges int) {
	t.Helper()
	if len(graph.Nodes) != wantNodes {
		t.Fatalf("expected %d exported nodes, got %d", wantNodes, len(graph.Nodes))
	}
	if len(graph.Edges) != wantEdges {
		t.Fatalf("expected %d exported edges, got %d", wantEdges, len(graph.Edges))
	}
}

func requireExportEdgeProperties(t *testing.T, graph exportedGraph) {
	t.Helper()

	found2020 := false
	found2021 := false
	for _, edge := range graph.Edges {
		switch jsonIntValue(t, edge.Properties["since"]) {
		case 2020:
			found2020 = true
			if status := fmt.Sprint(edge.Properties["status"]); status != "active" {
				t.Fatalf("expected 2020 edge status active, got %#v", edge.Properties["status"])
			}
		case 2021:
			found2021 = true
		}
	}
	if !found2020 || !found2021 {
		t.Fatalf("expected export to preserve both parallel edges, got %#v", graph.Edges)
	}
}

func requireSingleNodeID(t *testing.T, graph exportedGraph, wantID string) {
	t.Helper()
	count := 0
	for _, node := range graph.Nodes {
		if node.ID == wantID {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("expected node id %s exactly once in export, got %d matches", wantID, count)
	}
}

func jsonIntValue(t *testing.T, value any) int64 {
	t.Helper()
	switch v := value.(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	case json.Number:
		n, err := v.Int64()
		if err != nil {
			t.Fatalf("parse json number %q: %v", v, err)
		}
		return n
	default:
		t.Fatalf("unexpected numeric json value type %T (%#v)", value, value)
		return 0
	}
}
