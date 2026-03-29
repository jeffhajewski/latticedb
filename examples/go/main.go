package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	latticedb "github.com/jeffhajewski/latticedb/bindings/go"
)

type dataset struct {
	Papers    []paper    `json:"papers"`
	Citations [][]string `json:"citations"`
	Topics    []string   `json:"topics"`
}

type paper struct {
	Key      string   `json:"key"`
	Title    string   `json:"title"`
	Year     int      `json:"year"`
	Venue    string   `json:"venue"`
	Author   string   `json:"author"`
	Topics   []string `json:"topics"`
	Abstract string   `json:"abstract"`
}

type graphStats struct {
	Papers    int
	Authors   int
	Topics    int
	Citations int
}

type paperResult struct {
	NodeID   latticedb.NodeID
	Title    string
	Year     int
	Venue    string
	Abstract string
	Distance float32
	Score    float32
	Via      string
	Paths    int
}

func main() {
	data, err := loadDataset()
	if err != nil {
		fatalf("load dataset: %v", err)
	}

	dbPath := filepath.Join(os.TempDir(), "latticedb-go-paper-graph-rag.db")
	_ = os.Remove(dbPath)

	db, err := latticedb.Open(dbPath, latticedb.OpenOptions{
		Create:           true,
		EnableVectors:    true,
		VectorDimensions: 128,
	})
	if err != nil {
		fatalf("open db: %v", err)
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			fatalf("close db: %v", closeErr)
		}
	}()

	embed := func(text string) ([]float32, error) {
		return latticedb.HashEmbed(text, 128)
	}

	printSection("Act 0: Build The Knowledge Graph")
	stats, err := createGraph(db, data, embed)
	if err != nil {
		fatalf("create graph: %v", err)
	}
	fmt.Printf(
		"Built graph at %s with %d papers, %d authors, %d topics, %d citations\n",
		dbPath,
		stats.Papers,
		stats.Authors,
		stats.Topics,
		stats.Citations,
	)

	query := "How can retrieval improve large language model accuracy?"

	printSection("Act 1: Vector Search Only")
	seeds, err := vectorSearchOnly(db, data, query, embed, 5)
	if err != nil {
		fatalf("vector search: %v", err)
	}
	printPapers(seeds)

	printSection("Act 2: Graph Expansion")
	expanded, err := graphExpand(db, data, seeds)
	if err != nil {
		fatalf("graph expand: %v", err)
	}
	printPapers(limitPapers(expanded, 8))

	printSection("Act 3: Full-Text Search")
	fts, err := ftsSearchDemo(db, data, "retrieval augmented generation", 5)
	if err != nil {
		fatalf("fts search: %v", err)
	}
	printPapers(fts)

	printSection("Summary")
	fmt.Printf("Vector-only context papers: %d\n", len(seeds))
	fmt.Printf("Graph-expanded papers: %d\n", len(expanded))
	fmt.Printf("FTS keyword matches: %d\n", len(fts))
	fmt.Println("The graph finds related work through citations and shared authors, not just embedding similarity.")
}

func loadDataset() (*dataset, error) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return nil, fmt.Errorf("resolve example path")
	}

	dataPath := filepath.Join(filepath.Dir(filename), "..", "papers.json")
	raw, err := os.ReadFile(dataPath)
	if err != nil {
		return nil, err
	}

	var data dataset
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, err
	}
	return &data, nil
}

func createGraph(
	db *latticedb.DB,
	data *dataset,
	embed func(string) ([]float32, error),
) (graphStats, error) {
	paperIDs := make(map[string]latticedb.NodeID, len(data.Papers))
	authorIDs := make(map[string]latticedb.NodeID)
	topicIDs := make(map[string]latticedb.NodeID, len(data.Topics))

	err := db.Update(func(tx *latticedb.Tx) error {
		for _, name := range data.Topics {
			node, err := tx.CreateNode(latticedb.CreateNodeOptions{
				Labels:     []string{"Topic"},
				Properties: map[string]latticedb.Value{"name": name},
			})
			if err != nil {
				return err
			}
			topicIDs[name] = node.ID
		}

		for _, item := range data.Papers {
			if _, exists := authorIDs[item.Author]; exists {
				continue
			}
			node, err := tx.CreateNode(latticedb.CreateNodeOptions{
				Labels:     []string{"Author"},
				Properties: map[string]latticedb.Value{"name": item.Author},
			})
			if err != nil {
				return err
			}
			authorIDs[item.Author] = node.ID
		}

		for _, item := range data.Papers {
			node, err := tx.CreateNode(latticedb.CreateNodeOptions{
				Labels: []string{"Paper"},
				Properties: map[string]latticedb.Value{
					"title": item.Title,
					"year":  item.Year,
					"venue": item.Venue,
					"key":   item.Key,
				},
			})
			if err != nil {
				return err
			}
			paperIDs[item.Key] = node.ID

			text := item.Title + ". " + item.Abstract
			vec, err := embed(text)
			if err != nil {
				return err
			}
			if err := tx.SetVector(node.ID, "embedding", vec); err != nil {
				return err
			}
			if err := tx.FTSIndex(node.ID, text); err != nil {
				return err
			}
			if _, err := tx.CreateEdge(node.ID, authorIDs[item.Author], "AUTHORED_BY", latticedb.CreateEdgeOptions{}); err != nil {
				return err
			}
			for _, topicName := range item.Topics {
				if _, err := tx.CreateEdge(node.ID, topicIDs[topicName], "ABOUT", latticedb.CreateEdgeOptions{}); err != nil {
					return err
				}
			}
		}

		for _, citation := range data.Citations {
			if len(citation) != 2 {
				return fmt.Errorf("invalid citation entry: %#v", citation)
			}
			if _, err := tx.CreateEdge(
				paperIDs[citation[0]],
				paperIDs[citation[1]],
				"CITES",
				latticedb.CreateEdgeOptions{},
			); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return graphStats{}, err
	}

	return graphStats{
		Papers:    len(data.Papers),
		Authors:   len(authorIDs),
		Topics:    len(data.Topics),
		Citations: len(data.Citations),
	}, nil
}

func vectorSearchOnly(
	db *latticedb.DB,
	data *dataset,
	query string,
	embed func(string) ([]float32, error),
	k uint32,
) ([]paperResult, error) {
	queryVec, err := embed(query)
	if err != nil {
		return nil, err
	}

	results, err := db.VectorSearch(queryVec, latticedb.VectorSearchOptions{K: k})
	if err != nil {
		return nil, err
	}

	papers := make([]paperResult, 0, len(results))
	err = db.View(func(tx *latticedb.Tx) error {
		for _, result := range results {
			item, err := paperResultFromNode(tx, data, result.NodeID)
			if err != nil {
				return err
			}
			item.Distance = result.Distance
			papers = append(papers, item)
		}
		return nil
	})
	return papers, err
}

func graphExpand(db *latticedb.DB, data *dataset, seeds []paperResult) ([]paperResult, error) {
	seedIDs := make(map[latticedb.NodeID]struct{}, len(seeds))
	for _, seed := range seeds {
		seedIDs[seed.NodeID] = struct{}{}
	}

	discovered := make(map[latticedb.NodeID]int)
	discoveryMethod := make(map[latticedb.NodeID]string)
	addDiscovery := func(nodeID latticedb.NodeID, method string) {
		discovered[nodeID]++
		if _, exists := discoveryMethod[nodeID]; !exists {
			discoveryMethod[nodeID] = method
		}
	}

	err := db.View(func(tx *latticedb.Tx) error {
		for _, seed := range seeds {
			outgoing, err := tx.GetOutgoingEdges(seed.NodeID)
			if err != nil {
				return err
			}
			for _, edge := range outgoing {
				if edge.Type == "CITES" {
					if _, exists := seedIDs[edge.TargetID]; !exists {
						addDiscovery(edge.TargetID, fmt.Sprintf("cited by %q", shortTitle(seed.Title)))
					}
				}
			}

			incoming, err := tx.GetIncomingEdges(seed.NodeID)
			if err != nil {
				return err
			}
			for _, edge := range incoming {
				if edge.Type == "CITES" {
					if _, exists := seedIDs[edge.SourceID]; !exists {
						addDiscovery(edge.SourceID, fmt.Sprintf("cites %q", shortTitle(seed.Title)))
					}
				}
			}

			for _, edge := range outgoing {
				if edge.Type != "AUTHORED_BY" {
					continue
				}
				authorName, err := getStringProperty(tx, edge.TargetID, "name")
				if err != nil {
					return err
				}
				authorIncoming, err := tx.GetIncomingEdges(edge.TargetID)
				if err != nil {
					return err
				}
				for _, related := range authorIncoming {
					if related.Type != "AUTHORED_BY" {
						continue
					}
					if _, exists := seedIDs[related.SourceID]; exists {
						continue
					}
					addDiscovery(related.SourceID, fmt.Sprintf("same author (%s)", authorName))
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	type discoveredPaper struct {
		nodeID latticedb.NodeID
		paths  int
	}

	ordered := make([]discoveredPaper, 0, len(discovered))
	for nodeID, count := range discovered {
		ordered = append(ordered, discoveredPaper{nodeID: nodeID, paths: count})
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].paths != ordered[j].paths {
			return ordered[i].paths > ordered[j].paths
		}
		return ordered[i].nodeID < ordered[j].nodeID
	})

	expanded := make([]paperResult, 0, len(ordered))
	err = db.View(func(tx *latticedb.Tx) error {
		for _, item := range ordered {
			paper, err := paperResultFromNode(tx, data, item.nodeID)
			if err != nil {
				return err
			}
			paper.Via = discoveryMethod[item.nodeID]
			paper.Paths = item.paths
			expanded = append(expanded, paper)
		}
		return nil
	})
	return expanded, err
}

func ftsSearchDemo(db *latticedb.DB, data *dataset, query string, limit uint32) ([]paperResult, error) {
	results, err := db.FTSSearch(query, latticedb.FTSSearchOptions{Limit: limit})
	if err != nil {
		return nil, err
	}

	papers := make([]paperResult, 0, len(results))
	err = db.View(func(tx *latticedb.Tx) error {
		for _, result := range results {
			item, err := paperResultFromNode(tx, data, result.NodeID)
			if err != nil {
				return err
			}
			item.Score = result.Score
			papers = append(papers, item)
		}
		return nil
	})
	return papers, err
}

func paperResultFromNode(tx *latticedb.Tx, data *dataset, nodeID latticedb.NodeID) (paperResult, error) {
	title, err := getStringProperty(tx, nodeID, "title")
	if err != nil {
		return paperResult{}, err
	}
	year, err := getIntProperty(tx, nodeID, "year")
	if err != nil {
		return paperResult{}, err
	}
	venue, err := getStringProperty(tx, nodeID, "venue")
	if err != nil {
		return paperResult{}, err
	}
	key, err := getStringProperty(tx, nodeID, "key")
	if err != nil {
		return paperResult{}, err
	}

	return paperResult{
		NodeID:   nodeID,
		Title:    title,
		Year:     year,
		Venue:    venue,
		Abstract: findPaperAbstract(data, key),
	}, nil
}

func getStringProperty(tx *latticedb.Tx, nodeID latticedb.NodeID, key string) (string, error) {
	value, ok, err := tx.GetProperty(nodeID, key)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", fmt.Errorf("missing property %q on node %d", key, nodeID)
	}
	text, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("property %q on node %d is %T, want string", key, nodeID, value)
	}
	return text, nil
}

func getIntProperty(tx *latticedb.Tx, nodeID latticedb.NodeID, key string) (int, error) {
	value, ok, err := tx.GetProperty(nodeID, key)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, fmt.Errorf("missing property %q on node %d", key, nodeID)
	}
	switch v := value.(type) {
	case int64:
		return int(v), nil
	case int:
		return v, nil
	default:
		return 0, fmt.Errorf("property %q on node %d is %T, want int64", key, nodeID, value)
	}
}

func findPaperAbstract(data *dataset, key string) string {
	for _, item := range data.Papers {
		if item.Key == key {
			return item.Abstract
		}
	}
	return ""
}

func printSection(title string) {
	line := strings.Repeat("=", 70)
	fmt.Printf("\n%s\n  %s\n%s\n\n", line, title, line)
}

func printPapers(papers []paperResult) {
	for i, item := range papers {
		extra := ""
		if item.Via != "" {
			extra = " (" + item.Via + ")"
		}
		fmt.Printf("  %d. [%d] %s%s\n", i+1, item.Year, item.Title, extra)
	}
}

func shortTitle(title string) string {
	if len(title) <= 40 {
		return title
	}
	return title[:40] + "..."
}

func limitPapers(papers []paperResult, n int) []paperResult {
	if len(papers) <= n {
		return papers
	}
	return papers[:n]
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
