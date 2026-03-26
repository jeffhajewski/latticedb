package conformance

import (
	latticedb "github.com/jeffhajewski/latticedb/bindings/go"
)

type latticeDriver struct{}

func (latticeDriver) Open(path string, opts OpenOptions) (Database, error) {
	db, err := latticedb.Open(path, latticedb.OpenOptions{
		Create:           opts.Create,
		ReadOnly:         opts.ReadOnly,
		CacheSizeMB:      opts.CacheSizeMB,
		PageSize:         opts.PageSize,
		EnableVector:     opts.EnableVector,
		VectorDimensions: opts.VectorDimensions,
	})
	if err != nil {
		return nil, err
	}
	return &latticeDB{db: db}, nil
}

type latticeDB struct {
	db *latticedb.DB
}

func (db *latticeDB) Close() error {
	return db.db.Close()
}

func (db *latticeDB) Begin(readOnly bool) (Tx, error) {
	tx, err := db.db.Begin(readOnly)
	if err != nil {
		return nil, err
	}
	return &latticeTx{tx: tx}, nil
}

func (db *latticeDB) View(fn func(Tx) error) error {
	return db.db.View(func(tx *latticedb.Tx) error {
		return fn(&latticeTx{tx: tx})
	})
}

func (db *latticeDB) Update(fn func(Tx) error) error {
	return db.db.Update(func(tx *latticedb.Tx) error {
		return fn(&latticeTx{tx: tx})
	})
}

func (db *latticeDB) Query(cypher string, params map[string]Value) (QueryResult, error) {
	result, err := db.db.Query(cypher, params)
	if err != nil {
		return QueryResult{}, err
	}
	return QueryResult{
		Columns: append([]string(nil), result.Columns...),
		Rows:    cloneRows(result.Rows),
	}, nil
}

func (db *latticeDB) VectorSearch(vector []float32, opts VectorSearchOptions) ([]VectorSearchResult, error) {
	results, err := db.db.VectorSearch(vector, latticedb.VectorSearchOptions{
		K:        opts.K,
		EfSearch: opts.EfSearch,
	})
	if err != nil {
		return nil, err
	}

	out := make([]VectorSearchResult, 0, len(results))
	for _, result := range results {
		out = append(out, VectorSearchResult{
			NodeID:   uint64(result.NodeID),
			Distance: result.Distance,
		})
	}
	return out, nil
}

func (db *latticeDB) FTSSearch(query string, opts FTSSearchOptions) ([]FTSSearchResult, error) {
	results, err := db.db.FTSSearch(query, latticedb.FTSSearchOptions{
		Limit:         opts.Limit,
		MaxDistance:   opts.MaxDistance,
		MinTermLength: opts.MinTermLength,
	})
	if err != nil {
		return nil, err
	}

	out := make([]FTSSearchResult, 0, len(results))
	for _, result := range results {
		out = append(out, FTSSearchResult{
			NodeID: uint64(result.NodeID),
			Score:  result.Score,
		})
	}
	return out, nil
}

func (db *latticeDB) CacheClear() error {
	return db.db.CacheClear()
}

func (db *latticeDB) CacheStats() (QueryCacheStats, error) {
	stats, err := db.db.CacheStats()
	if err != nil {
		return QueryCacheStats{}, err
	}
	return QueryCacheStats{
		Entries: stats.Entries,
		Hits:    stats.Hits,
		Misses:  stats.Misses,
	}, nil
}

type latticeTx struct {
	tx *latticedb.Tx
}

func (tx *latticeTx) Commit() error {
	return tx.tx.Commit()
}

func (tx *latticeTx) Rollback() error {
	return tx.tx.Rollback()
}

func (tx *latticeTx) CreateNode(opts CreateNodeOptions) (Node, error) {
	node, err := tx.tx.CreateNode(latticedb.CreateNodeOptions{
		Labels:     append([]string(nil), opts.Labels...),
		Properties: cloneValueMap(opts.Properties),
	})
	if err != nil {
		return Node{}, err
	}
	return Node{
		ID:         uint64(node.ID),
		Labels:     append([]string(nil), node.Labels...),
		Properties: cloneValueMap(node.Properties),
	}, nil
}

func (tx *latticeTx) DeleteNode(nodeID uint64) error {
	return tx.tx.DeleteNode(latticedb.NodeID(nodeID))
}

func (tx *latticeTx) NodeExists(nodeID uint64) (bool, error) {
	return tx.tx.NodeExists(latticedb.NodeID(nodeID))
}

func (tx *latticeTx) GetNode(nodeID uint64) (*Node, error) {
	node, err := tx.tx.GetNode(latticedb.NodeID(nodeID))
	if err != nil {
		return nil, err
	}
	if node == nil {
		return nil, nil
	}
	return &Node{
		ID:         uint64(node.ID),
		Labels:     append([]string(nil), node.Labels...),
		Properties: cloneValueMap(node.Properties),
	}, nil
}

func (tx *latticeTx) SetProperty(nodeID uint64, key string, value Value) error {
	return tx.tx.SetProperty(latticedb.NodeID(nodeID), key, value)
}

func (tx *latticeTx) GetProperty(nodeID uint64, key string) (Value, bool, error) {
	return tx.tx.GetProperty(latticedb.NodeID(nodeID), key)
}

func (tx *latticeTx) SetVector(nodeID uint64, key string, vector []float32) error {
	return tx.tx.SetVector(latticedb.NodeID(nodeID), key, vector)
}

func (tx *latticeTx) FTSIndex(nodeID uint64, text string) error {
	return tx.tx.FTSIndex(latticedb.NodeID(nodeID), text)
}

func (tx *latticeTx) CreateEdge(sourceID, targetID uint64, edgeType string, opts CreateEdgeOptions) (Edge, error) {
	edge, err := tx.tx.CreateEdge(
		latticedb.NodeID(sourceID),
		latticedb.NodeID(targetID),
		edgeType,
		latticedb.CreateEdgeOptions{Properties: cloneValueMap(opts.Properties)},
	)
	if err != nil {
		return Edge{}, err
	}
	return Edge{
		ID:         uint64(edge.ID),
		SourceID:   uint64(edge.SourceID),
		TargetID:   uint64(edge.TargetID),
		Type:       edge.Type,
		Properties: cloneValueMap(edge.Properties),
	}, nil
}

func (tx *latticeTx) GetEdgeProperty(edgeID uint64, key string) (Value, bool, error) {
	return tx.tx.GetEdgeProperty(latticedb.EdgeID(edgeID), key)
}

func (tx *latticeTx) SetEdgeProperty(edgeID uint64, key string, value Value) error {
	return tx.tx.SetEdgeProperty(latticedb.EdgeID(edgeID), key, value)
}

func (tx *latticeTx) RemoveEdgeProperty(edgeID uint64, key string) error {
	return tx.tx.RemoveEdgeProperty(latticedb.EdgeID(edgeID), key)
}

func (tx *latticeTx) GetOutgoingEdges(nodeID uint64) ([]Edge, error) {
	edges, err := tx.tx.GetOutgoingEdges(latticedb.NodeID(nodeID))
	if err != nil {
		return nil, err
	}

	out := make([]Edge, 0, len(edges))
	for _, edge := range edges {
		out = append(out, Edge{
			ID:         uint64(edge.ID),
			SourceID:   uint64(edge.SourceID),
			TargetID:   uint64(edge.TargetID),
			Type:       edge.Type,
			Properties: cloneValueMap(edge.Properties),
		})
	}
	return out, nil
}

func cloneRows(rows []map[string]latticedb.Value) []map[string]Value {
	out := make([]map[string]Value, len(rows))
	for i, row := range rows {
		out[i] = cloneValueMap(row)
	}
	return out
}
