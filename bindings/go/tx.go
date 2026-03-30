package latticedb

import cgobridge "github.com/jeffhajewski/latticedb/bindings/go/internal/cgo"

type Tx struct {
	db       *DB
	raw      *cgobridge.Tx
	readOnly bool
	active   bool
}

func (tx *Tx) IsReadOnly() bool {
	return tx != nil && tx.readOnly
}

func (tx *Tx) IsActive() bool {
	return tx != nil && tx.active
}

func (tx *Tx) Commit() error {
	if tx == nil || !tx.active || tx.raw == nil {
		return ErrInactiveTx
	}
	err := wrapError(tx.raw.Commit())
	if err == nil {
		tx.raw = nil
		tx.active = false
	}
	return err
}

func (tx *Tx) Rollback() error {
	if tx == nil || !tx.active || tx.raw == nil {
		return nil
	}
	err := wrapError(tx.raw.Rollback())
	if err == nil {
		tx.raw = nil
		tx.active = false
	}
	return err
}

func (tx *Tx) CreateNode(opts CreateNodeOptions) (Node, error) {
	if err := tx.ensureWritable(); err != nil {
		return Node{}, err
	}

	var nodeID uint64
	var err error
	if len(opts.Labels) > 0 {
		nodeID, err = tx.raw.CreateNode(opts.Labels[0])
	} else {
		nodeID, err = tx.raw.CreateNode("")
	}
	if err != nil {
		return Node{}, wrapError(err)
	}

	for _, label := range opts.Labels[1:] {
		if err := wrapError(tx.raw.AddNodeLabel(nodeID, label)); err != nil {
			return Node{}, err
		}
	}

	for key, value := range opts.Properties {
		if err := wrapError(tx.raw.SetNodeProperty(nodeID, key, value)); err != nil {
			return Node{}, err
		}
	}

	return Node{
		ID:         NodeID(nodeID),
		Labels:     append([]string(nil), opts.Labels...),
		Properties: cloneValueMap(opts.Properties),
	}, nil
}

func (tx *Tx) DeleteNode(nodeID NodeID) error {
	if err := tx.ensureWritable(); err != nil {
		return err
	}
	return wrapError(tx.raw.DeleteNode(uint64(nodeID)))
}

func (tx *Tx) NodeExists(nodeID NodeID) (bool, error) {
	if err := tx.ensureActive(); err != nil {
		return false, err
	}
	exists, err := tx.raw.NodeExists(uint64(nodeID))
	return exists, wrapError(err)
}

func (tx *Tx) GetNode(nodeID NodeID) (*Node, error) {
	if err := tx.ensureActive(); err != nil {
		return nil, err
	}

	exists, err := tx.raw.NodeExists(uint64(nodeID))
	if err != nil {
		return nil, wrapError(err)
	}
	if !exists {
		return nil, nil
	}

	labels, err := tx.raw.GetNodeLabels(uint64(nodeID))
	if err != nil {
		return nil, wrapError(err)
	}

	return &Node{
		ID:         nodeID,
		Labels:     labels,
		Properties: map[string]Value{},
	}, nil
}

func (tx *Tx) SetProperty(nodeID NodeID, key string, value Value) error {
	if err := tx.ensureWritable(); err != nil {
		return err
	}
	return wrapError(tx.raw.SetNodeProperty(uint64(nodeID), key, value))
}

func (tx *Tx) SetVector(nodeID NodeID, key string, vector []float32) error {
	if err := tx.ensureWritable(); err != nil {
		return err
	}
	return wrapError(tx.raw.SetNodeVector(uint64(nodeID), key, vector))
}

func (tx *Tx) GetProperty(nodeID NodeID, key string) (Value, bool, error) {
	if err := tx.ensureActive(); err != nil {
		return nil, false, err
	}
	value, ok, err := tx.raw.GetNodeProperty(uint64(nodeID), key)
	return value, ok, wrapError(err)
}

func (tx *Tx) CreateEdge(sourceID, targetID NodeID, edgeType string, opts CreateEdgeOptions) (Edge, error) {
	if err := tx.ensureWritable(); err != nil {
		return Edge{}, err
	}

	edgeID, err := tx.raw.CreateEdge(uint64(sourceID), uint64(targetID), edgeType)
	if err != nil {
		return Edge{}, wrapError(err)
	}

	for key, value := range opts.Properties {
		if err := wrapError(tx.raw.SetEdgeProperty(edgeID, key, value)); err != nil {
			return Edge{}, err
		}
	}

	return Edge{
		ID:         EdgeID(edgeID),
		SourceID:   sourceID,
		TargetID:   targetID,
		Type:       edgeType,
		Properties: cloneValueMap(opts.Properties),
	}, nil
}

func (tx *Tx) DeleteEdge(sourceID, targetID NodeID, edgeType string) error {
	if err := tx.ensureWritable(); err != nil {
		return err
	}
	return wrapError(tx.raw.DeleteEdge(uint64(sourceID), uint64(targetID), edgeType))
}

func (tx *Tx) SetEdgeProperty(edgeID EdgeID, key string, value Value) error {
	if err := tx.ensureWritable(); err != nil {
		return err
	}
	return wrapError(tx.raw.SetEdgeProperty(uint64(edgeID), key, value))
}

func (tx *Tx) GetEdgeProperty(edgeID EdgeID, key string) (Value, bool, error) {
	if err := tx.ensureActive(); err != nil {
		return nil, false, err
	}
	value, ok, err := tx.raw.GetEdgeProperty(uint64(edgeID), key)
	return value, ok, wrapError(err)
}

func (tx *Tx) RemoveEdgeProperty(edgeID EdgeID, key string) error {
	if err := tx.ensureWritable(); err != nil {
		return err
	}
	return wrapError(tx.raw.RemoveEdgeProperty(uint64(edgeID), key))
}

func (tx *Tx) GetOutgoingEdges(nodeID NodeID) ([]Edge, error) {
	if err := tx.ensureActive(); err != nil {
		return nil, err
	}
	records, err := tx.raw.GetOutgoingEdges(uint64(nodeID))
	if err != nil {
		return nil, wrapError(err)
	}
	return edgeRecordsToEdges(records), nil
}

func (tx *Tx) GetIncomingEdges(nodeID NodeID) ([]Edge, error) {
	if err := tx.ensureActive(); err != nil {
		return nil, err
	}
	records, err := tx.raw.GetIncomingEdges(uint64(nodeID))
	if err != nil {
		return nil, wrapError(err)
	}
	return edgeRecordsToEdges(records), nil
}

// BatchInsertVectors inserts multiple vector-bearing nodes in a single call.
func (tx *Tx) BatchInsertVectors(label string, vectors [][]float32) ([]NodeID, error) {
	if err := tx.ensureWritable(); err != nil {
		return nil, err
	}
	nodeIDs, err := tx.raw.BatchInsert(label, vectors)
	if err != nil {
		return nil, wrapError(err)
	}
	out := make([]NodeID, len(nodeIDs))
	for i, nodeID := range nodeIDs {
		out[i] = NodeID(nodeID)
	}
	return out, nil
}

// Deprecated: use BatchInsertVectors.
func (tx *Tx) BatchInsert(label string, vectors [][]float32) ([]NodeID, error) {
	return tx.BatchInsertVectors(label, vectors)
}

func (tx *Tx) FTSIndex(nodeID NodeID, text string) error {
	if err := tx.ensureWritable(); err != nil {
		return err
	}
	return wrapError(tx.raw.FTSIndex(uint64(nodeID), text))
}

func (tx *Tx) Query(cypher string, params map[string]Value) (QueryResult, error) {
	if err := tx.ensureActive(); err != nil {
		return QueryResult{}, err
	}
	result, err := tx.raw.Query(cypher, params)
	if err != nil {
		return QueryResult{}, wrapError(err)
	}
	return QueryResult{
		Columns: result.Columns,
		Rows:    result.Rows,
	}, nil
}

func (tx *Tx) ensureActive() error {
	if tx == nil || !tx.active || tx.raw == nil {
		return ErrInactiveTx
	}
	return nil
}

func (tx *Tx) ensureWritable() error {
	if err := tx.ensureActive(); err != nil {
		return err
	}
	if tx.readOnly {
		return ErrReadOnlyTx
	}
	return nil
}

func edgeRecordsToEdges(records []cgobridge.EdgeRecord) []Edge {
	edges := make([]Edge, 0, len(records))
	for _, record := range records {
		edges = append(edges, Edge{
			ID:         EdgeID(record.ID),
			SourceID:   NodeID(record.SourceID),
			TargetID:   NodeID(record.TargetID),
			Type:       record.Type,
			Properties: map[string]Value{},
		})
	}
	return edges
}

func cloneValueMap(values map[string]Value) map[string]Value {
	if len(values) == 0 {
		return map[string]Value{}
	}
	cloned := make(map[string]Value, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}
