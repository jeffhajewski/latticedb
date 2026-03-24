package latticedb

type Value = any

type NodeID uint64
type EdgeID uint64

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
	ID         NodeID
	Labels     []string
	Properties map[string]Value
}

type Edge struct {
	ID         EdgeID
	SourceID   NodeID
	TargetID   NodeID
	Type       string
	Properties map[string]Value
}

type QueryResult struct {
	Columns []string
	Rows    []map[string]Value
}

func (o OpenOptions) withDefaults() OpenOptions {
	if o.CacheSizeMB == 0 {
		o.CacheSizeMB = 100
	}
	if o.PageSize == 0 {
		o.PageSize = 4096
	}
	if o.VectorDimensions == 0 {
		o.VectorDimensions = 128
	}
	return o
}
