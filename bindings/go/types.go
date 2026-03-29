package latticedb

import cgobridge "github.com/jeffhajewski/latticedb/bindings/go/internal/cgo"

type Value = any

type NodeID uint64
type EdgeID uint64

type OpenOptions struct {
	Create      bool
	ReadOnly    bool
	CacheSizeMB uint32
	PageSize    uint32
	// Preferred public option name.
	EnableVectors bool
	// Compatibility alias for EnableVectors.
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

type QueryCacheStats struct {
	Entries uint32
	Hits    uint64
	Misses  uint64
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

type VectorSearchResult struct {
	NodeID   NodeID
	Distance float32
}

type FTSSearchResult struct {
	NodeID NodeID
	Score  float32
}

type EmbeddingAPIFormat int

const (
	EmbeddingAPIFormatOllama EmbeddingAPIFormat = EmbeddingAPIFormat(cgobridge.EmbeddingAPIFormatOllama)
	EmbeddingAPIFormatOpenAI EmbeddingAPIFormat = EmbeddingAPIFormat(cgobridge.EmbeddingAPIFormatOpenAI)
)

type EmbeddingConfig struct {
	Endpoint  string
	Model     string
	APIFormat EmbeddingAPIFormat
	APIKey    string
	TimeoutMS uint32
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

func (o OpenOptions) vectorsEnabled() bool {
	return o.EnableVectors || o.EnableVector
}

func (o VectorSearchOptions) withDefaults() VectorSearchOptions {
	if o.K == 0 {
		o.K = 10
	}
	return o
}

func (o FTSSearchOptions) withDefaults() FTSSearchOptions {
	if o.Limit == 0 {
		o.Limit = 10
	}
	return o
}
