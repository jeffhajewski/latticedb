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
	// EnableWAL controls WAL-backed transactions. The normalized default is true.
	EnableWAL bool
	// DisableWAL explicitly opens without WAL-backed transactions. It exists because
	// Go bool fields cannot distinguish an omitted EnableWAL from EnableWAL=false.
	DisableWAL bool
	// EnableAdjacencyCache enables the in-memory graph adjacency cache.
	EnableAdjacencyCache bool
	// Preferred public option name.
	EnableVectors bool
	// Deprecated: use EnableVectors.
	EnableVector     bool
	VectorDimensions uint16
	// DisableLock opens without taking a lock on the file. A locked database
	// normally refuses a second process, which is what stops two of them
	// corrupting it; this exists for filesystems where locking does not work, and
	// it does not make concurrent access safe.
	//
	// It is phrased as a negative for the same reason as DisableWAL: a Go bool
	// cannot tell an omitted field from a deliberate false, and locking has to
	// stay on when the caller says nothing.
	DisableLock bool
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

type StreamRecord struct {
	Sequence uint64
	Kind     string
	Payload  Value
}

// Deprecated: use package github.com/jeffhajewski/latticedb/bindings/go/embedding.
type EmbeddingAPIFormat int

const (
	EmbeddingAPIFormatOllama EmbeddingAPIFormat = EmbeddingAPIFormat(cgobridge.EmbeddingAPIFormatOllama)
	EmbeddingAPIFormatOpenAI EmbeddingAPIFormat = EmbeddingAPIFormat(cgobridge.EmbeddingAPIFormatOpenAI)
)

// Deprecated: use package github.com/jeffhajewski/latticedb/bindings/go/embedding.
type EmbeddingConfig struct {
	// Endpoint is the full URL each request is POSTed to; nothing is
	// appended to it. For Ollama, "http://localhost:11434/api/embeddings";
	// for OpenAI, "https://api.openai.com/v1/embeddings".
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
	if o.DisableWAL {
		o.EnableWAL = false
	} else {
		o.EnableWAL = true
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
