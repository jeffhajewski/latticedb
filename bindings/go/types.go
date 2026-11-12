package latticedb

import cgobridge "github.com/jeffhajewski/latticedb/bindings/go/internal/cgo"

type Value = any

type NodeID uint64
type EdgeID uint64

type OpenOptions struct {
	Create   bool
	ReadOnly bool
	// CacheSizeMB is the page cache size. Zero leaves it to the
	// LATTICE_BUFFER_POOL_MB environment variable if it is set, else 100; a
	// size set here wins over the variable.
	CacheSizeMB uint32
	PageSize    uint32
	// EnableWAL logs every commit so it survives a crash. The normalized default
	// is true. Transactions work either way.
	EnableWAL bool
	// DisableWAL explicitly opens without the log. It exists because Go bool
	// fields cannot distinguish an omitted EnableWAL from EnableWAL=false.
	DisableWAL bool
	// EnableAdjacencyCache is accepted for compatibility. Queries and the other
	// methods here read edges from storage either way, so it does not change
	// their speed or results.
	EnableAdjacencyCache bool
	// EnableVectors creates a vector index if the database has none. A database
	// that already has one opens with it whatever this says.
	EnableVectors bool
	// Deprecated: use EnableVectors.
	EnableVector bool
	// VectorDimensions applies when EnableVectors is set (0 means 128). It must
	// match the index the file records, if it has one, or Open returns
	// ErrorConfigMismatch.
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
	// SyncMode sets when a commit waits for the disk. The zero value uses the
	// LATTICE_SYNC_MODE environment variable, else SyncModeFull.
	SyncMode SyncMode
	// FullFsync sets, on macOS, whether syncs ask the drive to flush its cache
	// (F_FULLFSYNC). No effect elsewhere. The zero value uses the
	// LATTICE_FULL_FSYNC environment variable, else off.
	FullFsync FullFsync
	// VectorMetric is how a new vector index measures distance. The zero value
	// means the file's metric, or cosine for a new index.
	VectorMetric VectorMetric
	// VectorM is HNSW connections per node for a new index, 2..32. 0 means the
	// file's, or 16.
	VectorM uint16
	// VectorEfConstruction is the HNSW candidate list size while building a new
	// index, 1..4096. 0 means the file's, or 200.
	VectorEfConstruction uint16
	// ChangefeedRetention is how much of the changefeed (see DB.Changes) to
	// keep: "all", "off", "last:<events>" or "age:<duration>" with a duration
	// such as "500ms", "90s", "30m", "12h" or "7d". The file records its
	// policy. The empty string keeps the file's, which is "all" unless
	// something set another (for a new database, the
	// LATTICE_CHANGEFEED_RETENTION environment variable). A value set here
	// replaces the file's policy when the database is opened for writing; a
	// read-only open never changes it. Retention drops the oldest events at
	// checkpoints; DB.TrimChangefeed applies it at once.
	ChangefeedRetention string
}

// SyncMode sets when a commit waits for the disk.
type SyncMode uint8

const (
	// SyncModeDefault uses LATTICE_SYNC_MODE from the environment, else full.
	SyncModeDefault SyncMode = 0
	// SyncModeFull syncs the log on every commit: a commit that has returned
	// survives a power cut.
	SyncModeFull SyncMode = 1
	// SyncModeNormal hands the commit to the operating system and returns. It
	// survives the process being killed; a power cut or OS crash can lose the
	// most recent commits, and the database stays consistent.
	SyncModeNormal SyncMode = 2
)

// FullFsync sets whether syncs use F_FULLFSYNC on macOS.
type FullFsync uint8

const (
	// FullFsyncDefault uses LATTICE_FULL_FSYNC from the environment, else off.
	FullFsyncDefault FullFsync = 0
	FullFsyncOff     FullFsync = 1
	FullFsyncOn      FullFsync = 2
)

// VectorMetric is how a vector index measures distance. Search results are
// ordered by it, smallest first. The database file records it; once an index
// exists, a different metric is ErrorConfigMismatch.
type VectorMetric uint8

const (
	// VectorMetricDefault means the file's metric, or cosine for a new index.
	VectorMetricDefault VectorMetric = 0
	// VectorMetricCosine is 1 - cos(a, b). Vectors are stored scaled to unit length.
	VectorMetricCosine VectorMetric = 1
	// VectorMetricEuclidean is the L2 distance. Vectors are stored as given.
	VectorMetricEuclidean VectorMetric = 2
	// VectorMetricDotProduct is the negative dot product. Vectors are stored as given.
	VectorMetricDotProduct VectorMetric = 3
)

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
