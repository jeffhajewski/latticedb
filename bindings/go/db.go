package latticedb

import (
	"errors"

	cgobridge "github.com/jeffhajewski/latticedb/bindings/go/internal/cgo"
)

type DB struct {
	path    string
	options OpenOptions
	raw     *cgobridge.DB
}

func Version() string {
	return cgobridge.Version()
}

func Open(path string, opts OpenOptions) (*DB, error) {
	opts = opts.withDefaults()

	raw, err := cgobridge.Open(path, cgobridge.OpenOptions{
		Create:               opts.Create,
		ReadOnly:             opts.ReadOnly,
		CacheSizeMB:          opts.CacheSizeMB,
		PageSize:             opts.PageSize,
		EnableWAL:            opts.EnableWAL,
		EnableAdjacencyCache: opts.EnableAdjacencyCache,
		EnableVector:         opts.vectorsEnabled(),
		VectorDimensions:     opts.VectorDimensions,
		Lock:                 !opts.DisableLock,
	})
	if err != nil {
		return nil, wrapError(err)
	}

	return &DB{
		path:    path,
		options: opts,
		raw:     raw,
	}, nil
}

// Serialize returns the whole database as bytes.
//
// The result is a database file. Write it anywhere and it opens, or hand it to
// Deserialize. This is what makes it practical to keep many small databases in
// object storage: upload the bytes with whatever client you already use.
//
// Pending writes are folded in first, so the bytes need no write-ahead log
// beside them. Returns an error if a transaction is open, because bytes captured
// while writes land underneath them are torn.
func (db *DB) Serialize() ([]byte, error) {
	if db == nil || db.raw == nil {
		return nil, errors.New("latticedb: database is closed")
	}
	out, err := db.raw.Serialize()
	if err != nil {
		return nil, wrapError(err)
	}
	return out, nil
}

// Deserialize opens a database from bytes produced by Serialize.
//
// Pair this with your own object storage client to keep many small databases in
// a bucket:
//
//	obj, _ := s3c.GetObject(ctx, &s3.GetObjectInput{Bucket: b, Key: k})
//	blob, _ := io.ReadAll(obj.Body)
//	db, _ := latticedb.Deserialize(blob, latticedb.OpenOptions{})
//	defer db.Close()
//	// ... change it ...
//	next, _ := db.Serialize()
//	s3c.PutObject(ctx, &s3.PutObjectInput{Bucket: b, Key: k, Body: bytes.NewReader(next), IfMatch: obj.ETag})
//
// The bytes are copied, so the caller may reuse the slice as soon as this
// returns. Changes made afterwards do not travel back to it.
//
// Passing IfMatch above is worth the trouble. Two workers that read the same
// object, change it, and write it back will otherwise silently overwrite each
// other, and nothing reports an error when they do.
func Deserialize(data []byte, opts OpenOptions) (*DB, error) {
	opts = opts.withDefaults()

	raw, err := cgobridge.Deserialize(data, cgobridge.OpenOptions{
		CacheSizeMB:          opts.CacheSizeMB,
		PageSize:             opts.PageSize,
		EnableWAL:            opts.EnableWAL,
		EnableAdjacencyCache: opts.EnableAdjacencyCache,
		EnableVector:         opts.vectorsEnabled(),
		VectorDimensions:     opts.VectorDimensions,
		Lock:                 !opts.DisableLock,
	})
	if err != nil {
		return nil, wrapError(err)
	}

	return &DB{
		path:    "<deserialized>",
		options: opts,
		raw:     raw,
	}, nil
}

func (db *DB) Close() error {
	if db == nil || db.raw == nil {
		return nil
	}
	consumed, closeErr := db.raw.Close()
	if consumed {
		db.raw = nil
	}
	return wrapError(closeErr)
}

func (db *DB) IsOpen() bool {
	return db != nil && db.raw != nil
}

func (db *DB) Path() string {
	if db == nil {
		return ""
	}
	return db.path
}

func (db *DB) begin(readOnly bool) (*Tx, error) {
	if db == nil || db.raw == nil {
		return nil, ErrDatabaseClosed
	}
	if !readOnly && db.options.ReadOnly {
		return nil, ErrReadOnlyDatabase
	}

	raw, err := db.raw.Begin(readOnly)
	if err != nil {
		return nil, wrapError(err)
	}

	return &Tx{
		db:       db,
		raw:      raw,
		readOnly: readOnly,
		active:   true,
	}, nil
}

func (db *DB) BeginRead() (*Tx, error) {
	return db.begin(true)
}

func (db *DB) BeginWrite() (*Tx, error) {
	return db.begin(false)
}

// Deprecated: use BeginRead or BeginWrite. Earliest removal is v0.6.0.
func (db *DB) Begin(readOnly bool) (*Tx, error) {
	if readOnly {
		return db.BeginRead()
	}
	return db.BeginWrite()
}

func (db *DB) View(fn func(*Tx) error) error {
	tx, err := db.BeginRead()
	if err != nil {
		return err
	}

	runErr := fn(tx)
	rollbackErr := tx.Rollback()
	if runErr != nil {
		if rollbackErr != nil {
			return errors.Join(runErr, rollbackErr)
		}
		return runErr
	}
	return rollbackErr
}

func (db *DB) Update(fn func(*Tx) error) error {
	tx, err := db.BeginWrite()
	if err != nil {
		return err
	}

	runErr := fn(tx)
	if runErr != nil {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			return errors.Join(runErr, rollbackErr)
		}
		return runErr
	}

	return tx.Commit()
}

func (db *DB) Query(cypher string, params map[string]Value) (QueryResult, error) {
	var result QueryResult
	run := func(tx *Tx) error {
		queryResult, err := tx.Query(cypher, params)
		if err != nil {
			return err
		}
		result = queryResult
		return nil
	}

	// Only one write transaction may be open at a time, so running every query
	// through Update would serialise reads that could have run together. Ask
	// the query which mode it actually needs.
	if db == nil || db.raw == nil {
		return result, ErrDatabaseClosed
	}
	if db.options.ReadOnly || !db.raw.QueryWrites(cypher) {
		return result, db.View(run)
	}
	return result, db.Update(run)
}

func (db *DB) CacheClear() error {
	if db == nil || db.raw == nil {
		return ErrDatabaseClosed
	}
	return wrapError(db.raw.CacheClear())
}

func (db *DB) CacheStats() (QueryCacheStats, error) {
	if db == nil || db.raw == nil {
		return QueryCacheStats{}, ErrDatabaseClosed
	}
	stats, err := db.raw.CacheStats()
	if err != nil {
		return QueryCacheStats{}, wrapError(err)
	}
	return QueryCacheStats{
		Entries: stats.Entries,
		Hits:    stats.Hits,
		Misses:  stats.Misses,
	}, nil
}

// CreateNodePropertyIndex creates an explicit equality index for a node
// label/property pair and indexes existing matching nodes.
func (db *DB) CreateNodePropertyIndex(label, property string) error {
	if db == nil || db.raw == nil {
		return ErrDatabaseClosed
	}
	return wrapError(db.raw.CreateNodePropertyIndex(label, property))
}

// DropNodePropertyIndex removes an explicit node property index.
func (db *DB) DropNodePropertyIndex(label, property string) error {
	if db == nil || db.raw == nil {
		return ErrDatabaseClosed
	}
	return wrapError(db.raw.DropNodePropertyIndex(label, property))
}

// CreateNodeFTSIndex declares a full-text index over one node label/property
// pair and indexes the text already stored in that property.
//
// The property holds the text, and only string properties are indexed. Writes
// maintain the index from then on.
//
// A Cypher `d.property @@ "query"` searches the index declared for that label
// and property, and fails when none is declared rather than returning no rows.
func (db *DB) CreateNodeFTSIndex(label, property string) error {
	if db == nil || db.raw == nil {
		return ErrDatabaseClosed
	}
	return wrapError(db.raw.CreateNodeFTSIndex(label, property))
}

// DropNodeFTSIndex removes a declared full-text index and everything it stored.
func (db *DB) DropNodeFTSIndex(label, property string) error {
	if db == nil || db.raw == nil {
		return ErrDatabaseClosed
	}
	return wrapError(db.raw.DropNodeFTSIndex(label, property))
}

// HasNodeFTSIndex reports whether a full-text index is declared for a label and
// property.
func (db *DB) HasNodeFTSIndex(label, property string) (bool, error) {
	if db == nil || db.raw == nil {
		return false, ErrDatabaseClosed
	}
	ok, err := db.raw.HasNodeFTSIndex(label, property)
	return ok, wrapError(err)
}

// CreateEdgePropertyIndex creates an explicit equality index for an edge
// type/property pair and indexes existing matching edges.
func (db *DB) CreateEdgePropertyIndex(edgeType, property string) error {
	if db == nil || db.raw == nil {
		return ErrDatabaseClosed
	}
	return wrapError(db.raw.CreateEdgePropertyIndex(edgeType, property))
}

// DropEdgePropertyIndex removes an explicit edge property index.
func (db *DB) DropEdgePropertyIndex(edgeType, property string) error {
	if db == nil || db.raw == nil {
		return ErrDatabaseClosed
	}
	return wrapError(db.raw.DropEdgePropertyIndex(edgeType, property))
}

// GetNodesByLabel returns every node id that currently carries label.
// An unknown label is not an error and yields an empty slice.
func (db *DB) GetNodesByLabel(label string) ([]NodeID, error) {
	if db == nil || db.raw == nil {
		return nil, ErrDatabaseClosed
	}

	ids, err := db.raw.GetNodesByLabel(label)
	if err != nil {
		return nil, wrapError(err)
	}

	out := make([]NodeID, len(ids))
	for i, id := range ids {
		out[i] = NodeID(id)
	}
	return out, nil
}

func (db *DB) VectorSearch(vector []float32, opts VectorSearchOptions) ([]VectorSearchResult, error) {
	if db == nil || db.raw == nil {
		return nil, ErrDatabaseClosed
	}
	opts = opts.withDefaults()

	results, err := db.raw.VectorSearch(vector, opts.K, opts.EfSearch)
	if err != nil {
		return nil, wrapError(err)
	}

	out := make([]VectorSearchResult, 0, len(results))
	for _, result := range results {
		out = append(out, VectorSearchResult{
			NodeID:   NodeID(result.NodeID),
			Distance: result.Distance,
		})
	}
	return out, nil
}

// FTSSearch searches one declared full-text index.
//
// It returns an error rather than an empty result when no index is declared for
// this label and property, because a mistyped property name and a query that
// found nothing are different situations.
func (db *DB) FTSSearch(label, property, query string, opts FTSSearchOptions) ([]FTSSearchResult, error) {
	if db == nil || db.raw == nil {
		return nil, ErrDatabaseClosed
	}
	opts = opts.withDefaults()

	results, err := db.raw.FTSSearch(label, property, query, opts.Limit)
	if err != nil {
		return nil, wrapError(err)
	}
	return convertFTSResults(results), nil
}

// FTSSearchFuzzy searches one declared index, tolerating typos in the query.
func (db *DB) FTSSearchFuzzy(label, property, query string, opts FTSSearchOptions) ([]FTSSearchResult, error) {
	if db == nil || db.raw == nil {
		return nil, ErrDatabaseClosed
	}
	opts = opts.withDefaults()

	results, err := db.raw.FTSSearchFuzzy(label, property, query, opts.Limit, opts.MaxDistance, opts.MinTermLength)
	if err != nil {
		return nil, wrapError(err)
	}
	return convertFTSResults(results), nil
}

func (db *DB) ReadStream(stream string, afterSequence uint64, limit uint, timeoutMs uint32) ([]StreamRecord, error) {
	if db == nil || db.raw == nil {
		return nil, ErrDatabaseClosed
	}

	records, err := db.raw.ReadStream(stream, afterSequence, limit, timeoutMs)
	if err != nil {
		return nil, wrapError(err)
	}
	return convertStreamRecords(records), nil
}

func (db *DB) GetStreamOffset(stream, consumer string) (uint64, bool, error) {
	if db == nil || db.raw == nil {
		return 0, false, ErrDatabaseClosed
	}

	offset, ok, err := db.raw.GetStreamOffset(stream, consumer)
	if err != nil {
		return 0, false, wrapError(err)
	}
	return offset, ok, nil
}

func (db *DB) Changes(afterSequence uint64, limit uint, timeoutMs uint32) ([]StreamRecord, error) {
	return db.ReadStream("__lattice_changes", afterSequence, limit, timeoutMs)
}

func convertFTSResults(results []cgobridge.FTSSearchResult) []FTSSearchResult {
	out := make([]FTSSearchResult, 0, len(results))
	for _, result := range results {
		out = append(out, FTSSearchResult{
			NodeID: NodeID(result.NodeID),
			Score:  result.Score,
		})
	}
	return out
}

func convertStreamRecords(records []cgobridge.StreamRecord) []StreamRecord {
	out := make([]StreamRecord, 0, len(records))
	for _, record := range records {
		out = append(out, StreamRecord{
			Sequence: record.Sequence,
			Kind:     record.Kind,
			Payload:  record.Payload,
		})
	}
	return out
}
