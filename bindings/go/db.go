package latticedb

import (
	"errors"
	"fmt"
	"math"
	"time"

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
	retention, retentionValue, err := changefeedRetentionCode(opts.ChangefeedRetention)
	if err != nil {
		return nil, err
	}

	raw, err := cgobridge.Open(path, cgobridge.OpenOptions{
		Create:                   opts.Create,
		ReadOnly:                 opts.ReadOnly,
		CacheSizeMB:              opts.CacheSizeMB,
		PageSize:                 opts.PageSize,
		EnableWAL:                opts.EnableWAL,
		EnableAdjacencyCache:     opts.EnableAdjacencyCache,
		EnableVector:             opts.vectorsEnabled(),
		VectorDimensions:         opts.VectorDimensions,
		Lock:                     !opts.DisableLock,
		SyncMode:                 uint8(opts.SyncMode),
		FullFsync:                uint8(opts.FullFsync),
		VectorMetric:             uint8(opts.VectorMetric),
		VectorM:                  opts.VectorM,
		VectorEfConstruction:     opts.VectorEfConstruction,
		ChangefeedRetention:      retention,
		ChangefeedRetentionValue: retentionValue,
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
// The options mean what they mean to Open, apart from Create and ReadOnly,
// which are ignored. Bytes that hold a vector index open with the configuration
// they record, and any vector option that is set must match it, or Deserialize
// returns ErrorConfigMismatch; EnableVectors creates an index, configured by the
// vector options, in bytes that have none.
//
// The bytes are copied, so the caller may reuse the slice as soon as this
// returns. Changes made afterwards do not travel back to it.
//
// Passing IfMatch above is worth the trouble. Two workers that read the same
// object, change it, and write it back will otherwise silently overwrite each
// other, and nothing reports an error when they do.
func Deserialize(data []byte, opts OpenOptions) (*DB, error) {
	opts = opts.withDefaults()
	retention, retentionValue, err := changefeedRetentionCode(opts.ChangefeedRetention)
	if err != nil {
		return nil, err
	}

	raw, err := cgobridge.Deserialize(data, cgobridge.OpenOptions{
		CacheSizeMB:              opts.CacheSizeMB,
		PageSize:                 opts.PageSize,
		EnableWAL:                opts.EnableWAL,
		EnableAdjacencyCache:     opts.EnableAdjacencyCache,
		EnableVector:             opts.vectorsEnabled(),
		VectorDimensions:         opts.VectorDimensions,
		Lock:                     !opts.DisableLock,
		SyncMode:                 uint8(opts.SyncMode),
		FullFsync:                uint8(opts.FullFsync),
		VectorMetric:             uint8(opts.VectorMetric),
		VectorM:                  opts.VectorM,
		VectorEfConstruction:     opts.VectorEfConstruction,
		ChangefeedRetention:      retention,
		ChangefeedRetentionValue: retentionValue,
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

// Deprecated: use BeginRead or BeginWrite.
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

// commitTx commits the transaction Update opened. It is a variable so tests
// can make a commit fail.
var commitTx = (*Tx).Commit

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

	commitErr := commitTx(tx)
	if commitErr != nil && tx.IsActive() {
		// A failed commit leaves the transaction open, and an open writer
		// blocks every later write. Nobody else holds tx, so end it here.
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return errors.Join(commitErr, rollbackErr)
		}
	}
	return commitErr
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

// Checkpoint folds the log (the -wal file) into the database file now.
//
// A commit is durable once it is in the log; a checkpoint writes the pages the
// log's commits changed into the database file and, with CheckpointTruncate,
// empties the log. See CheckpointMode for what each mode does.
//
// It returns ErrorInvalidArg for a mode other than the three constants, without
// calling the library; ErrorReadOnly on a read-only database; ErrorLockTimeout
// if a transaction already has records in the log, because a checkpoint would
// cut its commit off from them; and ErrorIO if writing failed, in which case the
// log still holds every commit.
func (db *DB) Checkpoint(mode CheckpointMode) (CheckpointStats, error) {
	switch mode {
	case CheckpointPassive, CheckpointFull, CheckpointTruncate:
	default:
		return CheckpointStats{}, &Error{
			Code:    ErrorInvalidArg,
			Message: fmt.Sprintf("latticedb: unknown checkpoint mode %d", int(mode)),
		}
	}
	if db == nil || db.raw == nil {
		return CheckpointStats{}, ErrDatabaseClosed
	}

	stats, err := db.raw.Checkpoint(int(mode))
	if err != nil {
		return CheckpointStats{}, wrapError(err)
	}
	duration := time.Duration(math.MaxInt64)
	if stats.DurationNS <= math.MaxInt64 {
		duration = time.Duration(stats.DurationNS)
	}
	return CheckpointStats{
		PagesFlushed:  stats.PagesFlushed,
		Duration:      duration,
		CheckpointLSN: stats.CheckpointLSN,
		WALTruncated:  stats.WALTruncated,
	}, nil
}

// SetAutoCheckpoint sets when commits checkpoint from now on.
//
// A commit runs a CheckpointTruncate checkpoint once the log holds
// maxWALFrames frames (one frame is one page, usually 4096 bytes), and no
// sooner than minInterval after the last automatic one. minInterval is passed
// in whole milliseconds, rounded up so it is never shorter than asked (a
// nonzero interval under a millisecond becomes 1ms); 0 means no minimum. maxWALFrames 0 switches automatic checkpoints off: the log
// grows until Checkpoint runs, though Close still folds it into the file.
//
// Without this call the LATTICE_AUTO_CHECKPOINT_FRAMES environment variable
// sets the frame count (0 for none), and without that the log may grow to an
// eighth of the page cache, from 1000 frames up to 64 MB. This call wins over
// the variable, and its count is used as given, not scaled to the cache. It has no effect on a read-only database.
//
// A larger interval means fewer checkpoints, each a longer pause for the
// commit that runs it, a longer log on disk and more to replay after a crash. A
// smaller one means shorter pauses, more often.
//
// A negative minInterval returns ErrorInvalidArg without calling the library.
func (db *DB) SetAutoCheckpoint(maxWALFrames uint64, minInterval time.Duration) error {
	if minInterval < 0 {
		return &Error{
			Code:    ErrorInvalidArg,
			Message: fmt.Sprintf("latticedb: negative checkpoint interval %v", minInterval),
		}
	}
	if db == nil || db.raw == nil {
		return ErrDatabaseClosed
	}

	return wrapError(db.raw.SetAutoCheckpoint(maxWALFrames, checkpointIntervalMS(minInterval)))
}

// checkpointIntervalMS converts a non-negative interval to whole milliseconds,
// rounding up so the interval is never shorter than asked.
func checkpointIntervalMS(d time.Duration) uint64 {
	ms := uint64(d / time.Millisecond)
	if d%time.Millisecond != 0 {
		ms++
	}
	return ms
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

// CreateEdgeFTSIndex declares a full-text index over one relationship
// type/property pair.
//
// A Cypher `-[x:TYPE]->` pattern with `x.property @@ "query"` searches it.
func (db *DB) CreateEdgeFTSIndex(edgeType, property string) error {
	if db == nil || db.raw == nil {
		return ErrDatabaseClosed
	}
	return wrapError(db.raw.CreateEdgeFTSIndex(edgeType, property))
}

// DropEdgeFTSIndex removes a declared relationship full-text index.
func (db *DB) DropEdgeFTSIndex(edgeType, property string) error {
	if db == nil || db.raw == nil {
		return ErrDatabaseClosed
	}
	return wrapError(db.raw.DropEdgeFTSIndex(edgeType, property))
}

// HasEdgeFTSIndex reports whether an index is declared for this relationship
// type and property.
func (db *DB) HasEdgeFTSIndex(edgeType, property string) (bool, error) {
	if db == nil || db.raw == nil {
		return false, ErrDatabaseClosed
	}
	ok, err := db.raw.HasEdgeFTSIndex(edgeType, property)
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

// VectorSearch returns the nearest nodes to vector. It returns
// ErrorUnsupported when the database was created without a vector index.
func (db *DB) VectorSearch(vector []float32, opts VectorSearchOptions) ([]VectorSearchResult, error) {
	if db == nil || db.raw == nil {
		return nil, ErrDatabaseClosed
	}
	opts = opts.withDefaults()

	results, err := db.raw.VectorSearch(vector, opts.K, opts.EfSearch)
	if err != nil {
		return nil, wrapVectorError(err, "search vectors")
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

// Changes reads graph changefeed records from __lattice_changes: up to limit
// records with a sequence greater than afterSequence, oldest first.
// afterSequence 0 reads from the oldest event held.
//
// When retention or a trim has dropped the events after afterSequence, the
// reader has missed changes and the error matches ErrChangesTrimmed (code
// ErrorNotFound). When the policy is "off" the error has code
// ErrorUnsupported.
func (db *DB) Changes(afterSequence uint64, limit uint, timeoutMs uint32) ([]StreamRecord, error) {
	records, err := db.ReadStream("__lattice_changes", afterSequence, limit, timeoutMs)
	var latticeErr *Error
	if errors.As(err, &latticeErr) {
		switch latticeErr.Code {
		case ErrorNotFound:
			return nil, &Error{Code: ErrorNotFound, Message: fmt.Sprintf(
				"the changefeed no longer holds the events after sequence %d: "+
					"its retention policy or a trim dropped them", afterSequence)}
		case ErrorUnsupported:
			return nil, &Error{Code: ErrorUnsupported, Message: `the changefeed is off (ChangefeedRetention "off")`}
		}
	}
	return records, err
}

// ChangefeedStatus returns the changefeed's retention policy and the events
// it holds.
func (db *DB) ChangefeedStatus() (ChangefeedStatus, error) {
	if db == nil || db.raw == nil {
		return ChangefeedStatus{}, ErrDatabaseClosed
	}
	mode, value, oldest, last, err := db.raw.ChangefeedStatus()
	if err != nil {
		return ChangefeedStatus{}, wrapError(err)
	}
	return ChangefeedStatus{
		Retention:      changefeedRetentionText(mode, value),
		OldestSequence: oldest,
		LastSequence:   last,
	}, nil
}

// TrimChangefeed applies the changefeed's retention policy in full now
// (checkpoints apply it a bounded number of events at a time) and makes that
// durable. It returns the number of events dropped, and an error with code
// ErrorLockTimeout while a write transaction is open.
func (db *DB) TrimChangefeed() (uint64, error) {
	if db == nil || db.raw == nil {
		return 0, ErrDatabaseClosed
	}
	removed, err := db.raw.ChangefeedTrim()
	if err != nil {
		return 0, wrapError(err)
	}
	return removed, nil
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
