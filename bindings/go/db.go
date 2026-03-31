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
		Create:           opts.Create,
		ReadOnly:         opts.ReadOnly,
		CacheSizeMB:      opts.CacheSizeMB,
		PageSize:         opts.PageSize,
		EnableVector:     opts.vectorsEnabled(),
		VectorDimensions: opts.VectorDimensions,
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

func (db *DB) Close() error {
	if db == nil || db.raw == nil {
		return nil
	}
	err := wrapError(db.raw.Close())
	if err == nil {
		db.raw = nil
	}
	return err
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
	err := db.View(func(tx *Tx) error {
		queryResult, err := tx.Query(cypher, params)
		if err != nil {
			return err
		}
		result = queryResult
		return nil
	})
	return result, err
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

func (db *DB) FTSSearch(query string, opts FTSSearchOptions) ([]FTSSearchResult, error) {
	if db == nil || db.raw == nil {
		return nil, ErrDatabaseClosed
	}
	opts = opts.withDefaults()

	results, err := db.raw.FTSSearch(query, opts.Limit)
	if err != nil {
		return nil, wrapError(err)
	}
	return convertFTSResults(results), nil
}

func (db *DB) FTSSearchFuzzy(query string, opts FTSSearchOptions) ([]FTSSearchResult, error) {
	if db == nil || db.raw == nil {
		return nil, ErrDatabaseClosed
	}
	opts = opts.withDefaults()

	results, err := db.raw.FTSSearchFuzzy(query, opts.Limit, opts.MaxDistance, opts.MinTermLength)
	if err != nil {
		return nil, wrapError(err)
	}
	return convertFTSResults(results), nil
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
