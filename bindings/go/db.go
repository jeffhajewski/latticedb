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
		EnableVector:     opts.EnableVector,
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

func (db *DB) Begin(readOnly bool) (*Tx, error) {
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

func (db *DB) View(fn func(*Tx) error) error {
	tx, err := db.Begin(true)
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
	tx, err := db.Begin(false)
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
