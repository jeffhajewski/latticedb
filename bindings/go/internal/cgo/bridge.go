package cgobridge

/*
#cgo CFLAGS: -I${SRCDIR}/../../../../include
#cgo darwin LDFLAGS: -L${SRCDIR}/../../../../zig-out/lib -llattice -Wl,-rpath,${SRCDIR}/../../../../zig-out/lib
#cgo linux LDFLAGS: -L${SRCDIR}/../../../../zig-out/lib -llattice -Wl,-rpath,${SRCDIR}/../../../../zig-out/lib
#cgo windows LDFLAGS: -L${SRCDIR}/../../../../zig-out/lib -llattice
#include "helpers.h"
*/
import "C"

import (
	"strings"
	"unsafe"
)

type ErrorCode int

const (
	ErrorOK              ErrorCode = 0
	ErrorGeneric         ErrorCode = -1
	ErrorIO              ErrorCode = -2
	ErrorCorruption      ErrorCode = -3
	ErrorNotFound        ErrorCode = -4
	ErrorAlreadyExists   ErrorCode = -5
	ErrorInvalidArg      ErrorCode = -6
	ErrorTxnAborted      ErrorCode = -7
	ErrorLockTimeout     ErrorCode = -8
	ErrorReadOnly        ErrorCode = -9
	ErrorFull            ErrorCode = -10
	ErrorVersionMismatch ErrorCode = -11
	ErrorChecksum        ErrorCode = -12
	ErrorOutOfMemory     ErrorCode = -13
	ErrorUnsupported     ErrorCode = -14
)

type QueryErrorStage int

const (
	QueryErrorStageNone      QueryErrorStage = 0
	QueryErrorStageParse     QueryErrorStage = 1
	QueryErrorStageSemantic  QueryErrorStage = 2
	QueryErrorStagePlan      QueryErrorStage = 3
	QueryErrorStageExecution QueryErrorStage = 4
)

type Error struct {
	Code    ErrorCode
	Message string
}

func (e *Error) Error() string {
	return e.Message
}

type QueryExecutionError struct {
	Code           ErrorCode
	Stage          QueryErrorStage
	Message        string
	DiagnosticCode string
	HasLocation    bool
	Line           uint32
	Column         uint32
	Length         uint32
}

func (e *QueryExecutionError) Error() string {
	return e.Message
}

type OpenOptions struct {
	Create           bool
	ReadOnly         bool
	CacheSizeMB      uint32
	PageSize         uint32
	EnableVector     bool
	VectorDimensions uint16
}

type QueryResult struct {
	Columns []string
	Rows    []map[string]any
}

type EdgeRecord struct {
	ID       uint64
	SourceID uint64
	TargetID uint64
	Type     string
}

type DB struct {
	ptr *C.lattice_database
}

type Tx struct {
	ptr *C.lattice_txn
	db  *DB
}

type queryHandle struct {
	ptr *C.lattice_query
}

type resultHandle struct {
	ptr *C.lattice_result
}

func Version() string {
	return C.GoString(C.lattice_version())
}

func Open(path string, opts OpenOptions) (*DB, error) {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	cOpts := C.lattice_open_options{
		create:            C.bool(opts.Create),
		read_only:         C.bool(opts.ReadOnly),
		cache_size_mb:     C.uint32_t(opts.CacheSizeMB),
		page_size:         C.uint32_t(opts.PageSize),
		enable_vector:     C.bool(opts.EnableVector),
		vector_dimensions: C.uint16_t(opts.VectorDimensions),
	}

	var db *C.lattice_database
	if err := errorFromCode(ErrorCode(C.lattice_open(cPath, &cOpts, &db))); err != nil {
		return nil, err
	}

	return &DB{ptr: db}, nil
}

func (db *DB) Close() error {
	if db == nil || db.ptr == nil {
		return nil
	}
	err := errorFromCode(ErrorCode(C.lattice_close(db.ptr)))
	if err == nil {
		db.ptr = nil
	}
	return err
}

func (db *DB) Begin(readOnly bool) (*Tx, error) {
	if db == nil || db.ptr == nil {
		return nil, &Error{Code: ErrorInvalidArg, Message: "database is not open"}
	}

	var mode C.lattice_txn_mode = C.LATTICE_TXN_READ_WRITE
	if readOnly {
		mode = C.LATTICE_TXN_READ_ONLY
	}

	var txn *C.lattice_txn
	if err := errorFromCode(ErrorCode(C.lattice_begin(db.ptr, mode, &txn))); err != nil {
		return nil, err
	}

	return &Tx{ptr: txn, db: db}, nil
}

func (tx *Tx) Commit() error {
	if tx == nil || tx.ptr == nil {
		return nil
	}
	err := errorFromCode(ErrorCode(C.lattice_commit(tx.ptr)))
	if err == nil {
		tx.ptr = nil
	}
	return err
}

func (tx *Tx) Rollback() error {
	if tx == nil || tx.ptr == nil {
		return nil
	}
	err := errorFromCode(ErrorCode(C.lattice_rollback(tx.ptr)))
	if err == nil {
		tx.ptr = nil
	}
	return err
}

func (tx *Tx) CreateNode(label string) (uint64, error) {
	cLabel := C.CString(label)
	defer C.free(unsafe.Pointer(cLabel))

	var nodeID C.lattice_node_id
	if err := errorFromCode(ErrorCode(C.lattice_node_create(tx.ptr, cLabel, &nodeID))); err != nil {
		return 0, err
	}
	return uint64(nodeID), nil
}

func (tx *Tx) AddNodeLabel(nodeID uint64, label string) error {
	cLabel := C.CString(label)
	defer C.free(unsafe.Pointer(cLabel))

	return errorFromCode(ErrorCode(C.lattice_node_add_label(tx.ptr, C.lattice_node_id(nodeID), cLabel)))
}

func (tx *Tx) DeleteNode(nodeID uint64) error {
	return errorFromCode(ErrorCode(C.lattice_node_delete(tx.ptr, C.lattice_node_id(nodeID))))
}

func (tx *Tx) NodeExists(nodeID uint64) (bool, error) {
	var exists C.bool
	if err := errorFromCode(ErrorCode(C.lattice_node_exists(tx.ptr, C.lattice_node_id(nodeID), &exists))); err != nil {
		return false, err
	}
	return bool(exists), nil
}

func (tx *Tx) GetNodeLabels(nodeID uint64) ([]string, error) {
	var labels *C.char
	if err := errorFromCode(ErrorCode(C.lattice_node_get_labels(tx.ptr, C.lattice_node_id(nodeID), &labels))); err != nil {
		return nil, err
	}
	if labels == nil {
		return []string{}, nil
	}
	defer C.lattice_free_string(labels)

	raw := C.GoString(labels)
	if raw == "" {
		return []string{}, nil
	}
	return strings.Split(raw, ","), nil
}

func (tx *Tx) SetNodeProperty(nodeID uint64, key string, value any) error {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	cValue, cleanup, err := encodeValue(value)
	if err != nil {
		return err
	}
	defer cleanup()

	return errorFromCode(ErrorCode(C.lattice_node_set_property(
		tx.ptr,
		C.lattice_node_id(nodeID),
		cKey,
		cValue,
	)))
}

func (tx *Tx) GetNodeProperty(nodeID uint64, key string) (any, bool, error) {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	var out C.lattice_value
	code := ErrorCode(C.lattice_node_get_property(tx.ptr, C.lattice_node_id(nodeID), cKey, &out))
	if code == ErrorNotFound {
		return nil, false, nil
	}
	if err := errorFromCode(code); err != nil {
		return nil, false, err
	}
	defer C.lattice_value_free(&out)

	value, err := decodeValue(&out)
	if err != nil {
		return nil, false, err
	}
	return value, true, nil
}

func (tx *Tx) CreateEdge(sourceID, targetID uint64, edgeType string) (uint64, error) {
	cType := C.CString(edgeType)
	defer C.free(unsafe.Pointer(cType))

	var edgeID C.lattice_edge_id
	if err := errorFromCode(ErrorCode(C.lattice_edge_create(
		tx.ptr,
		C.lattice_node_id(sourceID),
		C.lattice_node_id(targetID),
		cType,
		&edgeID,
	))); err != nil {
		return 0, err
	}

	return uint64(edgeID), nil
}

func (tx *Tx) DeleteEdge(sourceID, targetID uint64, edgeType string) error {
	cType := C.CString(edgeType)
	defer C.free(unsafe.Pointer(cType))

	return errorFromCode(ErrorCode(C.lattice_edge_delete(
		tx.ptr,
		C.lattice_node_id(sourceID),
		C.lattice_node_id(targetID),
		cType,
	)))
}

func (tx *Tx) SetEdgeProperty(edgeID uint64, key string, value any) error {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	cValue, cleanup, err := encodeValue(value)
	if err != nil {
		return err
	}
	defer cleanup()

	return errorFromCode(ErrorCode(C.lattice_edge_set_property(
		tx.ptr,
		C.lattice_edge_id(edgeID),
		cKey,
		cValue,
	)))
}

func (tx *Tx) GetEdgeProperty(edgeID uint64, key string) (any, bool, error) {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	var out C.lattice_value
	code := ErrorCode(C.lattice_edge_get_property(tx.ptr, C.lattice_edge_id(edgeID), cKey, &out))
	if code == ErrorNotFound {
		return nil, false, nil
	}
	if err := errorFromCode(code); err != nil {
		return nil, false, err
	}
	defer C.lattice_value_free(&out)

	value, err := decodeValue(&out)
	if err != nil {
		return nil, false, err
	}
	return value, true, nil
}

func (tx *Tx) RemoveEdgeProperty(edgeID uint64, key string) error {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	return errorFromCode(ErrorCode(C.lattice_edge_remove_property(
		tx.ptr,
		C.lattice_edge_id(edgeID),
		cKey,
	)))
}

func (tx *Tx) GetOutgoingEdges(nodeID uint64) ([]EdgeRecord, error) {
	return tx.readEdgeResults(func(resultOut **C.lattice_edge_result) C.lattice_error {
		return C.lattice_edge_get_outgoing(tx.ptr, C.lattice_node_id(nodeID), resultOut)
	})
}

func (tx *Tx) GetIncomingEdges(nodeID uint64) ([]EdgeRecord, error) {
	return tx.readEdgeResults(func(resultOut **C.lattice_edge_result) C.lattice_error {
		return C.lattice_edge_get_incoming(tx.ptr, C.lattice_node_id(nodeID), resultOut)
	})
}

func (tx *Tx) Query(cypher string, params map[string]any) (QueryResult, error) {
	query, err := tx.prepareQuery(cypher)
	if err != nil {
		return QueryResult{}, err
	}
	defer query.free()

	for name, value := range params {
		if err := query.bind(name, value); err != nil {
			return QueryResult{}, err
		}
	}

	result, err := query.execute(tx)
	if err != nil {
		return QueryResult{}, err
	}
	defer result.free()

	return result.materialize()
}

func (tx *Tx) readEdgeResults(fetch func(resultOut **C.lattice_edge_result) C.lattice_error) ([]EdgeRecord, error) {
	var result *C.lattice_edge_result
	if err := errorFromCode(ErrorCode(fetch(&result))); err != nil {
		return nil, err
	}
	defer C.lattice_edge_result_free(result)

	count := int(C.lattice_edge_result_count(result))
	edges := make([]EdgeRecord, 0, count)
	for i := 0; i < count; i++ {
		var edgeID C.lattice_edge_id
		if err := errorFromCode(ErrorCode(C.lattice_edge_result_get_id(result, C.uint32_t(i), &edgeID))); err != nil {
			return nil, err
		}

		var source C.lattice_node_id
		var target C.lattice_node_id
		var edgeType *C.char
		var edgeTypeLen C.uint32_t
		if err := errorFromCode(ErrorCode(C.lattice_edge_result_get(
			result,
			C.uint32_t(i),
			&source,
			&target,
			&edgeType,
			&edgeTypeLen,
		))); err != nil {
			return nil, err
		}

		edges = append(edges, EdgeRecord{
			ID:       uint64(edgeID),
			SourceID: uint64(source),
			TargetID: uint64(target),
			Type:     cStringN(edgeType, int(edgeTypeLen)),
		})
	}

	return edges, nil
}

func (tx *Tx) prepareQuery(cypher string) (*queryHandle, error) {
	cCypher := C.CString(cypher)
	defer C.free(unsafe.Pointer(cCypher))

	var query *C.lattice_query
	if err := errorFromCode(ErrorCode(C.lattice_query_prepare(tx.db.ptr, cCypher, &query))); err != nil {
		return nil, err
	}

	return &queryHandle{ptr: query}, nil
}

func (q *queryHandle) bind(name string, value any) error {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	cValue, cleanup, err := encodeValue(value)
	if err != nil {
		return err
	}
	defer cleanup()

	return errorFromCode(ErrorCode(C.lattice_query_bind(q.ptr, cName, cValue)))
}

func (q *queryHandle) execute(tx *Tx) (*resultHandle, error) {
	var result *C.lattice_result
	code := ErrorCode(C.lattice_query_execute(q.ptr, tx.ptr, &result))
	if code != ErrorOK {
		return nil, q.captureError(code)
	}
	return &resultHandle{ptr: result}, nil
}

func (q *queryHandle) captureError(code ErrorCode) error {
	messagePtr := C.lattice_query_last_error_message(q.ptr)
	message := ""
	if messagePtr != nil {
		message = C.GoString(messagePtr)
	}
	if message == "" {
		message = errorMessage(code)
	}

	diagnosticCode := ""
	if codePtr := C.lattice_query_last_error_code(q.ptr); codePtr != nil {
		diagnosticCode = C.GoString(codePtr)
	}

	hasLocation := bool(C.lattice_query_last_error_has_location(q.ptr))

	return &QueryExecutionError{
		Code:           code,
		Stage:          QueryErrorStage(C.lattice_query_last_error_stage(q.ptr)),
		Message:        message,
		DiagnosticCode: diagnosticCode,
		HasLocation:    hasLocation,
		Line:           uint32(C.lattice_query_last_error_line(q.ptr)),
		Column:         uint32(C.lattice_query_last_error_column(q.ptr)),
		Length:         uint32(C.lattice_query_last_error_length(q.ptr)),
	}
}

func (q *queryHandle) free() {
	if q == nil || q.ptr == nil {
		return
	}
	C.lattice_query_free(q.ptr)
	q.ptr = nil
}

func (r *resultHandle) free() {
	if r == nil || r.ptr == nil {
		return
	}
	C.lattice_result_free(r.ptr)
	r.ptr = nil
}

func (r *resultHandle) materialize() (QueryResult, error) {
	columnCount := int(C.lattice_result_column_count(r.ptr))
	columns := make([]string, 0, columnCount)
	for i := 0; i < columnCount; i++ {
		columns = append(columns, C.GoString(C.lattice_result_column_name(r.ptr, C.uint32_t(i))))
	}

	rows := make([]map[string]any, 0)
	for bool(C.lattice_result_next(r.ptr)) {
		row := make(map[string]any, len(columns))
		for i, name := range columns {
			var out C.lattice_value
			if err := errorFromCode(ErrorCode(C.lattice_result_get(r.ptr, C.uint32_t(i), &out))); err != nil {
				return QueryResult{}, err
			}
			value, err := decodeValue(&out)
			if err != nil {
				return QueryResult{}, err
			}
			row[name] = value
		}
		rows = append(rows, row)
	}

	return QueryResult{
		Columns: columns,
		Rows:    rows,
	}, nil
}

func errorFromCode(code ErrorCode) error {
	if code == ErrorOK {
		return nil
	}
	return &Error{
		Code:    code,
		Message: errorMessage(code),
	}
}

func errorMessage(code ErrorCode) string {
	return C.GoString(C.lattice_error_message(C.lattice_error(code)))
}

func cStringN(ptr *C.char, length int) string {
	if ptr == nil || length == 0 {
		return ""
	}
	return C.GoStringN(ptr, C.int(length))
}
