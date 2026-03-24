package latticedb

import (
	"errors"
	"fmt"

	cgobridge "github.com/jeffhajewski/latticedb/bindings/go/internal/cgo"
)

type ErrorCode int

const (
	ErrorOK              ErrorCode = ErrorCode(cgobridge.ErrorOK)
	ErrorGeneric         ErrorCode = ErrorCode(cgobridge.ErrorGeneric)
	ErrorIO              ErrorCode = ErrorCode(cgobridge.ErrorIO)
	ErrorCorruption      ErrorCode = ErrorCode(cgobridge.ErrorCorruption)
	ErrorNotFound        ErrorCode = ErrorCode(cgobridge.ErrorNotFound)
	ErrorAlreadyExists   ErrorCode = ErrorCode(cgobridge.ErrorAlreadyExists)
	ErrorInvalidArg      ErrorCode = ErrorCode(cgobridge.ErrorInvalidArg)
	ErrorTxnAborted      ErrorCode = ErrorCode(cgobridge.ErrorTxnAborted)
	ErrorLockTimeout     ErrorCode = ErrorCode(cgobridge.ErrorLockTimeout)
	ErrorReadOnly        ErrorCode = ErrorCode(cgobridge.ErrorReadOnly)
	ErrorFull            ErrorCode = ErrorCode(cgobridge.ErrorFull)
	ErrorVersionMismatch ErrorCode = ErrorCode(cgobridge.ErrorVersionMismatch)
	ErrorChecksum        ErrorCode = ErrorCode(cgobridge.ErrorChecksum)
	ErrorOutOfMemory     ErrorCode = ErrorCode(cgobridge.ErrorOutOfMemory)
	ErrorUnsupported     ErrorCode = ErrorCode(cgobridge.ErrorUnsupported)
)

type QueryErrorStage int

const (
	QueryErrorStageNone      QueryErrorStage = QueryErrorStage(cgobridge.QueryErrorStageNone)
	QueryErrorStageParse     QueryErrorStage = QueryErrorStage(cgobridge.QueryErrorStageParse)
	QueryErrorStageSemantic  QueryErrorStage = QueryErrorStage(cgobridge.QueryErrorStageSemantic)
	QueryErrorStagePlan      QueryErrorStage = QueryErrorStage(cgobridge.QueryErrorStagePlan)
	QueryErrorStageExecution QueryErrorStage = QueryErrorStage(cgobridge.QueryErrorStageExecution)
)

var (
	ErrDatabaseClosed   = errors.New("database is not open")
	ErrReadOnlyDatabase = errors.New("cannot write to a read-only database")
	ErrReadOnlyTx       = errors.New("cannot write in a read-only transaction")
	ErrInactiveTx       = errors.New("transaction is not active")
	ErrEmbeddingClosed  = errors.New("embedding client is closed")
)

type Error struct {
	Code    ErrorCode
	Message string
}

func (e *Error) Error() string {
	return e.Message
}

func (e *Error) Is(target error) bool {
	other, ok := target.(*Error)
	return ok && e.Code == other.Code
}

type QueryErrorLocation struct {
	Line   uint32
	Column uint32
	Length uint32
}

type QueryError struct {
	Code           ErrorCode
	Stage          QueryErrorStage
	Message        string
	DiagnosticCode string
	Location       *QueryErrorLocation
}

func (e *QueryError) Error() string {
	if e.DiagnosticCode != "" {
		return fmt.Sprintf("%s (%s)", e.Message, e.DiagnosticCode)
	}
	return e.Message
}

func wrapError(err error) error {
	if err == nil {
		return nil
	}

	var queryErr *cgobridge.QueryExecutionError
	if errors.As(err, &queryErr) {
		var location *QueryErrorLocation
		if queryErr.HasLocation {
			location = &QueryErrorLocation{
				Line:   queryErr.Line,
				Column: queryErr.Column,
				Length: queryErr.Length,
			}
		}
		return &QueryError{
			Code:           ErrorCode(queryErr.Code),
			Stage:          QueryErrorStage(queryErr.Stage),
			Message:        queryErr.Message,
			DiagnosticCode: queryErr.DiagnosticCode,
			Location:       location,
		}
	}

	var latticeErr *cgobridge.Error
	if errors.As(err, &latticeErr) {
		return &Error{
			Code:    ErrorCode(latticeErr.Code),
			Message: latticeErr.Message,
		}
	}

	return err
}
