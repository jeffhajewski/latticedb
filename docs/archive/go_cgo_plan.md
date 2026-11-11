# Go cgo Plan

> **Status: built and archived.** The Go binding in `bindings/go` implements this plan; its
> layout differs slightly (`types.go` instead of `value.go`/`query.go`). See `bindings/go/README.md`.

This plan assumes the nested-value C ABI contract in [c_api_nested_values.md](c_api_nested_values.md) lands first.

For the engine-level behavior a future pure-Go implementation should match, see [13_engine_conformance.md](../13_engine_conformance.md).

## Goal

Ship a first-class Go package with an idiomatic public API and a small internal cgo layer. The public Go API should be stable enough that a future pure-Go engine can target the same surface without forcing a breaking change on users.

## Non-goals

- Exposing raw C ABI structs directly as the public Go API
- Requiring users to reason about `lattice_value` unions
- Starting a pure-Go engine before the binding contract is stable

## Sequencing

1. Promote `LIST` and `MAP` into the public C ABI.
2. Update the existing Python and TypeScript bindings to the new ABI.
3. Add any missing packaging pieces the Go build will need:
   - installed public headers
   - a predictable shared/static library layout
   - ideally `pkg-config`
4. Implement the Go binding on top of that stabilized ABI.

The Go work should not start before step 1 is done. Otherwise the binding will immediately grow compatibility debt around value conversion.

## Public Go API Shape

The Go package should be designed as if it were native, not as a direct mirror of the C calls.

- `Open(path string, opts OpenOptions) (*DB, error)`
- `(*DB).Close() error`
- `(*DB).View(func(*Tx) error) error`
- `(*DB).Update(func(*Tx) error) error`
- Optional explicit transactions for advanced use
- Property APIs on `Tx`
- Query APIs on `Tx`
- Search APIs on `DB` or `Tx`, depending on current semantics

The future public `Value` model should be:

- `nil`
- `bool`
- `int64`
- `float64`
- `string`
- `[]byte`
- `[]float32`
- `[]Value`
- `map[string]Value`

## Important API Decisions

- Property getters should distinguish missing from stored `NULL`.
- Preferred getter shape: `(Value, bool, error)`.
- Query results should materialize into Go values in v1.
- The first version should optimize for correctness and ergonomics, not zero-copy everywhere.
- The cgo boundary should live under `internal/cgo`.
- A small C helper shim is acceptable if it simplifies union and nested-value access from Go.

## Proposed Package Layout

```text
bindings/go/
  go.mod
  db.go
  tx.go
  query.go
  value.go
  errors.go
  internal/cgo/
    bridge.go
    helpers.h
    helpers.c
```

If we decide to publish from a separate repository later, the same shape should move with minimal public API change.

## Binding Work Breakdown

1. Value conversion layer
   - Go value to `lattice_value`
   - `lattice_value` to Go value
   - recursive free and keepalive handling
2. Core handles
   - database open/close
   - transaction begin/commit/rollback
3. Property operations
   - node and edge property set/get/remove
4. Query operations
   - prepare, bind, execute
   - result iteration and materialization
5. Search operations
   - vector search
   - full-text search
6. Tests
   - reuse the same nested-value round-trips covered by the C API
   - add null-vs-missing cases explicitly

## Why This Order Matters

Without first-class `LIST` and `MAP` in the C ABI, the Go binding would either:

- expose a crippled value model and need a breaking redesign later, or
- invent Go-only behavior that diverges from the real binding boundary

Landing the ABI cleanup first avoids both problems and gives a future `latticedb-go` engine a clearer target.
