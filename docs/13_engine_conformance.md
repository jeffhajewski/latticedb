# Engine Conformance Spec

This document defines the engine-level contract that a future `latticedb-go` implementation should match.

The goal is not to freeze Zig internals. The goal is to freeze the observable database behavior above the storage engine and below the language bindings.

This document sits alongside, not above, the existing binding contracts:

- [archive/c_api_nested_values.md](archive/c_api_nested_values.md) records the C ABI nested-value contract (now built; `include/lattice.h` is authoritative).
- [archive/go_cgo_plan.md](archive/go_cgo_plan.md) records the Go binding plan (now built; see `bindings/go/README.md`).

## Purpose

The current Zig engine is the reference implementation. A future Go engine should be considered conformant if an application using the public database semantics cannot distinguish between the two engines except in areas this document explicitly leaves unspecified.

This spec is intentionally written in terms of:

- logical graph state
- transactions and visibility
- query and search behavior
- persistence and recovery behavior
- public management surfaces such as query cache stats

This spec does not require:

- on-disk format compatibility
- ABI compatibility
- identical planner internals
- identical scoring implementation details where only ordering matters

## Scope

In scope:

- nodes, edges, labels, and properties
- stable edge identity
- the public value model, including nested values
- transaction visibility and durability semantics
- query result semantics
- vector and full-text search behavior at the API/query level
- import/export invariants that affect logical graph state
- query cache functional behavior

Out of scope:

- binary file-format compatibility across engines
- exact memory layout or ABI details
- exact internal timestamps, WAL record shapes, or page layouts
- exact query plan shapes
- exact text of human-readable error messages

## Normative Interpretation

This document is the intended contract.

The extracted conformance suite in [`conformance/go`](../conformance/go/README.md) is the executable example of that contract. If the implementation and this document disagree, the disagreement should be resolved explicitly rather than silently allowing drift.

The suite currently runs against the public Go surface through adapters, including export invariants and recovery cases exercised through an engine-specific crash harness.

## Compatibility Boundaries

The project has four different compatibility surfaces. They should be treated separately:

1. Engine semantics
2. Binding/API semantics
3. Query-language semantics
4. On-disk format

A future `latticedb-go` engine should target semantic compatibility first. It does not automatically inherit obligations around the current C ABI or file format unless those are chosen explicitly in a later phase.

## Core Data Model

### Nodes

- A node is identified by an opaque node ID.
- Node IDs are stable within a database and remain valid across close/reopen for surviving nodes.
- Nodes may have zero or more labels.
- Unlabeled nodes are valid.
- Labels are strings.
- Label matching in queries is set-based and conjunctive:
  - `MATCH (n:Person:Employee)` means the node must have both labels.
- Direct node label enumeration preserves insertion order as exposed today by the direct APIs and bindings.
- Query semantics must not depend on label order.

### Edges

- An edge is directed.
- Multiple parallel edges with the same `(source, target, type)` are valid.
- Every edge has a stable edge ID distinct from its `(source, target, type)` triple.
- Edge IDs are:
  - unique
  - stable across close/reopen
  - monotonic
  - never reused after delete or rollback
- Mutations addressed by edge ID apply to exactly one edge instance, even when parallel edges exist.

### Properties And Values

The logical value model is:

- `NULL`
- `BOOL`
- `INT`
- `FLOAT`
- `STRING`
- `BYTES`
- `VECTOR`
- `LIST`
- `MAP`

The detailed nested-value contract lives in [archive/c_api_nested_values.md](archive/c_api_nested_values.md). At the engine level the important points are:

- lists are ordered and heterogeneous
- maps are string-keyed nested values
- nested values may contain other nested values recursively
- duplicate map keys are invalid input at public API boundaries
- bytes remain bytes
- vectors remain vectors
- query materialization must not coerce bytes into strings
- `UNWIND` and query projection must preserve nested values and vectors

### Missing Versus `NULL`

Missing property and stored `NULL` are distinct concepts.

- Direct property APIs must preserve the distinction.
- High-level bindings should expose a way to distinguish them.
- Query property access on a missing property yields `NULL`, not an error.
- Query results may therefore contain `NULL` for both:
  - an explicitly stored `NULL`
  - a missing property referenced through query evaluation

That distinction is only guaranteed through the direct property APIs, not through query projection alone.

### Property Mutation Semantics

The direct property APIs and Cypher mutation syntax are intentionally different:

- Direct property setters may store an explicit `NULL` value.
- In query mutation semantics, `SET n.prop = null` behaves like property removal.
- `REMOVE n.prop` also removes the property.

This distinction is part of the public contract and should not be normalized away by a future engine.

## Transaction Semantics

### Modes

- The database exposes read-only and read-write transactions.
- Read-only transactions must reject writes.

### Visibility

The current public transaction contract locks in these guarantees:

- a transaction sees its own uncommitted writes
- after a write transaction commits, a newly started transaction sees the committed state
- rolled-back changes are not visible after rollback

The current public contract does not freeze:

- cross-transaction visibility before commit
- the behavior of a long-lived read transaction across concurrent commits

Only one read-write transaction may be active per database handle. Beginning a
second writer fails with the public lock-timeout/contention error instead of
allowing overlapping overlays to overwrite each other's committed changes.
Read-only transactions may remain active while the writer runs. What a
long-lived reader sees of commits made while it is open is not frozen (see
above).

The internal transaction manager uses snapshot-oriented MVCC machinery, but that should not be treated as the required public engine contract yet because the end-to-end database APIs are not fully wired to expose those stronger guarantees consistently. Implementations may provide stronger isolation than the guarantees listed above, but callers must not depend on that stronger behavior for cross-engine portability.

### Atomicity

Transactions are all-or-nothing.

- A committed transaction makes all its writes visible.
- A rolled-back or aborted transaction leaves no visible logical changes.

Query execution also carries a statement-level atomicity requirement:

- if a mutation query fails during execution, it must not leave partial logical side effects behind
- this applies to cases such as invalid non-property values inside `CREATE`, `SET`, or `MERGE` patterns

### Durability

- Committed transactions must survive close/reopen.
- Committed transactions must survive crash/recovery.
- Uncommitted or aborted transactions must not become visible after recovery.

## Graph Semantics

### Labels

- Nodes may have multiple labels.
- Multi-label queries are conjunctions, not disjunctions.
- Removing a label affects only that label.
- Export/import and query logic must not duplicate a node merely because it has multiple labels.

### Edge Identity

Parallel edges are first-class graph state, not an implementation accident.

The following must hold:

- deleting one edge by stable edge ID leaves other parallel edges intact
- query mutations on a bound edge variable apply only to the matched edge instance
- `DELETE r` and `DELETE` by `id(r)` must be able to target one parallel edge without removing all siblings

### Directionality

- Edge existence and traversal are directional.
- `(a)-[:REL]->(b)` is not interchangeable with `(b)-[:REL]->(a)`.

## Query Semantics

This document does not attempt to restate the full Cypher subset grammar. It freezes the externally important semantics already exercised by the integration tests.

### General Rules

- Query parameters accept the same logical value model as direct property APIs.
- Query results return the same logical value model, including nested values.
- Explicit `RETURN ... AS alias` controls the output column name.
- Portable code should use explicit aliases for result column names.
- Without an explicit alias, the current derived-name behavior is not a required cross-engine compatibility guarantee.
- Unknown relationship types in `MATCH` produce an empty result, not an error.

### Property Access

- Property access on a node or edge may return `NULL`.
- `IS NULL` and `IS NOT NULL` operate on the resulting query value, not on direct-storage presence metadata.

### Mutation Semantics

The following behaviors are part of the contract:

- `CREATE` creates nodes and edges with labels and property maps
- `SET target.prop = expr` updates or removes a property depending on whether `expr` evaluates to non-`NULL` or `NULL`
- `SET target = {...}` replaces the property map on the target
- `SET target += {...}` merges into the property map on the target
- `REMOVE target.prop` removes a property
- `REMOVE target:Label` removes a label
- mutation against a bound edge variable targets the matched stable edge instance, not all parallel edges with the same endpoints

### Search Operators Inside Queries

The query-level vector and full-text operators must preserve row semantics:

- additional `MATCH` bindings are preserved
- input row multiplicity is preserved
- filtering around the search operator still constrains the candidate rows correctly

This matters more than exact internal planning strategy.

## Search API Semantics

### Vector Search

- Vector search is nearest-neighbor search over stored vectors.
- Result arrays are ordered by distance ascending.
- Lower distance is better.
- When one stored vector is an exact match for the query vector and another is not, the exact match should rank ahead.
- Exact floating-point distances are not part of the cross-engine contract.
- Tie order is not currently specified.

### Full-Text Search

- Full-text search returns scored matches for indexed text.
- Result arrays are ordered by score descending.
- Higher score is better.
- Exact score values are not part of the cross-engine contract.
- Tie order is not currently specified.
- Fuzzy search should be at least as permissive as a stricter exact configuration when given looser distance settings.

## Query Cache Semantics

The query cache is a public management surface even though it is not part of logical database state.

Required behavior:

- clearing the cache on an empty database succeeds
- fresh cache statistics report zero entries, hits, and misses
- executing a previously uncached query text increases misses by at least one
- executing the same query text again without clearing or reopening increases hits by at least one
- clearing the cache resets the entry count to zero

Not required:

- resetting cumulative hit/miss counters on clear
- durability across reopen
- exact cache eviction strategy
- exact cache size or internal keying details beyond current public behavior

## Persistence, Recovery, And Export Invariants

### Close/Reopen

The following must survive normal close/reopen:

- nodes
- edges
- labels
- properties
- stable edge IDs

### Crash Recovery

The following must hold after recovery:

- committed transactions are visible
- aborted or in-progress transactions are not made visible
- symbol/label resolution remains consistent for recovered records

### Export Invariants

Current export behavior establishes several logical invariants worth preserving across engines:

- multi-label nodes are exported once, not once per label
- parallel edges are preserved as distinct edges
- edge properties remain attached to the correct edge instance
- the public `dump` command emits canonical JSON for cross-engine state comparison
- canonical dump includes unlabeled nodes
- canonical dump orders nodes by node ID ascending
- canonical dump orders edges by source ID, target ID, type name, then edge ID
- canonical dump sorts labels, property keys, and nested map keys lexicographically
- canonical dump includes stable edge IDs so parallel edges remain distinguishable in state comparisons
- repeated dumps of unchanged logical state are byte-stable

### Import Compatibility Boundary

Current CLI import compatibility is intentionally narrower than the logical value model.

- The JSON import path maps `null`, booleans, numbers and strings to `NULL`, `BOOL`, numeric and `STRING`.
- It also maps arrays to `LIST`, objects to `MAP`, arrays of numbers to `VECTOR`, and any string of even length made only of hex digits to `BYTES`.
- These rules are lossy. A string such as `"2024"` or `"cafe"` is imported as two bytes, not as a string, and a list of numbers cannot be imported as a `LIST`.
- Only the scalar mappings are part of the guaranteed import contract. Cross-engine compatibility should therefore target the direct property/query APIs and canonical dump/export behavior, not full-fidelity nested-value interchange through the current CLI JSON import format.

## Deliberately Unspecified Areas

The following are intentionally left open for now:

- on-disk format compatibility
- exact map iteration order
- exact vector distance values
- exact BM25/FTS score values
- exact tie-breaking when scores or distances are equal
- non-aliased result column naming
- exact human-readable error wording
- exact planner/operator tree shape

These areas may be tightened later if real cross-engine compatibility pressure appears.

## Current Extracted Coverage

The current extracted suite already covers black-box cases drawn from these sources:

- `tests/integration/database_test.zig`
  - persistence across reopen
  - multi-label semantics
  - stable monotonic edge IDs
  - exact deletion of one parallel edge
- `tests/integration/mvcc_test.zig`
  - internal MVCC visibility rules
  - own-write visibility
  - deleted-version invisibility
- `conformance/go/suite_test.go`
  - persistence across reopen and stable edge IDs
  - missing versus `NULL` properties, and nested values
  - mutation atomicity and parallel edges
  - search operators and the query cache
  - read-only rejection
  - own-write visibility
  - newly started transactions observe committed state
  - rollback cleanup
  - rejection of a second live writer on the same database handle
- `tests/integration/query_mutation_test.zig`
  - mutation atomicity
  - edge-specific mutation semantics
  - alias propagation
  - vector and full-text operator row semantics
  - `NULL` behavior in query execution
- `tests/integration/import_export_test.zig`
  - export deduplication and parallel-edge preservation
  - dump/export logical-state invariants through the public CLI
- `tests/crash/crash_test.zig`
  - committed graph-state recovery through WAL replay
  - multi-label and secondary-label lookup recovery through WAL replay
  - committed node-property update recovery through WAL replay
  - committed edge-property update recovery through WAL replay
  - aborted tail inserts remaining invisible after replay
  - post-recovery edge-ID monotonicity relative to committed state
- binding integration tests
  - missing-versus-`NULL` distinction in direct property APIs
  - query cache surface behavior
  - nested value round-trips through public APIs

The extracted suite keeps its assertions black-box:

- no direct access to Zig internals
- assertions written in terms of public behavior only

Some adapters are still engine-specific:

- the current export adapter shells out to the public `lattice` CLI
- the current recovery adapter simulates crash/reset using the current engine's file layout to force WAL replay

That adapter-specific knowledge is intentional; it lets the suite stay engine-neutral at the behavioral level while still exercising crash/export behavior on the current engine.

## Immediate Follow-Ups

1. Widen the canonical dump conformance coverage as new value shapes or exported fields are added.
2. Write a cleaner engine-neutral crash-injection interface if `latticedb-go` needs a different recovery trigger than the current file-reset harness.
3. Extend the writer-contention coverage if a future engine introduces
   cross-handle or cross-process coordination beyond the current per-handle
   single-writer contract.
