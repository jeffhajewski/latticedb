# Improvement Tracker

> **Status: complete and archived.** Every issue below is done.

This tracks the issue-by-issue cleanup pass started on 2026-03-19. Each item is intended to land as its own user-reviewed commit.

- [x] Issue 1: Expose edge properties through the C API, Python/TypeScript bindings, and CLI import/export
- [x] Issue 2: Support correct multi-label and unlabeled node semantics across the C API and bindings
- [x] Issue 3: Make list/map public API behavior explicit and consistent
- [x] Issue 4: Finish or hide incomplete CLI commands and import batching
- [x] Issue 5: Improve the packaging/install story for Python and TypeScript
- [x] Issue 6: Expand user-facing docs and examples
- [x] Issue 7: Promote LIST/MAP into the public C ABI and land the binding contract in `docs/archive/c_api_nested_values.md`
- [x] Issue 8: Build the Go cgo binding against the stabilized nested-value ABI in `docs/archive/go_cgo_plan.md`
- [x] Issue 9: Draft the engine-neutral conformance contract in `docs/13_engine_conformance.md`

## 2026-04-02 Read-Path Audit Follow-Ups

This audit started from a large-graph traversal failure where read-only edge expansion
could fail on specific reachable nodes with a generic engine error. The immediate fix
in the current worktree is to make traversal-only expansion use adjacency refs instead
of full edge payload deserialization. The follow-up issues below should land as
separate commits after that fix is committed.

- [x] Issue 10 (485d4d5): Commit the current traversal fix that decouples `getOutgoingEdges()` and query expand operators from edge payload deserialization
- [x] Issue 11 (b6f4e5d): Fix the `BTree.get()` lifetime contract so callers cannot observe slices backed by already-unpinned pages
- [x] Issue 12 (b6f4e5d): Audit graph/node/symbol read paths that consume `BTree.get()` results as stable memory
  Examples: `src/graph/node.zig`, `src/graph/symbols.zig`, and any remaining `EdgeStore.getById()`-driven traversal helpers
- [x] Issue 13 (b6f4e5d): Audit FTS read paths that consume `BTree.get()` results as stable memory
  Examples: `src/fts/dictionary.zig`, `src/fts/reverse_index.zig`, and `src/fts/scorer.zig`
- [x] Issue 14 (b6f4e5d): Audit vector/HNSW persisted-state loads that assume `BTree.get()` values remain valid after return
  Example: `src/vector/hnsw.zig`
- [x] Issue 15 (b5c24b6): Fix `Database.getNodeProperties()` to stop reimplementing node decoding incorrectly
  Current concerns: wrong key endianness, invalid ownership/freeing of `BTree.get()` results, and partial property-type decoding drift versus `NodeStore`
- [x] Issue 16 (8a2f5f3): Convert remaining lightweight graph read helpers to ref-based iteration where payload materialization is unnecessary
  Examples: edge counting and any other adjacency-only read paths still built on `EdgeIterator`
