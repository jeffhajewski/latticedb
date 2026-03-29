# C API Nested Values Contract

This document defines the public contract for promoting `LIST` and `MAP` into the stable C ABI.

## Why this change exists

The core engine already supports nested values in `PropertyValue`, query parameters, property storage, and query results. The current C ABI does not. That mismatch is now the main blocker for a first-class Go binding and an avoidable source of drift across bindings.

The goal here is to make nested values part of the real public contract, not an internal capability hidden behind `LATTICE_ERROR_UNSUPPORTED`.

## Decisions

- `LIST` and `MAP` are first-class public value types.
- The logical value model is:
  - `NULL`
  - `BOOL`
  - `INT`
  - `FLOAT`
  - `STRING`
  - `BYTES`
  - `VECTOR`
  - `LIST`
  - `MAP`
- Lists are ordered and heterogeneous.
- Maps are string-keyed nested values.
- Nested values may contain other nested values recursively.
- Duplicate map keys are invalid input at the C API boundary.
- The stable C API keeps the existing enum tags. The layout of `lattice_value` changes.

## New ABI Shape

The flat `lattice_value` union is replaced by a recursive representation with pointer indirection for container values.

```c
typedef struct lattice_value lattice_value;
typedef struct lattice_list lattice_list;
typedef struct lattice_map_entry lattice_map_entry;
typedef struct lattice_map lattice_map;

struct lattice_list {
    lattice_value* items;
    size_t len;
};

struct lattice_map_entry {
    const char* key;
    size_t key_len;
    lattice_value value;
};

struct lattice_map {
    lattice_map_entry* entries;
    size_t len;
};

struct lattice_value {
    lattice_value_type type;
    union {
        bool bool_val;
        int64_t int_val;
        double float_val;
        struct {
            const char* ptr;
            size_t len;
        } string_val;
        struct {
            const uint8_t* ptr;
            size_t len;
        } bytes_val;
        struct {
            const float* ptr;
            uint32_t dimensions;
        } vector_val;
        lattice_list* list_val;
        lattice_map* map_val;
    } data;
};
```

This shape is intentional:

- The `lattice_value` tag stays compact and easy to inspect.
- Recursive containers are expressible in plain C.
- Go, Python, and TypeScript can all build and decode the same structure.
- Owning and borrowed APIs can share the same surface area.

## Ownership And Lifetime

Ownership rules are the critical part of this change.

- Input APIs borrow caller memory during the call only.
  - `lattice_node_set_property`
  - `lattice_edge_set_property`
  - `lattice_query_bind`
- These functions must not retain pointers into the caller's nested value tree after they return.
- Owning getters return a deep-owned `lattice_value` tree that the caller must release with `lattice_value_free`.
  - `lattice_node_get_property`
  - `lattice_edge_get_property`
- `lattice_value_free` must free recursively:
  - strings
  - bytes
  - vectors
  - list structs
  - list item arrays
  - map structs
  - map entry arrays
  - map keys
  - descendant values
- `lattice_result_get` keeps its existing borrowed contract.
- For `lattice_result_get`, every pointer inside the returned tree is borrowed from the result handle and remains valid until `lattice_result_free`.
- Borrowed result values must not be passed to `lattice_value_free`.
- Borrowed result values must not be mutated by callers.

The implementation consequence is that result handles need internal storage for borrowed list/map wrapper objects, because query results are currently stored as `ResultValue`, not as `lattice_value`.

## Validation Rules

These rules should be enforced consistently by the C API.

- `STRING`, `BYTES`, and `VECTOR` may use `ptr == NULL` only when their length or dimension count is zero.
- `LIST` requires `data.list_val != NULL`.
- `MAP` requires `data.map_val != NULL`.
- `lattice_list.items` may be `NULL` only when `len == 0`.
- `lattice_map.entries` may be `NULL` only when `len == 0`.
- `lattice_map_entry.key` may be `NULL` only when `key_len == 0`.
- Map keys are UTF-8 text with explicit length. They are not required to be null-terminated.
- Duplicate map keys are rejected with `LATTICE_ERROR_INVALID_ARG`.
- Empty lists and empty maps are valid values.
- Heterogeneous lists are valid values.
- Nested maps and nested lists are valid values.

## Behavioral Contract

The public semantics should be the same no matter how a value enters the system.

- The same nested value model must be accepted by:
  - `lattice_node_set_property`
  - `lattice_edge_set_property`
  - `lattice_query_bind`
- The same nested value model must be observable through:
  - `lattice_node_get_property`
  - `lattice_edge_get_property`
  - `lattice_result_get`
- Nested values should round-trip across storage and query paths.
- `LIST` and `MAP` are part of parameter binding, property storage, and query results.
- `NULL` remains distinct from property absence.
- This change does not itself redefine result ordering or map iteration ordering at the query level.
- Callers must not depend on a stable map entry order across engines or bindings.

## Known Remaining Limitation

The nested-value ABI work is complete for direct property APIs, query binding, and query results. The remaining limitation is in the CLI import/export surface:

- The current JSON import path accepts scalar property values only: `NULL`, `BOOL`, numeric, and `STRING`.
- Nested `LIST`/`MAP`, `BYTES`, and `VECTOR` values round-trip through the direct property/query APIs and canonical export paths, but they are not yet part of the guaranteed JSON import contract.
- Callers should not treat the current CLI JSON import format as a full-fidelity interchange format for nested values.

## Compatibility Notes

This is a real ABI change.

- `sizeof(lattice_value)` changes.
- The set of valid `lattice_value.data` union members changes.
- All language bindings must be updated in the same branch.
- The release notes must call out the struct layout change explicitly.

The enum tag values do not change, which keeps error handling and basic type switching stable.

## Implementation Plan

1. Update `include/lattice.h` to publish the recursive container structs and new ownership docs.
2. Update `src/api/c_api.zig` to:
   - parse nested input values recursively
   - convert owned property values into owned C trees
   - convert query result values into borrowed C trees
   - make `lattice_value_free` recursive
3. Replace explicit unsupported-value tests with round-trip tests for:
   - node properties
   - edge properties
   - bound query parameters
   - query results
4. Update Python and TypeScript bindings to encode and decode nested values.
5. Resolve or document the remaining JSON-import limitation called out above.
6. Update user-facing docs and examples to show nested values as supported public behavior.

## Out Of Scope For This Change

- A batched/materialized query result API
- Changes to label APIs
- A `pkg-config` or install story cleanup
- The Go binding implementation itself
- A pure-Go engine

Those remain separate work items after the ABI contract lands.
