# Property Lookup and Index Design

This note defines the intended API shape for property lookup ergonomics without
implementing property indexes in the current patch set.

## Goals

- Keep hot-path lookup APIs honest about whether they are index-backed.
- Add explicit property indexes before exposing APIs that imply indexed lookup.
- Support common graph access patterns such as `(:Person {email: ...})` without
  forcing callers through Cypher when they want direct API access.

## Explicit Property Indexes

Property indexes should be explicit schema objects. Creating an index should
name the entity kind, optional label or edge type, and property key:

```c
lattice_node_property_index_create(db, "Person", "email");
lattice_edge_property_index_create(db, "OWNS", "serial_number");
```

The storage layer can then maintain index B+Trees transactionally alongside the
node and edge records. Index keys should encode the scoped label/type id,
property key id, normalized property value, and entity id so equality lookups
are deterministic and duplicate values are supported.

## Indexed Equality Lookup APIs

Once explicit indexes exist, direct lookup APIs can make the index requirement
clear:

```c
lattice_nodes_find_by_label_property(
    txn,
    "Person",
    "email",
    &email_value,
    limit,
    &node_ids,
    &count
);
```

This API should fail with an unsupported or missing-index error when the
required `(label, property)` index does not exist. It should not silently fall
back to scanning, because callers will reasonably place it on request paths.

## Scan-Backed Convenience APIs

If the library adds convenience lookups before property indexes are available,
they must be named and documented as scans:

```c
lattice_nodes_scan_by_label_property(...);
lattice_edges_scan_by_type_property(...);
```

Scan APIs may be useful for migrations, admin tools, tests, and small local
datasets, but their names should make the cost visible. Documentation and
binding names should preserve that distinction until real property indexes are
implemented.
