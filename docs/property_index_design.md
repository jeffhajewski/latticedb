# Property Lookup and Index Design

LatticeDB provides explicit, durable indexes for scoped node and edge
properties. Indexes are schema objects: callers opt into their storage and write
cost, and indexed lookup APIs fail instead of silently falling back to a scan.

## Goals

- Keep hot-path lookup APIs honest about whether they are index-backed.
- Keep explicit property indexes as the prerequisite for APIs that imply indexed lookup.
- Support common graph access patterns such as `(:Person {email: ...})` without
  forcing callers through Cypher when they want direct API access.

## Explicit Property Indexes

Creating an index names the entity kind, label or edge type, and property key:

```c
lattice_node_property_index_create(db, "Person", "email");
lattice_edge_property_index_create(db, "OWNS", "serial_number");
```

Python exposes `create_node_property_index()` and
`create_edge_property_index()` on `Database`; TypeScript and Go use the
equivalent camel-case methods. Matching drop methods remove the schema
definition and its entries.

The storage layer maintains a durable definition catalog plus separate node and
edge B+Trees. Entry keys encode the scoped label/type ID, property key ID, the
value, and the entity ID. The catalog entry records how the value is encoded:

- **Ordered** (indexes created by 0.17.0 and later). Byte order of the key is
  the order `ORDER BY` puts values in. Numbers are the largest double not above
  the value (8 bytes, sign-adjusted so they sort) plus a 16-bit remainder, so
  integers and floats share one order and `3` and `3.0` share a key; NaN has its
  own tag after every number. Strings and byte arrays keep their first 128
  bytes, with `0x00` escaped, and a terminator that says whether the value was
  longer. Booleans are one byte. Lists, maps and vectors are keyed by a SHA-256
  digest and answer equality only. Null is not indexed. Integers and floats
  compare exactly in the key, as the query engine compares them. Creating an
  ordered index raises the file's `min_reader_version` to 4 before it writes a
  key, so 0.16.0 refuses to open the file instead of reading the keys as
  digests or writing digests among them. `min_writer_version` would not do:
  0.16.0 does not know that field, and would misread the index anyway.
- **Digest** (indexes created by 0.16.0). A SHA-256 digest of the typed value.
  Such an index keeps answering equality, and is never used for ranges; drop and
  create it again to get the ordered encoding.

Lookups re-read and compare source properties, so a digest collision or a
truncated string key cannot produce an incorrect result.

## Indexed Equality Lookup APIs

Direct lookup APIs make the index requirement clear:

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

`lattice_edges_find_by_type_property()` provides the edge equivalent. Python
transaction methods are `find_nodes_by_label_property()` and
`find_edges_by_type_property()`; TypeScript and Go use camel-case names. Each
accepts a positive result limit.

The lookup fails with `LATTICE_ERROR_UNSUPPORTED` (or the binding's corresponding
exception) when the required index does not exist. It never silently scans.
Creating an index scans existing matching records, and subsequent direct and
transactional mutations maintain it. Transactional lookups include staged
changes and preserve reader snapshot visibility.

The Cypher planner uses an available index for independent inline equality
patterns, including parameterized forms such as
`MATCH (n:Person {email: $email}) RETURN n`. It also recognizes equality
predicates in the `WHERE` clause immediately following a `MATCH`:

```cypher
MATCH (n:Person)
WHERE n.email = $email
RETURN n
```

The comparison may be reversed, and eligible equality predicates may appear
within an `AND` expression. The indexed value must be independent of the row,
such as a literal, parameter, list, or map composed from those values. The
planner does not constrain scans from `OR` branches because doing so could omit
valid rows. It retains the ordinary `WHERE` or inline-property filter above the
index scan as a semantic guard.

With an ordered index the planner also answers, on the same labeled node scan:

- `<`, `<=`, `>`, `>=` against an independent value, either way round, and
  `STARTS WITH`, as one index range per property (a conjunction on one property
  becomes one range). The conjuncts the range answers leave the `WHERE`; the
  scan re-checks them only where the index cannot decide (a string bound longer
  than 128 bytes, a NaN bound, NaN values). Without statistics, a range is
  always read from the index: at 200,000 nodes a range matching every node costs
  about what the label scan and filter cost (195 against 181 ms), and anything
  narrower is much cheaper.
- `MATCH ... RETURN ... ORDER BY n.p [SKIP s] LIMIT k`, ascending, with a
  single pattern and a RETURN that neither aggregates nor deduplicates: the
  index is read in order and reading stops once no further row can be among the
  first `s + k`. Nodes without the property follow the indexed ones, where
  ascending order puts nulls. Descending order still sorts every row.

Read-only queries stream the index in chunks of 256 keys; queries that write
collect the matching ids when the scan opens.

Index selection applies while planning the labeled node scan in the immediately
preceding `MATCH`. It does not rewrite scans from earlier query parts across
`WITH` boundaries, and relationship-property predicates continue to use ordinary
filtering.

## Scan-Backed Convenience APIs

Any future scan-backed convenience lookup must remain explicitly named as a
scan:

```c
lattice_nodes_scan_by_label_property(...);
lattice_edges_scan_by_type_property(...);
```

Scan APIs may be useful for migrations, admin tools, tests, and small local
datasets, but their names must keep the cost visible.
