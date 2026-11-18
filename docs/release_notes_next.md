# LatticeDB 0.18.1 Release Notes

## Summary

0.18.1 is a patch release. No write path stores a null as a property any more:
`SET n += {x: null}` removes `x`, as Cypher says it should, and `SET n = {...}`,
`CREATE`, `ON CREATE SET` and `ON MATCH SET` leave a null out or remove the
property instead of storing it, on nodes and relationships alike. The API
follows the same rule: `lattice_node_set_property` and
`lattice_edge_set_property` with a null value, and every binding's
`set_property`, remove the property, a binding's create call leaves a null
property out, and so do the JSON, JSONL and CSV importers. Setting,
batch-inserting or searching vectors on a database created without a vector
index now fails as unsupported, with a message that says why, instead of as an
I/O error, and a Cypher `<=>` `WHERE` search there fails with a message that
says the same instead of "could not create execution plan". The release also stops the repository tracking the TypeScript
binding's coverage report.

## Fixes

- **A null in a Cypher property map removes the property.** In 0.18.0 only
  `SET n.x = null` removed a property. Every other way of writing one stored the
  null: after `SET n += {x: null}`, `properties(n)` returned `{x: null, ...}`,
  the changefeed recorded `node.property_set` with a null `new_value`, and the
  property was still there after reopening (`n.x IS NULL` was true either way,
  and `exists(n.x)` false). `SET n = {x: null}`, `CREATE (n {x: null})`,
  `ON CREATE SET n.x = null` and `ON MATCH SET n.x = null` did the same, on
  relationships as on nodes, and so did a null that arrived in a parameter
  (`SET n += $map`, `CREATE (n {x: $v})`). Now `SET n += {...}`, `ON CREATE SET`
  and `ON MATCH SET` remove the property, logging `node.property_remove` (or
  `edge.property_remove`) when there was one to remove and nothing when there
  was not, and `SET n = {...}` and `CREATE` leave it out.
- **A null written through the API removes the property.**
  `lattice_node_set_property` and `lattice_edge_set_property` stored a null
  value as a property, so the API could write what Cypher had just stopped
  writing: `set_property(id, "x", None)` left `x: null` in `properties(n)` and
  logged `node.property_set` with a null `new_value`. Now both calls remove the
  property, logging `node.property_remove` (or `edge.property_remove`) when
  there was one and nothing when there was not, exactly as `SET n.x = null`
  does; a node or edge that does not exist is still `LATTICE_ERROR_NOT_FOUND`.
  The C API has no create call that takes properties: each binding's
  `create_node` and `create_edge` sets them one by one, so a null property is
  now left out of the stored node, and each binding also leaves it out of the
  `Node` or `Edge` it returns (Python `None`, TypeScript `null` or `undefined`,
  Go `nil`, Java `null`). The rule lives in `Database.setNodeProperty` and
  `Database.setEdgePropertyById`, which every one of these and the importers
  write through, so `lattice import` of a JSON, JSONL or CSV file leaves a
  `null` property out as well. A null inside a list or a map is a value and is
  stored as given. The RDF importer never produced a null property and is
  unchanged.
- **A relationship `MERGE` with a null pattern property fails.** A node `MERGE`
  has always refused `MERGE (n {x: null})`, because no node can match a null.
  The relationship form stored the null instead and matched it on the next run.
  With nulls no longer stored it would have created a new relationship every
  run, so it now fails with the node form's error: `MERGE cannot match a null
  property value: 'w' is null; use MATCH, or give it a value`.
- **A vector call on a database without a vector index says so.** On a
  database created without a vector index, `lattice_node_set_vector`,
  `lattice_batch_insert`, `lattice_vector_search` and
  `lattice_vector_search_txn` returned `LATTICE_ERROR_IO`, whose text is "I/O
  error", which reads like a disk fault. The engine's vector check
  (`Database.validateVectorValue`) reported a missing index as an I/O error.
  They now return `LATTICE_ERROR_UNSUPPORTED`, the code an undeclared property
  or full-text index already gives, and `lattice_batch_insert` returns it before
  it creates any node; in 0.18.0 it created the first node, without a vector,
  and then failed. Each binding raises its unsupported error with a message
  that says the database was created without a vector index and how to add one:
  `LatticeUnsupportedError` in Python, a `LatticeError` with code `Unsupported`
  in TypeScript, `ErrorUnsupported` in Go and `ErrorCode.UNSUPPORTED` in Java,
  from `set_vector`, `batch_insert_vectors` and both vector searches. Reading a
  vector on such a database still finds none and removing one still does
  nothing, as documented.
- **A Cypher vector search on a database without a vector index says so.**
  When the whole `WHERE` of a `MATCH` is a `<=>` predicate
  (`WHERE n.embedding <=> $q < 0.5`, or `n.embedding <=> $q` on its own), the
  planner answers it with the vector index. On a database created without one,
  0.18.0 failed with "Plan error: could not create execution plan". It now
  fails at the same stage, with the same error code, and the message "This
  database was created without a vector index, so `WHERE ... <=> ...` has no
  index to search. Open it with vectors enabled and vector dimensions set to add
  one." The diagnostic code is `MissingVectorIndex`. `<=>` used as an
  expression is unchanged: in an `ORDER BY`, a `RETURN`, a `WITH` or a `WHERE`
  that combines it with other conditions it still measures an ordinary
  `embedding` property row by row with cosine, as documented, and a node
  without that property has a null distance and sorts last.
- **The TypeScript coverage report is no longer tracked.**
  `bindings/typescript/coverage/`, which `npm run test:coverage` rewrites on
  every run, was committed, so a coverage run left the working tree dirty. It is
  now removed from the repository and ignored. Neither CI's coverage gate, which
  measures a fresh run, nor the npm package, which ships only `dist` and `lib`,
  read those files.

## API Notes

- The `embedding` key is unchanged. On a database with a vector index,
  `SET n += {embedding: null}`, `SET n = {..., embedding: null}` and
  `SET n.embedding = null` remove the node's vector, as in 0.18.0, and
  `CREATE (n {embedding: null})` creates a node without one. On a database
  without a vector index `embedding` is an ordinary property, so a null now
  removes it like any other.
- `lattice_node_set_property` and `lattice_edge_set_property` keep their
  signatures; a `LATTICE_VALUE_NULL` value now removes the property and returns
  `LATTICE_OK` whether or not it was there. A NULL `value` pointer is still
  `LATTICE_ERROR_INVALID_ARG`. On a database with a vector index, setting
  `embedding` to null through these calls removes an ordinary `embedding`
  property only and leaves the node's vector alone, as setting a list there
  never set one; `lattice_node_remove_vector` removes the vector.
- The C API's signatures are unchanged. `lattice_node_set_vector`,
  `lattice_batch_insert`, `lattice_vector_search` and
  `lattice_vector_search_txn` return a different code on a database without a
  vector index, `LATTICE_ERROR_UNSUPPORTED` (-14) instead of
  `LATTICE_ERROR_IO` (-2). `lattice_error_message(-14)` is still the generic
  "Unsupported operation or value type": the bindings supply the specific
  message. `lattice_node_set_vector` also returns -14 for a key other than
  `"embedding"` or `""`, as before; the bindings tell the two apart by the key.
- Cypher reaches the vector index only through a `<=>` `WHERE` search, which
  now fails with its own message on a database without one (see Fixes). The C
  code is unchanged: a plan failure is `LATTICE_ERROR` (-1), as an undeclared
  full-text index in `@@` is, not the `LATTICE_ERROR_UNSUPPORTED` the vector
  API calls return.

## Upgrade Notes

- No file or log format change, and no migration. A database written by 0.18.0
  may hold null properties written by the statements above. 0.18.1 reads them
  back as 0.18.0 did: `properties(n)`, `lattice dump` and the API's property
  reads show the key with a null value, and `n.x IS NULL` is true. Any removal
  takes one away: `SET n.x = null`, `SET n += {x: null}`, `SET n = {...}`,
  `REMOVE n.x` or setting it to null through the API. To clear them all for a key, run
  `MATCH (n) WHERE n.x IS NULL SET n.x = null` (and the same for relationships).
- A statement that relied on `SET n += {x: null}` keeping a null-valued `x` in
  `properties(n)` or in the changefeed now sees the key gone and a
  `property_remove` event.
- `MERGE (a)-[r:T {w: null}]->(b)` now fails instead of succeeding.
- Setting a property to null through the API (`lattice_node_set_property`,
  `lattice_edge_set_property`, `set_property` / `setProperty` /
  `SetProperty` and the edge forms in every binding) removes it instead of
  storing a null. Code that wrote a null to mark "known to be empty" and then
  checked for the key, through `properties(n)`, a getter that tells a missing
  property from a null one (Go's `GetProperty` returns `ok == false` now), or
  the changefeed, sees the key gone and a `property_remove` event instead of a
  `property_set`. Store a sentinel value instead if the difference matters.
- A binding's `create_node` / `createNode` / `CreateNode` and `create_edge`
  forms leave a null property out of the stored entity and of the `Node` or
  `Edge` they return; in 0.18.0 the returned object listed it.
- Export, then import, drops the null properties a 0.18.0 database holds.
  `lattice export` writes what the file has, so a 0.18.0 null still appears as
  `"x": null` in JSON and JSONL and as `null` in a CSV `:json` column; importing
  that file into 0.18.1 leaves `x` out, so the copy has no `x` where the
  original had a null one. Every query reads the two the same way (`n.x` is
  null and `n.x IS NULL` is true); only `properties(n)`, a getter that tells a
  missing property from a null one, a dump and the changefeed see the
  difference.
  The RDF exporter never wrote a null property, so a Turtle or N-Triples round
  trip is unchanged.
- Code that caught the I/O error (`LatticeIOError`, `IoError`, `ErrorIO`,
  `ErrorCode.IO`) from setting, batch-inserting or searching vectors on a
  database without a vector index now gets the unsupported error instead.

## Validation Notes

- New integration tests (`lnp_null_property_test.zig`) cover each write path on
  nodes and relationships, parameters, the changefeed events, reopening, the
  `embedding` key with and without a vector index, and how a null property
  already in the file reads and is removed, by Cypher or by the API; the null
  is written below the API, since nothing above it stores one now. Another
  covers the property setters with a null value, inside and outside a
  transaction, with the changefeed events compared to Cypher's.
- A C API unit test covers `lattice_node_set_property` and
  `lattice_edge_set_property` with a null value on a present and an absent key
  and on a missing node or edge. An import test reads a JSON, a JSONL and a CSV
  export holding null properties and checks they are left out while a null in a
  list is kept. Each binding has a test for its create and set calls with a null
  value, the returned object, the stored properties and the changefeed events.
- Two tests asserted the old behaviour and were changed: the Go binding's
  nested-value test and the Go conformance suite's
  `TestConformanceMissingVsNullAndNestedRoundTrip` created a node with
  `"note": nil` and expected a stored null; they now expect no `note`. Two
  import/export round-trip tests stored a top-level null property; the CSV one
  now stores a list holding a null, and the all-formats one no longer stores
  one.
- A new C API unit test covers every vector call on a database without a vector
  index, that the refused batch insert creates no node, and that reopening with
  vectors enabled adds an index; each of the Python, TypeScript, Go and Java
  suites has a test that checks the error type and message.
- `git status` stays clean after `npm run test:coverage` in
  `bindings/typescript`.
- A new integration test checks that the `<=>` `WHERE` search fails with the
  missing-index message on a database without a vector index, and that `<=>`
  in a combined `WHERE` and in `ORDER BY` still measures there.
