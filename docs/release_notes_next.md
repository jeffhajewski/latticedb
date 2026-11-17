# LatticeDB 0.18.1 Release Notes

## Summary

0.18.1 is a patch release. Cypher no longer stores a null as a property:
`SET n += {x: null}` removes `x`, as Cypher says it should, and `SET n = {...}`,
`CREATE`, `ON CREATE SET` and `ON MATCH SET` leave a null out or remove the
property instead of storing it, on nodes and relationships alike. Setting,
batch-inserting or searching vectors on a database created without a vector
index now fails as unsupported, with a message that says why, instead of as an
I/O error. The release also stops the repository tracking the TypeScript
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
- `lattice_node_set_property` and `lattice_edge_set_property` (and the
  bindings' `set_property`) are unchanged: given a null value, they still store
  it.
- The C API's signatures are unchanged. `lattice_node_set_vector`,
  `lattice_batch_insert`, `lattice_vector_search` and
  `lattice_vector_search_txn` return a different code on a database without a
  vector index, `LATTICE_ERROR_UNSUPPORTED` (-14) instead of
  `LATTICE_ERROR_IO` (-2). `lattice_error_message(-14)` is still the generic
  "Unsupported operation or value type": the bindings supply the specific
  message. `lattice_node_set_vector` also returns -14 for a key other than
  `"embedding"` or `""`, as before; the bindings tell the two apart by the key.
- Cypher is unaffected: on a database without a vector index `embedding` is an
  ordinary property, so Cypher never reaches the vector index there.

## Upgrade Notes

- No file or log format change, and no migration. A database written by 0.18.0
  may hold null properties written by the statements above. 0.18.1 reads them
  back as 0.18.0 did: `properties(n)`, `lattice dump` and the API's property
  reads show the key with a null value, and `n.x IS NULL` is true. Any removal
  takes one away: `SET n.x = null`, `SET n += {x: null}`, `SET n = {...}` or
  `REMOVE n.x`. To clear them all for a key, run
  `MATCH (n) WHERE n.x IS NULL SET n.x = null` (and the same for relationships).
- A statement that relied on `SET n += {x: null}` keeping a null-valued `x` in
  `properties(n)` or in the changefeed now sees the key gone and a
  `property_remove` event.
- `MERGE (a)-[r:T {w: null}]->(b)` now fails instead of succeeding.
- Code that caught the I/O error (`LatticeIOError`, `IoError`, `ErrorIO`,
  `ErrorCode.IO`) from setting, batch-inserting or searching vectors on a
  database without a vector index now gets the unsupported error instead.

## Validation Notes

- New integration tests (`lnp_null_property_test.zig`) cover each write path on
  nodes and relationships, parameters, the changefeed events, reopening, the
  `embedding` key with and without a vector index, and how a null property
  already in the file reads and is removed.
- A new C API unit test covers every vector call on a database without a vector
  index, that the refused batch insert creates no node, and that reopening with
  vectors enabled adds an index; each of the Python, TypeScript, Go and Java
  suites has a test that checks the error type and message.
- `git status` stays clean after `npm run test:coverage` in
  `bindings/typescript`.
