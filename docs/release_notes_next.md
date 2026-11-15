# LatticeDB 0.18.0 Release Notes

## Summary

0.17.0 had no way to remove a node's vector short of deleting the node. Its
release notes said to "clear its vector", but there was nothing to clear it
with: the C API and the bindings could only set a vector, and in Cypher
`REMOVE n.embedding` and `SET n.embedding = null` changed an ordinary property
that the vector hides on read, so they appeared to succeed and left the vector
in search. 0.18.0 adds reading and removing a node's vector to every API and
makes those two Cypher statements remove it.

## Highlights

- **Read and remove a node's vector.** The C API gains
  `lattice_node_get_vector`, `lattice_node_remove_vector` and
  `lattice_free_vector`, and every binding gains the pair on its transaction
  object: `get_vector`/`remove_vector` in Python, `getVector`/`removeVector` in
  TypeScript and Java, `GetVector`/`RemoveVector` in Go. Each takes the same key
  as `set_vector` (`"embedding"` or `""`).
  - Reading returns the vector the transaction sees, or none (`NULL`, `None`,
    `null`, `ok == false`, an empty `Optional`) when the node has no vector. A
    node that does not exist is a not-found error. Under the cosine metric, the
    default, a committed vector reads back normalised to unit length (`[3, 4]`
    reads back as `[0.6, 0.8]`), because the index keeps only that copy; an
    uncommitted vector reads back as written.
  - Removing takes the vector out of the index. The node keeps its labels and
    properties, and vector search no longer returns it. Inside a transaction the
    removal is visible to that transaction at once and to everyone else when it
    commits, and it survives a crash once committed. Removing a vector the node
    does not have does nothing. A missing node is a not-found error and a
    read-only transaction refuses the removal.
- **`REMOVE n.embedding` and `SET n.embedding = null` remove the vector.** On a
  database with a vector index both statements now remove the node's vector, and
  any ordinary property named `embedding` with it. On a database without a vector
  index `embedding` is an ordinary property, as before.

## API Notes

- The C API change is additive: three new functions, no changed signatures.
  The TypeScript binding loads them as optional, so it still loads against an
  older library, where the two new methods reject with `Unsupported`.
- Python's `Node.get_vector` and `Node.set_vector` read and wrote a dictionary
  on the `Node` object and never reached the database, so `get_vector` returned
  `None` for a node that had a stored vector. They now emit a
  `DeprecationWarning` pointing at `Transaction.get_vector` and
  `Transaction.set_vector`.
- Vector writes, removals included, emit no changefeed event, as in 0.17.0: the
  changefeed records node and relationship changes only.

## Upgrade Notes

- No file or log format change. A removal is logged as the same record that
  deleting a node with a vector has always logged, so 0.17.0 replays a log
  written by 0.18.0.
- If a 0.17.0 statement ran `REMOVE n.embedding` or `SET n.embedding = null`
  on a node with a vector expecting nothing to happen, it now removes the vector.

## Validation Notes

- None yet.
