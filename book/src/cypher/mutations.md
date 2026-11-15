# CREATE, SET, DELETE

Mutation clauses modify the graph structure.

A query that only writes returns no columns and no rows; the CLI prints "Query
executed". To get back what was written, end it with a `RETURN`:

```cypher
CREATE (a:Person {name: "Alice"})-[:KNOWS]->(b:Person {name: "Bob"})
RETURN id(a), id(b)
```

## CREATE

### Create a Node

```cypher
CREATE (n:Person {name: "Alice", age: 30})
```

Create a node with multiple labels:

```cypher
CREATE (n:Person:Employee {name: "Alice"})
```

### Create an Edge

Create an edge between existing nodes:

```cypher
MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"})
CREATE (a)-[:KNOWS]->(b)
```

### Create Nodes and Edges Together

```cypher
CREATE (a:Person {name: "Alice"})-[:KNOWS]->(b:Person {name: "Bob"})
```

A relationship in `CREATE` has exactly one type. A variable that is already bound
can be used in `CREATE` to connect to it, as above, but not given new labels or
properties there (`MATCH (n) CREATE (n:Extra)` is rejected); use `SET`.

Both ends of a relationship must exist when it is created. A `CREATE` that would
connect a node deleted earlier in the same query fails, with the same error code
as a direct relationship create to a missing node, `LATTICE_ERROR_NOT_FOUND` (-4):

```text
$ lattice exec db.lattice --query="MATCH (a:C {name: 'x'}) DETACH DELETE a CREATE (a)-[:S]->(:C)"
Error: Execution error (NotFound): Cannot complete creating a relationship from node 1 to node 3: a node or relationship it needs does not exist, or was deleted earlier in the query
```

## SET

### Set Properties

```cypher
MATCH (n:Person {name: "Alice"})
SET n.age = 31
```

Set multiple properties:

```cypher
MATCH (n:Person {name: "Alice"})
SET n.age = 31, n.city = "NYC"
```

### Set Labels

Add labels to a node:

```cypher
MATCH (n:Person {name: "Alice"})
SET n:Admin:Verified
```

### Remove a Property via SET

Setting a property to `NULL` removes it:

```cypher
MATCH (n:Person {name: "Alice"})
SET n.city = NULL
```

## DELETE

### Delete a Node

Delete a node:

```cypher
MATCH (n:Person {name: "Charlie"})
DELETE n
```

A node that still has relationships cannot be deleted with plain `DELETE`: the
query fails and deletes nothing.

```text
$ lattice exec db.lattice --query="MATCH (n:C {name: 'x'}) DELETE n"
Error: Execution error (NodeHasRelationships) at 1:32: Cannot delete node 1 because it still has relationships; delete them first, or use DETACH DELETE
  1 | MATCH (n:C {name: 'x'}) DELETE n
    |                                ^
```

Through the C API and the bindings the query returns the same error code as a
direct node delete, `LATTICE_ERROR_HAS_RELATIONSHIPS` (-17). Either delete the
relationships in the same statement, which are removed before any node is
checked:

```cypher
MATCH (n:Person {name: "Dana"})-[r]-()
DELETE r, n
```

or use `DETACH DELETE`. `DELETE` takes nodes and relationships only; deleting a
value such as `n.name` is rejected, and deleting null does nothing.

### DETACH DELETE

Delete a node and all its edges:

```cypher
MATCH (n:Person {name: "Charlie"})
DETACH DELETE n
```

## REMOVE

### Remove a Property

```cypher
MATCH (n:Person {name: "Alice"})
REMOVE n.city
```

### Remove a Label

```cypher
MATCH (n:Person {name: "Alice"})
REMOVE n:Verified
```

### Remove a Node's Vector

`n.embedding` reads a node's vector. On a database with a vector index,
`REMOVE n.embedding` and `SET n.embedding = null` remove that vector, and any
ordinary property named `embedding` with it:

```cypher
MATCH (d:Doc {id: 42})
REMOVE d.embedding
```

The node keeps its labels and other properties, and vector search no longer
returns it. Removing a vector the node does not have does nothing. On a database
without a vector index, `embedding` is an ordinary property like any other.
Before 0.18.0 both statements changed only the ordinary property, which the
vector hides on read, so they appeared to succeed and left the vector in place.
