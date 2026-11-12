# CREATE, SET, DELETE

Mutation clauses modify the graph structure.

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
query fails with `NodeHasRelationships` and deletes nothing. Either delete the
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
