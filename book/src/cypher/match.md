# MATCH and Patterns

`MATCH` finds patterns in the graph. It is the primary way to read data.

## Node Patterns

Match all nodes:

```cypher
MATCH (n) RETURN n
```

Match nodes with a label:

```cypher
MATCH (p:Person) RETURN p.name
```

Match nodes with inline property filtering:

```cypher
MATCH (p:Person {name: "Alice"}) RETURN p
```

When an explicit index exists for the label/property pair, independent inline
equality patterns use it automatically, including parameterized values:

```cypher
MATCH (p:Person {email: $email}) RETURN p
```

Create the index through the C, Python, TypeScript, or Go database API before
running the query. Equality predicates in the immediately following `WHERE`
also use an available index:

```cypher
MATCH (p:Person)
WHERE p.email = $email
RETURN p
```

The comparison may be reversed or nested within an `AND` expression. `OR`
predicates remain scan-backed so the planner cannot discard rows from the
other branch.

## Variables

Parentheses bind a matched node to a variable:

```cypher
MATCH (person:Person)
RETURN person.name, person.age
```

Variables are used in `WHERE`, `RETURN`, `SET`, and other clauses to reference matched elements.

## Edge Patterns

Match edges between nodes:

```cypher
// Outgoing edge
MATCH (a:Person)-[:KNOWS]->(b:Person)
RETURN a.name, b.name

// Incoming edge
MATCH (a:Person)<-[:KNOWS]-(b:Person)
RETURN a.name, b.name

// Either direction
MATCH (a:Person)-[:KNOWS]-(b:Person)
RETURN a.name, b.name
```

### Edge Variables

Bind edges to variables to access their properties:

```cypher
MATCH (a)-[r:KNOWS]->(b)
RETURN a.name, r, b.name
```

### Edge Type Filtering

Match specific edge types:

```cypher
MATCH (a)-[:KNOWS]->(b) RETURN b
MATCH (a)-[:AUTHORED_BY]->(b) RETURN b
```

## Multi-Hop Patterns

Chain patterns to traverse multiple hops:

```cypher
MATCH (chunk:Chunk)-[:PART_OF]->(doc:Document)-[:AUTHORED_BY]->(author:Person)
RETURN chunk.text, doc.title, author.name
```

## Variable-Length Paths

Match paths of varying length:

```cypher
// 1 to 3 hops
MATCH (a)-[*1..3]->(b) RETURN b

// Exactly 2 hops
MATCH (a)-[*2]->(b) RETURN b

// 2 or more hops
MATCH (a)-[*2..]->(b) RETURN b

// Any number of hops
MATCH (a)-[*]->(b) RETURN b
```

See [Variable-Length Paths](./variable-length-paths.md) for details.

## Multiple MATCH Clauses

Use multiple MATCH clauses to combine patterns:

```cypher
MATCH (a:Person {name: "Alice"})
MATCH (b:Person {name: "Bob"})
RETURN a, b
```

Each variable can be introduced only once in a query. A later `MATCH` — or a
`MATCH` after `WITH` — cannot name a variable that an earlier clause bound; the
query is rejected with "Variable 'a' is already defined". Write the whole path as
one pattern instead:

```cypher
// Rejected: a is bound by the first MATCH
MATCH (a:Person {name: "Alice"})
MATCH (a)-[:KNOWS]->(b)
RETURN b.name

// Works
MATCH (a:Person {name: "Alice"})-[:KNOWS]->(b)
RETURN b.name
```

Relationship type alternation (`-[:KNOWS|LIKES]->`) is not supported either; run
one query per type.
