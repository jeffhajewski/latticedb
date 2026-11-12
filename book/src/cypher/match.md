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

List several types with `|` to match a relationship of any of them:

```cypher
MATCH (a:Person {name: "Alice"})-[r:KNOWS|LIKES]->(b)
RETURN type(r), b.name
```

A relationship has one type, so `-[:KNOWS:LIKES]->` is a parse error. `|` is for
`MATCH` only: `CREATE` and `MERGE` need exactly one type.

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

A node variable that is already bound can be named again, in a later `MATCH`, after
`WITH`, or later in the same pattern, and it stands for the same node. Labels and
properties written at the second mention are checked against that node:

```cypher
MATCH (a:Person {name: "Alice"})
MATCH (a)-[:KNOWS]->(b)
RETURN b.name

// A cycle back to the starting node
MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(c)-[:KNOWS]->(a)
RETURN a.name
```

A relationship variable cannot be matched again: `MATCH (a)-[r]->(b) MATCH
(c)-[r]->(d)` is rejected.

## Relationship Uniqueness

Within one `MATCH`, each relationship is used at most once, as Cypher specifies. A
pattern that walks out over an undirected relationship cannot come straight back
over it, so this does not return Alice as `c`:

```cypher
MATCH (a:Person {name: "Alice"})-[:KNOWS]-(b)-[:KNOWS]-(c)
RETURN c.name
```

The same holds across the comma-separated patterns of one `MATCH`, and for the
relationships a variable-length hop walks: a later relationship of the same `MATCH`
cannot use one of them again. Over the two-relationship cycle `x->y->x`:

```cypher
MATCH (a:C)-[:R*1..2]->(b)-[:R]->(c)
RETURN a.name, b.name, c.name
// x y x
// y x y
```

The last hop never reuses a relationship of the path before it, so this returns
two rows, not four. Separate `MATCH` clauses may reuse a relationship.
