# RETURN and Projections

`RETURN` specifies which values to include in the query output.

## Returning Properties

```cypher
MATCH (n:Person)
RETURN n.name, n.age
```

## Returning Nodes

Return a node. The value returned is the node's integer ID:

```cypher
MATCH (n:Person)
RETURN n
```

## Aliases

Use `AS` to rename output columns:

```cypher
MATCH (n:Person)
RETURN n.name AS name, n.age AS age
```

Without `AS`, a column is named after the text of its expression: `n.name`,
`n.age + 1`, `coalesce(n.nickname, n.name)`, `count(*)`.
Two columns cannot have the same name: `RETURN n.name, n.name` fails with
"Column `n.name` is returned twice; give one of them another name with AS".

## Every Variable: `RETURN *`

`*` stands for every variable in scope, each as a column named after it, in
alphabetical order. More items may follow it:

```cypher
MATCH (a:Person)-[k:KNOWS]->(b)
RETURN *, b.name AS friend
// columns: a, b, k, friend
```

Variables in scope are those the earlier clauses bound by name: pattern
variables of `MATCH`, `OPTIONAL MATCH`, `CREATE` and `MERGE`, named paths,
`UNWIND ... AS`, `CALL ... YIELD` columns, and what the last `WITH` passed on.
Anonymous nodes and relationships are not included, nor is a `FOREACH` variable.
`RETURN *` with nothing in scope fails with "RETURN * is not allowed when there
are no variables in scope", and `*` must be the first item.

## Expressions

Return computed values:

```cypher
MATCH (n:Person)
RETURN n.name, n.age + 1
```

A query can start with `RETURN` (or `WITH`), which then runs once:

```cypher
RETURN 1 + 2
```

`RETURN` and `WITH` take at most 16 items; a query with more fails to plan.

## Map Projection

`variable {...}` builds a map from a node, relationship or map:

```cypher
MATCH (n:Person)-[k:KNOWS]->(f)
RETURN n {.name, .age, friend: f.name, k}
// {name: "Alice", age: 30, friend: "Bob", k: <relationship>}
```

- `.key` is the property `key`, null if it is missing;
- `.*` is every property;
- `key: expression` is any value;
- a variable `x` on its own is the entry `x: x`.

Entries keep the order written, and a later entry replaces an earlier one with
the same key, so `n {.*, name: 'anonymous'}` overrides one property. A null
subject (from an `OPTIONAL MATCH` that found nothing) gives null; any other
value that is not a node, relationship or map is an error.

## Literals

Return literal values:

```cypher
MATCH (n:Person)
RETURN n.name, "person" AS type
```

## DISTINCT

Remove duplicate rows:

```cypher
MATCH (a:Person)-[:KNOWS]->(b:Person)
RETURN DISTINCT b.name
```

## Ordering

Use `ORDER BY` to sort results:

```cypher
MATCH (n:Person)
RETURN n.name, n.age
ORDER BY n.age DESC
```

Sort by multiple columns:

```cypher
MATCH (n:Person)
RETURN n.name, n.age
ORDER BY n.field, n.age DESC
```

Null sorts last when ascending and first when descending. Values of different
types sort in a fixed order: maps, nodes, relationships, lists, vectors, bytes,
strings, booleans, numbers, then null. Integers and floats compare by exact value,
and NaN sorts above every other number. Rows with equal keys keep the order they
arrived in. If a sort key cannot be evaluated for some row (`ORDER BY n.age / 0`),
the query fails.

With a following `LIMIT`, the sort keeps only the rows it can return, so
`ORDER BY ... LIMIT 10` does not hold every row in memory.

`ORDER BY` sees both the variables of the query and the columns the `RETURN`
names, so an alias can be used inside an expression:
`RETURN n.name AS name ORDER BY toUpper(name)`. After an aggregating `RETURN`
only the returned columns are left, and expressions over them:

```cypher
MATCH (p:Person)-[:KNOWS]->(f)
RETURN p, count(f) AS friends
ORDER BY p.name, friends * -1
```

`ORDER BY f.name` there fails before the query runs, since `f` is not returned
and has no single value per group; so does ordering by an aggregate the
`RETURN` does not return.

## Pagination

Use `LIMIT` and `SKIP` for pagination:

```cypher
// First 10 results
MATCH (n:Person) RETURN n.name LIMIT 10

// Skip first 10, return next 10
MATCH (n:Person) RETURN n.name SKIP 10 LIMIT 10
```

## Aggregations in RETURN

Aggregation functions can be used directly in RETURN:

```cypher
MATCH (doc:Document)-[:AUTHORED_BY]->(p:Person)
RETURN p.name, count(doc) AS papers
ORDER BY papers DESC
```

See [Aggregations](./aggregations.md) for the full list.
