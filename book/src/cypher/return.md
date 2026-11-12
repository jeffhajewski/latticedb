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
