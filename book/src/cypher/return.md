# RETURN and Projections

`RETURN` specifies which values to include in the query output.

## Returning Properties

```cypher
MATCH (n:Person)
RETURN n.name, n.age
```

## Returning Nodes

Return a node reference:

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

## Expressions

Return computed values:

```cypher
MATCH (n:Person)
RETURN n.name, n.age + 1
```

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

## Pagination

Use `LIMIT` and `SKIP` for pagination:

```cypher
-- First 10 results
MATCH (n:Person) RETURN n.name LIMIT 10

-- Skip first 10, return next 10
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
