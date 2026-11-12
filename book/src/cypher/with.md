# WITH and Chaining

`WITH` pipes the output of one query part into the next. It acts as a boundary between query segments, allowing you to transform, filter, or aggregate intermediate results.

## Basic Chaining

```cypher
MATCH (p:Person)
WITH p.name AS name, p.age AS age
WHERE age > 30
RETURN name
```

## Aggregation then Filtering

`WITH` is essential for filtering on aggregated values, since `WHERE` cannot reference aggregation results directly:

```cypher
MATCH (doc:Document)-[:AUTHORED_BY]->(p:Person)
WITH p.name AS author, count(doc) AS papers
WHERE papers > 5
RETURN author, papers
ORDER BY papers DESC
```

## Variable Scoping

Variables from before `WITH` are only available after it if they are explicitly passed through:

```cypher
MATCH (a:Person)-[:KNOWS]->(b:Person)
WITH a, count(b) AS friend_count
RETURN a.name, friend_count
```

In this example, `b` is not available after the `WITH` clause — only `a` and `friend_count` are. Naming `b` after it fails with "Variable 'b' is not defined".

`WITH *` passes on every variable in scope, and items after it add more:

```cypher
MATCH (a:Person)-[:KNOWS]->(b:Person)
WITH *, a.age - b.age AS gap
WHERE gap > 10
RETURN a.name, b.name, gap
```

## Multi-Stage Queries

A `MATCH` after `WITH` can continue from a node the `WITH` passed on:

```cypher
MATCH (a:Person {name: "Alice"})
WITH a
MATCH (a)-[:KNOWS]->(b)
RETURN a.name, b.name
```

Chain multiple `WITH` clauses for complex queries:

```cypher
MATCH (p:Person)-[:AUTHORED_BY]-(doc:Document)
WITH p, count(doc) AS docs
WITH p, docs WHERE docs > 2
RETURN p.name, docs
```

## Subqueries: `CALL { }`

`CALL { ... }` runs a query once for each incoming row. A leading `WITH` imports
variables from outside (only plain variables, as in `WITH a, b`); without one,
the query sees none of them. Aggregation, `ORDER BY` and `LIMIT` inside apply to
each run on its own, which is what per-row top-k and per-row counts need:

```cypher
MATCH (t:Team)
CALL {
  WITH t
  MATCH (p:Player)-[:PLAYS_FOR]->(t)
  RETURN p.name AS top ORDER BY p.goals DESC LIMIT 1
}
RETURN t.name, top
```

Each row the subquery returns extends the incoming row with its columns, so an
incoming row it returns nothing for is dropped (put an `OPTIONAL MATCH` or an
aggregate inside to keep one). Returned values must be named (`AS`), and their
names must be new. A variable inside may not reuse the name of one outside that
it did not import. The queries of a `UNION` inside each import on their own.

A subquery without `RETURN` runs for its writes, and each incoming row passes on
once:

```cypher
MATCH (t:Team)
CALL { WITH t CREATE (:Audit {team: t.name}) }
RETURN count(*)
```

A `MATCH` inside searches as it would on its own, once for each incoming row:
`WHERE d.text @@ 'x'` scores its matches, and a bare `WHERE d.embedding <=> $q`
with a `LIMIT` gives each row its own nearest:

```cypher
MATCH (p:Person)
CALL {
  WITH p
  MATCH (p)-[:WROTE]->(d:Doc) WHERE d.embedding <=> $q
  RETURN d.title AS nearest LIMIT 1
}
RETURN p.name, nearest
```

`WITH * WHERE ...` is a `WITH`'s `WHERE`, checked row by row: `@@` there
matches without a score, and `<=>` needs a bound.

`CALL { ... } IN TRANSACTIONS` is not supported.
