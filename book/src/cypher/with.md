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

In this example, `b` is not available after the `WITH` clause — only `a` and `friend_count` are.

## Multi-Stage Queries

Chain multiple `WITH` clauses for complex queries:

```cypher
MATCH (p:Person)-[:AUTHORED_BY]-(doc:Document)
WITH p, count(doc) AS docs
WITH p, docs WHERE docs > 2
RETURN p.name, docs
```
