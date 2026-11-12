# Aggregations

Aggregation functions compute summary values across groups of rows.

Every aggregate except `count(*)` skips nulls: `count(n.email)` counts the people
who have an email, `avg` divides by the number of non-null values, and `collect`
leaves nulls out of its list.

## Functions

### count

Count the number of values:

```cypher
MATCH (n:Person) RETURN count(n) AS total
```

`count(*)` counts rows, nulls included, and its column is named `count(*)`. With
nothing before it, `RETURN count(*)` returns 1. `count(DISTINCT x)` counts distinct
non-null values.

Grouping and `DISTINCT` treat values that `=` calls equal as one value: `1` and
`1.0` fall into one group, and so do `0` and `-0.0`. Every NaN is one value, though
`=` never matches it.

Count with grouping:

```cypher
MATCH (doc:Document)-[:AUTHORED_BY]->(p:Person)
RETURN p.name, count(doc) AS papers
```

### sum

Sum numeric values:

```cypher
MATCH (o:Order)
RETURN sum(o.amount) AS total_revenue
```

A sum of integers is an integer, computed exactly; if it does not fit in 64 bits
the query fails. If any value is a float, the sum is a float. The sum of no values
is 0. A value that is not a number fails the query with a type error, for `sum`
and for `avg`.

### avg

Average numeric values:

```cypher
MATCH (n:Person)
RETURN avg(n.age) AS average_age
```

`avg` always returns a float, and null when there are no values.

### min / max

Find minimum or maximum values:

```cypher
MATCH (n:Person)
RETURN min(n.age) AS youngest, max(n.age) AS oldest
```

Values of different types are compared the way `ORDER BY` orders them, so
`min` over `1`, `'a'` and `true` is `'a'` and `max` is `1`, and an integer and
a float compare exactly.

### collect

Collect values into a list:

```cypher
MATCH (p:Person)-[:KNOWS]->(f:Person)
RETURN p.name, collect(f.name) AS friends
```

## Grouping

Non-aggregated columns in `RETURN` define the grouping:

```cypher
MATCH (doc:Document)-[:AUTHORED_BY]->(p:Person)
RETURN p.name, count(doc) AS papers, collect(doc.title) AS titles
ORDER BY papers DESC
```

Here `p.name` is the grouping key, and `count(doc)` and `collect(doc.title)` are computed per group.

A `COUNT { }`, `EXISTS { }`, `COLLECT { }` or pattern comprehension beside an
aggregate may read the grouping keys: `RETURN p, count(f) + COUNT { (p)<--() }`
groups by `p` and counts for each group. A subquery that reads a variable that
is not a grouping key fails before the query runs, as `RETURN p.name, count(f) +
COUNT { (p)<--() }` does, since grouping by `p.name` leaves no single `p`.

Aggregates belong in `RETURN` and `WITH`. In `WHERE`, `SET`, `UNWIND`,
`FOREACH`, `MATCH` properties, `SKIP` or `LIMIT` there are no rows to
aggregate, and the query fails before it runs:

```text
collect() aggregates rows, which FOREACH cannot do; aggregate in a WITH first, as in WITH collect(n.v) AS vs
```

Aggregate in a `WITH` and use the result:

```cypher
MATCH (p:Person)
WITH collect(p.name) AS names
FOREACH (name IN names | CREATE (:Tag {name: name}))
```

## Filtering Aggregated Results

Use `WITH` to filter on aggregated values:

```cypher
MATCH (p:Person)-[:KNOWS]->(f:Person)
WITH p, count(f) AS friend_count
WHERE friend_count > 5
RETURN p.name, friend_count
```
