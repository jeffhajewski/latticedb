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

## Filtering Aggregated Results

Use `WITH` to filter on aggregated values:

```cypher
MATCH (p:Person)-[:KNOWS]->(f:Person)
WITH p, count(f) AS friend_count
WHERE friend_count > 5
RETURN p.name, friend_count
```
