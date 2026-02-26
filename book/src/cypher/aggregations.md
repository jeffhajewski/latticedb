# Aggregations

Aggregation functions compute summary values across groups of rows.

## Functions

### count

Count the number of values:

```cypher
MATCH (n:Person) RETURN count(n) AS total
```

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

### avg

Average numeric values:

```cypher
MATCH (n:Person)
RETURN avg(n.age) AS average_age
```

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
