# Functions

LatticeDB supports the following built-in functions in Cypher expressions.

## id()

Returns the internal ID of a node:

```cypher
MATCH (n:Person)
RETURN id(n), n.name
```

## coalesce()

Returns the first non-null argument:

```cypher
MATCH (n:Person)
RETURN coalesce(n.nickname, n.name) AS display_name
```

## abs()

Returns the absolute value of a number:

```cypher
MATCH (n:Person)
RETURN n.name, abs(n.score) AS abs_score
```

## size()

Returns the length of a list:

```cypher
MATCH (p:Person)-[:KNOWS]->(f:Person)
WITH p, collect(f) AS friends
RETURN p.name, size(friends) AS friend_count
```

## toInteger()

Converts a value to an integer:

```cypher
MATCH (n:Person)
RETURN n.name, toInteger(n.score) AS int_score
```

## toFloat()

Converts a value to a float:

```cypher
MATCH (n:Person)
RETURN n.name, toFloat(n.age) AS float_age
```

## Type Coercion

Numeric operations automatically promote integers to floats when mixed:

```cypher
RETURN 42 + 3.14   -- Returns 45.14 (float)
```

Null propagates through most operations:

```cypher
RETURN null + 1     -- Returns null
RETURN null = null  -- Returns true (special case)
```
