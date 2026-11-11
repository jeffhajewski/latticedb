# Functions

LatticeDB supports the following built-in functions in Cypher expressions.
Function names are case-sensitive (`toLower`, not `tolower`); aggregate names
such as `count` are not. Aggregates are covered in
[Aggregations](./aggregations.md).

## id()

Returns the internal ID of a node or relationship:

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

Returns the length of a list, or of a string:

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

## Other functions

| Function | Returns |
|----------|---------|
| `toString(x)` | `x` as a string |
| `toLower(s)` / `toUpper(s)` | `s` in lower or upper case |
| `trim(s)` | `s` without leading and trailing whitespace |
| `substring(s, start[, length])` | Part of `s`, from a zero-based `start` |
| `replace(s, search, replacement)` | `s` with every `search` replaced |
| `split(s, delimiter)` | A list of the parts of `s` |
| `head(list)` / `last(list)` | The first or last element |
| `tail(list)` | Every element but the first |
| `range(start, end[, step])` | A list of integers from `start` to `end`, inclusive |
| `type(r)` | A relationship's type |
| `labels(n)` | A node's labels, as a list |
| `properties(x)` | A node's or relationship's properties, as a map |

`IN` tests list membership (`n.age IN [30, 40]`). There is no regular-expression
operator (`=~`).

## Type Coercion

Numeric operations automatically promote integers to floats when mixed:

```cypher
RETURN 42 + 3.14   // Returns 45.14 (float)
```

Null propagates through comparisons, arithmetic and the string operators, and
`NOT`, `AND` and `OR` use three-valued logic; `IS NULL` and `IS NOT NULL` are always
true or false. A `WHERE` keeps a row only when its predicate is true, so
`WHERE n.x <> 5` skips nodes that have no `x`:

```cypher
RETURN null + 1       // Returns null
RETURN null = null    // Returns null
RETURN false AND null // Returns false
RETURN true OR null   // Returns true
RETURN NOT null       // Returns null
RETURN 2 IN [1, null] // Returns null
```
