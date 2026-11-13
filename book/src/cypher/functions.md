# Functions

LatticeDB supports the following built-in functions in Cypher expressions.
Function names are not case-sensitive: `toLower`, `tolower` and `TOLOWER` are the
same function. Aggregates are covered in [Aggregations](./aggregations.md).

Every function's name and number of arguments are checked before the query
runs, so a misspelt name or a wrong argument count fails the same way whether the
query would match rows or not:

```text
$ lattice exec db.lattice --query="MATCH (n:Nope) RETURN lenght(n)"
Error: Semantic error (unknown_function) at 1:23: Unknown function 'lenght'
  1 | MATCH (n:Nope) RETURN lenght(n)
    |                       ^^^^^^
$ lattice exec db.lattice --query="RETURN toLower('a', 'b')"
Error: Semantic error (wrong_argument_count) at 1:8: toLower takes (string), but was given 2 arguments
```

An argument of the wrong type is found when the call is evaluated, and fails the
query with a message naming what the function takes:

```text
$ lattice exec db.lattice --query="RETURN toLower(1)"
Error: Execution error (TypeError) at 1:8: Type error: toLower(integer) is not defined; toLower takes (string)
```

## id()

Returns the internal ID of a node or relationship:

```cypher
MATCH (n:Person)
RETURN id(n), n.name
```

A `WHERE` right after the `MATCH` that names the node by id, `id(n) = 42`,
`id(n) = $id` or `id(n) IN [1, 2, 3]` (alone or joined to other conditions
by `AND`), reads those nodes directly instead of scanning every node or
every node with the label, and expands from there:

```cypher
MATCH (a:Person)-[:KNOWS]->(b)-[:KNOWS]->(c)
WHERE id(a) = 1
RETURN b.name, c.name
```

```text
┌─────────┬────────┐
│ b.name  │ c.name │
├─────────┼────────┤
│ "Bob"   │ "Alan" │
│ "Carol" │ "Alan" │
└─────────┴────────┘
```

The id must be written as a value, a parameter or a list of them; a
condition such as `id(n) = m.ref` that depends on another node is checked
row by row.

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

Returns the length of a list, or the number of characters (Unicode code points)
in a string (`size('Müller')` is 6):

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

A float is truncated toward zero (`toInteger(3.9)` is 3). A float outside the
64-bit integer range, NaN or an infinity fails the query. A string that is not an
integer gives null.

## toFloat()

Converts a value to a float:

```cypher
MATCH (n:Person)
RETURN n.name, toFloat(n.age) AS float_age
```

## Other functions

| Function | Returns |
|----------|---------|
| `toString(x)` | `x` as a string; null for a list or map |
| `toLower(s)` / `toUpper(s)` | `s` in lower or upper case (see [Strings](#strings)) |
| `trim(s)` | `s` without leading and trailing ASCII whitespace |
| `substring(s, start[, length])` | Part of `s`, from a zero-based `start`, both counted in characters; a negative `start` counts as 0 |
| `replace(s, search, replacement)` | `s` with every `search` replaced; an empty `search` returns `s` |
| `split(s, delimiter)` | A list of the parts of `s`; an empty delimiter splits `s` into characters |
| `head(list)` / `last(list)` | The first or last element; null for an empty list |
| `tail(list)` | Every element but the first |
| `range(start, end[, step])` | A list of integers from `start` to `end`, inclusive; a step of 0, or more than ten million elements, fails the query |
| `type(r)` | A relationship's type |
| `labels(n)` | A node's labels, as a list |
| `properties(x)` | A node's or relationship's properties, as a map; a map is returned as it is |
| `exists(x.prop)` | Whether the property is present: the same as `x.prop IS NOT NULL` |
| `length(p)` | The number of relationships in a named path |
| `nodes(p)` / `relationships(p)` | The nodes or the relationships of a named path, as a list; see [Variable-Length Paths](./variable-length-paths.md) |
| `score(d)` | The BM25 score of the full-text search that matched `d`; see [Full-Text Search](./full-text-search.md#reading-the-score) |

`IN` tests list membership (`n.age IN [30, 40]`). `=~` matches a regular
expression; see [WHERE](./where.md#regular-expressions). Lists and maps compare
by value: `[1, 2] = [1, 2]` is true.

## Strings

`size`, `substring` and `split` work in characters (Unicode code points), not
bytes, so they never cut a character in half. A byte that is not valid UTF-8 counts
as one character.

```cypher
RETURN size('Müller')           // 6
RETURN substring('Ärger', 1, 3) // "rge"
RETURN split('añb', '')         // ["a", "ñ", "b"]
RETURN toUpper('müller straße') // "MÜLLER STRAßE"
RETURN toLower('ÄRGER ΣΟΦΙΑ')   // "ärger σοφια"
```

`toLower` and `toUpper` use Unicode's simple, one-to-one case mappings for Latin
(including Latin-1, Latin Extended-A, -B and Additional), Greek, Cyrillic,
Armenian and fullwidth ASCII letters; other characters are left as they are. Rules
that need the full mappings or the surrounding text are not applied: `ß` stays `ß`
in `toUpper` (not `SS`), a final `Σ` lowercases to `σ` (not `ς`), and there is no
Turkish or other language-specific casing. `trim` removes ASCII whitespace only.

## Arithmetic

Numeric operations promote integers to floats when mixed:

```cypher
RETURN 42 + 3.14   // Returns 45.14 (float)
RETURN 7 / 2       // Returns 3: integer division truncates
RETURN -7 % 2      // Returns -1: % takes the sign of the dividend
RETURN 7.5 % 2     // Returns 1.5
```

Integer `+`, `-`, `*`, `/`, unary minus and `abs()` fail the query when the result
does not fit in a 64-bit integer (`9223372036854775807 + 1`). Dividing by zero, or
taking `%` of zero, fails the query for integers and floats alike.
`-9223372036854775808` can be written as a literal.

```text
$ lattice exec db.lattice --query="RETURN 9223372036854775807 + 1"
Error: Execution error (IntegerOverflow) at 1:28: Integer overflow: 9223372036854775807 + 1 does not fit in a 64-bit integer
```

Comparisons between an integer and a float use their exact values, without
rounding the integer to a float first: `9007199254740993 = 9007199254740992.0` is
false and `9007199254740993 > 9007199254740992.0` is true, as `ORDER BY` already
ordered them. `1 = 1.0` and `-0.0 = 0` are true, and `DISTINCT` and grouping keep
to the same equality: `1` and `1.0` are one value, and so are `0` and `-0.0`.

## Null

A function given a null argument returns null (`toString(null)`, `size(null)`
and `substring(null, 1)` are null); `coalesce` exists to replace nulls. A null in
any argument wins over a wrong type in another, so `replace('abc', null, 1)` is
null, not a type error. A wrong type is otherwise a type error, except that
`toInteger`, `toFloat` and `toString` return null for a value they cannot
convert.

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
