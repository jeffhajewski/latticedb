# Cypher Overview

LatticeDB uses the Cypher query language for graph operations. Cypher is a declarative language where you describe patterns to match and operations to perform, rather than specifying how to execute them.

## Syntax at a Glance

Nodes are written in parentheses, edges in square brackets:

```cypher
// Match a pattern
MATCH (person:Person)-[:KNOWS]->(friend:Person)
WHERE person.name = "Alice"
RETURN friend.name
```

## Supported Clauses

| Clause | Purpose |
|--------|---------|
| [MATCH](./match.md) | Find patterns in the graph |
| [WHERE](./where.md) | Filter results with conditions |
| [RETURN](./return.md) | Specify output columns |
| [CREATE](./mutations.md) | Create nodes and edges |
| [SET](./mutations.md) | Update properties and labels |
| [DELETE](./mutations.md) | Remove nodes and edges |
| [REMOVE](./mutations.md) | Remove properties and labels |
| [MERGE](./merge.md) | Match or create a pattern |
| [WITH](./with.md) | Chain query parts, pipe results |
| [UNWIND](./unwind.md) | Expand lists into rows |
| ORDER BY | Sort results |
| LIMIT | Limit number of results |
| SKIP | Skip first N results |

A query may begin with `MATCH`, `CREATE`, `MERGE`, `UNWIND`, `WITH` or `RETURN`;
`RETURN 1 + 2` on its own returns one row.

## Not Supported

These parts of Cypher are not implemented. A query that uses one fails with a
parse error pointing at the first token it could not read:

- `OPTIONAL MATCH`
- `CASE`
- `UNION`
- `CALL`
- list indexing (`list[0]`)
- list comprehensions
- named paths (`p = (a)-->(b)`)
- `NULLS FIRST` / `NULLS LAST`

`exists()` is not a function either, and fails before the query runs as an
unknown function. The parse error names the clauses it expected rather than the
feature, so `CASE` in `RETURN CASE WHEN ...` is reported as unexpected text at
`WHEN`.

## Errors

A query that fails says where and why: the stage that refused it (parse,
semantic, plan or execution), a diagnostic code, a message, and the line and column
of the part of the query at fault. The CLI prints them with a marker under that
part:

```text
$ lattice exec db.lattice --query="RETURN 1/0"
Error: Execution error (DivisionByZero) at 1:9: Division by zero: 1 / 0
  1 | RETURN 1/0
    |         ^
$ lattice exec db.lattice --query="RETURN 1 LIMIT -1"
Error: Execution error (TypeError) at 1:16: SKIP and LIMIT take a non-negative integer, but this is -1
  1 | RETURN 1 LIMIT -1
    |                ^
$ lattice exec db.lattice --query="RETURN \$nope"
Error: Execution error (ParameterNotFound) at 1:8: Parameter $nope was not given a value
  1 | RETURN $nope
    |        ^^^^^
```

The bindings carry the same fields on their query error (in Python,
`LatticeQueryError` with `stage`, `diagnostic_code` and `location`), and the C API
returns them through `lattice_query_last_error_*`:

```python
from latticedb import LatticeQueryError

try:
    db.query("RETURN 1/0")
except LatticeQueryError as e:
    print(e, e.code, e.stage, e.diagnostic_code, e.location)
# Division by zero: 1 / 0 -1 execution DivisionByZero {'line': 1, 'column': 9, 'length': 1}
```

The error code tells what kind of failure it was:

| Failure | Code |
|---------|------|
| Parse or semantic error (syntax, unknown function, wrong argument count) | `LATTICE_ERROR_INVALID_ARG` (-6) |
| Plan or execution error in the query (division by zero, type error, overflow) | `LATTICE_ERROR` (-1) |
| `DELETE` of a node that still has relationships | `LATTICE_ERROR_HAS_RELATIONSHIPS` (-17), as from `lattice_node_delete` |
| A write to a node or relationship that does not exist, or was deleted earlier in the query | `LATTICE_ERROR_NOT_FOUND` (-4), as from `lattice_edge_create` |
| The storage underneath failed while the query read or wrote | `LATTICE_ERROR_IO` (-2) |
| Memory ran out | `LATTICE_ERROR_OUT_OF_MEMORY` (-13) |

A failure to read stored data fails the query. It is never read as null, so a
`WHERE` cannot quietly drop a row because a read failed. Null stands only for
what is really absent: a missing property, a deleted node or relationship, a
property name the database has never seen, or a parameter passed as null.

## LatticeDB Extensions

LatticeDB extends Cypher with two operators for search:

### Vector Distance (`<=>`)

Find nodes with similar embeddings:

```cypher
MATCH (chunk:Chunk)
WHERE chunk.embedding <=> $query_vector < 0.5
RETURN chunk.text
ORDER BY chunk.embedding <=> $query_vector
```

See [Vector Search](./vector-search.md) for details.

### Full-Text Search (`@@`)

Search indexed text content:

```cypher
MATCH (doc:Document)
WHERE doc.content @@ "neural networks"
RETURN doc.title
```

See [Full-Text Search](./full-text-search.md) for details.

## Expressions

### Operators

| Category | Operators |
|----------|-----------|
| Comparison | `=`, `<>`, `<`, `<=`, `>`, `>=` |
| Logical | `AND`, `OR`, `NOT`, `XOR` |
| Arithmetic | `+`, `-`, `*`, `/`, `%`, `^` |
| String | `CONTAINS`, `STARTS WITH`, `ENDS WITH`, `=~` (regular expression) |
| List | `IN` |
| Null | `IS NULL`, `IS NOT NULL` |
| Search | `<=>` (vector distance), `@@` (full-text) |

### Functions

| Function | Description |
|----------|-------------|
| `id(node)` | Get node ID |
| `coalesce(a, b, ...)` | Return first non-null value |
| `abs(x)` | Absolute value |
| `size(x)` | Length of a list, or characters in a string |
| `toInteger(x)` | Convert to integer |
| `toFloat(x)` | Convert to float |

Function names are not case-sensitive. See [Functions](./functions.md) for the
full list.

### Aggregations

| Function | Description |
|----------|-------------|
| `count(x)` | Count non-null values |
| `count(*)` | Count rows |
| `sum(x)` | Sum values |
| `avg(x)` | Average values |
| `min(x)` | Minimum value |
| `max(x)` | Maximum value |
| `collect(x)` | Collect into list |

See [Aggregations](./aggregations.md) for details.

## Parameters

Use `$name` syntax to pass values safely:

```cypher
MATCH (n:Person) WHERE n.name = $name RETURN n
```

See [Parameters](./parameters.md) for language-specific binding.
