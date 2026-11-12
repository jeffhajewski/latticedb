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
| OPTIONAL MATCH | Match a pattern, or keep the row with nulls when it is absent |
| UNION / UNION ALL | Combine the rows of queries with the same columns |
| FOREACH | Run `CREATE`, `SET`, `MERGE`, `DELETE` or `REMOVE` for each element of a list |
| [CALL { }](./with.md#subqueries-call--) | Run a subquery once for each row |
| CALL db.labels() | List the catalog: `db.labels()`, `db.relationshipTypes()`, `db.propertyKeys()`, `db.indexes()` |
| ORDER BY | Sort results, with `NULLS FIRST` or `NULLS LAST` |
| LIMIT | Limit number of results |
| SKIP | Skip first N results |

A query may begin with `MATCH`, `OPTIONAL MATCH`, `CREATE`, `MERGE`, `UNWIND`,
`WITH`, `RETURN`, `CALL` or `FOREACH`; `RETURN 1 + 2` on its own returns one row.

```cypher
MATCH (p:Person)
OPTIONAL MATCH (p)-[:MANAGES]->(r:Person)
RETURN p.name AS name, collect(r.name) AS reports
UNION
MATCH (t:Team) RETURN t.name AS name, [] AS reports
```

```cypher
MATCH (a:Person {name: "Alice"})
FOREACH (tag IN ["admin", "owner"] | CREATE (a)-[:HAS]->(:Tag {name: tag}))
```

A `CALL` of a catalog procedure that is the whole query returns its columns;
inside a larger query it names them with `YIELD`:

```cypher
CALL db.labels() YIELD label
RETURN label ORDER BY label
```

### Expressions beyond the basics

| Construct | Example |
|-----------|---------|
| `CASE` | `CASE WHEN n.age >= 18 THEN 'adult' ELSE 'minor' END`, `CASE n.kind WHEN 'a' THEN 1 END` |
| Indexing and slicing | `list[0]`, `list[-1]`, `list[1..3]`, `map['key']`, `n[$prop]` |
| List comprehension | `[x IN list WHERE x > 0 \| x * 2]` |
| List predicates | `all(x IN list WHERE x > 0)`, `any(...)`, `none(...)`, `single(...)` |
| `reduce` | `reduce(total = 0, x IN list \| total + x)` |
| Pattern predicate | `WHERE (n)-[:KNOWS]->()`, `WHERE NOT (n)-->()` |
| `EXISTS { }`, `COUNT { }`, `COLLECT { }` | `EXISTS { (n)-[:KNOWS]->(m) WHERE m.age > 30 }`, `COUNT { (n)--() }`, `COLLECT { MATCH (n)-->(m) RETURN m.name ORDER BY m.name }`; see [Subqueries in expressions](./with.md#subqueries-in-expressions-exists-count-collect) |
| Pattern comprehension | `[(n)-[:KNOWS]->(m) WHERE m.age > 30 \| m.name]` |
| Named path | `MATCH p = (a)-[:KNOWS*]->(b) RETURN length(p), nodes(p), relationships(p)` |
| Shortest path | `MATCH p = shortestPath((a)-[:KNOWS*]-(b))`; see [Shortest Paths](./variable-length-paths.md#shortest-paths) |
| Label test | `n:Person`, `n:Person:Admin`, `n:Person\|Robot`, `n:!Admin`; `r:KNOWS` tests a relationship's type; see [Label Expressions](./match.md#label-expressions) |
| `exists()` on a property | `exists(n.email)`, the same as `n.email IS NOT NULL` |
| Map projection | `n {.name, .age, friends: size(fs)}`, `n {.*}`; see [RETURN](./return.md) |
| `RETURN *`, `WITH *` | every variable in scope; see [RETURN](./return.md) |
| Literals | `0x1F`, `0o17`, `.5`, `` `a name with spaces` `` |

## Not Supported

These parts of Cypher are not implemented. Each fails before the query runs,
with an error that names it:

- quantified and parenthesised path patterns, such as `((a)-->(b)){1,3}` or
  `(x) ((a)-->(b))+`, and quantified relationships such as `-[:R]->{1,3}` or
  `-[:R]->+`; use a variable-length relationship `-[:R*1..3]->`
- `&`, `!` and `%` in a relationship's types, as in `-[:!KNOWS]->`; use
  `WHERE NOT r:KNOWS`
- `WHERE` inside a pattern, as in `(n WHERE n.age > 30)`
- writing inside `EXISTS { }`, `COUNT { }` or `COLLECT { }`
- `CALL { } IN TRANSACTIONS`, `OPTIONAL CALL`, and procedures other than the
  four catalog procedures above
- `LOAD CSV`; import files with `lattice import` or through the API
- type predicates (`x IS :: INTEGER`) and `||` for concatenation (use `+`)
- schema and administration statements: `CREATE INDEX`, `CREATE CONSTRAINT`,
  `DROP`, `SHOW`, `USE`, `EXPLAIN`, `PROFILE`, `GRANT` and the like. Indexes are
  created through the API.
- the GQL clauses `FINISH`, `FILTER`, `LET` and `INSERT`, and the legacy `START`
- temporal and spatial values and the functions on them (`date()`, `point()`)
- an integer with a leading zero, `012`: openCypher 9 read it as octal and other
  dialects as decimal, so it is refused with a message giving both; write `12`,
  or `0o12` for octal. Floats may start with zero (`01.5`).

The limits of `shortestPath` are listed under
[Shortest Paths](./variable-length-paths.md#shortest-paths).

## Errors

A query that fails says where and why: the stage that refused it (parse,
semantic, plan or execution), a diagnostic code, a message, and the line and column
of the part of the query at fault. A plan error has no line and column; its
message names the part instead, as in "ORDER BY item 2 aggregates, but the
RETURN does not return that aggregate". The CLI prints the place with a marker
under that part:

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
