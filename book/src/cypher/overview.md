# Cypher Overview

LatticeDB uses the Cypher query language for graph operations. Cypher is a declarative language where you describe patterns to match and operations to perform, rather than specifying how to execute them.

## Syntax at a Glance

Nodes are written in parentheses, edges in square brackets:

```cypher
-- Match a pattern
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
| String | `CONTAINS`, `STARTS WITH`, `ENDS WITH` |
| Null | `IS NULL`, `IS NOT NULL` |
| Search | `<=>` (vector distance), `@@` (full-text) |

### Functions

| Function | Description |
|----------|-------------|
| `id(node)` | Get node ID |
| `coalesce(a, b, ...)` | Return first non-null value |
| `abs(x)` | Absolute value |
| `size(list)` | List length |
| `toInteger(x)` | Convert to integer |
| `toFloat(x)` | Convert to float |

See [Functions](./functions.md) for details.

### Aggregations

| Function | Description |
|----------|-------------|
| `count(x)` | Count values |
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
