# Parameters

Parameters allow you to pass values into queries safely using the `$name` syntax. This prevents injection attacks and enables query plan caching.

## Syntax

Use `$` followed by the parameter name in the query:

```cypher
MATCH (n:Person) WHERE n.name = $name RETURN n
MATCH (n:Person) WHERE n.age > $min_age RETURN n
```

## Python

```python
# String parameter
result = db.query(
    "MATCH (n:Person) WHERE n.name = $name RETURN n",
    parameters={"name": "Alice"},
)

# Numeric parameter
result = db.query(
    "MATCH (n:Person) WHERE n.age > $min_age RETURN n",
    parameters={"min_age": 25},
)

# Vector parameter
from latticedb import hash_embed

result = db.query(
    "MATCH (n) WHERE n.embedding <=> $vec < 0.5 RETURN n",
    parameters={"vec": hash_embed("query text", dimensions=128)},
)
```

## TypeScript

```typescript
// String parameter
const result = await db.query(
  "MATCH (n:Person) WHERE n.name = $name RETURN n",
  { name: "Alice" }
);

// Numeric parameter
const result = await db.query(
  "MATCH (n:Person) WHERE n.age > $min_age RETURN n",
  { min_age: 25 }
);

// Vector parameter
import { hashEmbed } from "@hajewski/latticedb";

const result = await db.query(
  "MATCH (n) WHERE n.embedding <=> $vec < 0.5 RETURN n",
  { vec: hashEmbed("query text", 128) }
);
```

## C

```c
lattice_query* query;
lattice_query_prepare(db, "MATCH (n) WHERE n.name = $name RETURN n", &query);

// Bind string parameter
lattice_value val = {
    .type = LATTICE_VALUE_STRING,
    .data.string_val = { "Alice", 5 }
};
lattice_query_bind(query, "name", &val);

// Bind vector parameter
float vec[128] = { /* ... */ };
lattice_query_bind_vector(query, "embedding", vec, 128);
```

## Supported Parameter Types

| Type | Python | TypeScript | C |
|------|--------|------------|---|
| String | `str` | `string` | `LATTICE_VALUE_STRING` |
| Integer | `int` | `number` | `LATTICE_VALUE_INT` |
| Float | `float` | `number` | `LATTICE_VALUE_FLOAT` |
| Boolean | `bool` | `boolean` | `LATTICE_VALUE_BOOL` |
| Null | `None` | `null` | `LATTICE_VALUE_NULL` |
| Vector | `numpy.ndarray` | `Float32Array` | `float*` (via `lattice_query_bind_vector`) |

## Query Caching

Parameterized queries are cached by their query text. The same query with different parameter values reuses the cached plan, improving performance for repeated queries.

```python
# These share the same cached plan:
db.query("MATCH (n) WHERE n.name = $name RETURN n", parameters={"name": "Alice"})
db.query("MATCH (n) WHERE n.name = $name RETURN n", parameters={"name": "Bob"})
```
