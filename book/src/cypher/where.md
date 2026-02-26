# WHERE and Filtering

`WHERE` filters matched patterns based on conditions.

## Comparison Operators

```cypher
MATCH (n:Person) WHERE n.age > 30 RETURN n.name
MATCH (n:Person) WHERE n.age >= 30 RETURN n.name
MATCH (n:Person) WHERE n.age < 30 RETURN n.name
MATCH (n:Person) WHERE n.age <= 30 RETURN n.name
MATCH (n:Person) WHERE n.name = "Alice" RETURN n
MATCH (n:Person) WHERE n.name <> "Alice" RETURN n
```

## Boolean Logic

Combine conditions with `AND`, `OR`, `NOT`, and `XOR`:

```cypher
MATCH (n:Person)
WHERE n.age > 25 AND n.field = "ML"
RETURN n.name

MATCH (n:Person)
WHERE n.field = "ML" OR n.field = "AI"
RETURN n.name

MATCH (n:Person)
WHERE NOT n.field = "Systems"
RETURN n.name
```

## Null Checks

```cypher
MATCH (n:Person) WHERE n.email IS NULL RETURN n.name
MATCH (n:Person) WHERE n.email IS NOT NULL RETURN n.name
```

## String Predicates

```cypher
MATCH (n:Person) WHERE n.name CONTAINS "li" RETURN n
MATCH (n:Person) WHERE n.name STARTS WITH "A" RETURN n
MATCH (n:Person) WHERE n.name ENDS WITH "ce" RETURN n
```

## Property Existence

Test whether a property exists by checking for null:

```cypher
MATCH (n:Person) WHERE n.email IS NOT NULL RETURN n
```

## Inline Property Matching

Properties in the MATCH pattern act as equality filters:

```cypher
-- These are equivalent:
MATCH (n:Person {name: "Alice"}) RETURN n
MATCH (n:Person) WHERE n.name = "Alice" RETURN n
```

## Vector Distance

Filter by vector similarity using `<=>`:

```cypher
MATCH (c:Chunk)
WHERE c.embedding <=> $query < 0.5
RETURN c.text
```

See [Vector Search](./vector-search.md).

## Full-Text Search

Filter by text content using `@@`:

```cypher
MATCH (d:Document)
WHERE d.content @@ "neural networks"
RETURN d.title
```

See [Full-Text Search](./full-text-search.md).
