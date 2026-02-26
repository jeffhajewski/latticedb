# Variable-Length Paths

Variable-length path patterns match paths of varying depth in a single query.

## Syntax

```cypher
MATCH (a)-[*min..max]->(b)
```

- `*1..3` — match paths of length 1 to 3
- `*2` — match paths of exactly length 2
- `*2..` — match paths of length 2 or more
- `*` — match paths of any length (at least 1)

## Examples

### Fixed Range

```cypher
-- Friends of friends (exactly 2 hops)
MATCH (a:Person {name: "Alice"})-[:KNOWS*2]->(b:Person)
RETURN b.name
```

### Bounded Range

```cypher
-- Reachable within 1 to 3 hops
MATCH (a:Person {name: "Alice"})-[:KNOWS*1..3]->(b:Person)
RETURN DISTINCT b.name
```

### Open-Ended

```cypher
-- All reachable nodes (2+ hops)
MATCH (a:Person {name: "Alice"})-[*2..]->(b)
RETURN b
```

### Any Length

```cypher
-- All nodes reachable at any depth
MATCH (a:Person {name: "Alice"})-[*]->(b)
RETURN DISTINCT b
```

## Performance

Variable-length paths are implemented using BFS with bitset-based visited tracking. Performance characteristics:

- Short paths (1-3 hops): microseconds
- Deep traversals scale linearly with the number of reachable nodes
- `DISTINCT` is recommended to avoid duplicate results from multiple paths to the same node

At 10K nodes with 50K edges, a variable path `*1..5` completes in ~82 us. See [Benchmarks](../performance/benchmarks.md) for detailed numbers.
