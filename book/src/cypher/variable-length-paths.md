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

A type, or several separated by `|`, restricts every hop: `-[:KNOWS|LIKES*1..2]->`.

Each path is one result. A path uses each relationship at most once, but may pass
through a node more than once, including the one it started from. A node reached
by two different paths is therefore returned twice, which is why the examples
below use `DISTINCT` where they want each node once.

The relationships of a path also count toward the rest of its `MATCH`: no other
relationship of the same `MATCH`, single-hop or variable-length, can use one of them
again. See [Relationship Uniqueness](./match.md#relationship-uniqueness).

## Examples

### Fixed Range

```cypher
// Friends of friends (exactly 2 hops)
MATCH (a:Person {name: "Alice"})-[:KNOWS*2]->(b:Person)
RETURN b.name
```

### Bounded Range

```cypher
// Reachable within 1 to 3 hops
MATCH (a:Person {name: "Alice"})-[:KNOWS*1..3]->(b:Person)
RETURN DISTINCT b.name
```

### Open-Ended

```cypher
// All reachable nodes (2+ hops)
MATCH (a:Person {name: "Alice"})-[*2..]->(b)
RETURN b
```

### Any Length

```cypher
// All nodes reachable at any depth
MATCH (a:Person {name: "Alice"})-[*]->(b)
RETURN DISTINCT b
```

A variable-length relationship cannot be given a variable or inline properties:
`-[r:KNOWS*1..3]->` fails with the semantic error "Variable-length relationships
do not support edge variables", and `-[:KNOWS*1..3 {since: 2020}]->` is rejected
too.

If the far end is a node that is already bound, only paths that end at it are
returned.

## Performance

Paths are expanded depth-first, one path at a time. There is no hop limit: an
unbounded `*` or `*n..` ends because a path cannot use more relationships than
exist. The number of paths, though, can grow exponentially with the size of a
densely connected graph, so give unbounded patterns over large, well-connected
graphs an upper bound.

- Short paths (1-3 hops): microseconds
- The cost grows with the number of paths, not the number of reachable nodes
- `DISTINCT` removes the duplicate rows that several paths to one node produce

At 10K nodes with 50K edges, a variable path `*1..5` completes in ~82 us. See [Benchmarks](../performance/benchmarks.md) for detailed numbers.
