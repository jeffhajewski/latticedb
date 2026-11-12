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

### The relationships of the path

A variable on a variable-length relationship is the list of the relationships
the path went through, in the order it walked them; zero hops give an empty
list:

```cypher
MATCH (a:Person {name: "Alice"})-[r:KNOWS*1..3]->(b:Person)
WHERE all(k IN r WHERE k.since >= 2020)
RETURN b.name, size(r) AS hops, [k IN r | k.since] AS years
```

The variable is a list, not a relationship: `DELETE r` and `SET r.x = 1` are
errors, while `r[0]`, `size(r)`, comprehensions and quantifiers work on it.

An inline property map holds for every relationship of the path:
`-[:KNOWS*1..3 {since: 2020}]->` matches paths whose relationships all have
`since = 2020`. Paths are walked first and filtered after, so a selective map
on an unbounded pattern still costs as much as the walk without it.

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
