# Sample Graph Database

A generated research-citation graph, large enough to make traversal, aggregation,
and query plans behave like they do on real data: **1,072 nodes and 5,882 edges**.

The graph is produced by a seeded generator, so it is reproducible: the same
`--seed` always yields a byte-identical JSON file.

## Files

| File | What it is |
| --- | --- |
| `generate_graph.ts` | Deterministic generator; writes the import JSON |
| `sample_graph.json` | Generated graph in `lattice import` format (~1.1 MB, git-ignored) |
| `sample_graph.lattice` | The database built from that JSON (git-ignored) |
| `queries.cypher` | A ready-to-run example query |

## Build It

From the repository root, with the CLI already built (`zig build`):

```bash
node examples/sample-graph/generate_graph.ts
./zig-out/bin/lattice create examples/sample-graph/sample_graph.lattice
./zig-out/bin/lattice import examples/sample-graph/sample_graph.lattice \
  --file=examples/sample-graph/sample_graph.json --batch-size=500
./zig-out/bin/lattice checkpoint examples/sample-graph/sample_graph.lattice
./zig-out/bin/lattice count examples/sample-graph/sample_graph.lattice
```

The generator needs Node 22.18+ (it runs TypeScript directly, no build step) and
accepts `--out=<file>`, `--seed=<n>`, `--scale=<f>`, and `--pretty`. `--scale=10`
gives roughly ten times the nodes and edges; `--scale=0.1` a tenth.

## Shape

Nodes (1,072):

| Label | Count | Notable properties |
| --- | --- | --- |
| `Chunk` | 420 | `text`, `section`, `ordinal`, `token_count` |
| `Document` / `Paper` | 320 | `title`, `year`, `doi`, `abstract`, `peer_reviewed` |
| `Person` / `Researcher` | 240 | `name`, `field`, `h_index`, `started_year`, `is_faculty` |
| `Organization` | 36 | `name`, `kind`, `city`, `country`, `headcount` |
| `Topic` | 40 | `name`, `slug`, `field`, `maturity` |
| `Venue` | 16 | `name`, `kind`, `founded`, `acceptance_rate` |

Edges (5,882):

| Type | Count | From → To |
| --- | --- | --- |
| `COLLABORATES_WITH` | 1,522 | `Person` → `Person` |
| `AUTHORED` | 1,139 | `Person` → `Paper` |
| `CITES` | 1,107 | `Paper` → `Paper` (older) |
| `ABOUT` | 676 | `Paper` → `Topic` |
| `PART_OF` | 420 | `Chunk` → `Paper` |
| `MENTIONS` | 343 | `Chunk` → `Topic` |
| `PUBLISHED_IN` | 320 | `Paper` → `Venue` |
| `AFFILIATED_WITH` | 282 | `Person` → `Organization` |
| `RELATED_TO` | 73 | `Topic` → `Topic` |

Two distributions are deliberately skewed rather than uniform, because a
uniformly random graph makes every traversal cost the same and hides the
behavior worth testing:

- **Citations** follow preferential attachment damped by an exponential recency
  term, so a few papers accumulate most references (the top one is cited 64
  times), most references stay within a few years of the citing paper, and the
  tail is thin.
- **Authorship** is preferential too, giving both prolific authors and
  one-paper authors.
- Citations only ever point at a paper published in the same year or earlier,
  so the `CITES` subgraph is acyclic.

Two relationships are derived rather than invented, so the graph stays
internally consistent: a `COLLABORATES_WITH` edge exists between two people
exactly when they share a paper (`papers_together` counts how many), and
`CITES.is_self_citation` is true exactly when the two papers share an author
(103 of the 1,107 citations).

## Query It

```bash
./zig-out/bin/lattice exec examples/sample-graph/sample_graph.lattice \
  --file=examples/sample-graph/queries.cypher
```

Prolific authors with their institution:

```cypher
MATCH (o:Organization)<-[:AFFILIATED_WITH]-(p:Person)-[:AUTHORED]->(d:Paper)
RETURN p.name, o.name, count(d) AS papers
ORDER BY papers DESC
LIMIT 10
```

Two-hop citation neighborhood of one paper:

```cypher
MATCH (a:Paper)-[:CITES*1..2]->(b:Paper)
WHERE a.doi = '10.5555/lattice.2023.0120'
RETURN DISTINCT b.title
LIMIT 10
```

Chunks that belong to papers on a given topic:

```cypher
MATCH (c:Chunk)-[:PART_OF]->(d:Paper)-[:ABOUT]->(t:Topic)
WHERE t.name = 'hybrid search'
RETURN d.title, c.section, c.text
LIMIT 10
```

Interactive exploration:

```bash
./zig-out/bin/lattice query examples/sample-graph/sample_graph.lattice
```

## Vector and Full-Text Search

This database carries graph structure and properties only. Full-text indexes
and vector embeddings are not part of the CLI import format: a full-text index
has to be declared through the C API (`lattice_node_fts_index_create`, which
backfills existing nodes) or a binding, and vectors have to be written with
`set_vector` into a database created with `--enable-vector`. Until then, a
Cypher `@@` predicate over `Paper.abstract` reports `MissingFtsIndex`.

For a demo that combines all three search modes, see
[../README.md](../README.md) and the Python/TypeScript/Go retrieval examples.
