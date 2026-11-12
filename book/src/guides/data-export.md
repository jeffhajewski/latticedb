# Data Export

Use the CLI to export graph data from a `.lattice` database file, and `lattice
import` to load it into another.

## Basic Usage

```bash
lattice export <path-to-db> --file=<output-path>
```

Examples:

```bash
# Full graph as one JSON document
lattice export knowledge.db --file=backup.json

# One JSON object per line (stream-friendly)
lattice export knowledge.db --file=backup.jsonl

# Graphviz DOT (for visualization tools)
lattice export knowledge.db --file=graph.dot
```

JSON, JSONL and CSV exports can be read back with `lattice import`. DOT is for
looking at, not for importing.

An export that fails removes the file it was writing, so a file that exists is
complete.

## CSV Export

CSV export writes two files (nodes and edges), using the `--file` value as a base name:

```bash
lattice export knowledge.db --file=backup.csv
```

This generates:

- `backup_nodes.csv`, with `_id`, `_labels` (separated by `;`), a column per
  property, and `_vector` when the export includes vectors
- `backup_edges.csv`, with `_source`, `_target`, `_type` and a column per property

Property columns are named `<key>:json` and hold each value in the JSON
encoding described below, so every type reads back as itself. An empty cell
means the node or edge does not have that property. Fields are quoted as RFC
4180 describes when they need it:

```text
_id,_labels,age:json,name:json
1,"Person",29,"""Carol"""
2,"Person",35,"""Dan"""
```

To import the pair, give `lattice import` the same `--file` value; it reads both
files and connects the edges to the nodes through the `_id` column:

```bash
lattice import copy.db --file=backup.csv
```

A label that CSV cannot hold (empty, containing `;`, or with leading or trailing
spaces) makes a CSV export fail; use JSON for such a graph.

## Choosing What to Export

Use `--labels` to export only nodes with selected labels (comma-separated). Edges
between exported nodes are included.

```bash
lattice export knowledge.db --file=people.json --labels=Person
lattice export knowledge.db --file=content.dot --labels=Document,Chunk
```

Use `--query` to export what a Cypher query returns: every node and
relationship in its rows (including inside lists and maps), plus the endpoints
of each relationship, so the file imports on its own.

```bash
lattice export knowledge.db --file=knows.json --query="MATCH (a:Person)-[k:KNOWS]->(b:Person) RETURN k"
```

The query must return nodes or relationships themselves. One whose rows hold
only other values is refused:

```text
Error: Export failed: the query returned no nodes or relationships to export. Return them, for example MATCH (n:Person) RETURN n.
```

`--labels` and `--query` cannot be used together. `lattice dump` accepts both
options too.

## Vectors

When the database has a vector index, exports include each node's vector, and
JSON exports and dumps record the dimensions. No vector options are needed, because
the database file records its index:

```bash
lattice export embeddings.db --file=docs.json
```

```json
{"version":2,"vector_dimensions":3,"nodes":[{"id":"1","labels":["Doc"],"properties":{"title":"cafe"},"vector":[0.6,0.8,0.0]}],"edges":[]}
```

Under the cosine metric a vector is stored normalised, so the exported vector is
the unit-length one: a node given `[3, 4, 0]` exports as `[0.6,0.8,0.0]`. Under
the `euclidean` and `dot` metrics vectors are stored as given and export as
`[3.0,4.0,0.0]`. An export records the dimensions but not the metric or the HNSW
settings: create the database you import into with the ones you want.

A database last written by 0.16.0 whose dimensions cannot be read from its vector
pages is the one case where an export leaves vectors out, with a warning, unless
it is given `--enable-vector --vector-dims=<n>`.

Importing a file that holds vectors needs a target database with a vector index of
the same dimensions; a database without one can be given one by the import itself,
with `--enable-vector --vector-dims=<n>`. A JSON import checks this before writing
anything (here `copy.db` was created with four dimensions); JSONL and CSV
imports fail at the first node whose vector does not fit:

```text
Error: Import failed: the vectors in docs.json do not have the 4 dimensions copy.db was opened with.
```

## The JSON Format

JSON exports start with `"version": 2`. Values are written so their type
survives the round trip. Strings, integers, booleans, null, lists and maps are
plain JSON; floats always have a decimal point or an exponent (`2.0`, not `2`).
The types JSON has no notation for are written as an object with one `$` key:

| Value | Written as |
|-------|------------|
| Bytes | `{"$bytes": "3q2+7w=="}` (base64) |
| A vector-valued property | `{"$vector": [1.5, 2.5]}` |
| NaN, infinity | `{"$float": "NaN"}`, `{"$float": "Infinity"}`, `{"$float": "-Infinity"}` |
| A map whose only key starts with `$` | `{"$map": {"$ref": "x"}}` |

A node's own embedding is the separate `"vector"` field, not a property.

JSONL uses the same encoding, one node or edge per line with a `"kind"` field,
and no version or dimensions line.

### Files written by 0.16.0 and earlier

A JSON file with no `"version"` field is read as the old format, so exports
from earlier releases still import, with one difference: a string is always
imported as a string. Earlier releases exported bytes as hex strings, which
cannot be told apart from a string like `"cafe"`, so bytes in an old export come
back as strings. A numeric array in an old file is still read as a vector
property, as it was then. A file with a version newer than this release knows
is refused rather than misread.

## What Is Not Exported

Exports hold nodes, labels, properties, edges and, when asked for, vectors.
They do not hold full-text or property index definitions, or anything else
about how the database was configured, and every node gets a new ID when an
export is imported.

To copy a whole database, use `lattice backup` (see
[Backup and Replication](./backup-and-replication.md)) or `serialize()` from a
binding.

## Choosing a Format

- `.json`: single structured snapshot of the graph, for interchange.
- `.jsonl`: one record per line, for pipelines.
- `.csv`: tabular extraction for spreadsheets and BI tooling.
- `.dot`: graph visualization with Graphviz-compatible tooling.
