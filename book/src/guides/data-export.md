# Data Export

Use the CLI to export graph data from a `.lattice` database file.

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

## CSV Export

CSV export writes two files (nodes and edges), using the `--file` value as a base name:

```bash
lattice export knowledge.db --file=backup.csv
```

This generates:

- `backup_nodes.csv`
- `backup_edges.csv`

## Label Filtering

Use `--labels` to export only nodes with selected labels (comma-separated). Matching edges between exported nodes are included.

```bash
lattice export knowledge.db --file=people.json --labels=Person
lattice export knowledge.db --file=content.dot --labels=Document,Chunk
```

## Choosing a Format

- `.json`: single structured snapshot for backups and interchange.
- `.jsonl`: append/stream-friendly format for pipelines.
- `.csv`: tabular extraction for spreadsheets and BI tooling.
- `.dot`: graph visualization with Graphviz-compatible tooling.
