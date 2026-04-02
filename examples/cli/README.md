# CLI Quickstart

This is the smallest end-to-end example for the `lattice` CLI.

It covers:

- creating a database
- importing graph data from JSON
- running a query from a `.cypher` file
- checking database-file checksums

## Files

- `people_graph.json` contains a small author/document graph
- `authored_docs.cypher` returns authored documents by person

## Run It

If `lattice` is already installed, use it directly.

From a repository checkout, build the CLI once and point an env var at the local binary:

```bash
zig build
export LATTICE=./zig-out/bin/lattice
```

Or, if `lattice` is already on your `PATH`:

```bash
export LATTICE=lattice
```

Create a fresh database:

```bash
$LATTICE create /tmp/lattice-cli-example.lattice
```

Import the sample graph:

```bash
$LATTICE import /tmp/lattice-cli-example.lattice --file=examples/cli/people_graph.json
```

Inspect counts:

```bash
$LATTICE count /tmp/lattice-cli-example.lattice
```

Run the example query:

```bash
$LATTICE exec /tmp/lattice-cli-example.lattice --file=examples/cli/authored_docs.cypher
```

Check the main database file:

```bash
$LATTICE check /tmp/lattice-cli-example.lattice
```

## Expected Shape

After import, the graph contains:

- 2 `Person` nodes
- 2 `Document` nodes
- 3 edges

The sample query returns authored documents with person names and titles.

`lattice check` validates the main database file page checksums. If a sibling `-wal` file exists, the CLI reports that it is present but not yet validated.
