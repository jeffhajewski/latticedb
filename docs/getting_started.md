# Getting Started

This guide is the shortest path from a fresh install to a working local graph database.

## Choose a Starting Point

Use this path if you want the fastest route to a working example:

| Path | Best for | Start here |
|------|----------|------------|
| CLI | importing data, running queries, inspecting files | [../examples/cli/README.md](../examples/cli/README.md) |
| Python | local apps, scripts, notebooks | [../bindings/python/README.md](../bindings/python/README.md) |
| TypeScript | Node.js services and tooling | [../bindings/typescript/README.md](../bindings/typescript/README.md) |
| Go | cgo-backed native client | [../bindings/go/README.md](../bindings/go/README.md) |

For a larger graph/vector/text demo, see [../examples/README.md](../examples/README.md).

## Five-Minute CLI Walkthrough

If you already installed the CLI, use `lattice` directly.

If you are working from a repository checkout instead, build it once and use the local binary:

```bash
zig build
export LATTICE=./zig-out/bin/lattice
```

If `lattice` is already on your `PATH`, you can use:

```bash
export LATTICE=lattice
```

Create a database:

```bash
$LATTICE create /tmp/lattice-quickstart.lattice
```

Import the sample graph from this repository:

```bash
$LATTICE import /tmp/lattice-quickstart.lattice --file=examples/cli/people_graph.json
```

Inspect counts:

```bash
$LATTICE count /tmp/lattice-quickstart.lattice
```

Run a query from a file:

```bash
$LATTICE exec /tmp/lattice-quickstart.lattice --file=examples/cli/authored_docs.cypher
```

Verify the main database file checksums:

```bash
$LATTICE check /tmp/lattice-quickstart.lattice
```

## Notes on Install Workflows

- Published Python wheels are expected to bundle `liblattice` on supported platforms.
- Published TypeScript package tarballs are expected to bundle `liblattice` on supported platforms.
- Go currently uses an installed-prefix or repo-local shared-library workflow rather than bundled native artifacts.

If you are developing from a source checkout, see the binding READMEs for staged-library and installed-prefix workflows.

## What to Read Next

- [../examples/cli/README.md](../examples/cli/README.md) for a copy-paste CLI example
- [../examples/README.md](../examples/README.md) for the larger research-paper retrieval demos
- [client_api_migration.md](client_api_migration.md) for current preferred client imports and compatibility aliases
- [../ARCHITECTURE_SPEC.md](../ARCHITECTURE_SPEC.md) if you want the engine-level design after the user-facing overview
