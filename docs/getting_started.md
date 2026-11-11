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
| Java | JVM applications using JNI | [../bindings/java/README.md](../bindings/java/README.md) |

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

- The CLI installer (`dist/install.sh`) runs on Linux and macOS. On Windows, take the
  `.zip` archive from the GitHub releases page, which holds `lattice.exe` and
  `lattice.dll`.
- Published Python wheels bundle `liblattice` for Linux (x86_64, aarch64), macOS
  (x86_64, arm64) and Windows (x64, ARM64).
- The published TypeScript package bundles `liblattice` for the same six platforms.
  Electron apps must unpack it from `app.asar`; see the TypeScript README.
- Python and TypeScript are tested on Linux, macOS and Windows x64. Windows ARM64
  is built but not run, and Go and Java are tested on Linux and macOS only.
- Go currently uses an installed-prefix or repo-local shared-library workflow rather than bundled native artifacts.
- Java currently compiles its JNI bridge and stages `liblattice` from a source checkout during the Maven build.

If you are developing from a source checkout, see the binding READMEs for staged-library and installed-prefix workflows.

## What to Read Next

- [../examples/cli/README.md](../examples/cli/README.md) for a copy-paste CLI example
- [../examples/README.md](../examples/README.md) for the larger research-paper retrieval demos
- [14_durable_streams.md](14_durable_streams.md) for durable streams and the graph changefeed
- [client_api_migration.md](client_api_migration.md) for current preferred client imports and compatibility aliases
- [00_introduction.md](00_introduction.md) if you want the engine-level design after the user-facing overview
