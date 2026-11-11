# Building from Source

LatticeDB is written in Zig with no external dependencies beyond the C library it
links. (The SQLite comparison benchmarks also need the system `sqlite3` headers.)

## Prerequisites

- [Zig](https://ziglang.org/download/) 0.16.0 (the version used by CI and release workflows)

## Clone and Build

```bash
git clone https://github.com/jeffhajewski/latticedb.git
cd latticedb
zig build                  # build everything
```

## Build Targets

```bash
zig build                      # Build and install everything: static and shared library, CLI, header, lattice.pc
zig build lib                  # Compile the static library only (installs nothing to zig-out)
zig build cli                  # Build and install the CLI only
zig build shared               # Build and install the shared library, header and lattice.pc for bindings
```

## Optimized Builds

```bash
zig build -Doptimize=ReleaseSafe   # Release build with safety checks
zig build -Doptimize=ReleaseFast   # Optimized release build
```

## Building Language Bindings

### Python

```bash
# Build the shared library first
zig build shared

# The Python bindings use ctypes to load the shared library
cd bindings/python
pip install -e .
```

### TypeScript

```bash
# Build the shared library first
zig build shared

# Build the TypeScript bindings
cd bindings/typescript
npm install
npm run build
```

### Go

```bash
zig build shared
cd bindings/go
go test -tags repolocal ./...   # repolocal links against include/ and zig-out/lib
```

### Java

```bash
zig build shared
cd bindings/java
mvn test                        # JDK 21+ and Maven; compiles the JNI bridge
```

## Project Structure

```text
src/
├── core/           # Core types and utilities
├── storage/        # B+Tree, page management, WAL
├── vector/         # HNSW index, vector operations
├── fts/            # Full-text search, tokenizer
├── graph/          # Node, edge, label and property-index storage
├── query/          # Cypher parser, planner, executor
├── stream/         # Durable streams and the graph changefeed
├── transaction/    # Transaction manager, WAL payloads
├── concurrency/    # Locking, latches
├── api/            # C API bindings
└── cli/            # CLI tool

include/
└── lattice.h       # C API header

bindings/
├── python/         # Python bindings
├── typescript/     # TypeScript/Node.js bindings
├── go/             # Go bindings
└── java/           # Java bindings (JNI)

conformance/
└── go/             # Engine conformance suite, run against the C API
```
