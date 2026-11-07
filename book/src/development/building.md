# Building from Source

LatticeDB is written in Zig with zero external dependencies.

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
zig build                      # Build everything
zig build lib                  # Build static library only
zig build cli                  # Build CLI tool only
zig build shared               # Build shared library for bindings
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

### Java

The Java bindings require JDK 21+, Maven, and a C compiler. Maven compiles the
JNI bridge and stages it next to the shared LatticeDB library:

```bash
# Build the shared library first
zig build

# Build and test the Java bindings
cd bindings/java
mvn test
```

Use `mvn compile exec:java@run-example` to run the bundled knowledge-graph
example after building the shared library.

## Project Structure

```text
src/
├── core/           # Core types and utilities
├── storage/        # B+Tree, page management, WAL
├── vector/         # HNSW index, vector operations
├── fts/            # Full-text search, tokenizer
├── query/          # Cypher parser, planner, executor
├── transaction/    # Transaction management, MVCC
├── concurrency/    # Locking, latches
├── api/            # C API bindings
└── cli/            # CLI tool

include/
└── lattice.h       # C API header

bindings/
├── python/         # Python bindings
├── typescript/     # TypeScript/Node.js bindings
├── go/             # Go bindings
└── java/           # Java/JNI bindings
```
