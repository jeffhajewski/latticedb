# Building from Source

LatticeDB is written in Zig with zero external dependencies.

## Prerequisites

- [Zig](https://ziglang.org/download/) (0.15.x or later)

## Clone and Build

```bash
git clone https://github.com/jeffhajewski/latticedb.git
cd latticedb
zig build                  # build everything
```

## Build Targets

```bash
zig build                      # Build everything
zig build library              # Build static library only
zig build cli                  # Build CLI tool only
zig build amalgamation         # Create single-file distribution
zig build shared               # Build shared library for bindings
```

## Optimized Builds

```bash
zig build release-safe         # Release build with safety checks
zig build release-fast         # Optimized release build
zig build -Doptimize=ReleaseFast   # Alternative optimized build
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

## Project Structure

```
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
└── typescript/     # TypeScript/Node.js bindings
```
