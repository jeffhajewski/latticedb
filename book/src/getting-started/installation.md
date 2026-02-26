# Installation

## CLI

```bash
curl -fsSL https://raw.githubusercontent.com/jeffhajewski/latticedb/main/dist/install.sh | bash
```

## Python

```bash
pip install latticedb
```

Requires Python 3.9+ and NumPy. The native shared library (`liblattice.dylib` / `liblattice.so`) must be available on the system.

## TypeScript / Node.js

```bash
npm install @hajewski/latticedb
```

Requires Node.js 18+. The native shared library must be available on the system.

## Building from Source

LatticeDB is written in Zig with zero dependencies.

```bash
git clone https://github.com/jeffhajewski/latticedb.git
cd latticedb
zig build                  # build everything
zig build test             # run tests
zig build -Doptimize=ReleaseFast   # optimized build
```

Build the shared library for language bindings:

```bash
zig build shared
```

This produces `liblattice.dylib` (macOS) or `liblattice.so` (Linux).

See [Building from Source](../development/building.md) for more details.
