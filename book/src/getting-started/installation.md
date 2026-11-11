# Installation

## CLI

On Linux and macOS:

```bash
curl -fsSL https://raw.githubusercontent.com/jeffhajewski/latticedb/main/dist/install.sh | bash
```

The script installs the `lattice` binary and the shared library. It does not run on
Windows; Windows users take the `.zip` archive from the
[GitHub releases](https://github.com/jeffhajewski/latticedb/releases) page, which
holds `lattice.exe` and `lattice.dll`.

## Python

```bash
pip install latticedb
```

Requires Python 3.9+ and NumPy. The published wheels bundle the native library
for Linux (x86_64, aarch64), macOS (x86_64, arm64) and Windows (x64, ARM64), so
nothing else needs installing.

## TypeScript / Node.js

```bash
npm install @hajewski/latticedb
```

Requires Node.js 18+. The published package bundles the native library for Linux
(x64, arm64), macOS (x64, arm64) and Windows (x64, arm64). Electron apps must unpack
it from `app.asar`; see the
[TypeScript README](https://github.com/jeffhajewski/latticedb/blob/main/bindings/typescript/README.md#electron).

## Go and Java

The Go binding needs the shared library, header and pkg-config file installed; see
[Go](../api/go.md). The Java binding is built from source; see [Java](../api/java.md).
Both are tested on Linux and macOS only.

## Platforms

CI runs the core test suites and the Python and TypeScript binding tests on
Linux, macOS and Windows x64. Windows ARM64 is built but not run.

## Building from Source

LatticeDB is written in Zig and needs no other dependencies (it links the C
library).

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

This produces `liblattice.dylib` (macOS) or `liblattice.so` (Linux) in
`zig-out/lib`, or `lattice.dll` in `zig-out/bin` (Windows).

See [Building from Source](../development/building.md) for more details.
