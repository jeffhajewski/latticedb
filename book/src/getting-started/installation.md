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

## Java

The Java bindings are built from a source checkout. They require JDK 21+, Maven,
Zig, and a C compiler. Build the native library, then build and test the JNI
bridge:

```bash
git clone https://github.com/jeffhajewski/latticedb.git
cd latticedb
zig build
cd bindings/java
mvn test
```

Maven compiles the JNI bridge and stages it with `liblattice` for the tests. See
the [Java bindings README](https://github.com/jeffhajewski/latticedb/tree/main/bindings/java)
for configuration and example commands.

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
