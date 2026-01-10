# Contributing to LatticeDB

This guide covers how to set up your development environment, build the project, and run tests.

## Prerequisites

### Required

- **Zig 0.15.x** - [Installation guide](https://ziglang.org/download/)
  ```bash
  # Verify installation
  zig version  # Should show 0.15.x
  ```

- **Python 3.9+** - For Python bindings
  ```bash
  python3 --version  # Should show 3.9 or higher
  ```

- **Node.js 18+** - For TypeScript bindings
  ```bash
  node --version  # Should show v18 or higher
  ```

### Optional

- **NumPy** - Required for vector operations in Python bindings
- **pytest** - Required for running Python tests

## Project Structure

```
latticedb/
├── src/                    # Zig source code
│   ├── api/               # C API implementation
│   ├── core/              # Core types and utilities
│   ├── storage/           # Storage engine (B+Tree, WAL, etc.)
│   ├── graph/             # Graph model (nodes, edges)
│   ├── query/             # Cypher parser and executor
│   ├── vector/            # HNSW vector index
│   ├── fts/               # Full-text search
│   └── cli/               # CLI tool
├── include/               # C header files
│   └── lattice.h
├── bindings/
│   ├── python/            # Python bindings
│   └── typescript/        # TypeScript/Node.js bindings
├── tests/
│   ├── unit/              # Zig unit tests
│   ├── integration/       # Integration tests
│   ├── fuzz/              # Fuzz tests
│   └── benchmark/         # Performance benchmarks
├── docs/                  # Implementation documentation
└── context/               # Architecture decisions
```

## Building

### Build Everything

```bash
zig build
```

This builds:
- `zig-out/lib/liblattice.a` - Static library
- `zig-out/bin/lattice` - CLI executable

### Build Specific Targets

```bash
zig build lib      # Static library only
zig build cli      # CLI executable only
```

### Build Shared Library (for Python bindings)

The Python bindings require a shared library:

```bash
zig build shared
```

This creates the shared library in `zig-out/lib/`:
- macOS: `liblattice.dylib`
- Linux: `liblattice.so`
- Windows: `lattice.dll`

### Release Builds

```bash
zig build -Doptimize=ReleaseSafe   # With safety checks
zig build -Doptimize=ReleaseFast   # Maximum performance
```

## Running Tests

### Zig Tests

Run all Zig unit tests:

```bash
zig build test
```

This runs:
- Library tests (tests within `src/`)
- Unit tests (tests in `tests/unit/`)

### Python Tests

1. **Install the Python package in development mode:**

   ```bash
   cd bindings/python
   pip install -e ".[dev]"
   ```

2. **Run unit tests** (don't require native library):

   ```bash
   pytest tests/test_basic.py -v
   ```

3. **Run integration tests** (require native library):

   First, build the shared library (see above), then:

   ```bash
   pytest tests/test_integration.py -v
   ```

   Integration tests are automatically skipped if the native library is not found.

4. **Run all Python tests:**

   ```bash
   pytest tests/ -v
   ```

### TypeScript Tests

1. **Build the shared library and TypeScript bindings:**

   ```bash
   zig build shared
   cd bindings/typescript
   npm install
   npm run build:all
   ```

2. **Run tests:**

   ```bash
   npm test
   ```

### Fuzz Tests

Run fuzz tests to verify robustness against malformed input:

```bash
zig build fuzz
```

For continuous fuzzing (longer runs):

```bash
zig build fuzz -- --fuzz
```

### Test Output

- Unit tests: Always run, test Python types and data structures
- Integration tests: Skipped if library unavailable, test full database operations

Example output when library is not built:
```
tests/test_basic.py: 13 passed
tests/test_integration.py: 21 skipped (Native library not found)
```

## Development Workflow

### Making Changes to Zig Code

1. Make your changes in `src/`
2. Run `zig build test` to verify tests pass
3. If changing the C API, update `include/lattice.h`

### Making Changes to Python Bindings

1. Make your changes in `bindings/python/src/lattice/`
2. Run `pytest tests/test_basic.py` for quick feedback
3. Build the shared library and run `pytest tests/` for full testing

### Making Changes to TypeScript Bindings

1. Make your changes in `bindings/typescript/src/`
2. Build with `npm run build:all`
3. Run tests with `npm test`

### Adding New C API Functions

1. Add the function declaration to `include/lattice.h`
2. Implement the function in `src/api/c_api.zig`
3. For Python: Add ctypes signature in `bindings/python/src/lattice/_bindings.py`
4. For Python: Add wrapper in `database.py` or `transaction.py`
5. For TypeScript: Add N-API wrapper in `bindings/typescript/src/addon.cpp`
6. For TypeScript: Add TypeScript wrapper in appropriate source file
7. Add tests for all bindings

## Code Style

### Zig

- Follow the [Zig Style Guide](https://ziglang.org/documentation/master/#Style-Guide)
- Use `zig fmt` to format code
- All public functions should have doc comments

### Python

- Follow PEP 8
- Use type hints for all public functions
- Run `mypy` for type checking: `mypy src/lattice/`

### TypeScript

- Use TypeScript strict mode
- Provide type definitions for all public APIs
- Format with Prettier

## Pull Request Process

1. Create a feature branch from `main`
2. Make your changes with clear, focused commits
3. Ensure all tests pass:
   - Zig: `zig build test`
   - Python: `pytest tests/`
   - TypeScript: `npm test` (in `bindings/typescript/`)
4. Update documentation if needed
5. Submit a PR with a clear description

## Troubleshooting

### "Native library not found" in Python tests

The shared library hasn't been built or isn't in the expected location.

1. Build the shared library (see [Build Shared Library](#build-shared-library-for-python-bindings))
2. Verify it exists: `ls zig-out/lib/liblattice.*`

### Zig build errors after updating Zig version

LatticeDB requires Zig 0.15.x. Check your version:

```bash
zig version
```

### Python import errors

Make sure the package is installed in development mode:

```bash
cd bindings/python
pip install -e .
```

## Getting Help

- Check existing issues on GitHub
- Open a new issue with a clear description and reproduction steps
