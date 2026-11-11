# Contributing

Contributions to LatticeDB are welcome.

## Getting Started

1. Fork the repository on GitHub
2. Clone your fork and create a branch
3. Make your changes
4. Run the test suite: `zig build test`
5. Submit a pull request

## Development Setup

```bash
git clone https://github.com/YOUR_USERNAME/latticedb.git
cd latticedb
zig build test    # Verify everything builds and passes
```

## Code Style

- Follow existing Zig conventions in the codebase
- All allocation goes through explicit allocator parameters
- Fail fast: detect and report corruption, don't hide it
- Keep the C API as the contract: all bindings wrap `include/lattice.h` (Java
  through a JNI bridge). If you change the C API, update every binding — Python,
  TypeScript, Go and Java — in the same branch.

## Testing

All changes should include appropriate tests:

- **Unit tests** for new functions or modules
- **Integration tests** for end-to-end behavior changes
- **Fuzz tests** for parser or serialization changes
- **Crash tests** for durability-related changes

Run the full test suite before submitting:

```bash
zig build test
zig build integration-test
```

[CONTRIBUTING.md](https://github.com/jeffhajewski/latticedb/blob/main/CONTRIBUTING.md)
in the repository has the full development setup.

## Pull Requests

- Keep PRs focused on a single change
- Include a clear description of what changed and why
- Ensure all tests pass
- Add tests for new functionality

## Reporting Issues

File issues on [GitHub](https://github.com/jeffhajewski/latticedb/issues) with:

- A clear description of the problem
- Steps to reproduce
- Expected vs actual behavior
- LatticeDB version and platform
