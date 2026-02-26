# Running Tests

LatticeDB has a comprehensive test suite covering unit tests, integration tests, concurrency tests, crash recovery tests, and benchmarks.

## Test Commands

```bash
zig build test                 # Run unit tests
zig build integration-test     # Run integration tests
zig build concurrency-test     # Run concurrency tests
zig build crash-test           # Run crash recovery tests
```

## Benchmarks

```bash
zig build benchmark                        # Core operation benchmarks
zig build vector-benchmark -- --quick      # Vector benchmarks (1K/10K/100K, ~7 min)
zig build vector-benchmark                 # Full vector benchmarks including 1M (~70 min)
zig build graph-benchmark -- --quick       # Graph traversal benchmarks
```

## Test Structure

```
tests/
├── unit/           # Unit tests for individual modules
├── integration/    # End-to-end integration tests
├── fuzz/           # Fuzzing targets for parser and serialization
├── crash/          # Crash recovery tests (kill process mid-transaction)
├── concurrency/    # Multi-threaded concurrency tests
└── performance/    # Performance benchmarks
```

## Testing Standards

- Aim for 100% branch coverage on core modules
- Fuzzing is mandatory for the parser and serialization code
- Crash recovery is tested by killing the process mid-transaction and verifying data integrity
- Concurrency tests cover all multi-threaded code paths

## TypeScript Binding Tests

```bash
cd bindings/typescript
npm test
```

## Python Binding Tests

```bash
cd bindings/python
pytest
```
