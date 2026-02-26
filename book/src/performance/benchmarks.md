# Benchmarks

Benchmarked on Apple M1, single-threaded, with auto-scaled buffer pool.

## Core Operations

| Operation | Latency | Throughput | Target | Status |
|-----------|---------|------------|--------|--------|
| Node lookup | 0.13 us | 7.9M ops/sec | < 1 us | PASS |
| Node creation | 0.65 us | 1.5M ops/sec | — | — |
| Edge traversal | 9 us | 111K ops/sec | — | — |
| Full-text search (100 docs) | 19 us | 53K ops/sec | — | — |
| 10-NN vector search (1M vectors) | 0.83 ms | 1.2K ops/sec | < 10 ms @ 1M | PASS |

## Vector Search (HNSW) at Scale

128-dimensional cosine vectors, M=16, ef_construction=200, ef_search=64, k=10.

| Scale | Mean Latency | P99 Latency | Recall@10 | Memory |
|-------|-------------|-------------|-----------|--------|
| 1,000 | 65 us | 70 us | 100% | 1 MB |
| 10,000 | 174 us | 695 us | 99% | 10 MB |
| 100,000 | 438 us | 1.2 ms | 99% | 101 MB |
| 1,000,000 | 832 us | 1.8 ms | 100% | 1,040 MB |

Search latency scales sub-linearly (O(log N)) with 99-100% recall@10. Uses heuristic neighbor selection (HNSW paper Algorithm 4) for diverse graph connectivity, connection page packing for ~4.5x memory reduction, and pre-normalized dot product for fast cosine distance.

### ef_search Sensitivity (1M vectors)

| ef_search | Mean Latency | Recall@10 |
|-----------|-------------|-----------|
| 16 | 506 us | 57% |
| 32 | 1.9 ms | 79% |
| 64 | 990 us | 100% |
| 128 | 3.2 ms | 100% |
| 256 | 11.6 ms | 100% |

## Optimization History

### Baseline (pre-optimization)

| Scale | Insert Rate | Search Mean | Recall@10 |
|-------|------------|-------------|-----------|
| 1K | ~91/sec | 1.7ms | 100% |
| 10K | ~42/sec | 3.8ms | 99% |
| 100K | ~23/sec | 4.5ms | 99% |
| 1M | ~14/sec | 6.4ms | 100% |

### Post-optimization (Phase 2)

Six optimizations applied: last_page tracking, pre-sized search structures, stack-buffer connection I/O, cached vectors in heuristic pruning, pre-normalize + dot product for cosine.

| Scale | Insert Rate | Search Mean | Recall@10 |
|-------|------------|-------------|-----------|
| 1K | ~954/sec | 65us | 100% |
| 10K | ~726/sec | 174us | 99% |
| 100K | ~526/sec | 438us | 99% |
| 1M | ~248/sec | 832us | 100% |

### Improvement Summary

| Scale | Insert Speedup | Search Speedup |
|-------|----------------|----------------|
| 1K | 10.5x | 26x |
| 10K | 17.5x | 22x |
| 100K | 22.8x | 10x |
| 1M | 17.7x | 7.7x |

## Reproducing

```bash
zig build benchmark                        # Core operation benchmarks
zig build vector-benchmark -- --quick      # Vector benchmarks (1K/10K/100K, ~7 min)
zig build vector-benchmark                 # Full vector benchmarks including 1M (~70 min)
zig build graph-benchmark -- --quick       # Graph traversal benchmarks
```
