# LatticeDB Go Bindings

Go bindings for [LatticeDB](https://github.com/jeffhajewski/latticedb), an embedded single-file property-graph database with native vector and BM25 full-text search.

Migration note: embedding helpers now live in the dedicated Go embedding subpackage, and explicit transaction entrypoints now prefer `BeginRead()` / `BeginWrite()`. See [../../docs/client_api_migration.md](../../docs/client_api_migration.md) for the preferred API names and deprecated compatibility aliases.

## Status

This is a cgo-backed client built on the stable C ABI. The current API supports:

- database open/close
- read/write transactions
- nodes, edges, and nested property values
- explicit node and edge property indexes with equality lookup
- Cypher queries with nested parameters and materialized results
- vector storage and search
- full-text search and fuzzy search
- bulk insertion of vector-bearing nodes
- durable streams and graph changefeeds
- built-in hash embeddings
- HTTP embedding client

CI builds and tests the Go binding on Linux and macOS; it has not been tested on Windows.

The Go client now supports two native-library workflows:

- installed development: default, resolves `lattice` through `pkg-config`
- repo-local development: use the `repolocal` build tag to link against `zig-out/lib` in this repository

## Prerequisites

Use Go 1.22 or newer.

## Installed Workflow

This is the default workflow for published-module consumers.

Install the shared library, public header, and `pkg-config` metadata into a prefix:

```bash
zig build install --prefix /tmp/lattice-install
```

Then point `pkg-config` at that prefix and run Go commands from `bindings/go`:

```bash
export PKG_CONFIG_PATH=/tmp/lattice-install/lib/pkgconfig
export DYLD_LIBRARY_PATH=/tmp/lattice-install/lib
cd bindings/go
go test ./...
```

On Linux, use `LD_LIBRARY_PATH` instead of `DYLD_LIBRARY_PATH`.

The default cgo path expects `pkg-config` to resolve `lattice.h` and `liblattice`.

## Repo-Local Workflow

For development inside this repository, you can still link directly against `zig-out/lib`.

Build the native shared library from the repo root:

```bash
zig build shared
```

Then run the Go binding with the explicit `repolocal` build tag:

```bash
cd bindings/go
go test -tags repolocal ./...
```

CI validates both workflows:

- installed-prefix testing with the default `pkg-config` path
- repo-local testing against `zig-out/lib` via `-tags repolocal`

## Quick Start

```go
package main

import (
	"fmt"
	"log"

	latticedb "github.com/jeffhajewski/latticedb/bindings/go"
)

func main() {
	db, err := latticedb.Open("knowledge.db", latticedb.OpenOptions{
		Create:           true,
		EnableVectors:    true,
		VectorDimensions: 4,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := db.CreateNodeFTSIndex("Person", "bio"); err != nil {
		log.Fatal(err)
	}

	err = db.Update(func(tx *latticedb.Tx) error {
		alice, err := tx.CreateNode(latticedb.CreateNodeOptions{
			Labels: []string{"Person"},
			Properties: map[string]latticedb.Value{
				"name": "Alice",
				"profile": map[string]latticedb.Value{
					"skills": []latticedb.Value{"graph", "search"},
					"active": true,
				},
			},
		})
		if err != nil {
			return err
		}

		if err := tx.SetVector(alice.ID, "embedding", []float32{1, 0, 0, 0}); err != nil {
			return err
		}
		return tx.SetProperty(alice.ID, "bio", "Alice works on graph retrieval systems")
	})
	if err != nil {
		log.Fatal(err)
	}

	result, err := db.Query(
		"MATCH (n:Person) RETURN n.name AS name, $meta AS meta",
		map[string]latticedb.Value{
			"meta": map[string]latticedb.Value{"source": "go"},
		},
	)
	if err != nil {
		log.Fatal(err)
	}

	for _, row := range result.Rows {
		fmt.Println(row["name"], row["meta"])
	}

	neighbors, err := db.VectorSearch([]float32{1, 0, 0, 0}, latticedb.VectorSearchOptions{K: 3})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("vector results:", neighbors)

	fts, err := db.FTSSearch("Person", "bio", "graph retrieval", latticedb.FTSSearchOptions{Limit: 5})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("fts results:", fts)
}
```

For a larger end-to-end example, see [examples/go](../../examples/go).

## API Notes

- Prefer `BeginRead()` and `BeginWrite()` for manual transaction control. `Begin(readOnly bool)` remains as a deprecated compatibility alias.
- Property getters return `(value, ok, error)` so stored `NULL` is distinguishable from a missing property.
- Nested values use ordinary Go shapes:
  - `nil`
  - `bool`
  - `int64`
  - `float64`
  - `string`
  - `[]byte`
  - `[]float32`
  - `[]any`
  - `map[string]any`
- Query results are materialized into `[]map[string]Value`.
- A node has one vector, which Cypher reads as `n.embedding`. `Tx.SetVector` takes a key that must be `"embedding"` (or `""`); any other key returns `ErrorUnsupported`, and a vector with NaN or infinite components returns `ErrorInvalidArg`.
- `DB.Update` (and `DB.Query` for a query that writes) rolls the transaction back when its commit fails, so a failed commit does not keep the single writer slot.
- `Tx.Commit` that fails with `ErrorTxnAborted` ends the transaction: the native handle is gone, so `IsActive` is false and `Rollback` is a no-op. Any other commit failure leaves it open for `Rollback`.
- `Tx.DeleteNode` refuses a node that still has relationships with `ErrorHasRelationships` and deletes nothing, as Cypher's `DELETE` does; `Tx.DetachDeleteNode` deletes the node and all its relationships, as `DETACH DELETE` does. Deleting a node that does not exist succeeds. `Tx.CreateEdge` returns `ErrorNotFound` if either end does not exist as the transaction sees it.
- A database file records its vector index (dimensions, metric, `VectorM`, `VectorEfConstruction`), and a database that has one opens with it whether or not `EnableVectors` is set, so reopening needs no vector options. Any of `VectorMetric`, `VectorM` and `VectorEfConstruction` that is set, and `VectorDimensions` when `EnableVectors` is set (it defaults to 128), must match the file or `Open` returns `ErrorConfigMismatch`. `VectorMetricCosine` stores vectors at unit length and returns cosine distance; `VectorMetricEuclidean` returns the L2 distance and `VectorMetricDotProduct` the negative dot product, both over vectors stored as given. Results come smallest first.
- `OpenOptions.SyncMode` chooses when a commit waits for the disk: `SyncModeFull` syncs the log on every commit; `SyncModeNormal` returns once the operating system has the commit, which survives the process being killed but not necessarily a power cut or OS crash. `FullFsync: FullFsyncOn` asks macOS drives to flush their cache. The zero values defer to the `LATTICE_SYNC_MODE` and `LATTICE_FULL_FSYNC` environment variables, else full sync without `F_FULLFSYNC`. These options need a native library with `lattice_open_v5`, which `Open` always calls.
- `Deserialize` takes the same `OpenOptions` as `Open`, through `lattice_deserialize_v5`, and they mean the same, apart from `Create` and `ReadOnly`, which it ignores. Bytes that hold a vector index open with the configuration they record, and any vector option you set must match it (`ErrorConfigMismatch`); `EnableVectors` with the vector options creates and configures an index in bytes that have none.
- Create explicit equality indexes with `DB.CreateNodePropertyIndex` or
  `DB.CreateEdgePropertyIndex`, then query them from an active transaction with
  `Tx.FindNodesByLabelProperty` or `Tx.FindEdgesByTypeProperty`. Lookup limits
  must be positive, and a missing index returns `ErrorUnsupported` rather than
  falling back to a scan.
- A file that a newer release has marked as not writable by this one opens with `ReadOnly: true`; a writable `Open` returns `ErrorNeedsNewerWriter` and writes nothing. A file in a format too new for this release to read at all returns `ErrorVersionMismatch`, read-only or not.
- `ReadOnly: true` on a database that was not closed cleanly recovers it in memory: the handle sees every committed transaction and nothing is written to the files. A recovery that needs more than 64 MB plus four times the size of the log and journal fails with `ErrorFull`; open the database read-write once to recover it on disk.
- A failed query returns a `*QueryError` with `Code`, `Stage`, `Message`, `DiagnosticCode` and `Location` (nil when the failure does not belong to one place in the query); `RETURN 1/0` gives `Division by zero: 1 / 0 (DivisionByZero)` at line 1, column 9. Parse and semantic failures have `ErrorInvalidArg` and execution failures `ErrorGeneric`, except that a `DELETE` of a node with relationships has `ErrorHasRelationships` and a relationship created to a node deleted earlier in the query `ErrorNotFound`; a storage failure under a query has `ErrorIO` and an allocation failure `ErrorOutOfMemory`.
- Durable streams use explicit cursors: `DB.ReadStream`, `DB.GetStreamOffset`, `DB.Changes`, `Tx.PublishStream`, `Tx.SetStreamOffset`, and `Tx.TrimStream`.
- `DB.Changes` returns a transaction's net effect in a replayable order: the edges it deletes, the nodes that exist after it, the edges that exist after it, then the nodes it deletes, each group by ascending id, so an edge is created after both its nodes and deleted before either (see the [changefeed guide](https://docs.latticedb.org/guides/changefeed)).

## Embeddings

Embedding helpers are available from the dedicated package `github.com/jeffhajewski/latticedb/bindings/go/embedding`. The root `latticedb` package still exposes deprecated compatibility aliases, but new code should prefer the subpackage.

Built-in deterministic embeddings:

```go
import latticeembedding "github.com/jeffhajewski/latticedb/bindings/go/embedding"

vec, err := latticeembedding.Hash("hello world", 128)
```

HTTP embedding client:

```go
import latticeembedding "github.com/jeffhajewski/latticedb/bindings/go/embedding"

client, err := latticeembedding.NewClient(latticeembedding.Config{
	Endpoint:  "http://localhost:11434/api/embeddings",
	Model:     "nomic-embed-text",
	APIFormat: latticeembedding.APIFormatOllama,
})
if err != nil {
	log.Fatal(err)
}
defer client.Close()

vec, err := client.Embed("hello world")
```
