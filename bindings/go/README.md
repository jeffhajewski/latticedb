# LatticeDB Go Bindings

Go bindings for [LatticeDB](https://github.com/jeffhajewski/latticedb), the embedded knowledge graph database for AI and RAG applications.

## Status

This is a cgo-backed client built on the stable C ABI. The current API supports:

- database open/close
- read/write transactions
- nodes, edges, and nested property values
- Cypher queries with nested parameters and materialized results
- vector storage and search
- full-text search and fuzzy search
- batch insert of nodes with vectors
- built-in hash embeddings
- HTTP embedding client

The packaging story is still source-first. Today the easiest way to use it is from this repository while the install/release flow is finalized.

## Prerequisites

Build the native shared library from the repo root:

```bash
zig build shared
```

Then run Go commands from `bindings/go`:

```bash
cd bindings/go
go test ./...
```

The cgo bridge links against `zig-out/lib/liblattice` in this repository.

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
		EnableVector:     true,
		VectorDimensions: 4,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

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
		return tx.FTSIndex(alice.ID, "Alice works on graph retrieval systems")
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

	fts, err := db.FTSSearch("graph retrieval", latticedb.FTSSearchOptions{Limit: 5})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("fts results:", fts)
}
```

## API Notes

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

## Embeddings

Built-in deterministic embeddings:

```go
vec, err := latticedb.HashEmbed("hello world", 128)
```

HTTP embedding client:

```go
client, err := latticedb.NewEmbeddingClient(latticedb.EmbeddingConfig{
	Endpoint:  "http://localhost:11434/api/embeddings",
	Model:     "nomic-embed-text",
	APIFormat: latticedb.EmbeddingAPIFormatOllama,
})
if err != nil {
	log.Fatal(err)
}
defer client.Close()

vec, err := client.Embed("hello world")
```
