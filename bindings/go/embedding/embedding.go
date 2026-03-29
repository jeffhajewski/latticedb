// Package embedding provides optional embedding helpers for LatticeDB.
//
// The core database API lives in the parent latticedb package. This package
// groups convenience helpers for generating or fetching vectors so new code can
// keep the graph/vector/text database surface separate from embedding-specific
// utilities.
package embedding

import latticedb "github.com/jeffhajewski/latticedb/bindings/go"

type Client = latticedb.EmbeddingClient

type APIFormat = latticedb.EmbeddingAPIFormat

const (
	APIFormatOllama = latticedb.EmbeddingAPIFormatOllama
	APIFormatOpenAI = latticedb.EmbeddingAPIFormatOpenAI
)

type Config = latticedb.EmbeddingConfig

func Hash(text string, dimensions uint16) ([]float32, error) {
	return latticedb.HashEmbed(text, dimensions)
}

func HashEmbed(text string, dimensions uint16) ([]float32, error) {
	return latticedb.HashEmbed(text, dimensions)
}

func NewClient(config Config) (*Client, error) {
	return latticedb.NewEmbeddingClient(config)
}

func NewEmbeddingClient(config Config) (*Client, error) {
	return latticedb.NewEmbeddingClient(config)
}
