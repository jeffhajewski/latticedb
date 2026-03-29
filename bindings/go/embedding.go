package latticedb

import cgobridge "github.com/jeffhajewski/latticedb/bindings/go/internal/cgo"

// EmbeddingClient is a compatibility-root export for the optional embedding
// helpers. Prefer package github.com/jeffhajewski/latticedb/bindings/go/embedding
// in new code.
type EmbeddingClient struct {
	raw *cgobridge.EmbeddingClient
}

// HashEmbed generates a deterministic built-in embedding.
//
// Prefer package github.com/jeffhajewski/latticedb/bindings/go/embedding in
// new code.
func HashEmbed(text string, dimensions uint16) ([]float32, error) {
	return cgobridge.HashEmbed(text, dimensions)
}

// NewEmbeddingClient constructs an HTTP embedding client.
//
// Prefer package github.com/jeffhajewski/latticedb/bindings/go/embedding in
// new code.
func NewEmbeddingClient(config EmbeddingConfig) (*EmbeddingClient, error) {
	raw, err := cgobridge.NewEmbeddingClient(cgobridge.EmbeddingConfig{
		Endpoint:  config.Endpoint,
		Model:     config.Model,
		APIFormat: cgobridge.EmbeddingAPIFormat(config.APIFormat),
		APIKey:    config.APIKey,
		TimeoutMS: config.TimeoutMS,
	})
	if err != nil {
		return nil, wrapError(err)
	}
	return &EmbeddingClient{raw: raw}, nil
}

func (client *EmbeddingClient) Embed(text string) ([]float32, error) {
	if client == nil || client.raw == nil {
		return nil, ErrEmbeddingClosed
	}
	vector, err := client.raw.Embed(text)
	return vector, wrapError(err)
}

func (client *EmbeddingClient) Close() error {
	if client == nil || client.raw == nil {
		return nil
	}
	err := wrapError(client.raw.Close())
	if err == nil {
		client.raw = nil
	}
	return err
}
