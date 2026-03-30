package latticedb_test

import (
	"reflect"
	"testing"

	latticedb "github.com/jeffhajewski/latticedb/bindings/go"
	latticeembedding "github.com/jeffhajewski/latticedb/bindings/go/embedding"
)

func TestEmbeddingPackageHashAndClientLifecycle(t *testing.T) {
	v1, err := latticeembedding.Hash("hello world", 16)
	if err != nil {
		t.Fatalf("hash embed v1: %v", err)
	}
	v2, err := latticeembedding.Hash("hello world", 16)
	if err != nil {
		t.Fatalf("hash embed v2: %v", err)
	}
	v3, err := latticeembedding.Hash("different text", 16)
	if err != nil {
		t.Fatalf("hash embed v3: %v", err)
	}

	if len(v1) != 16 {
		t.Fatalf("unexpected embedding length: %d", len(v1))
	}
	if !reflect.DeepEqual(v1, v2) {
		t.Fatalf("expected deterministic hash embeddings")
	}
	if reflect.DeepEqual(v1, v3) {
		t.Fatalf("expected different inputs to produce different embeddings")
	}

	var client latticeembedding.Client
	if err := client.Close(); err != nil {
		t.Fatalf("close zero client: %v", err)
	}
	if _, err := client.Embed("hello"); err == nil {
		t.Fatalf("expected embed on closed client to fail")
	}
}

func TestDeprecatedEmbeddingCompatibilityAliases(t *testing.T) {
	v1, err := latticedb.HashEmbed("hello world", 8)
	if err != nil {
		t.Fatalf("hash embed via deprecated alias: %v", err)
	}
	v2, err := latticeembedding.Hash("hello world", 8)
	if err != nil {
		t.Fatalf("hash embed via preferred package: %v", err)
	}
	if !reflect.DeepEqual(v1, v2) {
		t.Fatalf("deprecated hash alias diverged from preferred package")
	}

	var client latticedb.EmbeddingClient
	if err := client.Close(); err != nil {
		t.Fatalf("close zero client via deprecated alias: %v", err)
	}
	if _, err := client.Embed("hello"); err == nil {
		t.Fatalf("expected embed on closed deprecated alias client to fail")
	}
}
