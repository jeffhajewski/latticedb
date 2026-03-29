/**
 * High-level embedding utilities for LatticeDB.
 *
 * Provides hash-based embeddings (built-in, no external service) and
 * an HTTP embedding client for services like Ollama and OpenAI.
 */

import { getFFI, EmbeddingClientHandle } from './ffi';
import { EmbeddingConfig } from './types';

export { EmbeddingApiFormat, EmbeddingConfig } from './types';

/**
 * Generate a hash embedding for text (built-in, no external service).
 *
 * Deterministic and fast. Useful for testing or simple keyword-based similarity.
 *
 * @param text - The text to embed.
 * @param dimensions - Number of embedding dimensions (default 128).
 * @returns A Float32Array of the specified dimensions.
 */
export function hashEmbed(text: string, dimensions: number = 128): Float32Array {
  return getFFI().hashEmbed(text, dimensions);
}

/**
 * HTTP embedding client for services like Ollama and OpenAI.
 *
 * @example
 * ```typescript
 * const client = new EmbeddingClient({ endpoint: "http://localhost:11434" });
 * const vec = client.embed("hello world");
 * client.close();
 * ```
 */
export class EmbeddingClient {
  private handle: EmbeddingClientHandle | null;

  /**
   * Create an embedding client connected to an HTTP service.
   *
   * @param config - Embedding service configuration.
   */
  constructor(config: EmbeddingConfig) {
    const ffi = getFFI();
    this.handle = ffi.embeddingClientCreate({
      endpoint: config.endpoint,
      model: config.model,
      apiFormat: config.apiFormat,
      apiKey: config.apiKey,
      timeoutMs: config.timeoutMs,
    });
  }

  /**
   * Generate an embedding for text via the HTTP service.
   *
   * @param text - The text to embed.
   * @returns A Float32Array embedding vector.
   */
  embed(text: string): Float32Array {
    if (this.handle === null) {
      throw new Error('EmbeddingClient has been closed');
    }
    return getFFI().embeddingClientEmbed(this.handle, text);
  }

  /**
   * Free the underlying native client. Safe to call multiple times.
   */
  close(): void {
    if (this.handle !== null) {
      getFFI().embeddingClientFree(this.handle);
      this.handle = null;
    }
  }
}
