package io.latticedb;

/**
 * Embedding helpers: the built-in deterministic hash embedding plus an HTTP
 * embedding client backed by native code (mirrors the Go binding's embedding
 * package).
 */
public final class Embedding {
    /** API wire format for {@link Client}. */
    public enum APIFormat {
        OLLAMA(0),
        OPENAI(1);

        private final int value;

        APIFormat(int value) {
            this.value = value;
        }

        int value() {
            return value;
        }
    }

    /**
     * Configuration for {@link Client}.
     *
     * <p>{@code endpoint} is the full URL each request is POSTed to; nothing is
     * appended to it. For Ollama, {@code http://localhost:11434/api/embeddings};
     * for OpenAI, {@code https://api.openai.com/v1/embeddings}.
     */
    public record Config(String endpoint, String model, APIFormat apiFormat,
                         String apiKey, int timeoutMs) {
        public Config {
            if (apiFormat == null) {
                apiFormat = APIFormat.OLLAMA;
            }
            if (timeoutMs <= 0) {
                timeoutMs = 0; /* engine default: 30s */
            }
        }
    }

    private Embedding() {
    }

    /**
     * Generates a deterministic placeholder embedding with no external
     * service. Similar text does NOT produce nearby vectors; use a real
     * model for meaningful similarity.
     */
    public static float[] hashEmbed(String text, int dimensions) {
        return Native.hashEmbed(text, dimensions);
    }

    /** HTTP embedding client. Close when done. */
    public static final class Client implements AutoCloseable {
        private long handle;

        private Client(long handle) {
            this.handle = handle;
        }

        public static Client open(Config config) {
            return new Client(Native.embeddingClientCreate(config.endpoint(),
                    config.model(), config.apiFormat().value(), config.apiKey(),
                    config.timeoutMs()));
        }

        public synchronized float[] embed(String text) {
            if (handle == 0) {
                throw new LatticeException(ErrorCode.ERROR, "embedding client is closed");
            }
            return Native.embeddingClientEmbed(handle, text);
        }

        @Override
        public synchronized void close() {
            if (handle == 0) {
                return;
            }
            Native.embeddingClientFree(handle);
            handle = 0;
        }
    }
}
