package io.latticedb;

/**
 * How a vector index measures distance ({@code lattice_vector_metric}).
 * Search results are ordered by it, smallest first. The database file records
 * the metric; once an index exists a different one is
 * {@link ErrorCode#CONFIG_MISMATCH}.
 */
public enum VectorMetric {
    /** {@code 1 - cos(a, b)}. Vectors are stored scaled to unit length. */
    COSINE(1),
    /** Euclidean (L2) distance. Vectors are stored as given. */
    EUCLIDEAN(2),
    /** Negative dot product: the largest dot product comes first. Vectors are
     * stored as given. */
    DOT_PRODUCT(3);

    private final int code;

    VectorMetric(int code) {
        this.code = code;
    }

    /** The native {@code lattice_vector_metric} value. */
    public int code() {
        return code;
    }
}
