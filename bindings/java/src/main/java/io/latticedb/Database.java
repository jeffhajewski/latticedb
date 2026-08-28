package io.latticedb;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Objects;

/**
 * An open LatticeDB database.
 *
 * <p>Instances own a native database handle; always close them (try-with-resources):
 * <pre>{@code
 * try (Database db = Database.open("knowledge.db", OpenOptions.defaults().create(true))) {
 *     try (Transaction txn = db.beginWrite()) {
 *         Node n = txn.createNode(List.of("Person"), Map.of("name", "Alice"));
 *         txn.commit();
 *     }
 * }
 * }</pre>
 */
public final class Database implements AutoCloseable {
    private final String path;
    private final OpenOptions options;
    private long handle;

    private Database(String path, OpenOptions options, long handle) {
        this.path = path;
        this.options = options;
        this.handle = handle;
    }

    /** The engine version string. */
    public static String version() {
        return Native.version();
    }

    /** Opens (or creates) a database file with default options. */
    public static Database open(String path) {
        return open(path, OpenOptions.defaults());
    }

    public static Database open(String path, OpenOptions options) {
        long handle = Native.open(path, options.create(), options.readOnly(),
                options.cacheSizeMB(), options.pageSize(), options.enableVectors(),
                options.vectorDimensions(), options.enableWal(),
                options.enableAdjacencyCache(), options.lock());
        return new Database(path, options, handle);
    }

    /** Opens an in-memory database from bytes produced by {@link #serialize()}. */
    public static Database deserialize(byte[] bytes) {
        return deserialize(bytes, OpenOptions.defaults());
    }

    /**
     * Opens an in-memory database from bytes produced by {@link #serialize()}.
     * The bytes are copied, so the caller may reuse or discard them after this
     * method returns. Changes do not affect the input array.
     */
    public static Database deserialize(byte[] bytes, OpenOptions options) {
        Objects.requireNonNull(bytes, "bytes");
        Objects.requireNonNull(options, "options");
        long handle = Native.deserialize(bytes, options.cacheSizeMB(), options.pageSize(),
                options.enableVectors(), options.vectorDimensions(), options.enableWal(),
                options.enableAdjacencyCache(), options.lock());
        return new Database("<deserialized>", options, handle);
    }

    public String getPath() {
        return path;
    }

    public boolean isOpen() {
        return handle != 0;
    }

    @Override
    public synchronized void close() {
        if (handle == 0) {
            return;
        }
        long h = handle;
        handle = 0;
        Native.close(h);
    }

    // Synchronized against close(): without this, one thread can read a
    // live handle while another closes and frees it, leaving the first
    // with a stale jlong. The C API's handle registry turns a freed handle
    // into err_invalid_arg rather than a crash, but address reuse could
    // still route an operation to a different database.
    synchronized long handle() {
        long h = handle;
        if (h == 0) {
            throw new LatticeException(ErrorCode.ERROR, "database is not open");
        }
        return h;
    }

    /**
     * Returns the complete database file as bytes, including pending WAL data.
     * Serialization fails with {@link ErrorCode#LOCK_TIMEOUT} while a
     * transaction is open.
     */
    public byte[] serialize() {
        return Native.serialize(handle());
    }

    /* ------------------------------------------------------------------ */
    /* Transactions                                                        */
    /* ------------------------------------------------------------------ */

    /** Begins a read-only transaction. */
    public Transaction beginRead() {
        return new Transaction(this, true);
    }

    /** Begins a read-write transaction. At most one writer may be active. */
    public Transaction beginWrite() {
        if (options.readOnly()) {
            throw new LatticeException(ErrorCode.READ_ONLY,
                    "cannot write to a read-only database");
        }
        return new Transaction(this, false);
    }

    /** Runs {@code body} in an auto-managed read-only transaction (mirrors
     * the Go binding's View). */
    public <T> T read(java.util.function.Function<Transaction, T> body) {
        try (Transaction txn = beginRead()) {
            return body.apply(txn);
        }
    }

    /**
     * Runs {@code body} in a write transaction, committing on success and
     * rolling back on exception (mirrors the Go binding's Update).
     */
    public <T> T write(java.util.function.Function<Transaction, T> body) {
        try (Transaction txn = beginWrite()) {
            T result = body.apply(txn);
            txn.commit();
            return result;
        }
    }

    /* ------------------------------------------------------------------ */
    /* Query                                                               */
    /* ------------------------------------------------------------------ */

    /**
     * Executes a Cypher query with auto-managed transaction mode: write
     * queries run in a write transaction, everything else in a read
     * transaction (mirrors DB.Query in the Go binding).
     */
    public QueryResult query(String cypher, Map<String, Object> parameters) {
        if (!isOpen()) {
            throw new LatticeException(ErrorCode.ERROR, "database is not open");
        }
        boolean writes = !options.readOnly() && Native.queryWrites(handle(), cypher);
        if (writes) {
            try (Transaction txn = beginWrite()) {
                QueryResult result = txn.query(cypher, parameters);
                txn.commit();
                return result;
            }
        }
        try (Transaction txn = beginRead()) {
            return txn.query(cypher, parameters);
        }
    }

    public QueryResult query(String cypher) {
        return query(cypher, Map.of());
    }

    /* ------------------------------------------------------------------ */
    /* Search                                                              */
    /* ------------------------------------------------------------------ */

    public List<VectorSearchResult> vectorSearch(float[] vector, VectorSearchOptions opts) {
        int k = opts == null ? 10 : opts.k();
        int ef = opts == null ? 0 : opts.efSearch();
        Object[] out = Native.vectorSearch(handle(), vector, k, ef);
        return zipVectorResults(out);
    }

    /**
     * Declares a full-text index over one label/property pair.
     *
     * <p>The property holds the text. Declaring reads it from every node already
     * carrying the label, and writes maintain it from then on. Only string
     * properties are indexed.
     */
    public void createNodeFtsIndex(String label, String property) {
        Native.createNodeFtsIndex(handle(), label, property);
    }

    /** Removes a declared full-text index and everything it stored. */
    public void dropNodeFtsIndex(String label, String property) {
        Native.dropNodeFtsIndex(handle(), label, property);
    }

    /** Whether a full-text index is declared for this label and property. */
    public boolean hasNodeFtsIndex(String label, String property) {
        return Native.hasNodeFtsIndex(handle(), label, property);
    }

    /**
     * Searches one declared full-text index.
     *
     * <p>Throws {@link ErrorCode#UNSUPPORTED} when no index is declared for this
     * label and property, rather than returning an empty list: a mistyped
     * property name and a query that found nothing are different situations.
     */
    public List<FTSSearchResult> ftsSearch(String label, String property,
                                           String query, FTSSearchOptions opts) {
        int limit = opts == null ? 10 : opts.limit();
        Object[] out = Native.ftsSearch(handle(), label, property, query, limit);
        return zipFtsResults(out);
    }

    public List<FTSSearchResult> ftsSearchFuzzy(String label, String property,
                                                String query, FTSSearchOptions opts) {
        int limit = opts == null ? 10 : opts.limit();
        int maxDistance = opts == null ? 0 : opts.maxDistance();
        int minTermLength = opts == null ? 0 : opts.minTermLength();
        Object[] out = Native.ftsSearchFuzzy(handle(), label, property, query, limit,
                maxDistance, minTermLength);
        return zipFtsResults(out);
    }

    static List<VectorSearchResult> zipVectorResults(Object[] out) {
        long[] ids = (long[]) out[0];
        float[] dists = (float[]) out[1];
        List<VectorSearchResult> results = new ArrayList<>(ids.length);
        for (int i = 0; i < ids.length; i++) {
            results.add(new VectorSearchResult(ids[i], dists[i]));
        }
        return results;
    }

    static List<FTSSearchResult> zipFtsResults(Object[] out) {
        long[] ids = (long[]) out[0];
        float[] scores = (float[]) out[1];
        List<FTSSearchResult> results = new ArrayList<>(ids.length);
        for (int i = 0; i < ids.length; i++) {
            results.add(new FTSSearchResult(ids[i], scores[i]));
        }
        return results;
    }

    /* ------------------------------------------------------------------ */
    /* Schema / labels                                                     */
    /* ------------------------------------------------------------------ */

    /** Creates an explicit equality index for a node label/property pair. */
    public void createNodePropertyIndex(String label, String property) {
        Native.createNodePropertyIndex(handle(), label, property);
    }

    public void dropNodePropertyIndex(String label, String property) {
        Native.dropNodePropertyIndex(handle(), label, property);
    }

    /** Creates an explicit equality index for an edge type/property pair. */
    public void createEdgePropertyIndex(String edgeType, String property) {
        Native.createEdgePropertyIndex(handle(), edgeType, property);
    }

    public void dropEdgePropertyIndex(String edgeType, String property) {
        Native.dropEdgePropertyIndex(handle(), edgeType, property);
    }

    /**
     * Returns every node id carrying the given label. Unknown labels yield
     * an empty list.
     */
    public List<Long> getNodesByLabel(String label) {
        return toIdList(Native.getNodesByLabel(handle(), label));
    }

    static List<Long> toIdList(long[] ids) {
        List<Long> out = new ArrayList<>(ids.length);
        for (long id : ids) {
            out.add(id);
        }
        return out;
    }

    /* ------------------------------------------------------------------ */
    /* Streams                                                             */
    /* ------------------------------------------------------------------ */

    /**
     * Reads records after a sequence cursor without committing offsets. If no
     * records are available, waits up to {@code timeoutMs} for a same-process
     * commit wakeup.
     */
    public List<StreamRecord> readStream(String stream, long afterSequence,
                                         int limit, int timeoutMs) {
        Object[] out = Native.streamRead(handle(), stream, afterSequence, limit, timeoutMs);
        long[] seqs = (long[]) out[0];
        String[] kinds = (String[]) out[1];
        Object[] payloads = (Object[]) out[2];
        List<StreamRecord> records = new ArrayList<>(seqs.length);
        for (int i = 0; i < seqs.length; i++) {
            records.add(new StreamRecord(seqs[i], kinds[i], payloads[i]));
        }
        return records;
    }

    /** Consumer offset for a stream, empty when the consumer has none. */
    public OptionalLong getStreamOffset(String stream, String consumer) {
        Object[] out = Native.streamGetOffset(handle(), stream, consumer);
        boolean exists = (Boolean) out[1];
        if (!exists) {
            return OptionalLong.empty();
        }
        return OptionalLong.of((Long) out[0]);
    }

    /** Latest sequence published to a stream, or 0 when it has no records. */
    public long getLastSequence(String stream) {
        return Native.streamGetLastSequence(handle(), stream);
    }

    /** Reads the built-in graph changefeed. */
    public List<StreamRecord> changes(long afterSequence, int limit, int timeoutMs) {
        return readStream("__lattice_changes", afterSequence, limit, timeoutMs);
    }

    /* ------------------------------------------------------------------ */
    /* Query cache                                                         */
    /* ------------------------------------------------------------------ */

    public void cacheClear() {
        Native.cacheClear(handle());
    }

    public QueryCacheStats cacheStats() {
        Object[] out = Native.cacheStats(handle());
        return new QueryCacheStats((Integer) out[0], (Long) out[1], (Long) out[2]);
    }

    @Override
    public String toString() {
        return "Database[path=" + path + ", open=" + isOpen() + "]";
    }

    /** Suppress unused-import warning helper used by query row building. */
    static Map<String, Object> newRow() {
        return new LinkedHashMap<>();
    }
}
