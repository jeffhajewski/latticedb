package io.latticedb;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * An active transaction. Obtained via {@link Database#beginRead()},
 * {@link Database#beginWrite()}, or the auto-managed {@code read}/
 * {@code write}/{@code query} helpers.
 *
 * <p>Commit or rollback ends the transaction; closing an uncommitted
 * transaction rolls it back.
 */
public final class Transaction implements AutoCloseable {
    private final Database db;
    private final boolean readOnly;
    private long handle;
    private boolean active = true;

    Transaction(Database db, boolean readOnly) {
        this.db = db;
        this.readOnly = readOnly;
        this.handle = Native.begin(db.handle(), readOnly);
    }

    public Database getDatabase() {
        return db;
    }

    public synchronized boolean isReadOnly() {
        return readOnly && active;
    }

    public synchronized boolean isActive() {
        return active;
    }

    /**
     * Commits the transaction. A committed handle must not be reused.
     *
     * <p>If the commit fails with {@link ErrorCode#TXN_ABORTED}, the native
     * transaction is already gone and this one is over: {@link #rollback()}
     * and {@link #close()} then do nothing. After any other failure it stays
     * open, and closing it rolls it back.</p>
     */
    public synchronized void commit() {
        ensureActive();
        try {
            Native.commit(handle);
        } catch (LatticeException e) {
            if (e.getErrorCode() == ErrorCode.TXN_ABORTED) {
                // The native side has retired the handle; it cannot be
                // committed or rolled back again.
                handle = 0;
                active = false;
            }
            throw e;
        }
        handle = 0;
        active = false;
    }

    /**
     * Rolls back the transaction. Rolling back an already-ended transaction
     * is a no-op (mirrors the Go binding), so try-with-resources cleanup is
     * always safe.
     */
    public synchronized void rollback() {
        if (!active) {
            return;
        }
        Native.rollback(handle);
        handle = 0;
        active = false;
    }

    @Override
    public synchronized void close() {
        rollback();
    }

    long handle() {
        if (!active) {
            throw new LatticeException(ErrorCode.TXN_ABORTED, "transaction is not active");
        }
        return handle;
    }

    private void ensureActive() {
        if (!active) {
            throw new LatticeException(ErrorCode.TXN_ABORTED, "transaction is not active");
        }
    }

    private void ensureWritable() {
        ensureActive();
        if (readOnly) {
            throw new LatticeException(ErrorCode.READ_ONLY,
                    "cannot write in a read-only transaction");
        }
    }

    /* ------------------------------------------------------------------ */
    /* Nodes                                                               */
    /* ------------------------------------------------------------------ */

    /** Creates a node with labels and properties. A property whose value is
     * null is left out, as in Cypher's CREATE. */
    public Node createNode(List<String> labels, Map<String, Object> properties) {
        ensureWritable();
        String first = labels == null || labels.isEmpty() ? "" : labels.get(0);
        long id = Native.nodeCreate(handle, first);
        if (labels != null) {
            for (int i = 1; i < labels.size(); i++) {
                Native.nodeAddLabel(handle, id, labels.get(i));
            }
        }
        Map<String, Object> props = new LinkedHashMap<>();
        if (properties != null) {
            for (Map.Entry<String, Object> e : properties.entrySet()) {
                Native.nodeSetProperty(handle, id, e.getKey(), e.getValue());
                if (e.getValue() != null) {
                    props.put(e.getKey(), e.getValue());
                }
            }
        }
        return new Node(id, labels == null ? List.of() : List.copyOf(labels), props);
    }

    public Node createNode(List<String> labels) {
        return createNode(labels, null);
    }

    public Node createNode() {
        return createNode(null, null);
    }

    /**
     * Deletes a node. A node that still has relationships is not deleted: this
     * throws {@link ErrorCode#HAS_RELATIONSHIPS}, as Cypher's {@code DELETE}
     * does, since deleting it alone would leave edges pointing at nothing. Use
     * {@link #detachDeleteNode(long)} to delete them with it. Deleting a node
     * that does not exist does nothing.
     */
    public void deleteNode(long nodeId) {
        ensureWritable();
        Native.nodeDelete(handle, nodeId);
    }

    /** Deletes a node and every relationship it has, as Cypher's
     * {@code DETACH DELETE} does. All of it is part of this transaction. */
    public void detachDeleteNode(long nodeId) {
        ensureWritable();
        Native.nodeDetachDelete(handle, nodeId);
    }

    public boolean nodeExists(long nodeId) {
        ensureActive();
        return Native.nodeExists(handle, nodeId);
    }

    /**
     * Loads a node's labels by id; empty when the node does not exist.
     * Properties are not populated (the C API exposes per-property reads).
     */
    public Optional<Node> getNode(long nodeId) {
        ensureActive();
        if (!Native.nodeExists(handle, nodeId)) {
            return Optional.empty();
        }
        return Optional.of(new Node(nodeId,
                List.of(Native.nodeGetLabels(handle, nodeId)), Map.of()));
    }

    public void addLabel(long nodeId, String label) {
        ensureWritable();
        Native.nodeAddLabel(handle, nodeId, label);
    }

    public void removeLabel(long nodeId, String label) {
        ensureWritable();
        Native.nodeRemoveLabel(handle, nodeId, label);
    }

    /** Sets a property on a node. A null value removes the property, as
     * Cypher's {@code SET n.x = null} does; removing one the node does not
     * have does nothing. (Up to 0.18.0 it stored the null.) */
    public void setProperty(long nodeId, String key, Object value) {
        ensureWritable();
        Native.nodeSetProperty(handle, nodeId, key, value);
    }

    /** Property value, or empty when the key is absent. */
    public Optional<Object> getProperty(long nodeId, String key) {
        ensureActive();
        return Optional.ofNullable(Native.nodeGetProperty(handle, nodeId, key));
    }

    /**
     * Sets the node's vector. {@code key} must be {@code "embedding"} or
     * {@code ""}. Throws {@link ErrorCode#UNSUPPORTED} when the database was
     * created without a vector index, or for another key, and
     * {@link ErrorCode#INVALID_ARG} for a vector without the index's
     * dimensions or with a NaN or infinite element.
     */
    public void setVector(long nodeId, String key, float[] vector) {
        ensureWritable();
        // Another key is refused as unsupported before the index is looked at.
        if (key != null && !key.isEmpty() && !key.equals("embedding")) {
            Native.nodeSetVector(handle, nodeId, key, vector);
            return;
        }
        LatticeException.needingVectorIndex("set a vector", () -> {
            Native.nodeSetVector(handle, nodeId, key, vector);
            return null;
        });
    }

    /**
     * The node's vector as this transaction sees it, or empty when it has
     * none. {@code key} must be {@code "embedding"} or {@code ""}, as in
     * {@link #setVector}. Throws {@link ErrorCode#NOT_FOUND} when the node does
     * not exist.
     *
     * <p>Under the cosine metric, the default, a committed vector reads back
     * scaled to unit length: {@code {3, 4}} reads back as {@code {0.6, 0.8}}.
     * The direction, and so every cosine distance, is unchanged. Under the
     * euclidean and dot-product metrics it reads back as stored. A vector set
     * earlier in this transaction reads back as written.
     */
    public Optional<float[]> getVector(long nodeId, String key) {
        ensureActive();
        return Optional.ofNullable(Native.nodeGetVector(handle, nodeId, key));
    }

    /**
     * Removes the node's vector. The node keeps its labels and properties;
     * vector search no longer returns it. Removing a vector the node does not
     * have does nothing. Throws {@link ErrorCode#NOT_FOUND} when the node does
     * not exist.
     */
    public void removeVector(long nodeId, String key) {
        ensureWritable();
        Native.nodeRemoveVector(handle, nodeId, key);
    }

    /**
     * Finds node ids through a required explicit equality index. Throws
     * {@link ErrorCode#UNSUPPORTED} when the index does not exist.
     */
    public List<Long> findNodesByLabelProperty(String label, String property,
                                               Object value, int limit) {
        ensureActive();
        return Database.toIdList(
                Native.findNodesByLabelProperty(handle, label, property, value, limit));
    }

    /**
     * Inserts multiple vector-bearing nodes with the same label in one call.
     * Throws {@link ErrorCode#UNSUPPORTED}, and creates no node, when the
     * database was created without a vector index.
     */
    public List<Long> batchInsertVectors(String label, float[][] vectors) {
        ensureWritable();
        return Database.toIdList(LatticeException.needingVectorIndex(
                "insert nodes with vectors", () -> Native.batchInsert(handle, label, vectors)));
    }

    /* ------------------------------------------------------------------ */
    /* Edges                                                               */
    /* ------------------------------------------------------------------ */

    /** Creates an edge. Throws {@link ErrorCode#NOT_FOUND} if either node does
     * not exist as this transaction sees it. A property whose value is null is
     * left out. */
    public Edge createEdge(long sourceId, long targetId, String type,
                           Map<String, Object> properties) {
        ensureWritable();
        long edgeId = Native.edgeCreate(handle, sourceId, targetId, type);
        Map<String, Object> props = new LinkedHashMap<>();
        if (properties != null) {
            for (Map.Entry<String, Object> e : properties.entrySet()) {
                Native.edgeSetProperty(handle, edgeId, e.getKey(), e.getValue());
                if (e.getValue() != null) {
                    props.put(e.getKey(), e.getValue());
                }
            }
        }
        return new Edge(edgeId, sourceId, targetId, type, props);
    }

    public Edge createEdge(long sourceId, long targetId, String type) {
        return createEdge(sourceId, targetId, type, null);
    }

    public void deleteEdge(long sourceId, long targetId, String type) {
        ensureWritable();
        Native.edgeDelete(handle, sourceId, targetId, type);
    }

    /** Sets a property on an edge. A null value removes the property, as
     * {@link #setProperty} does. */
    public void setEdgeProperty(long edgeId, String key, Object value) {
        ensureWritable();
        Native.edgeSetProperty(handle, edgeId, key, value);
    }

    public Optional<Object> getEdgeProperty(long edgeId, String key) {
        ensureActive();
        return Optional.ofNullable(Native.edgeGetProperty(handle, edgeId, key));
    }

    public void removeEdgeProperty(long edgeId, String key) {
        ensureWritable();
        Native.edgeRemoveProperty(handle, edgeId, key);
    }

    /**
     * Finds edge ids through a required explicit equality index. Throws
     * {@link ErrorCode#UNSUPPORTED} when the index does not exist.
     */
    public List<Long> findEdgesByTypeProperty(String edgeType, String property,
                                              Object value, int limit) {
        ensureActive();
        return Database.toIdList(
                Native.findEdgesByTypeProperty(handle, edgeType, property, value, limit));
    }

    public List<Edge> getOutgoingEdges(long nodeId) {
        ensureActive();
        return toEdges(Native.edgesOutgoing(handle, nodeId));
    }

    public List<Edge> getIncomingEdges(long nodeId) {
        ensureActive();
        return toEdges(Native.edgesIncoming(handle, nodeId));
    }

    public List<Edge> getOutgoingEdgesByType(long nodeId, String edgeType, int limit) {
        ensureActive();
        return toEdges(Native.edgesOutgoingByType(handle, nodeId, edgeType, limit));
    }

    public List<Edge> getIncomingEdgesByType(long nodeId, String edgeType, int limit) {
        ensureActive();
        return toEdges(Native.edgesIncomingByType(handle, nodeId, edgeType, limit));
    }

    /** Scans edges of a type ({@code null} = all). Admin/rebuild use. */
    public List<Edge> scanEdges(String edgeType, int limit) {
        ensureActive();
        return toEdges(Native.edgesScan(handle, edgeType, limit));
    }

    static List<Edge> toEdges(Object[] packed) {
        long[] triples = (long[]) packed[0];
        String[] types = (String[]) packed[1];
        List<Edge> edges = new ArrayList<>(types.length);
        for (int i = 0; i < types.length; i++) {
            edges.add(new Edge(triples[3 * i], triples[3 * i + 1],
                    triples[3 * i + 2], types[i], Map.of()));
        }
        return edges;
    }

    /* ------------------------------------------------------------------ */
    /* Search (transaction-scoped)                                         */
    /* ------------------------------------------------------------------ */

    /**
     * Vector search within this transaction's snapshot. Throws
     * {@link ErrorCode#UNSUPPORTED} when the database was created without a
     * vector index.
     */
    public List<VectorSearchResult> queryVector(float[] vector, int k, int efSearch) {
        ensureActive();
        return Database.zipVectorResults(LatticeException.needingVectorIndex(
                "search vectors", () -> Native.vectorSearchTxn(handle, vector, k, efSearch)));
    }

    /** Searches one declared index within this transaction's snapshot. */
    public List<FTSSearchResult> ftsSearch(String label, String property,
                                           String query, FTSSearchOptions opts) {
        ensureActive();
        return Database.zipFtsResults(Native.ftsSearchTxn(handle, label, property, query,
                opts == null ? 10 : opts.limit()));
    }

    /**
     * Fuzzy search of one declared index within this transaction's snapshot.
     *
     * <p>Text this transaction has written but not committed is matched by term
     * presence rather than edit distance, so a typo will not find a document the
     * transaction has only just written.
     */
    public List<FTSSearchResult> ftsSearchFuzzy(String label, String property,
                                                String query, FTSSearchOptions opts) {
        ensureActive();
        Object[] out = Native.ftsSearchFuzzyTxn(handle, label, property, query,
                opts == null ? 10 : opts.limit(),
                opts == null ? 0 : opts.maxDistance(),
                opts == null ? 0 : opts.minTermLength());
        return Database.zipFtsResults(out);
    }

    /* ------------------------------------------------------------------ */
    /* Streams (write side)                                                */
    /* ------------------------------------------------------------------ */

    /** Publishes a record; kind {@code null} defaults to "message". */
    public void publishStream(String stream, String kind, Object payload) {
        ensureWritable();
        Native.streamPublish(handle, stream, kind, payload);
    }

    /** Publishes and returns the sequence assigned within this transaction. */
    public long publishStreamGetSequence(String stream, String kind, Object payload) {
        ensureWritable();
        return Native.streamPublishGetSequence(handle, stream, kind, payload);
    }

    public void setStreamOffset(String stream, String consumer, long sequence) {
        ensureWritable();
        Native.streamSetOffset(handle, stream, consumer, sequence);
    }

    public void trimStream(String stream, long throughSequence) {
        ensureWritable();
        Native.streamTrim(handle, stream, throughSequence);
    }

    /* ------------------------------------------------------------------ */
    /* Query                                                               */
    /* ------------------------------------------------------------------ */

    /** Prepares, binds and executes a Cypher query inside this transaction. */
    public QueryResult query(String cypher, Map<String, Object> parameters) {
        ensureActive();
        long q = Native.queryPrepare(db.handle(), cypher);
        try {
            if (parameters != null) {
                for (Map.Entry<String, Object> e : parameters.entrySet()) {
                    if (e.getValue() instanceof float[] vector) {
                        Native.queryBindVector(q, e.getKey(), vector);
                    } else {
                        Native.queryBind(q, e.getKey(), e.getValue());
                    }
                }
            }
            long result = Native.queryExecute(q, handle);
            try {
                return collectResult(result);
            } finally {
                Native.resultFree(result);
            }
        } finally {
            Native.queryFree(q);
        }
    }

    public QueryResult query(String cypher) {
        return query(cypher, null);
    }

    private static QueryResult collectResult(long result) {
        int columnCount = Native.resultColumnCount(result);
        String[] columns = new String[columnCount];
        for (int i = 0; i < columnCount; i++) {
            columns[i] = Native.resultColumnName(result, i);
        }
        List<Map<String, Object>> rows = new ArrayList<>();
        while (Native.resultNext(result)) {
            Map<String, Object> row = new LinkedHashMap<>();
            for (int i = 0; i < columnCount; i++) {
                row.put(columns[i], Native.resultGet(result, i));
            }
            rows.add(row);
        }
        return new QueryResult(List.of(columns), rows);
    }
}
