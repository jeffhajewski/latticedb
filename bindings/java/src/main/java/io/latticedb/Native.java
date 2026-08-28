package io.latticedb;

/**
 * Native entry points. Package-private: all native methods live here so JNI
 * symbol names stay uniform and the public API classes remain free of
 * {@code native} declarations.
 */
final class Native {
    private Native() {
    }

    static {
        NativeLoader.load();
    }

    /* Database lifecycle */
    static native String version();

    static native long open(String path, boolean create, boolean readOnly,
                            int cacheSizeMB, int pageSize, boolean enableVector,
                            int vectorDimensions, boolean enableWal,
                            boolean enableAdjacencyCache, boolean lock);

    static native byte[] serialize(long db);

    static native long deserialize(byte[] bytes, int cacheSizeMB, int pageSize,
                                   boolean enableVector, int vectorDimensions,
                                   boolean enableWal, boolean enableAdjacencyCache,
                                   boolean lock);

    static native void close(long db);

    static native long begin(long db, boolean readOnly);

    static native void commit(long txn);

    static native void rollback(long txn);

    /* Nodes */
    static native long nodeCreate(long txn, String label);

    static native void nodeAddLabel(long txn, long nodeId, String label);

    static native void nodeRemoveLabel(long txn, long nodeId, String label);

    static native void nodeDelete(long txn, long nodeId);

    static native void nodeSetProperty(long txn, long nodeId, String key, Object value);

    static native Object nodeGetProperty(long txn, long nodeId, String key);

    static native boolean nodeExists(long txn, long nodeId);

    static native String[] nodeGetLabels(long txn, long nodeId);

    static native long[] getNodesByLabelTxn(long txn, String label);

    static native long[] getAllNodesTxn(long txn);

    static native long[] getNodesByLabel(long db, String label);

    static native void createNodePropertyIndex(long db, String label, String property);

    static native void dropNodePropertyIndex(long db, String label, String property);

    static native long[] findNodesByLabelProperty(long txn, String label,
                                                  String property, Object value, int limit);

    static native void nodeSetVector(long txn, long nodeId, String key, float[] vector);

    static native long[] batchInsert(long txn, String label, float[][] vectors);

    /* Edges */
    static native long edgeCreate(long txn, long source, long target, String edgeType);

    static native void edgeDelete(long txn, long source, long target, String edgeType);

    static native void edgeSetProperty(long txn, long edgeId, String key, Object value);

    static native Object edgeGetProperty(long txn, long edgeId, String key);

    static native void edgeRemoveProperty(long txn, long edgeId, String key);

    static native void createEdgePropertyIndex(long db, String edgeType, String property);

    static native void dropEdgePropertyIndex(long db, String edgeType, String property);

    static native long[] findEdgesByTypeProperty(long txn, String edgeType,
                                                 String property, Object value, int limit);

    /** Returns Object[2]: long[]{id,source,target}*n and String[] types. */
    static native Object[] edgesOutgoing(long txn, long nodeId);

    static native Object[] edgesIncoming(long txn, long nodeId);

    static native Object[] edgesOutgoingByType(long txn, long nodeId, String edgeType, int limit);

    static native Object[] edgesIncomingByType(long txn, long nodeId, String edgeType, int limit);

    static native Object[] edgesScan(long txn, String edgeType, int limit);

    /* Vector search */
    /** Returns Object[2]: long[] nodeIds, float[] distances. */
    static native Object[] vectorSearch(long db, float[] vector, int k, int efSearch);

    static native Object[] vectorSearchTxn(long txn, float[] vector, int k, int efSearch);

    /* Full-text search */
    static native void createNodeFtsIndex(long db, String label, String property);

    static native void dropNodeFtsIndex(long db, String label, String property);

    static native boolean hasNodeFtsIndex(long db, String label, String property);

    /** Returns Object[2]: long[] nodeIds, float[] scores. */
    static native Object[] ftsSearch(long db, String label, String property,
                                     String query, int limit);

    static native Object[] ftsSearchFuzzy(long db, String label, String property,
                                          String query, int limit,
                                          int maxDistance, int minTermLength);

    static native Object[] ftsSearchTxn(long txn, String label, String property,
                                        String query, int limit);

    static native Object[] ftsSearchFuzzyTxn(long txn, String label, String property,
                                             String query, int limit,
                                             int maxDistance, int minTermLength);

    /* Streams */
    static native void streamPublish(long txn, String stream, String kind, Object payload);

    static native long streamPublishGetSequence(long txn, String stream, String kind,
                                                Object payload);

    /** Returns Object[3]: long[] sequences, String[] kinds, Object[] payloads. */
    static native Object[] streamRead(long db, String stream, long afterSequence,
                                      int limit, int timeoutMs);

    /** Returns Object[2]: Long offset (nullable), Boolean exists. */
    static native Object[] streamGetOffset(long db, String stream, String consumer);

    static native long streamGetLastSequence(long db, String stream);

    static native void streamSetOffset(long txn, String stream, String consumer, long sequence);

    static native void streamTrim(long txn, String stream, long throughSequence);

    /* Query engine */
    static native long queryPrepare(long db, String cypher);

    static native void queryBind(long query, String name, Object value);

    static native void queryBindVector(long query, String name, float[] vector);

    static native long queryExecute(long query, long txn);

    static native boolean queryWrites(long db, String cypher);

    static native boolean resultNext(long result);

    static native int resultColumnCount(long result);

    static native String resultColumnName(long result, int index);

    static native Object resultGet(long result, int index);

    static native void resultFree(long result);

    static native void queryFree(long query);

    /* Query cache */
    static native void cacheClear(long db);

    /** Returns Object[3]: Integer entries, Long hits, Long misses. */
    static native Object[] cacheStats(long db);

    /* Embedding helpers */
    static native float[] hashEmbed(String text, int dimensions);

    static native long embeddingClientCreate(String endpoint, String model,
                                             int apiFormat, String apiKey, int timeoutMs);

    static native float[] embeddingClientEmbed(long client, String text);

    static native void embeddingClientFree(long client);
}
