package io.latticedb;

import java.util.Objects;

/**
 * Options for opening a database, mirroring the Go binding's OpenOptions and
 * the native {@code lattice_open_options_v5} struct. All values have sane
 * defaults; use the builder to override them.
 *
 * <p>A database file records its vector index: dimensions, metric, M and
 * ef_construction. A database that has one opens with it whether or not
 * {@link #enableVectors(boolean)} is set. Any of {@link #vectorMetric},
 * {@link #vectorM} and {@link #vectorEfConstruction} that is set, and
 * {@link #vectorDimensions} when vectors are enabled, must match the file, or
 * opening throws {@link ErrorCode#CONFIG_MISMATCH}.</p>
 */
public final class OpenOptions {
    private boolean create = false;
    private boolean readOnly = false;
    private int cacheSizeMB = 100;
    private int pageSize = 4096;
    private boolean enableVectors = false;
    private int vectorDimensions = 128;
    private boolean enableWal = true;
    private boolean enableAdjacencyCache = false;
    private boolean lock = true;
    private SyncMode syncMode = null;
    private Boolean fullFsync = null;
    private VectorMetric vectorMetric = null;
    private int vectorM = 0;
    private int vectorEfConstruction = 0;

    private OpenOptions() {
    }

    public static OpenOptions defaults() {
        return new OpenOptions();
    }

    public OpenOptions create(boolean create) {
        this.create = create;
        return this;
    }

    public OpenOptions readOnly(boolean readOnly) {
        this.readOnly = readOnly;
        return this;
    }

    public OpenOptions cacheSizeMB(int cacheSizeMB) {
        this.cacheSizeMB = cacheSizeMB;
        return this;
    }

    public OpenOptions pageSize(int pageSize) {
        this.pageSize = pageSize;
        return this;
    }

    /** Create a vector index if the database has none. A database that
     * already has one opens with it whatever this says. */
    public OpenOptions enableVectors(boolean enableVectors) {
        this.enableVectors = enableVectors;
        return this;
    }

    /** Vector dimensions when vectors are enabled, 1..4096. Default 128. It
     * must match the index the file records, if it has one. */
    public OpenOptions vectorDimensions(int vectorDimensions) {
        if (vectorDimensions < 1 || vectorDimensions > 4096) {
            throw new IllegalArgumentException("vectorDimensions must be in 1..4096");
        }
        this.vectorDimensions = vectorDimensions;
        return this;
    }

    /** Enable WAL-backed transactions. Default true. */
    public OpenOptions enableWal(boolean enableWal) {
        this.enableWal = enableWal;
        return this;
    }

    /**
     * Accepted for compatibility. Queries and the other methods here read edges
     * from storage either way, so it does not change their speed or results.
     * Default false.
     */
    public OpenOptions enableAdjacencyCache(boolean enableAdjacencyCache) {
        this.enableAdjacencyCache = enableAdjacencyCache;
        return this;
    }

    /**
     * Take a lock on the database file. Default true.
     *
     * <p>Turn this off only for filesystems where locking does not work; it
     * does not make concurrent access safe.</p>
     */
    public OpenOptions lock(boolean lock) {
        this.lock = lock;
        return this;
    }

    /**
     * When a commit waits for the disk. {@code null} (the default) uses the
     * {@code LATTICE_SYNC_MODE} environment variable, else
     * {@link SyncMode#FULL}.
     */
    public OpenOptions syncMode(SyncMode syncMode) {
        this.syncMode = syncMode;
        return this;
    }

    /**
     * On macOS, whether syncs ask the drive to flush its cache
     * ({@code F_FULLFSYNC}). No effect elsewhere. {@code null} (the default)
     * uses the {@code LATTICE_FULL_FSYNC} environment variable, else off.
     */
    public OpenOptions fullFsync(Boolean fullFsync) {
        this.fullFsync = fullFsync;
        return this;
    }

    /** How a new vector index measures distance. {@code null} (the default)
     * means the file's metric, or cosine for a new index. */
    public OpenOptions vectorMetric(VectorMetric vectorMetric) {
        this.vectorMetric = vectorMetric;
        return this;
    }

    /** HNSW connections per node for a new index, 2..32, or 0 (the default)
     * for the file's, or 16. */
    public OpenOptions vectorM(int vectorM) {
        if (vectorM != 0 && (vectorM < 2 || vectorM > 32)) {
            throw new IllegalArgumentException("vectorM must be 0 or in 2..32");
        }
        this.vectorM = vectorM;
        return this;
    }

    /** HNSW candidate list size while building a new index, 1..4096, or 0
     * (the default) for the file's, or 200. */
    public OpenOptions vectorEfConstruction(int vectorEfConstruction) {
        if (vectorEfConstruction < 0 || vectorEfConstruction > 4096) {
            throw new IllegalArgumentException("vectorEfConstruction must be in 0..4096");
        }
        this.vectorEfConstruction = vectorEfConstruction;
        return this;
    }

    boolean create() { return create; }
    boolean readOnly() { return readOnly; }
    int cacheSizeMB() { return cacheSizeMB; }
    int pageSize() { return pageSize; }
    boolean enableVectors() { return enableVectors; }
    int vectorDimensions() { return vectorDimensions; }
    boolean enableWal() { return enableWal; }
    boolean enableAdjacencyCache() { return enableAdjacencyCache; }
    boolean lock() { return lock; }
    int syncModeCode() { return syncMode == null ? 0 : syncMode.code(); }
    int fullFsyncCode() { return fullFsync == null ? 0 : (fullFsync ? 2 : 1); }
    int vectorMetricCode() { return vectorMetric == null ? 0 : vectorMetric.code(); }
    int vectorM() { return vectorM; }
    int vectorEfConstruction() { return vectorEfConstruction; }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof OpenOptions other)) return false;
        return create == other.create && readOnly == other.readOnly
                && cacheSizeMB == other.cacheSizeMB && pageSize == other.pageSize
                && enableVectors == other.enableVectors
                && vectorDimensions == other.vectorDimensions
                && enableWal == other.enableWal
                && enableAdjacencyCache == other.enableAdjacencyCache
                && lock == other.lock
                && syncMode == other.syncMode
                && Objects.equals(fullFsync, other.fullFsync)
                && vectorMetric == other.vectorMetric
                && vectorM == other.vectorM
                && vectorEfConstruction == other.vectorEfConstruction;
    }

    @Override
    public int hashCode() {
        return Objects.hash(create, readOnly, cacheSizeMB, pageSize, enableVectors,
                vectorDimensions, enableWal, enableAdjacencyCache, lock, syncMode,
                fullFsync, vectorMetric, vectorM, vectorEfConstruction);
    }
}
