package io.latticedb;

/**
 * How {@link Database#checkpoint(CheckpointMode)} folds the log (the
 * {@code -wal} file) into the database file ({@code lattice_checkpoint_mode}).
 */
public enum CheckpointMode {
    /** Write the changed pages unless one is in use, in which case nothing is
     * written and the stats report 0 pages. Leaves the log in place. */
    PASSIVE(0),
    /** Write every changed page. Leaves the log in place, so anything reading
     * it keeps its position. */
    FULL(1),
    /** {@link #FULL}, then empty the log. The only mode that bounds the log's
     * size, and what automatic checkpoints use. */
    TRUNCATE(2);

    private final int code;

    CheckpointMode(int code) {
        this.code = code;
    }

    /** The native {@code lattice_checkpoint_mode} value. */
    public int code() {
        return code;
    }
}
