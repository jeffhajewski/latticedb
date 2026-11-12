package io.latticedb;

/** When a commit waits for the disk ({@code lattice_sync_mode}). */
public enum SyncMode {
    /** Sync the log on every commit: a commit that has returned survives a
     * power cut. */
    FULL(1),
    /** Hand the commit to the operating system and return. It survives the
     * process being killed; a power cut or OS crash can lose the most recent
     * commits, from the end only, and the database stays consistent. */
    NORMAL(2);

    private final int code;

    SyncMode(int code) {
        this.code = code;
    }

    /** The native {@code lattice_sync_mode} value. */
    public int code() {
        return code;
    }
}
