package io.latticedb;

/**
 * What a checkpoint did ({@code lattice_checkpoint_stats}).
 *
 * @param pagesFlushed  pages written into the database file
 * @param durationNs    how long the checkpoint took, in nanoseconds
 * @param checkpointLsn the log position the database file now holds
 * @param walTruncated  whether the log (the {@code -wal} file) was emptied
 */
public record CheckpointStats(int pagesFlushed, long durationNs, long checkpointLsn,
                              boolean walTruncated) {
}
