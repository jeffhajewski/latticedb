//! The durability settings an open passes take precedence over the
//! environment variables that stand in for them.

const std = @import("std");
const lattice = @import("lattice");

const DatabaseConfig = lattice.storage.database.DatabaseConfig;
const env = @import("compat").env;

test "lfmt: an explicit sync setting wins over the environment" {
    env.set("LATTICE_SYNC_MODE", "normal");
    env.set("LATTICE_FULL_FSYNC", "1");
    defer env.unset("LATTICE_SYNC_MODE");
    defer env.unset("LATTICE_FULL_FSYNC");

    const unset = DatabaseConfig{};
    try std.testing.expectEqual(DatabaseConfig.SyncMode.normal, unset.effectiveSyncMode());
    try std.testing.expect(unset.effectiveFullFsync());

    const explicit = DatabaseConfig{ .sync_mode = .full, .full_fsync = false };
    try std.testing.expectEqual(DatabaseConfig.SyncMode.full, explicit.effectiveSyncMode());
    try std.testing.expect(!explicit.effectiveFullFsync());

    env.unset("LATTICE_SYNC_MODE");
    env.unset("LATTICE_FULL_FSYNC");
    try std.testing.expectEqual(DatabaseConfig.SyncMode.full, unset.effectiveSyncMode());
    try std.testing.expect(!unset.effectiveFullFsync());
}

test "lfmt: an explicit buffer pool size wins over LATTICE_BUFFER_POOL_MB, which is the default" {
    const mib: usize = 1024 * 1024;
    env.set("LATTICE_BUFFER_POOL_MB", "3");
    defer env.unset("LATTICE_BUFFER_POOL_MB");

    const explicit = DatabaseConfig{ .buffer_pool_size = 8 * mib };
    try std.testing.expectEqual(8 * mib, explicit.effectiveBufferPoolSize());
    const unset = DatabaseConfig{ .enable_fts = false };
    try std.testing.expectEqual(3 * mib, unset.effectiveBufferPoolSize());

    // A value that is not a positive number of megabytes is ignored.
    env.set("LATTICE_BUFFER_POOL_MB", "lots");
    try std.testing.expectEqual(16 * mib, unset.effectiveBufferPoolSize());
    env.unset("LATTICE_BUFFER_POOL_MB");
    try std.testing.expectEqual(16 * mib, unset.effectiveBufferPoolSize());
    try std.testing.expectEqual(8 * mib, explicit.effectiveBufferPoolSize());
}
