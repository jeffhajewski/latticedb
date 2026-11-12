//! The durability settings an open passes take precedence over the
//! environment variables that stand in for them.

const std = @import("std");
const lattice = @import("lattice");

const DatabaseConfig = lattice.storage.database.DatabaseConfig;

test "lfmt: an explicit sync setting wins over the environment" {
    const setenv = struct {
        extern "c" fn setenv(name: [*:0]const u8, value: [*:0]const u8, overwrite: c_int) c_int;
        extern "c" fn unsetenv(name: [*:0]const u8) c_int;
    };
    _ = setenv.setenv("LATTICE_SYNC_MODE", "normal", 1);
    _ = setenv.setenv("LATTICE_FULL_FSYNC", "1", 1);
    defer _ = setenv.unsetenv("LATTICE_SYNC_MODE");
    defer _ = setenv.unsetenv("LATTICE_FULL_FSYNC");

    const unset = DatabaseConfig{};
    try std.testing.expectEqual(DatabaseConfig.SyncMode.normal, unset.effectiveSyncMode());
    try std.testing.expect(unset.effectiveFullFsync());

    const explicit = DatabaseConfig{ .sync_mode = .full, .full_fsync = false };
    try std.testing.expectEqual(DatabaseConfig.SyncMode.full, explicit.effectiveSyncMode());
    try std.testing.expect(!explicit.effectiveFullFsync());

    _ = setenv.unsetenv("LATTICE_SYNC_MODE");
    _ = setenv.unsetenv("LATTICE_FULL_FSYNC");
    try std.testing.expectEqual(DatabaseConfig.SyncMode.full, unset.effectiveSyncMode());
    try std.testing.expect(!unset.effectiveFullFsync());
}

test "lfmt: an explicit buffer pool size wins over LATTICE_BUFFER_POOL_MB, which is the default" {
    const env = struct {
        extern "c" fn setenv(name: [*:0]const u8, value: [*:0]const u8, overwrite: c_int) c_int;
        extern "c" fn unsetenv(name: [*:0]const u8) c_int;
    };
    const mib: usize = 1024 * 1024;
    _ = env.setenv("LATTICE_BUFFER_POOL_MB", "3", 1);
    defer _ = env.unsetenv("LATTICE_BUFFER_POOL_MB");

    const explicit = DatabaseConfig{ .buffer_pool_size = 8 * mib };
    try std.testing.expectEqual(8 * mib, explicit.effectiveBufferPoolSize());
    const unset = DatabaseConfig{ .enable_fts = false };
    try std.testing.expectEqual(3 * mib, unset.effectiveBufferPoolSize());

    // A value that is not a positive number of megabytes is ignored.
    _ = env.setenv("LATTICE_BUFFER_POOL_MB", "lots", 1);
    try std.testing.expectEqual(16 * mib, unset.effectiveBufferPoolSize());
    _ = env.unsetenv("LATTICE_BUFFER_POOL_MB");
    try std.testing.expectEqual(16 * mib, unset.effectiveBufferPoolSize());
    try std.testing.expectEqual(8 * mib, explicit.effectiveBufferPoolSize());
}
