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
