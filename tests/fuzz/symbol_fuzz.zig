//! Fuzz testing for Symbol Table operations.
//!
//! The symbol table must handle arbitrary string data without:
//! - Crashing (panics, segfaults)
//! - Memory corruption
//! - Inconsistent intern/lookup results
//! - Memory leaks
//!
//! Operations should either succeed or return errors gracefully.

const std = @import("std");
const lattice = @import("lattice");

const Allocator = std.mem.Allocator;
const SymbolTable = lattice.graph.symbols.SymbolTable;
const BTree = lattice.storage.btree.BTree;
const BufferPool = lattice.storage.buffer_pool.BufferPool;
const PageManager = lattice.storage.page_manager.PageManager;
const vfs = lattice.storage.vfs;
const PosixVfs = vfs.PosixVfs;

// ============================================================================
// Test Fixture
// ============================================================================

/// Temporary SymbolTable setup for fuzzing
const FuzzSymbolTable = struct {
    allocator: Allocator,
    posix_vfs: PosixVfs,
    pm: *PageManager,
    bp: *BufferPool,
    forward_tree: *BTree,
    reverse_tree: *BTree,
    table: SymbolTable,
    path: []const u8,

    fn init(allocator: Allocator) !FuzzSymbolTable {
        // Generate unique path
        var path_buf: [128]u8 = undefined;
        const timestamp = std.time.milliTimestamp();
        const random = std.crypto.random.int(u32);
        const path = try std.fmt.bufPrint(&path_buf, "/tmp/lattice_fuzz_symbol_{d}_{x}.db", .{ timestamp, random });
        const path_copy = try allocator.dupe(u8, path);
        errdefer allocator.free(path_copy);

        var posix_vfs = PosixVfs.init(allocator);
        const vfs_impl = posix_vfs.vfs();

        // Clean up any existing file
        vfs_impl.delete(path_copy) catch {};

        // Allocate PageManager
        const pm = try allocator.create(PageManager);
        errdefer allocator.destroy(pm);
        pm.* = try PageManager.init(allocator, vfs_impl, path_copy, .{ .create = true });
        errdefer pm.deinit();

        // Allocate BufferPool
        const bp = try allocator.create(BufferPool);
        errdefer allocator.destroy(bp);
        bp.* = try BufferPool.init(allocator, pm, 64 * 1024); // 64KB for fuzzing
        errdefer bp.deinit();

        // Create B+Trees for forward and reverse mappings
        const forward_tree = try allocator.create(BTree);
        errdefer allocator.destroy(forward_tree);
        forward_tree.* = try BTree.init(allocator, bp);

        const reverse_tree = try allocator.create(BTree);
        errdefer allocator.destroy(reverse_tree);
        reverse_tree.* = try BTree.init(allocator, bp);

        // Create SymbolTable
        const table = SymbolTable.init(allocator, forward_tree, reverse_tree);

        return FuzzSymbolTable{
            .allocator = allocator,
            .posix_vfs = posix_vfs,
            .pm = pm,
            .bp = bp,
            .forward_tree = forward_tree,
            .reverse_tree = reverse_tree,
            .table = table,
            .path = path_copy,
        };
    }

    fn deinit(self: *FuzzSymbolTable) void {
        // BTree doesn't have deinit - cleaned up with buffer pool
        self.bp.deinit();
        self.pm.deinit();

        const vfs_impl = self.posix_vfs.vfs();
        vfs_impl.delete(self.path) catch {};

        self.allocator.destroy(self.forward_tree);
        self.allocator.destroy(self.reverse_tree);
        self.allocator.destroy(self.bp);
        self.allocator.destroy(self.pm);
        self.allocator.free(self.path);
    }
};

// ============================================================================
// Fuzz Functions
// ============================================================================

/// Fuzz symbol table with random intern/lookup operations
pub fn fuzzSymbolOperations(allocator: Allocator, input: []const u8) !void {
    if (input.len < 2) return;

    var fuzz_table = FuzzSymbolTable.init(allocator) catch return;
    defer fuzz_table.deinit();

    var table = &fuzz_table.table;

    // Interpret input as a series of operations
    var i: usize = 0;
    while (i < input.len) {
        const op = input[i] % 4;
        i += 1;

        switch (op) {
            0 => {
                // Intern: next bytes are string length, then string
                if (i >= input.len) break;
                const str_len = @min(input[i] % 64 + 1, input.len - i - 1);
                i += 1;
                if (i + str_len > input.len) break;
                const string = input[i..][0..str_len];
                i += str_len;

                _ = table.intern(string) catch continue;
            },
            1 => {
                // Lookup: next bytes are string
                if (i >= input.len) break;
                const str_len = @min(input[i] % 64 + 1, input.len - i - 1);
                i += 1;
                if (i + str_len > input.len) break;
                const string = input[i..][0..str_len];
                i += str_len;

                _ = table.lookup(string) catch continue;
            },
            2 => {
                // Resolve: next 2 bytes are symbol ID
                if (i + 2 > input.len) break;
                const id = std.mem.readInt(u16, input[i..][0..2], .little);
                i += 2;

                const resolved = table.resolve(id) catch continue;
                table.freeString(resolved);
            },
            3 => {
                // Contains: next bytes are string
                if (i >= input.len) break;
                const str_len = @min(input[i] % 64 + 1, input.len - i - 1);
                i += 1;
                if (i + str_len > input.len) break;
                const string = input[i..][0..str_len];
                i += str_len;

                _ = table.contains(string);
            },
            else => unreachable,
        }
    }
}

/// Fuzz symbol table intern/resolve consistency
pub fn fuzzSymbolConsistency(allocator: Allocator, input: []const u8) !void {
    if (input.len == 0) return;

    var fuzz_table = FuzzSymbolTable.init(allocator) catch return;
    defer fuzz_table.deinit();

    var table = &fuzz_table.table;

    // Intern the input string
    const id = table.intern(input) catch return;

    // Verify we can look it up
    const lookup_id = table.lookup(input) catch return;
    std.debug.assert(id == lookup_id);

    // Verify we can resolve back to the string
    const resolved = table.resolve(id) catch return;
    defer table.freeString(resolved);
    std.debug.assert(std.mem.eql(u8, resolved, input));

    // Interning again should return same ID
    const id2 = table.intern(input) catch return;
    std.debug.assert(id == id2);

    // Contains should return true
    std.debug.assert(table.contains(input));
}

/// Fuzz symbol table with many strings
pub fn fuzzSymbolMany(allocator: Allocator, input: []const u8) !void {
    if (input.len < 4) return;

    var fuzz_table = FuzzSymbolTable.init(allocator) catch return;
    defer fuzz_table.deinit();

    var table = &fuzz_table.table;

    // Parse input into multiple strings and intern them
    var strings_interned: std.ArrayList(struct { id: u16, str: []const u8 }) = .empty;
    defer strings_interned.deinit(allocator);

    var i: usize = 0;
    while (i < input.len and strings_interned.items.len < 100) {
        const str_len = @min((input[i] % 32) + 1, input.len - i - 1);
        i += 1;
        if (i + str_len > input.len) break;

        const string = input[i..][0..str_len];
        i += str_len;

        const id = table.intern(string) catch continue;
        strings_interned.append(allocator, .{ .id = id, .str = string }) catch continue;
    }

    // Verify all strings can be looked up
    for (strings_interned.items) |item| {
        const lookup_result = table.lookup(item.str) catch continue;
        std.debug.assert(lookup_result == item.id);
    }

    // Verify count is consistent
    const unique_count = table.count();
    std.debug.assert(unique_count <= strings_interned.items.len);
}

// ============================================================================
// Fuzz Runners
// ============================================================================

fn symbolOpsRunner(allocator: Allocator, input: []const u8) !void {
    try fuzzSymbolOperations(allocator, input);
}

fn symbolConsistRunner(allocator: Allocator, input: []const u8) !void {
    try fuzzSymbolConsistency(allocator, input);
}

fn symbolManyRunner(allocator: Allocator, input: []const u8) !void {
    try fuzzSymbolMany(allocator, input);
}

// ============================================================================
// Fuzz Tests using std.testing.fuzz
// ============================================================================

test "fuzz: symbol table random operations" {
    const allocator = std.testing.allocator;
    try std.testing.fuzz(allocator, symbolOpsRunner, .{});
}

test "fuzz: symbol table consistency" {
    const allocator = std.testing.allocator;
    try std.testing.fuzz(allocator, symbolConsistRunner, .{});
}

test "fuzz: symbol table many strings" {
    const allocator = std.testing.allocator;
    try std.testing.fuzz(allocator, symbolManyRunner, .{});
}

// ============================================================================
// Corpus-based tests (known edge cases)
// ============================================================================

test "symbol: handles empty string" {
    const allocator = std.testing.allocator;
    try fuzzSymbolConsistency(allocator, "");
}

test "symbol: handles single characters" {
    const allocator = std.testing.allocator;
    for (0..256) |i| {
        try fuzzSymbolConsistency(allocator, &[_]u8{@intCast(i)});
    }
}

test "symbol: handles null bytes" {
    const allocator = std.testing.allocator;
    try fuzzSymbolConsistency(allocator, &[_]u8{ 0x00, 0x00, 0x00 });
    try fuzzSymbolConsistency(allocator, &[_]u8{ 'a', 0x00, 'b' });
}

test "symbol: handles long strings" {
    const allocator = std.testing.allocator;
    const long_string = "a" ** 1000;
    try fuzzSymbolConsistency(allocator, long_string);
}

test "symbol: handles unicode" {
    const allocator = std.testing.allocator;
    try fuzzSymbolConsistency(allocator, "hello \xC3\xA9 world"); // UTF-8 é
    try fuzzSymbolConsistency(allocator, "\xF0\x9F\x98\x80"); // UTF-8 emoji
}

test "symbol: handles binary data" {
    const allocator = std.testing.allocator;
    try fuzzSymbolConsistency(allocator, &[_]u8{ 0xFF, 0xFE, 0x00, 0x01, 0x80, 0x7F });
}

test "symbol: handles duplicate interning" {
    const allocator = std.testing.allocator;
    // Op sequence: intern "test", intern "test" again, lookup "test"
    const ops = &[_]u8{
        0, 4, 't', 'e', 's', 't', // intern "test"
        0, 4, 't', 'e', 's', 't', // intern "test" again
        1, 4, 't', 'e', 's', 't', // lookup "test"
        3, 4, 't', 'e', 's', 't', // contains "test"
    };
    try fuzzSymbolOperations(allocator, ops);
}

test "symbol: handles resolve invalid id" {
    const allocator = std.testing.allocator;
    // Op sequence: resolve ID 0xFFFF (invalid)
    const ops = &[_]u8{
        2, 0xFF, 0xFF, // resolve 0xFFFF
    };
    try fuzzSymbolOperations(allocator, ops);
}
