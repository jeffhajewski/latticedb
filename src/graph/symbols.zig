//! Symbol Table for String Interning.
//!
//! Stores strings (labels, property keys, edge types) as compact u16 IDs.
//! Two B+Trees maintain the bidirectional mapping:
//!   - SYMBOLS: string -> symbol_id
//!   - SYMBOLS_REVERSE: symbol_id -> string
//!
//! Symbol ID ranges:
//!   - 0: Reserved (null/invalid)
//!   - 1-999: Reserved for system use
//!   - 1000-65535: User-defined symbols

const std = @import("std");
const lattice = @import("lattice");

const Allocator = std.mem.Allocator;

const btree = lattice.storage.btree;
const BTree = btree.BTree;
const BTreeError = btree.BTreeError;

/// Symbol identifier type
pub const SymbolId = u16;

/// Invalid/null symbol
pub const NULL_SYMBOL: SymbolId = 0;

/// First user-defined symbol ID
pub const USER_SYMBOL_START: SymbolId = 1000;

/// Maximum symbol ID
pub const MAX_SYMBOL_ID: SymbolId = 65535;

/// Symbol table errors
pub const SymbolError = error{
    /// Symbol table is full (65535 symbols)
    TableFull,
    /// Symbol not found
    NotFound,
    /// Invalid symbol ID
    InvalidSymbol,
    /// I/O error
    IoError,
    /// Out of memory
    OutOfMemory,
    /// B+Tree error
    BTreeError,
};

/// Symbol table for string interning
pub const SymbolTable = struct {
    allocator: Allocator,
    /// Forward mapping: string -> symbol_id
    forward: *BTree,
    /// Reverse mapping: symbol_id -> string
    reverse: *BTree,
    /// Next available symbol ID
    next_id: SymbolId,

    const Self = @This();

    /// Initialize symbol table with existing B+Trees
    pub fn init(allocator: Allocator, forward: *BTree, reverse: *BTree) Self {
        return Self{
            .allocator = allocator,
            .forward = forward,
            .reverse = reverse,
            .next_id = USER_SYMBOL_START,
        };
    }

    /// Get or create a symbol ID for a string
    /// Returns the existing ID if the string is already interned,
    /// or creates a new symbol and returns its ID
    pub fn intern(self: *Self, string: []const u8) SymbolError!SymbolId {
        // Check if already interned
        if (self.lookup(string)) |existing_id| {
            return existing_id;
        } else |err| {
            if (err != SymbolError.NotFound) return err;
        }

        // Allocate new symbol ID
        if (self.next_id == MAX_SYMBOL_ID) {
            return SymbolError.TableFull;
        }

        const new_id = self.next_id;
        self.next_id += 1;

        // Insert into forward table (string -> id)
        var id_bytes: [2]u8 = undefined;
        std.mem.writeInt(u16, &id_bytes, new_id, .little);

        self.forward.insert(string, &id_bytes) catch |err| {
            return mapBTreeError(err);
        };

        // Insert into reverse table (id -> string)
        self.reverse.insert(&id_bytes, string) catch |err| {
            return mapBTreeError(err);
        };

        return new_id;
    }

    /// Look up a symbol ID by string
    pub fn lookup(self: *Self, string: []const u8) SymbolError!SymbolId {
        const result = self.forward.get(string) catch |err| {
            return mapBTreeError(err);
        };

        if (result) |value| {
            if (value.len == 2) {
                return std.mem.readInt(u16, value[0..2], .little);
            }
        }

        return SymbolError.NotFound;
    }

    /// Resolve a symbol ID to its string
    pub fn resolve(self: *Self, id: SymbolId) SymbolError![]const u8 {
        if (id == NULL_SYMBOL) {
            return SymbolError.InvalidSymbol;
        }

        var id_bytes: [2]u8 = undefined;
        std.mem.writeInt(u16, &id_bytes, id, .little);

        const result = self.reverse.get(&id_bytes) catch |err| {
            return mapBTreeError(err);
        };

        if (result) |value| {
            // Copy to allocated memory since btree buffer is temporary
            const copy = self.allocator.alloc(u8, value.len) catch {
                return SymbolError.OutOfMemory;
            };
            @memcpy(copy, value);
            return copy;
        }

        return SymbolError.NotFound;
    }

    /// Check if a string is already interned
    pub fn contains(self: *Self, string: []const u8) bool {
        return if (self.lookup(string)) |_| true else |_| false;
    }

    /// Get the number of interned symbols
    pub fn count(self: *Self) u32 {
        if (self.next_id <= USER_SYMBOL_START) return 0;
        return @as(u32, self.next_id) - USER_SYMBOL_START;
    }

    /// Free a resolved string (allocated by resolve)
    pub fn freeString(self: *Self, string: []const u8) void {
        self.allocator.free(string);
    }
};

/// Map B+Tree errors to Symbol errors
fn mapBTreeError(err: BTreeError) SymbolError {
    return switch (err) {
        BTreeError.KeyNotFound => SymbolError.NotFound,
        BTreeError.OutOfMemory => SymbolError.OutOfMemory,
        else => SymbolError.BTreeError,
    };
}

// ============================================================================
// Tests
// ============================================================================

test "symbol table intern and lookup" {
    const allocator = std.testing.allocator;

    const vfs = @import("../storage/vfs.zig");
    const buffer_pool = @import("../storage/buffer_pool.zig");
    const page_manager = @import("../storage/page_manager.zig");

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_symbol_test.db";
    vfs_impl.delete(db_path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try buffer_pool.BufferPool.init(allocator, &pm, 64 * 4096);
    defer bp.deinit();

    // Create two B+Trees for symbol table
    var forward_tree = try BTree.init(allocator, &bp);

    var reverse_tree = try BTree.init(allocator, &bp);

    var symbols = SymbolTable.init(allocator, &forward_tree, &reverse_tree);

    // Intern some strings
    const id1 = try symbols.intern("Person");
    const id2 = try symbols.intern("name");
    const id3 = try symbols.intern("KNOWS");

    // IDs should be sequential starting from USER_SYMBOL_START
    try std.testing.expectEqual(USER_SYMBOL_START, id1);
    try std.testing.expectEqual(USER_SYMBOL_START + 1, id2);
    try std.testing.expectEqual(USER_SYMBOL_START + 2, id3);

    // Lookup should return same IDs
    try std.testing.expectEqual(id1, try symbols.lookup("Person"));
    try std.testing.expectEqual(id2, try symbols.lookup("name"));
    try std.testing.expectEqual(id3, try symbols.lookup("KNOWS"));

    // Interning again should return existing ID
    try std.testing.expectEqual(id1, try symbols.intern("Person"));

    // Resolve should return original strings
    const resolved = try symbols.resolve(id1);
    defer symbols.freeString(resolved);
    try std.testing.expectEqualStrings("Person", resolved);

    // Count should be 3
    try std.testing.expectEqual(@as(u32, 3), symbols.count());
}

test "symbol table not found" {
    const allocator = std.testing.allocator;

    const vfs = @import("../storage/vfs.zig");
    const buffer_pool = @import("../storage/buffer_pool.zig");
    const page_manager = @import("../storage/page_manager.zig");

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_symbol_notfound_test.db";
    vfs_impl.delete(db_path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try buffer_pool.BufferPool.init(allocator, &pm, 64 * 4096);
    defer bp.deinit();

    var forward_tree = try BTree.init(allocator, &bp);

    var reverse_tree = try BTree.init(allocator, &bp);

    var symbols = SymbolTable.init(allocator, &forward_tree, &reverse_tree);

    // Lookup non-existent symbol
    try std.testing.expectError(SymbolError.NotFound, symbols.lookup("nonexistent"));

    // Resolve invalid symbol
    try std.testing.expectError(SymbolError.InvalidSymbol, symbols.resolve(NULL_SYMBOL));
    try std.testing.expectError(SymbolError.NotFound, symbols.resolve(12345));
}
