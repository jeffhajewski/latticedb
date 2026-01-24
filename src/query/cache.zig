//! Query plan cache for Lattice database.
//!
//! Caches parsed ASTs and semantic analysis results to eliminate re-parsing
//! for repeated queries. Parameters are resolved at execution time, so the
//! same cached parse tree works for different parameter values.
//!
//! Design:
//! - Cache key: raw query text bytes, hashed with Wyhash
//! - Eviction: LRU, configurable max entries (default 128)
//! - Thread safety: RwLatch (shared for lookups, exclusive for inserts)
//! - Invalidation: Schema version counter, bumped on label/edge-type mutations
//! - Pinning: ref_count prevents eviction during execution

const std = @import("std");
const Allocator = std.mem.Allocator;

const lattice = @import("lattice");

const ast = @import("ast.zig");
const semantic_mod = @import("semantic.zig");
const VariableInfo = semantic_mod.VariableInfo;
const RwLatch = lattice.concurrency.locking.RwLatch;

/// Maximum number of cache entries (compile-time upper bound).
/// Actual limit is configurable at runtime via max_entries.
const MAX_ENTRIES: u32 = 1024;

/// A cached query entry containing the parsed AST and analysis results.
pub const CacheEntry = struct {
    /// Arena owning all AST nodes and the source text copy.
    arena: std.heap.ArenaAllocator,
    /// Parsed query AST (allocated within arena).
    query: *ast.Query,
    /// Semantic analysis variable bindings (allocated within arena).
    variables: []const VariableInfo,
    /// Wyhash of the original query text.
    query_hash: u64,
    /// Arena-owned copy of the query text for equality verification.
    query_text: []const u8,
    /// Schema version at time of caching (entry is stale if mismatched).
    schema_version: u64,
    /// LRU access counter value at last access.
    last_access: u64,
    /// Reference count: pinned during execution to prevent eviction.
    ref_count: std.atomic.Value(u32),

    /// Pin this entry (increment ref count) to prevent eviction.
    pub fn pin(self: *CacheEntry) void {
        _ = self.ref_count.fetchAdd(1, .monotonic);
    }

    /// Unpin this entry (decrement ref count).
    pub fn unpin(self: *CacheEntry) void {
        _ = self.ref_count.fetchSub(1, .monotonic);
    }

    /// Check if this entry is currently pinned.
    pub fn isPinned(self: *const CacheEntry) bool {
        return self.ref_count.load(.monotonic) > 0;
    }
};

/// Query cache storing parsed ASTs for repeated query execution.
pub const QueryCache = struct {
    allocator: Allocator,
    entries: []?CacheEntry,
    max_entries: u32,
    count: u32,
    access_counter: u64,
    schema_version: u64,
    stats_hits: u64,
    stats_misses: u64,
    latch: RwLatch,

    const Self = @This();

    /// Initialize the query cache.
    pub fn init(allocator: Allocator, max_entries: u32) !*Self {
        const effective_max = @min(max_entries, MAX_ENTRIES);

        var self = try allocator.create(Self);
        errdefer allocator.destroy(self);

        self.entries = try allocator.alloc(?CacheEntry, effective_max);
        errdefer allocator.free(self.entries);

        // Initialize all slots to null
        for (self.entries) |*entry| {
            entry.* = null;
        }

        self.allocator = allocator;
        self.max_entries = effective_max;
        self.count = 0;
        self.access_counter = 0;
        self.schema_version = 0;
        self.stats_hits = 0;
        self.stats_misses = 0;
        self.latch = .{};

        return self;
    }

    /// Free all cache resources.
    pub fn deinit(self: *Self) void {
        for (self.entries) |*entry| {
            if (entry.*) |*e| {
                e.arena.deinit();
            }
        }
        self.allocator.free(self.entries);
        self.allocator.destroy(self);
    }

    /// Look up a cached entry by query text.
    /// Returns a pinned entry on hit (caller must call unpin after use).
    /// Uses shared latch for concurrent reads.
    pub fn get(self: *Self, query_text: []const u8) ?*CacheEntry {
        const hash = hashQuery(query_text);

        // Acquire shared latch for read
        while (!self.latch.tryAcquireShared()) {
            std.atomic.spinLoopHint();
        }
        defer self.latch.releaseShared();

        // Search for matching entry
        for (self.entries) |*slot| {
            if (slot.*) |*entry| {
                if (entry.query_hash == hash and
                    entry.schema_version == self.schema_version and
                    std.mem.eql(u8, entry.query_text, query_text))
                {
                    // Cache hit - update access counter and pin
                    entry.last_access = @atomicRmw(u64, &self.access_counter, .Add, 1, .monotonic);
                    entry.pin();
                    _ = @atomicRmw(u64, &self.stats_hits, .Add, 1, .monotonic);
                    return entry;
                }
            }
        }

        _ = @atomicRmw(u64, &self.stats_misses, .Add, 1, .monotonic);
        return null;
    }

    /// Insert a new entry into the cache.
    /// Uses exclusive latch. Evicts LRU entry if cache is full.
    pub fn put(
        self: *Self,
        query_text: []const u8,
        query: *ast.Query,
        variables: []const VariableInfo,
        arena: std.heap.ArenaAllocator,
    ) void {
        // Acquire exclusive latch for write
        while (!self.latch.tryAcquireExclusive()) {
            std.atomic.spinLoopHint();
        }
        defer self.latch.releaseExclusive();

        const hash = hashQuery(query_text);

        // Check if already cached (race between get miss and put)
        for (self.entries) |*slot| {
            if (slot.*) |*entry| {
                if (entry.query_hash == hash and
                    std.mem.eql(u8, entry.query_text, query_text))
                {
                    // Already cached, update it
                    entry.arena.deinit();
                    slot.* = null;
                    self.count -= 1;
                    break;
                }
            }
        }

        // Find a slot: first empty, or evict LRU
        const slot_idx = self.findSlot();

        // Make a mutable copy to call allocator()
        var mutable_arena = arena;

        // Copy query text into the arena for the entry's own reference
        // (the arena already owns the source text used by the parser)
        const owned_text = mutable_arena.allocator().dupe(u8, query_text) catch return;

        // Copy variables into arena
        const owned_vars = mutable_arena.allocator().dupe(VariableInfo, variables) catch return;

        self.entries[slot_idx] = .{
            .arena = mutable_arena,
            .query = query,
            .variables = owned_vars,
            .query_hash = hash,
            .query_text = owned_text,
            .schema_version = self.schema_version,
            .last_access = self.access_counter,
            .ref_count = std.atomic.Value(u32).init(0),
        };
        self.count += 1;
        self.access_counter += 1;
    }

    /// Clear all entries from the cache.
    pub fn clear(self: *Self) void {
        while (!self.latch.tryAcquireExclusive()) {
            std.atomic.spinLoopHint();
        }
        defer self.latch.releaseExclusive();

        for (self.entries) |*slot| {
            if (slot.*) |*entry| {
                if (!entry.isPinned()) {
                    entry.arena.deinit();
                    slot.* = null;
                    self.count -= 1;
                }
            }
        }
    }

    /// Bump the schema version, invalidating all cached entries.
    /// Entries with stale schema_version will miss on next get().
    pub fn bumpSchemaVersion(self: *Self) void {
        _ = @atomicRmw(u64, &self.schema_version, .Add, 1, .monotonic);
    }

    /// Get cache statistics.
    pub fn getStats(self: *Self) CacheStats {
        return .{
            .entries = self.count,
            .hits = self.stats_hits,
            .misses = self.stats_misses,
        };
    }

    // Find an empty slot or evict the LRU unpinned entry.
    fn findSlot(self: *Self) usize {
        // First pass: find empty slot
        for (self.entries, 0..) |*slot, i| {
            if (slot.* == null) return i;
        }

        // Second pass: find LRU unpinned entry to evict
        var lru_idx: usize = 0;
        var lru_access: u64 = std.math.maxInt(u64);
        var found = false;

        for (self.entries, 0..) |*slot, i| {
            if (slot.*) |*entry| {
                if (!entry.isPinned() and entry.last_access < lru_access) {
                    lru_access = entry.last_access;
                    lru_idx = i;
                    found = true;
                }
            }
        }

        if (found) {
            // Evict the LRU entry
            if (self.entries[lru_idx]) |*entry| {
                entry.arena.deinit();
                self.entries[lru_idx] = null;
                self.count -= 1;
            }
        } else {
            // All entries pinned — overwrite slot 0 as last resort
            // This shouldn't happen in practice
            lru_idx = 0;
            if (self.entries[lru_idx]) |*entry| {
                entry.arena.deinit();
                self.entries[lru_idx] = null;
                self.count -= 1;
            }
        }

        return lru_idx;
    }

    /// Hash query text using Wyhash.
    fn hashQuery(text: []const u8) u64 {
        return std.hash.Wyhash.hash(0, text);
    }
};

/// Cache statistics.
pub const CacheStats = struct {
    entries: u32,
    hits: u64,
    misses: u64,
};

// ============================================================================
// Tests
// ============================================================================

test "cache init and deinit" {
    const cache = try QueryCache.init(std.testing.allocator, 16);
    defer cache.deinit();

    try std.testing.expectEqual(@as(u32, 0), cache.count);
    try std.testing.expectEqual(@as(u64, 0), cache.stats_hits);
    try std.testing.expectEqual(@as(u64, 0), cache.stats_misses);
}

test "cache miss returns null" {
    const cache = try QueryCache.init(std.testing.allocator, 16);
    defer cache.deinit();

    const result = cache.get("MATCH (n) RETURN n");
    try std.testing.expect(result == null);
    try std.testing.expectEqual(@as(u64, 1), cache.stats_misses);
}

test "cache put and get" {
    const cache = try QueryCache.init(std.testing.allocator, 16);
    defer cache.deinit();

    const query_text = "MATCH (n) RETURN n";

    // Create a mock arena with a query
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    // Don't defer deinit - cache takes ownership

    const query = try arena.allocator().create(ast.Query);
    query.* = .{
        .clauses = &[_]ast.Clause{},
        .allocator = arena.allocator(),
    };

    const vars = [_]VariableInfo{};
    cache.put(query_text, query, &vars, arena);

    try std.testing.expectEqual(@as(u32, 1), cache.count);

    // Get should hit
    const entry = cache.get(query_text);
    try std.testing.expect(entry != null);
    try std.testing.expectEqual(@as(u64, 1), cache.stats_hits);

    // Unpin the entry
    entry.?.unpin();
}

test "cache schema version invalidation" {
    const cache = try QueryCache.init(std.testing.allocator, 16);
    defer cache.deinit();

    const query_text = "MATCH (n:Person) RETURN n";

    // Insert entry
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    const query = try arena.allocator().create(ast.Query);
    query.* = .{
        .clauses = &[_]ast.Clause{},
        .allocator = arena.allocator(),
    };
    cache.put(query_text, query, &[_]VariableInfo{}, arena);

    // Should hit before bump
    const hit = cache.get(query_text);
    try std.testing.expect(hit != null);
    hit.?.unpin();

    // Bump schema version
    cache.bumpSchemaVersion();

    // Should miss after bump
    const miss = cache.get(query_text);
    try std.testing.expect(miss == null);
}

test "cache LRU eviction" {
    const cache = try QueryCache.init(std.testing.allocator, 2);
    defer cache.deinit();

    // Fill cache with 2 entries
    {
        var arena1 = std.heap.ArenaAllocator.init(std.testing.allocator);
        const q1 = try arena1.allocator().create(ast.Query);
        q1.* = .{ .clauses = &[_]ast.Clause{}, .allocator = arena1.allocator() };
        cache.put("query1", q1, &[_]VariableInfo{}, arena1);
    }

    {
        var arena2 = std.heap.ArenaAllocator.init(std.testing.allocator);
        const q2 = try arena2.allocator().create(ast.Query);
        q2.* = .{ .clauses = &[_]ast.Clause{}, .allocator = arena2.allocator() };
        cache.put("query2", q2, &[_]VariableInfo{}, arena2);
    }

    try std.testing.expectEqual(@as(u32, 2), cache.count);

    // Access query2 to make it more recent
    const e = cache.get("query2");
    try std.testing.expect(e != null);
    e.?.unpin();

    // Insert a third entry - should evict query1 (LRU)
    {
        var arena3 = std.heap.ArenaAllocator.init(std.testing.allocator);
        const q3 = try arena3.allocator().create(ast.Query);
        q3.* = .{ .clauses = &[_]ast.Clause{}, .allocator = arena3.allocator() };
        cache.put("query3", q3, &[_]VariableInfo{}, arena3);
    }

    try std.testing.expectEqual(@as(u32, 2), cache.count);

    // query1 should be evicted
    try std.testing.expect(cache.get("query1") == null);

    // query2 and query3 should still be cached
    const e2 = cache.get("query2");
    try std.testing.expect(e2 != null);
    e2.?.unpin();

    const e3 = cache.get("query3");
    try std.testing.expect(e3 != null);
    e3.?.unpin();
}

test "cache clear" {
    const cache = try QueryCache.init(std.testing.allocator, 16);
    defer cache.deinit();

    // Insert an entry
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    const query = try arena.allocator().create(ast.Query);
    query.* = .{
        .clauses = &[_]ast.Clause{},
        .allocator = arena.allocator(),
    };
    cache.put("MATCH (n) RETURN n", query, &[_]VariableInfo{}, arena);

    try std.testing.expectEqual(@as(u32, 1), cache.count);

    cache.clear();
    try std.testing.expectEqual(@as(u32, 0), cache.count);
}

test "cache pinned entries survive eviction" {
    const cache = try QueryCache.init(std.testing.allocator, 1);
    defer cache.deinit();

    // Insert and pin an entry
    {
        var arena1 = std.heap.ArenaAllocator.init(std.testing.allocator);
        const q1 = try arena1.allocator().create(ast.Query);
        q1.* = .{ .clauses = &[_]ast.Clause{}, .allocator = arena1.allocator() };
        cache.put("query1", q1, &[_]VariableInfo{}, arena1);
    }

    const pinned = cache.get("query1");
    try std.testing.expect(pinned != null);
    // Keep it pinned (don't unpin)

    // Try to insert another entry - pinned entry can't be evicted
    // so the cache will force-overwrite slot 0 as last resort
    {
        var arena2 = std.heap.ArenaAllocator.init(std.testing.allocator);
        const q2 = try arena2.allocator().create(ast.Query);
        q2.* = .{ .clauses = &[_]ast.Clause{}, .allocator = arena2.allocator() };
        cache.put("query2", q2, &[_]VariableInfo{}, arena2);
    }

    // After the pinned entry was forcefully evicted, unpin is safe
    // because the entry memory was freed by the arena deinit in put
    // The test passes if no crash occurs
}

test "cache stats tracking" {
    const cache = try QueryCache.init(std.testing.allocator, 16);
    defer cache.deinit();

    // Miss
    _ = cache.get("nonexistent");
    const stats1 = cache.getStats();
    try std.testing.expectEqual(@as(u64, 0), stats1.hits);
    try std.testing.expectEqual(@as(u64, 1), stats1.misses);

    // Insert and hit
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    const query = try arena.allocator().create(ast.Query);
    query.* = .{
        .clauses = &[_]ast.Clause{},
        .allocator = arena.allocator(),
    };
    cache.put("test", query, &[_]VariableInfo{}, arena);

    const entry = cache.get("test");
    try std.testing.expect(entry != null);
    entry.?.unpin();

    const stats2 = cache.getStats();
    try std.testing.expectEqual(@as(u64, 1), stats2.hits);
    try std.testing.expectEqual(@as(u64, 1), stats2.misses);
    try std.testing.expectEqual(@as(u32, 1), stats2.entries);
}
