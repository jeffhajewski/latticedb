//! B+Tree implementation for Lattice database.
//!
//! Provides the primary index structure for nodes, edges, and properties.

const std = @import("std");
const types = @import("../core/types.zig");
const page = @import("page.zig");

pub const PageId = types.PageId;
pub const NodeId = types.NodeId;

/// Key type for B+Tree parameterization
pub const KeyType = enum {
    /// u64 node ID for direct lookup
    node_id,
    /// (source_id, edge_type, label_id, target_id) for adjacency
    edge_key,
    /// (label_id, node_id) for label index
    label_node_id,
    /// (node_id, property_id) for property lookup
    property_key,
    /// Variable-length interned string
    string_intern,
};

/// B+Tree internal node header
pub const InternalNodeHeader = extern struct {
    base: page.PageHeader,
    num_keys: u16,
    level: u16,
    right_child: PageId,
};

/// B+Tree leaf node header
pub const LeafNodeHeader = extern struct {
    base: page.PageHeader,
    num_entries: u16,
    flags: u16,
    next_leaf: PageId,
    prev_leaf: PageId,
};

/// B+Tree configuration
pub const BTreeConfig = struct {
    /// Page size in bytes
    page_size: u32 = page.DEFAULT_PAGE_SIZE,
    /// Minimum fill factor (0.0-1.0)
    min_fill_factor: f32 = 0.5,
    /// Key type for this tree
    key_type: KeyType = .node_id,
};

/// B+Tree statistics
pub const BTreeStats = struct {
    height: u32,
    internal_pages: u64,
    leaf_pages: u64,
    total_entries: u64,
    total_bytes: u64,
};

/// B+Tree cursor for iteration
pub const BTreeCursor = struct {
    page_id: PageId,
    slot_index: u16,
    key_type: KeyType,

    pub fn isValid(self: *const BTreeCursor) bool {
        return self.page_id != types.NULL_PAGE;
    }
};

test "internal node header size" {
    try std.testing.expectEqual(@as(usize, 16), @sizeOf(InternalNodeHeader));
}

test "leaf node header size" {
    try std.testing.expectEqual(@as(usize, 20), @sizeOf(LeafNodeHeader));
}
