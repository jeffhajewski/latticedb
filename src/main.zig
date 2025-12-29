//! Lattice: Embedded Knowledge Graph Database
//!
//! A single-file knowledge graph database designed for AI/RAG applications.
//! Combines property graph storage, HNSW vector search, and BM25 full-text search.

const std = @import("std");

// Core modules
pub const core = struct {
    pub const types = @import("core/types.zig");
};

// Storage modules
pub const storage = struct {
    pub const vfs = @import("storage/vfs.zig");
    pub const page = @import("storage/page.zig");
    pub const page_manager = @import("storage/page_manager.zig");
    pub const buffer_pool = @import("storage/buffer_pool.zig");
    pub const btree = @import("storage/btree.zig");
    pub const wal = @import("storage/wal.zig");
    pub const checkpoint = @import("storage/checkpoint.zig");
    pub const recovery = @import("storage/recovery.zig");
};

// Vector search
pub const vector = struct {
    pub const hnsw = @import("vector/hnsw.zig");
};

// Full-text search
pub const fts = struct {
    pub const tokenizer = @import("fts/tokenizer.zig");
};

// Query system
pub const query = struct {
    pub const parser = @import("query/parser.zig");
};

// Transaction management
pub const transaction = struct {
    pub const manager = @import("transaction/manager.zig");
};

// Concurrency primitives
pub const concurrency = struct {
    pub const locking = @import("concurrency/locking.zig");
};

// C API
pub const c_api = @import("api/c_api.zig");

// Re-export common types at top level
pub const NodeId = core.types.NodeId;
pub const EdgeId = core.types.EdgeId;
pub const PageId = core.types.PageId;
pub const PropertyValue = core.types.PropertyValue;

pub const PageType = storage.page.PageType;
pub const PageHeader = storage.page.PageHeader;
pub const FileHeader = storage.page.FileHeader;

pub const Vfs = storage.vfs.Vfs;
pub const VfsFile = storage.vfs.File;
pub const OpenFlags = storage.vfs.OpenFlags;
pub const PosixVfs = storage.vfs.PosixVfs;

pub const PageManager = storage.page_manager.PageManager;
pub const BufferPool = storage.buffer_pool.BufferPool;
pub const BufferFrame = storage.buffer_pool.BufferFrame;

pub const BTree = storage.btree.BTree;
pub const BTreeError = storage.btree.BTreeError;

pub const WalManager = storage.wal.WalManager;
pub const WalError = storage.wal.WalError;
pub const WalRecordType = storage.wal.WalRecordType;

pub const Checkpointer = storage.checkpoint.Checkpointer;
pub const CheckpointMode = storage.checkpoint.CheckpointMode;
pub const CheckpointError = storage.checkpoint.CheckpointError;
pub const CheckpointStats = storage.checkpoint.CheckpointStats;

pub const RecoveryManager = storage.recovery.RecoveryManager;
pub const RecoveryError = storage.recovery.RecoveryError;
pub const RecoveryStats = storage.recovery.RecoveryStats;
pub const recoverDatabase = storage.recovery.recoverDatabase;

pub const Transaction = transaction.manager.Transaction;
pub const TxnManager = transaction.manager.TxnManager;
pub const TxnState = transaction.manager.TxnState;
pub const TxnMode = transaction.manager.TxnMode;
pub const TxnError = transaction.manager.TxnError;
pub const IsolationLevel = transaction.manager.IsolationLevel;

pub const HnswConfig = vector.hnsw.HnswConfig;
pub const DistanceMetric = vector.hnsw.DistanceMetric;

// Version information
pub const VERSION = "0.1.0";
pub const VERSION_MAJOR = 0;
pub const VERSION_MINOR = 1;
pub const VERSION_PATCH = 0;

test {
    // Run tests from all modules
    std.testing.refAllDecls(@This());
}
