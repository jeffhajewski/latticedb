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
    pub const database = @import("storage/database.zig");
};

// Vector search
pub const vector = struct {
    pub const hnsw = @import("vector/hnsw.zig");
    pub const storage = @import("vector/storage.zig");
    pub const embedding = @import("vector/embedding.zig");
    pub const distance = @import("vector/distance.zig");
};

// Full-text search
pub const fts = struct {
    pub const tokenizer = @import("fts/tokenizer.zig");
    pub const dictionary = @import("fts/dictionary.zig");
    pub const posting = @import("fts/posting.zig");
    pub const scorer = @import("fts/scorer.zig");
    pub const index = @import("fts/index.zig");
    pub const stemmer = @import("fts/stemmer.zig");
    pub const fuzzy = @import("fts/fuzzy.zig");
    pub const prefix = @import("fts/prefix.zig");
    pub const stopwords = @import("fts/stopwords.zig");
    pub const highlight = @import("fts/highlight.zig");
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

// Graph storage
pub const graph = struct {
    pub const symbols = @import("graph/symbols.zig");
    pub const node = @import("graph/node.zig");
    pub const edge = @import("graph/edge.zig");
    pub const label_index = @import("graph/label_index.zig");
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
pub const BTreeIterator = storage.btree.BTree.Iterator;
pub const BTreeEntry = storage.btree.Entry;

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
pub const HnswIndex = vector.hnsw.HnswIndex;
pub const HnswError = vector.hnsw.HnswError;
pub const SearchResult = vector.hnsw.SearchResult;
pub const DistanceMetric = vector.hnsw.DistanceMetric;
pub const VectorStorage = vector.storage.VectorStorage;
pub const VectorStorageError = vector.storage.VectorStorageError;

pub const EmbeddingClient = vector.embedding.EmbeddingClient;
pub const EmbeddingConfig = vector.embedding.Config;
pub const EmbeddingError = vector.embedding.EmbeddingError;
pub const EmbeddingApiFormat = vector.embedding.ApiFormat;

pub const SymbolTable = graph.symbols.SymbolTable;
pub const SymbolId = graph.symbols.SymbolId;
pub const SymbolError = graph.symbols.SymbolError;

pub const NodeStore = graph.node.NodeStore;
pub const Node = graph.node.Node;
pub const NodeError = graph.node.NodeError;

pub const EdgeStore = graph.edge.EdgeStore;
pub const Edge = graph.edge.Edge;
pub const EdgeError = graph.edge.EdgeError;
pub const EdgeKey = graph.edge.EdgeKey;
pub const Direction = graph.edge.Direction;
pub const EdgeIterator = graph.edge.EdgeStore.EdgeIterator;

pub const LabelIndex = graph.label_index.LabelIndex;
pub const LabelKey = graph.label_index.LabelKey;
pub const LabelIndexError = graph.label_index.LabelIndexError;
pub const LabelNodeIterator = graph.label_index.LabelIndex.NodeIterator;

// Full-text search re-exports
pub const FtsIndex = fts.index.FtsIndex;
pub const FtsConfig = fts.index.FtsConfig;
pub const FtsError = fts.index.FtsError;
pub const FtsStats = fts.index.FtsStats;
pub const Tokenizer = fts.tokenizer.Tokenizer;
pub const TokenizerConfig = fts.tokenizer.TokenizerConfig;
pub const ScoredDoc = fts.scorer.ScoredDoc;
pub const Bm25Config = fts.scorer.Bm25Config;
pub const HighlightConfig = fts.highlight.HighlightConfig;
pub const HighlightResult = fts.highlight.HighlightResult;
pub const HighlightError = fts.highlight.HighlightError;
pub const Snippet = fts.highlight.Snippet;

// Version information
pub const VERSION = "0.1.0";
pub const VERSION_MAJOR = 0;
pub const VERSION_MINOR = 1;
pub const VERSION_PATCH = 0;

test {
    // Run tests from all modules
    std.testing.refAllDecls(@This());
}
