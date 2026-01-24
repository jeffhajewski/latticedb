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
    pub const lexer = @import("query/lexer.zig");
    pub const parser = @import("query/parser.zig");
    pub const ast = @import("query/ast.zig");
    pub const semantic = @import("query/semantic.zig");
    pub const executor = @import("query/executor.zig");
    pub const expression = @import("query/expression.zig");
    pub const planner = @import("query/planner.zig");
    pub const cache = @import("query/cache.zig");

    pub const operators = struct {
        pub const scan = @import("query/operators/scan.zig");
        pub const filter = @import("query/operators/filter.zig");
        pub const project = @import("query/operators/project.zig");
        pub const expand = @import("query/operators/expand.zig");
        pub const vector = @import("query/operators/vector.zig");
        pub const fts = @import("query/operators/fts.zig");
        pub const limit = @import("query/operators/limit.zig");
        pub const mutation = @import("query/operators/mutation.zig");
        pub const unwind = @import("query/operators/unwind.zig");
    };
};

// Transaction management
pub const transaction = struct {
    pub const manager = @import("transaction/manager.zig");
    pub const wal_payload = @import("transaction/wal_payload.zig");
    pub const mvcc = @import("transaction/mvcc.zig");
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
    pub const adjacency_cache = @import("graph/adjacency_cache.zig");
};

// C API - explicitly export all C API functions for shared library
pub const c_api = @import("api/c_api.zig");

// Force C API exports to be included in the library
comptime {
    _ = &c_api.lattice_open;
    _ = &c_api.lattice_close;
    _ = &c_api.lattice_begin;
    _ = &c_api.lattice_commit;
    _ = &c_api.lattice_rollback;
    _ = &c_api.lattice_node_create;
    _ = &c_api.lattice_node_delete;
    _ = &c_api.lattice_node_set_property;
    _ = &c_api.lattice_node_get_property;
    _ = &c_api.lattice_node_exists;
    _ = &c_api.lattice_node_get_labels;
    _ = &c_api.lattice_free_string;
    _ = &c_api.lattice_node_set_vector;
    _ = &c_api.lattice_edge_create;
    _ = &c_api.lattice_edge_delete;
    _ = &c_api.lattice_query_prepare;
    _ = &c_api.lattice_query_bind;
    _ = &c_api.lattice_query_bind_vector;
    _ = &c_api.lattice_query_execute;
    _ = &c_api.lattice_query_free;
    _ = &c_api.lattice_result_next;
    _ = &c_api.lattice_result_column_count;
    _ = &c_api.lattice_result_column_name;
    _ = &c_api.lattice_result_get;
    _ = &c_api.lattice_result_free;
    _ = &c_api.lattice_query_cache_clear;
    _ = &c_api.lattice_query_cache_stats;
    _ = &c_api.lattice_version;
    _ = &c_api.lattice_error_message;
}

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

pub const Snapshot = transaction.mvcc.Snapshot;
pub const VersionInfo = transaction.mvcc.VersionInfo;
pub const VersionChain = transaction.mvcc.VersionChain;
pub const isVisible = transaction.mvcc.isVisible;
pub const canGarbageCollect = transaction.mvcc.canGarbageCollect;

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

pub const AdjacencyCache = graph.adjacency_cache.AdjacencyCache;
pub const CachedEdge = graph.adjacency_cache.CachedEdge;

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

// Semantic analyzer re-exports
pub const SemanticAnalyzer = query.semantic.SemanticAnalyzer;
pub const SemanticError = query.semantic.SemanticError;
pub const AnalysisResult = query.semantic.AnalysisResult;
pub const VariableInfo = query.semantic.VariableInfo;
pub const VariableKind = query.semantic.VariableKind;

// Query cache re-exports
pub const QueryCache = query.cache.QueryCache;
pub const CacheEntry = query.cache.CacheEntry;
pub const CacheStats = query.cache.CacheStats;

// Version information
pub const VERSION = "0.1.0";
pub const VERSION_MAJOR = 0;
pub const VERSION_MINOR = 1;
pub const VERSION_PATCH = 0;

test {
    // Run tests from all modules
    std.testing.refAllDecls(@This());
}
