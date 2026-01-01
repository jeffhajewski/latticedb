//! Full-Text Search Index.
//!
//! Coordinates dictionary, posting lists, and scoring for FTS queries.
//! Provides high-level indexing and search operations.

const std = @import("std");
const lattice = @import("lattice");

const Allocator = std.mem.Allocator;
const StringHashMap = std.StringHashMap;
const ArrayListUnmanaged = std.ArrayListUnmanaged;

const btree = lattice.storage.btree;
const buffer_pool = lattice.storage.buffer_pool;
const BTree = btree.BTree;
const BufferPool = buffer_pool.BufferPool;
const NodeId = lattice.core.types.NodeId;

const tokenizer_mod = @import("tokenizer.zig");
const dictionary_mod = @import("dictionary.zig");
const posting_mod = @import("posting.zig");
const scorer_mod = @import("scorer.zig");

const Tokenizer = tokenizer_mod.Tokenizer;
const TokenizerConfig = tokenizer_mod.TokenizerConfig;
const Dictionary = dictionary_mod.Dictionary;
const DictionaryEntry = dictionary_mod.DictionaryEntry;
const TokenId = dictionary_mod.TokenId;
const PostingStore = posting_mod.PostingStore;
const PostingEntry = posting_mod.PostingEntry;
const DocId = posting_mod.DocId;
const DocLengthStore = scorer_mod.DocLengthStore;
const Bm25Scorer = scorer_mod.Bm25Scorer;
const Bm25Config = scorer_mod.Bm25Config;
const ScoredDoc = scorer_mod.ScoredDoc;

/// FTS Index configuration
pub const FtsConfig = struct {
    /// Tokenizer configuration
    tokenizer: TokenizerConfig = .{},
    /// BM25 scoring configuration
    bm25: Bm25Config = .{},
    /// Store term positions for phrase queries
    store_positions: bool = false,
};

/// FTS Index errors
pub const FtsError = error{
    /// Document not found
    NotFound,
    /// I/O error
    IoError,
    /// Out of memory
    OutOfMemory,
    /// Invalid query
    InvalidQuery,
    /// Tokenizer error
    TokenizerError,
    /// Dictionary error
    DictionaryError,
    /// Posting error
    PostingError,
    /// Scorer error
    ScorerError,
    /// B+Tree error
    BTreeError,
};

/// FTS Index statistics
pub const FtsStats = struct {
    /// Total documents indexed
    total_docs: u64,
    /// Total tokens indexed
    total_tokens: u64,
    /// Unique tokens (vocabulary size)
    unique_tokens: u32,
    /// Average document length
    avg_doc_length: f32,
};

/// Full-Text Search Index
pub const FtsIndex = struct {
    allocator: Allocator,
    config: FtsConfig,

    // Component storage
    dictionary: Dictionary,
    posting_store: PostingStore,
    doc_lengths: DocLengthStore,
    scorer: Bm25Scorer,

    // B+Trees (references, not owned)
    dict_tree: *BTree,
    lengths_tree: *BTree,

    const Self = @This();

    /// Initialize a new FTS index
    pub fn init(
        allocator: Allocator,
        bp: *BufferPool,
        dict_tree: *BTree,
        lengths_tree: *BTree,
        config: FtsConfig,
    ) Self {
        var doc_lengths = DocLengthStore.init(allocator, lengths_tree);

        return Self{
            .allocator = allocator,
            .config = config,
            .dictionary = Dictionary.init(allocator, dict_tree),
            .posting_store = PostingStore.init(allocator, bp),
            .doc_lengths = doc_lengths,
            .scorer = Bm25Scorer.init(config.bm25, &doc_lengths),
            .dict_tree = dict_tree,
            .lengths_tree = lengths_tree,
        };
    }

    // ========================================================================
    // Indexing Operations
    // ========================================================================

    /// Index a document's text content
    /// Returns the number of tokens indexed
    pub fn indexDocument(
        self: *Self,
        doc_id: DocId,
        text: []const u8,
    ) FtsError!u32 {
        // 1. Tokenize text
        var tok = Tokenizer.init(self.allocator, text, self.config.tokenizer);
        const tokens = tok.tokenizeAll() catch {
            return FtsError.TokenizerError;
        };
        defer self.allocator.free(tokens);

        if (tokens.len == 0) {
            return 0;
        }

        // 2. Count term frequencies
        var term_freqs = StringHashMap(u32).init(self.allocator);
        defer term_freqs.deinit();

        for (tokens) |token| {
            // Normalize to lowercase
            var lower_buf: [64]u8 = undefined;
            const normalized = if (token.text.len <= 64) blk: {
                for (token.text, 0..) |c, i| {
                    lower_buf[i] = std.ascii.toLower(c);
                }
                break :blk lower_buf[0..token.text.len];
            } else {
                continue; // Skip tokens > 64 chars
            };

            const result = term_freqs.getOrPut(normalized) catch {
                return FtsError.OutOfMemory;
            };
            if (result.found_existing) {
                result.value_ptr.* += 1;
            } else {
                // Need to copy the key since normalized is stack-allocated
                const key_copy = self.allocator.dupe(u8, normalized) catch {
                    return FtsError.OutOfMemory;
                };
                result.key_ptr.* = key_copy;
                result.value_ptr.* = 1;
            }
        }

        // 3. Update dictionary and posting lists for each term
        var iter = term_freqs.iterator();
        while (iter.next()) |entry| {
            const term = entry.key_ptr.*;
            const freq = entry.value_ptr.*;

            // Get or create token in dictionary
            const token_id = self.dictionary.getOrCreate(term) catch {
                self.allocator.free(term);
                return FtsError.DictionaryError;
            };

            // Get current dictionary entry
            var dict_entry = self.dictionary.get(term) catch {
                self.allocator.free(term);
                return FtsError.DictionaryError;
            } orelse {
                self.allocator.free(term);
                return FtsError.DictionaryError;
            };

            // Create posting list if needed
            if (dict_entry.posting_page == 0) {
                const page_id = self.posting_store.create(token_id) catch {
                    self.allocator.free(term);
                    return FtsError.PostingError;
                };
                self.dictionary.setPostingPage(term, page_id) catch {
                    self.allocator.free(term);
                    return FtsError.DictionaryError;
                };
                dict_entry.posting_page = page_id;
            }

            // Append posting entry
            _ = self.posting_store.append(dict_entry.posting_page, PostingEntry{
                .doc_id = doc_id,
                .term_freq = freq,
                .positions = null,
            }) catch {
                self.allocator.free(term);
                return FtsError.PostingError;
            };

            // Update document frequency
            self.dictionary.incrementDocFreq(term) catch {
                self.allocator.free(term);
                return FtsError.DictionaryError;
            };

            // Free the copied key
            self.allocator.free(term);
        }

        // 4. Store document length
        self.doc_lengths.setLength(doc_id, @intCast(tokens.len)) catch {
            return FtsError.ScorerError;
        };

        // Update scorer's reference to doc_lengths
        self.scorer = Bm25Scorer.init(self.config.bm25, &self.doc_lengths);

        return @intCast(tokens.len);
    }

    /// Remove a document from the index
    pub fn removeDocument(self: *Self, doc_id: DocId) FtsError!void {
        // Remove from doc lengths (this updates stats)
        self.doc_lengths.removeDoc(doc_id) catch {
            return FtsError.ScorerError;
        };

        // Note: Ideally we should also remove entries from posting lists
        // and update doc frequencies. For now, we skip this as it requires
        // scanning all posting lists. Documents will be filtered during search.
    }

    // ========================================================================
    // Search Operations
    // ========================================================================

    /// Search with a text query
    /// Returns scored documents sorted by relevance (highest first)
    pub fn search(
        self: *Self,
        query_text: []const u8,
        limit: u32,
    ) FtsError![]ScoredDoc {
        // Tokenize query
        var tok = Tokenizer.init(self.allocator, query_text, self.config.tokenizer);
        const tokens = tok.tokenizeAll() catch {
            return FtsError.TokenizerError;
        };
        defer self.allocator.free(tokens);

        if (tokens.len == 0) {
            return &[_]ScoredDoc{};
        }

        // Collect unique query terms (max 32 terms)
        var query_term_buf: [32][]const u8 = undefined;
        var query_term_allocs: [32][]u8 = undefined;
        var num_terms: usize = 0;

        for (tokens) |token| {
            if (num_terms >= 32) break;

            // Normalize
            var lower_buf: [64]u8 = undefined;
            if (token.text.len <= 64) {
                for (token.text, 0..) |c, i| {
                    lower_buf[i] = std.ascii.toLower(c);
                }
                const normalized = lower_buf[0..token.text.len];

                // Check if already added
                var found = false;
                for (query_term_buf[0..num_terms]) |t| {
                    if (std.mem.eql(u8, t, normalized)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    // Allocate copy for query_terms
                    const copy = self.allocator.dupe(u8, normalized) catch {
                        // Clean up allocated terms
                        for (query_term_allocs[0..num_terms]) |t| {
                            self.allocator.free(t);
                        }
                        return FtsError.OutOfMemory;
                    };
                    query_term_buf[num_terms] = copy;
                    query_term_allocs[num_terms] = copy;
                    num_terms += 1;
                }
            }
        }

        // Defer cleanup
        defer {
            for (query_term_allocs[0..num_terms]) |t| {
                self.allocator.free(t);
            }
        }

        if (num_terms == 0) {
            return &[_]ScoredDoc{};
        }

        // For single term query, do simple posting list scan
        if (num_terms == 1) {
            return self.searchSingleTerm(query_term_buf[0], limit);
        }

        // For multi-term query, combine results
        return self.searchMultiTerm(query_term_buf[0..num_terms], limit);
    }

    /// Search for a single term
    fn searchSingleTerm(self: *Self, term: []const u8, limit: u32) FtsError![]ScoredDoc {
        // Get dictionary entry
        const dict_entry = self.dictionary.get(term) catch {
            return FtsError.DictionaryError;
        } orelse {
            // Term not in index
            return &[_]ScoredDoc{};
        };

        if (dict_entry.posting_page == 0) {
            return &[_]ScoredDoc{};
        }

        // Iterate posting list and score documents
        var results: ArrayListUnmanaged(ScoredDoc) = .{};
        errdefer results.deinit(self.allocator);

        var iter = self.posting_store.iterate(dict_entry.posting_page) catch {
            return FtsError.PostingError;
        };
        defer iter.deinit();

        while (iter.next() catch null) |entry| {
            // Get document length for scoring
            const doc_length = self.doc_lengths.getLength(entry.doc_id) catch {
                continue; // Skip documents not found (may have been deleted)
            };

            const score = self.scorer.scoreTerm(
                entry.term_freq,
                dict_entry.doc_freq,
                doc_length,
            );

            results.append(self.allocator, .{
                .doc_id = entry.doc_id,
                .score = score,
            }) catch {
                return FtsError.OutOfMemory;
            };
        }

        // Sort by score descending
        std.mem.sort(ScoredDoc, results.items, {}, ScoredDoc.lessThan);

        // Return top K
        const result_slice = results.toOwnedSlice(self.allocator) catch {
            return FtsError.OutOfMemory;
        };

        if (result_slice.len > limit) {
            // Free excess elements
            self.allocator.free(result_slice[limit..]);
            return result_slice[0..limit];
        }
        return result_slice;
    }

    /// Search for multiple terms (AND semantics - documents must contain all terms)
    fn searchMultiTerm(self: *Self, terms: []const []const u8, limit: u32) FtsError![]ScoredDoc {
        if (terms.len == 0) {
            return &[_]ScoredDoc{};
        }

        // Collect posting iterators and dict entries for all terms
        var doc_scores = std.AutoHashMap(DocId, f32).init(self.allocator);
        defer doc_scores.deinit();

        var doc_term_count = std.AutoHashMap(DocId, u32).init(self.allocator);
        defer doc_term_count.deinit();

        const term_count = terms.len;

        for (terms) |term| {
            const dict_entry = self.dictionary.get(term) catch {
                continue;
            } orelse {
                continue;
            };

            if (dict_entry.posting_page == 0) continue;

            var iter = self.posting_store.iterate(dict_entry.posting_page) catch {
                continue;
            };
            defer iter.deinit();

            while (iter.next() catch null) |entry| {
                const doc_length = self.doc_lengths.getLength(entry.doc_id) catch {
                    continue;
                };

                const score = self.scorer.scoreTerm(
                    entry.term_freq,
                    dict_entry.doc_freq,
                    doc_length,
                );

                // Accumulate score
                const score_result = doc_scores.getOrPut(entry.doc_id) catch {
                    return FtsError.OutOfMemory;
                };
                if (score_result.found_existing) {
                    score_result.value_ptr.* += score;
                } else {
                    score_result.value_ptr.* = score;
                }

                // Count terms matched
                const count_result = doc_term_count.getOrPut(entry.doc_id) catch {
                    return FtsError.OutOfMemory;
                };
                if (count_result.found_existing) {
                    count_result.value_ptr.* += 1;
                } else {
                    count_result.value_ptr.* = 1;
                }
            }
        }

        // Filter to documents that match all terms and collect results
        var results: ArrayListUnmanaged(ScoredDoc) = .{};
        errdefer results.deinit(self.allocator);

        var score_iter = doc_scores.iterator();
        while (score_iter.next()) |entry| {
            const doc_id = entry.key_ptr.*;
            const matched_terms = doc_term_count.get(doc_id) orelse 0;

            // Only include if document matches all terms
            if (matched_terms == term_count) {
                results.append(self.allocator, .{
                    .doc_id = doc_id,
                    .score = entry.value_ptr.*,
                }) catch {
                    return FtsError.OutOfMemory;
                };
            }
        }

        // Sort by score descending
        std.mem.sort(ScoredDoc, results.items, {}, ScoredDoc.lessThan);

        // Return top K
        const result_slice = results.toOwnedSlice(self.allocator) catch {
            return FtsError.OutOfMemory;
        };

        if (result_slice.len > limit) {
            self.allocator.free(result_slice[limit..]);
            return result_slice[0..limit];
        }
        return result_slice;
    }

    /// Free search results
    pub fn freeResults(self: *Self, results: []ScoredDoc) void {
        self.allocator.free(results);
    }

    // ========================================================================
    // Statistics
    // ========================================================================

    /// Get FTS index statistics
    pub fn getStats(self: *Self) FtsStats {
        const doc_stats = self.doc_lengths.getStats();
        return FtsStats{
            .total_docs = doc_stats.total_docs,
            .total_tokens = doc_stats.total_tokens,
            .unique_tokens = self.dictionary.tokenCount(),
            .avg_doc_length = doc_stats.avg_doc_length,
        };
    }
};

// ============================================================================
// Tests
// ============================================================================

test "fts index basic" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const page_manager = lattice.storage.page_manager;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_fts_index_test.db";
    vfs_impl.delete(db_path) catch {};

    var pm = try page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
    defer {
        pm.deinit();
        vfs_impl.delete(db_path) catch {};
    }

    var bp = try BufferPool.init(allocator, &pm, 64 * 4096);
    defer bp.deinit();

    var dict_tree = try BTree.init(allocator, &bp);
    var lengths_tree = try BTree.init(allocator, &bp);

    var index = FtsIndex.init(allocator, &bp, &dict_tree, &lengths_tree, .{});

    // Index some documents
    const doc1_tokens = try index.indexDocument(1, "The quick brown fox jumps over the lazy dog");
    try std.testing.expect(doc1_tokens > 0);

    const doc2_tokens = try index.indexDocument(2, "A quick brown rabbit hops through the garden");
    try std.testing.expect(doc2_tokens > 0);

    const doc3_tokens = try index.indexDocument(3, "The lazy cat sleeps all day");
    try std.testing.expect(doc3_tokens > 0);

    // Check stats
    const stats = index.getStats();
    try std.testing.expectEqual(@as(u64, 3), stats.total_docs);
    try std.testing.expect(stats.unique_tokens > 0);

    // Search for "quick"
    const results1 = try index.search("quick", 10);
    defer index.freeResults(results1);
    try std.testing.expectEqual(@as(usize, 2), results1.len); // doc1 and doc2

    // Search for "lazy"
    const results2 = try index.search("lazy", 10);
    defer index.freeResults(results2);
    try std.testing.expectEqual(@as(usize, 2), results2.len); // doc1 and doc3

    // Search for multi-term "quick brown"
    const results3 = try index.search("quick brown", 10);
    defer index.freeResults(results3);
    try std.testing.expectEqual(@as(usize, 2), results3.len); // doc1 and doc2 have both

    // Search for non-existent term
    const results4 = try index.search("elephant", 10);
    defer index.freeResults(results4);
    try std.testing.expectEqual(@as(usize, 0), results4.len);
}
