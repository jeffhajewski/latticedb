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
const fuzzy_mod = @import("fuzzy.zig");
const prefix_mod = @import("prefix.zig");
const stopwords_mod = @import("stopwords.zig");
const reverse_index_mod = @import("reverse_index.zig");

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
const ReverseIndex = reverse_index_mod.ReverseIndex;

/// FTS Index configuration
pub const FtsConfig = struct {
    /// Tokenizer configuration
    tokenizer: TokenizerConfig = .{},
    /// BM25 scoring configuration
    bm25: Bm25Config = .{},
    /// Store term positions for phrase queries
    store_positions: bool = false,
};

/// Query mode for boolean search
pub const QueryMode = enum {
    /// All terms must match (default)
    @"and",
    /// Any term can match
    @"or",
};

/// Parsed query with terms and exclusions
pub const ParsedQuery = struct {
    /// Terms that must/should match (depending on mode)
    terms: [][]const u8,
    /// Terms that must NOT match (documents with these are excluded)
    excluded: [][]const u8,
    /// Phrase queries (terms that must appear adjacent)
    phrases: [][]const []const u8,
    /// How to combine terms
    mode: QueryMode,

    /// Number of allocated term strings
    term_count: usize,
    excluded_count: usize,
    phrase_count: usize,
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
    reverse_index: ?ReverseIndex,

    // B+Trees (references, not owned)
    dict_tree: *BTree,
    lengths_tree: *BTree,
    reverse_tree: ?*BTree,

    const Self = @This();

    /// Initialize a new FTS index
    /// reverse_tree is optional - if provided, enables proper document deletion
    pub fn init(
        allocator: Allocator,
        bp: *BufferPool,
        dict_tree: *BTree,
        lengths_tree: *BTree,
        reverse_tree: ?*BTree,
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
            .reverse_index = if (reverse_tree) |rt| ReverseIndex.init(allocator, rt) else null,
            .dict_tree = dict_tree,
            .lengths_tree = lengths_tree,
            .reverse_tree = reverse_tree,
        };
    }

    // ========================================================================
    // Indexing Operations
    // ========================================================================

    /// Term info for indexing (frequency + positions)
    const TermInfo = struct {
        freq: u32,
        positions: ArrayListUnmanaged(u32),
    };

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

        // 2. Count term frequencies and collect positions
        var term_infos = StringHashMap(TermInfo).init(self.allocator);
        defer {
            var it = term_infos.iterator();
            while (it.next()) |entry| {
                entry.value_ptr.positions.deinit(self.allocator);
                self.allocator.free(entry.key_ptr.*);
            }
            term_infos.deinit();
        }

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

            const result = term_infos.getOrPut(normalized) catch {
                return FtsError.OutOfMemory;
            };
            if (result.found_existing) {
                result.value_ptr.freq += 1;
                if (self.config.store_positions) {
                    result.value_ptr.positions.append(self.allocator, token.position) catch {
                        return FtsError.OutOfMemory;
                    };
                }
            } else {
                // Need to copy the key since normalized is stack-allocated
                const key_copy = self.allocator.dupe(u8, normalized) catch {
                    return FtsError.OutOfMemory;
                };
                result.key_ptr.* = key_copy;
                result.value_ptr.* = TermInfo{
                    .freq = 1,
                    .positions = .{},
                };
                if (self.config.store_positions) {
                    result.value_ptr.positions.append(self.allocator, token.position) catch {
                        return FtsError.OutOfMemory;
                    };
                }
            }
        }

        // 3. Update dictionary and posting lists for each term
        var iter = term_infos.iterator();
        while (iter.next()) |entry| {
            const term = entry.key_ptr.*;
            const info = entry.value_ptr.*;

            // Get or create token in dictionary
            const token_id = self.dictionary.getOrCreate(term) catch {
                return FtsError.DictionaryError;
            };

            // Get current dictionary entry
            var dict_entry = self.dictionary.get(term) catch {
                return FtsError.DictionaryError;
            } orelse {
                return FtsError.DictionaryError;
            };

            // Create posting list if needed
            if (dict_entry.posting_page == 0) {
                const page_id = self.posting_store.create(token_id) catch {
                    return FtsError.PostingError;
                };
                self.dictionary.setPostingPage(term, page_id) catch {
                    return FtsError.DictionaryError;
                };
                dict_entry.posting_page = page_id;
            }

            // Append posting entry with positions if configured
            const posting_entry = PostingEntry{
                .doc_id = doc_id,
                .term_freq = info.freq,
                .positions = if (self.config.store_positions and info.positions.items.len > 0)
                    info.positions.items
                else
                    null,
            };

            _ = self.posting_store.appendWithPositions(
                dict_entry.posting_page,
                posting_entry,
                self.config.store_positions,
            ) catch {
                return FtsError.PostingError;
            };

            // Update document frequency
            self.dictionary.incrementDocFreq(term) catch {
                return FtsError.DictionaryError;
            };
        }

        // 4. Store document length
        self.doc_lengths.setLength(doc_id, @intCast(tokens.len)) catch {
            return FtsError.ScorerError;
        };

        // 5. Store reverse index entry for document deletion support
        if (self.reverse_index) |*ri| {
            // Collect unique terms - allocate array directly
            const num_terms = term_infos.count();
            const term_slice = self.allocator.alloc([]const u8, num_terms) catch {
                return FtsError.OutOfMemory;
            };
            defer self.allocator.free(term_slice);

            var term_iter = term_infos.iterator();
            var idx: usize = 0;
            while (term_iter.next()) |entry| {
                term_slice[idx] = entry.key_ptr.*;
                idx += 1;
            }

            ri.setDocTerms(doc_id, term_slice) catch {
                return FtsError.IoError;
            };
        }

        // Update scorer's reference to doc_lengths
        self.scorer = Bm25Scorer.init(self.config.bm25, &self.doc_lengths);

        return @intCast(tokens.len);
    }

    /// Remove a document from the index
    /// If reverse_index is available, properly cleans up posting lists and stats
    pub fn removeDocument(self: *Self, doc_id: DocId) FtsError!void {
        // Track if any dictionary stat updates failed (non-fatal but should be reported)
        var stats_update_failed = false;

        // If we have a reverse index, do proper cleanup
        if (self.reverse_index) |*ri| {
            // 1. Get terms from reverse index
            const terms = ri.getDocTerms(doc_id) catch {
                return FtsError.IoError;
            } orelse {
                // Document not in reverse index, just remove doc length
                self.doc_lengths.removeDoc(doc_id) catch |err| {
                    return switch (err) {
                        error.OutOfMemory => FtsError.OutOfMemory,
                        else => FtsError.IoError,
                    };
                };
                return;
            };
            defer ri.freeTerms(terms);

            // 2. For each term, remove from posting list and update dictionary
            for (terms) |term| {
                const entry = self.dictionary.get(term) catch continue orelse continue;

                if (entry.posting_page != 0) {
                    const result = self.posting_store.removeEntry(entry.posting_page, doc_id) catch continue;
                    if (result.found) {
                        // Update dictionary statistics - track failures
                        self.dictionary.decrementDocFreq(term) catch {
                            stats_update_failed = true;
                        };
                        self.dictionary.subtractTotalFreq(term, result.term_freq) catch {
                            stats_update_failed = true;
                        };
                    }
                }
            }

            // 3. Remove reverse index entry
            ri.removeDoc(doc_id) catch |err| {
                return switch (err) {
                    error.OutOfMemory => FtsError.OutOfMemory,
                    else => FtsError.IoError,
                };
            };
        }

        // 4. Remove from doc lengths (this updates stats)
        self.doc_lengths.removeDoc(doc_id) catch |err| {
            return switch (err) {
                error.OutOfMemory => FtsError.OutOfMemory,
                else => FtsError.IoError,
            };
        };

        // If dictionary stats updates failed, report it
        // The document is removed but stats may be inconsistent
        if (stats_update_failed) {
            return FtsError.DictionaryError;
        }
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
            // Shrink to limit - allocate new smaller slice
            const trimmed = self.allocator.alloc(ScoredDoc, limit) catch {
                self.allocator.free(result_slice);
                return FtsError.OutOfMemory;
            };
            @memcpy(trimmed, result_slice[0..limit]);
            self.allocator.free(result_slice);
            return trimmed;
        }
        return result_slice;
    }

    /// Search for multiple terms (AND semantics - documents must contain all terms)
    /// Uses skip pointer optimization for efficient intersection
    fn searchMultiTerm(self: *Self, terms: []const []const u8, limit: u32) FtsError![]ScoredDoc {
        // Use optimized skip pointer intersection
        return self.searchMultiTermWithSkip(terms, limit);
    }

    /// Optimized multi-term AND search using skip pointers
    /// Uses the smallest posting list to drive intersection
    fn searchMultiTermWithSkip(self: *Self, terms: []const []const u8, limit: u32) FtsError![]ScoredDoc {
        if (terms.len == 0) {
            return &[_]ScoredDoc{};
        }

        if (terms.len == 1) {
            return self.searchSingleTerm(terms[0], limit);
        }

        // Gather dictionary entries and sort by doc_freq (smallest first)
        const SearchTermInfo = struct {
            term: []const u8,
            dict_entry: DictionaryEntry,
        };

        var term_infos_buf: [32]SearchTermInfo = undefined;
        var num_valid_terms: usize = 0;

        for (terms) |term| {
            if (num_valid_terms >= 32) break;

            const dict_entry = self.dictionary.get(term) catch continue orelse continue;
            if (dict_entry.posting_page == 0) continue;

            term_infos_buf[num_valid_terms] = .{
                .term = term,
                .dict_entry = dict_entry,
            };
            num_valid_terms += 1;
        }

        if (num_valid_terms == 0) {
            return &[_]ScoredDoc{};
        }

        // Sort by doc_freq ascending (smallest list first)
        const term_infos = term_infos_buf[0..num_valid_terms];
        std.mem.sort(SearchTermInfo, term_infos, {}, struct {
            fn lessThan(_: void, a: SearchTermInfo, b: SearchTermInfo) bool {
                return a.dict_entry.doc_freq < b.dict_entry.doc_freq;
            }
        }.lessThan);

        // Use smallest posting list to drive intersection
        const driver = term_infos[0];
        var driver_iter = self.posting_store.iterate(driver.dict_entry.posting_page) catch {
            return FtsError.PostingError;
        };
        defer driver_iter.deinit();

        // Create iterators for other terms
        var other_iters: [31]posting_mod.PostingIterator = undefined;
        var num_other_iters: usize = 0;

        for (term_infos[1..]) |info| {
            other_iters[num_other_iters] = self.posting_store.iterate(info.dict_entry.posting_page) catch continue;
            num_other_iters += 1;
        }
        defer {
            for (other_iters[0..num_other_iters]) |*iter| {
                iter.deinit();
            }
        }

        // Intersection with skip pointers
        var results: ArrayListUnmanaged(ScoredDoc) = .{};
        errdefer results.deinit(self.allocator);

        while (driver_iter.next() catch null) |driver_entry| {
            const doc_id = driver_entry.doc_id;
            var all_match = true;
            var total_score: f32 = 0.0;

            // Score from driver term
            const driver_doc_length = self.doc_lengths.getLength(doc_id) catch {
                continue;
            };
            total_score += self.scorer.scoreTerm(
                driver_entry.term_freq,
                driver.dict_entry.doc_freq,
                driver_doc_length,
            );

            // Check other terms using skipTo
            for (other_iters[0..num_other_iters], term_infos[1..][0..num_other_iters]) |*iter, info| {
                const other_entry = iter.skipTo(doc_id) catch null;
                if (other_entry == null or other_entry.?.doc_id != doc_id) {
                    all_match = false;
                    break;
                }

                // Add score contribution
                total_score += self.scorer.scoreTerm(
                    other_entry.?.term_freq,
                    info.dict_entry.doc_freq,
                    driver_doc_length,
                );
            }

            if (all_match) {
                results.append(self.allocator, .{
                    .doc_id = doc_id,
                    .score = total_score,
                }) catch {
                    return FtsError.OutOfMemory;
                };

                // Early exit if we have enough results (before sorting)
                // This is an optimization for large result sets
                if (results.items.len >= limit * 2) {
                    break;
                }
            }
        }

        // Sort by score descending
        std.mem.sort(ScoredDoc, results.items, {}, ScoredDoc.lessThan);

        // Return top K
        const result_slice = results.toOwnedSlice(self.allocator) catch {
            return FtsError.OutOfMemory;
        };

        if (result_slice.len > limit) {
            const trimmed = self.allocator.alloc(ScoredDoc, limit) catch {
                self.allocator.free(result_slice);
                return FtsError.OutOfMemory;
            };
            @memcpy(trimmed, result_slice[0..limit]);
            self.allocator.free(result_slice);
            return trimmed;
        }
        return result_slice;
    }

    /// Free search results
    pub fn freeResults(self: *Self, results: []ScoredDoc) void {
        // Empty slices from &[_]ScoredDoc{} are compile-time literals, not allocated
        if (results.len == 0) return;
        self.allocator.free(results);
    }

    // ========================================================================
    // Boolean Search Operations
    // ========================================================================

    /// Search with OR semantics (any term matches)
    pub fn searchOr(
        self: *Self,
        query_text: []const u8,
        limit: u32,
    ) FtsError![]ScoredDoc {
        return self.searchWithMode(query_text, .@"or", limit);
    }

    /// Search with fuzzy matching (typo tolerance)
    /// Terms with edit distance <= max_distance will match
    pub fn searchFuzzy(
        self: *Self,
        query_text: []const u8,
        fuzzy_config: fuzzy_mod.FuzzyConfig,
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

        // Collect all document scores (handles multiple fuzzy expansions)
        var doc_scores = std.AutoHashMap(DocId, f32).init(self.allocator);
        defer doc_scores.deinit();

        // Process each query term
        for (tokens) |token| {
            // Normalize token
            var lower_buf: [64]u8 = undefined;
            const normalized = if (token.text.len <= 64) blk: {
                for (token.text, 0..) |c, i| {
                    lower_buf[i] = std.ascii.toLower(c);
                }
                break :blk lower_buf[0..token.text.len];
            } else continue;

            // Skip short terms for fuzzy matching
            if (normalized.len < fuzzy_config.min_term_length) {
                // Try exact match for short terms
                const results = try self.searchSingleTerm(normalized, limit * 2);
                defer self.allocator.free(results);
                for (results) |doc| {
                    const current = doc_scores.get(doc.doc_id) orelse 0.0;
                    doc_scores.put(doc.doc_id, @max(current, doc.score)) catch continue;
                }
                continue;
            }

            // Find fuzzy matches for this term
            const fuzzy_matches = fuzzy_mod.expandFuzzyTerms(
                self.allocator,
                normalized,
                &self.dictionary,
                fuzzy_config.max_distance,
            ) catch {
                return FtsError.DictionaryError;
            };
            defer fuzzy_mod.freeMatches(self.allocator, fuzzy_matches);

            // Score documents from each fuzzy match
            for (fuzzy_matches) |match| {
                if (match.entry.posting_page == 0) continue;

                var iter = self.posting_store.iterate(match.entry.posting_page) catch continue;
                defer iter.deinit();

                while (iter.next() catch null) |posting| {
                    const doc_length = self.doc_lengths.getLength(posting.doc_id) catch continue;

                    // Score with fuzzy penalty
                    const score = self.scorer.scoreTermFuzzy(
                        posting.term_freq,
                        match.entry.doc_freq,
                        doc_length,
                        match.distance,
                        fuzzy_config.max_distance,
                    );

                    // Accumulate scores (max for same doc from different terms)
                    const current = doc_scores.get(posting.doc_id) orelse 0.0;
                    doc_scores.put(posting.doc_id, current + score) catch continue;
                }
            }
        }

        // Convert to result array
        var results: ArrayListUnmanaged(ScoredDoc) = .{};
        errdefer results.deinit(self.allocator);

        var iter = doc_scores.iterator();
        while (iter.next()) |entry| {
            results.append(self.allocator, .{
                .doc_id = entry.key_ptr.*,
                .score = entry.value_ptr.*,
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
            const trimmed = self.allocator.alloc(ScoredDoc, limit) catch {
                self.allocator.free(result_slice);
                return FtsError.OutOfMemory;
            };
            @memcpy(trimmed, result_slice[0..limit]);
            self.allocator.free(result_slice);
            return trimmed;
        }
        return result_slice;
    }

    /// Search with prefix/wildcard matching
    /// Terms ending with * are expanded to all matching dictionary terms
    /// Example: "optim*" matches "optimize", "optimization", "optimizer"
    pub fn searchWithPrefix(
        self: *Self,
        query_text: []const u8,
        prefix_config: prefix_mod.PrefixConfig,
        limit: u32,
    ) FtsError![]ScoredDoc {
        // Collect all document scores
        var doc_scores = std.AutoHashMap(DocId, f32).init(self.allocator);
        defer doc_scores.deinit();

        // Track term counts for AND semantics
        var doc_term_counts = std.AutoHashMap(DocId, u32).init(self.allocator);
        defer doc_term_counts.deinit();

        // Parse query manually to detect wildcards
        var i: usize = 0;
        var total_terms: u32 = 0;

        while (i < query_text.len) {
            // Skip whitespace
            while (i < query_text.len and std.ascii.isWhitespace(query_text[i])) {
                i += 1;
            }
            if (i >= query_text.len) break;

            // Check for exclusion prefix (skip for now - basic implementation)
            if (query_text[i] == '-') {
                // Skip excluded terms
                i += 1;
                while (i < query_text.len and !std.ascii.isWhitespace(query_text[i])) {
                    i += 1;
                }
                continue;
            }

            // Find end of term (including potential *)
            const start = i;
            while (i < query_text.len and !std.ascii.isWhitespace(query_text[i])) {
                i += 1;
            }

            if (i > start) {
                const term = query_text[start..i];
                total_terms += 1;

                // Check if this is a wildcard term
                if (prefix_mod.hasWildcard(term)) {
                    const prefix = prefix_mod.extractPrefix(term);

                    // Normalize to lowercase
                    var lower_buf: [64]u8 = undefined;
                    if (prefix.len > 64 or prefix.len < prefix_config.min_prefix_length) {
                        continue;
                    }
                    for (prefix, 0..) |c, j| {
                        lower_buf[j] = std.ascii.toLower(c);
                    }
                    const normalized_prefix = lower_buf[0..prefix.len];

                    // Expand prefix to matching terms
                    const prefix_matches = prefix_mod.expandPrefixTerms(
                        self.allocator,
                        normalized_prefix,
                        &self.dictionary,
                        prefix_config,
                    ) catch {
                        return FtsError.DictionaryError;
                    };
                    defer prefix_mod.freeMatches(self.allocator, prefix_matches);

                    // Score documents from each prefix match
                    for (prefix_matches) |match| {
                        if (match.entry.posting_page == 0) continue;

                        var iter = self.posting_store.iterate(match.entry.posting_page) catch continue;
                        defer iter.deinit();

                        while (iter.next() catch null) |posting| {
                            const doc_length = self.doc_lengths.getLength(posting.doc_id) catch continue;

                            // Score without penalty (exact prefix match)
                            const score = self.scorer.scoreTerm(
                                posting.term_freq,
                                match.entry.doc_freq,
                                doc_length,
                            );

                            // Accumulate scores
                            const current = doc_scores.get(posting.doc_id) orelse 0.0;
                            doc_scores.put(posting.doc_id, current + score) catch continue;

                            // Track term presence for AND semantics
                            const count = doc_term_counts.get(posting.doc_id) orelse 0;
                            if (count < total_terms) {
                                doc_term_counts.put(posting.doc_id, count + 1) catch continue;
                            }
                        }
                    }
                } else {
                    // Regular term - exact match
                    var lower_buf: [64]u8 = undefined;
                    if (term.len > 64 or term.len < self.config.tokenizer.min_token_length) {
                        continue;
                    }
                    for (term, 0..) |c, j| {
                        lower_buf[j] = std.ascii.toLower(c);
                    }
                    const normalized = lower_buf[0..term.len];

                    // Skip stop words
                    if (self.config.tokenizer.remove_stop_words and
                        stopwords_mod.isStopWord(normalized, self.config.tokenizer.language))
                    {
                        total_terms -= 1; // Don't count stop words
                        continue;
                    }

                    // Look up in dictionary
                    const dict_entry = self.dictionary.get(normalized) catch continue orelse continue;
                    if (dict_entry.posting_page == 0) continue;

                    var iter = self.posting_store.iterate(dict_entry.posting_page) catch continue;
                    defer iter.deinit();

                    while (iter.next() catch null) |posting| {
                        const doc_length = self.doc_lengths.getLength(posting.doc_id) catch continue;

                        const score = self.scorer.scoreTerm(
                            posting.term_freq,
                            dict_entry.doc_freq,
                            doc_length,
                        );

                        const current = doc_scores.get(posting.doc_id) orelse 0.0;
                        doc_scores.put(posting.doc_id, current + score) catch continue;

                        const count = doc_term_counts.get(posting.doc_id) orelse 0;
                        if (count < total_terms) {
                            doc_term_counts.put(posting.doc_id, count + 1) catch continue;
                        }
                    }
                }
            }
        }

        // Convert to result array (AND semantics: require all terms)
        var results: ArrayListUnmanaged(ScoredDoc) = .{};
        errdefer results.deinit(self.allocator);

        var score_iter = doc_scores.iterator();
        while (score_iter.next()) |entry| {
            const doc_id = entry.key_ptr.*;
            const term_count = doc_term_counts.get(doc_id) orelse 0;

            // Only include if document has all terms (AND semantics)
            if (total_terms == 0 or term_count >= total_terms) {
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
            const trimmed = self.allocator.alloc(ScoredDoc, limit) catch {
                self.allocator.free(result_slice);
                return FtsError.OutOfMemory;
            };
            @memcpy(trimmed, result_slice[0..limit]);
            self.allocator.free(result_slice);
            return trimmed;
        }
        return result_slice;
    }

    /// Search with explicit mode (AND or OR)
    /// Supports quoted phrases: `database "quick brown" -mysql`
    pub fn searchWithMode(
        self: *Self,
        query_text: []const u8,
        mode: QueryMode,
        limit: u32,
    ) FtsError![]ScoredDoc {
        // Parse query (handles -exclusions and "phrases")
        var terms_buf: [32][]u8 = undefined;
        var excluded_buf: [32][]u8 = undefined;
        var phrases_buf: [MAX_PHRASES][MAX_PHRASE_TERMS][]u8 = undefined;
        var phrase_lens: [MAX_PHRASES]usize = undefined;
        var num_terms: usize = 0;
        var num_excluded: usize = 0;
        var num_phrases: usize = 0;

        try self.parseQueryTerms(
            query_text,
            &terms_buf,
            &num_terms,
            &excluded_buf,
            &num_excluded,
            &phrases_buf,
            &phrase_lens,
            &num_phrases,
        );

        // Cleanup on exit
        defer {
            for (terms_buf[0..num_terms]) |t| self.allocator.free(t);
            for (excluded_buf[0..num_excluded]) |t| self.allocator.free(t);
            for (0..num_phrases) |p| {
                for (phrases_buf[p][0..phrase_lens[p]]) |t| self.allocator.free(t);
            }
        }

        if (num_terms == 0 and num_phrases == 0) {
            return &[_]ScoredDoc{};
        }

        // Cast to const slices
        var const_terms: [32][]const u8 = undefined;
        for (terms_buf[0..num_terms], 0..) |t, i| {
            const_terms[i] = t;
        }
        var const_excluded: [32][]const u8 = undefined;
        for (excluded_buf[0..num_excluded], 0..) |t, i| {
            const_excluded[i] = t;
        }

        // Get initial candidates based on terms and phrases
        var candidates: []ScoredDoc = undefined;
        var candidates_owned = false;

        if (num_phrases > 0 and self.config.store_positions) {
            // Search phrases first (most restrictive)
            var const_phrase: [MAX_PHRASE_TERMS][]const u8 = undefined;
            for (phrases_buf[0][0..phrase_lens[0]], 0..) |t, i| {
                const_phrase[i] = t;
            }
            candidates = try self.searchPhraseTerms(const_phrase[0..phrase_lens[0]], limit * 4);
            candidates_owned = true;

            // Intersect with additional phrases
            for (1..num_phrases) |p| {
                var next_phrase: [MAX_PHRASE_TERMS][]const u8 = undefined;
                for (phrases_buf[p][0..phrase_lens[p]], 0..) |t, i| {
                    next_phrase[i] = t;
                }
                const phrase_results = try self.searchPhraseTerms(next_phrase[0..phrase_lens[p]], limit * 4);
                defer self.allocator.free(phrase_results);

                // Build set of phrase matches
                var phrase_docs = std.AutoHashMap(DocId, f32).init(self.allocator);
                defer phrase_docs.deinit();
                for (phrase_results) |doc| {
                    phrase_docs.put(doc.doc_id, doc.score) catch continue;
                }

                // Filter candidates to those matching this phrase
                var filtered: ArrayListUnmanaged(ScoredDoc) = .{};
                for (candidates) |doc| {
                    if (phrase_docs.get(doc.doc_id)) |score| {
                        filtered.append(self.allocator, .{
                            .doc_id = doc.doc_id,
                            .score = doc.score + score,
                        }) catch continue;
                    }
                }

                self.allocator.free(candidates);
                candidates = filtered.toOwnedSlice(self.allocator) catch {
                    return FtsError.OutOfMemory;
                };
            }

            // Intersect with regular terms if any
            if (num_terms > 0) {
                const term_results = switch (mode) {
                    .@"and" => if (num_terms == 1)
                        try self.searchSingleTerm(const_terms[0], limit * 4)
                    else
                        try self.searchMultiTerm(const_terms[0..num_terms], limit * 4),
                    .@"or" => if (num_terms == 1)
                        try self.searchSingleTerm(const_terms[0], limit * 4)
                    else
                        try self.searchMultiTermOr(const_terms[0..num_terms], limit * 4),
                };
                defer self.allocator.free(term_results);

                // Build set of term matches
                var term_docs = std.AutoHashMap(DocId, f32).init(self.allocator);
                defer term_docs.deinit();
                for (term_results) |doc| {
                    term_docs.put(doc.doc_id, doc.score) catch continue;
                }

                // Filter phrase candidates by term matches (AND mode) or merge (OR mode)
                var filtered: ArrayListUnmanaged(ScoredDoc) = .{};
                if (mode == .@"and") {
                    for (candidates) |doc| {
                        if (term_docs.get(doc.doc_id)) |score| {
                            filtered.append(self.allocator, .{
                                .doc_id = doc.doc_id,
                                .score = doc.score + score,
                            }) catch continue;
                        }
                    }
                } else {
                    // OR mode: include all from both
                    var seen = std.AutoHashMap(DocId, void).init(self.allocator);
                    defer seen.deinit();
                    for (candidates) |doc| {
                        const extra = term_docs.get(doc.doc_id) orelse 0;
                        filtered.append(self.allocator, .{
                            .doc_id = doc.doc_id,
                            .score = doc.score + extra,
                        }) catch continue;
                        seen.put(doc.doc_id, {}) catch continue;
                    }
                    for (term_results) |doc| {
                        if (!seen.contains(doc.doc_id)) {
                            filtered.append(self.allocator, doc) catch continue;
                        }
                    }
                }

                self.allocator.free(candidates);
                candidates = filtered.toOwnedSlice(self.allocator) catch {
                    return FtsError.OutOfMemory;
                };
            }
        } else if (num_terms > 0) {
            // No phrases (or positions not stored), just use term search
            candidates = switch (mode) {
                .@"and" => if (num_terms == 1)
                    try self.searchSingleTerm(const_terms[0], limit * 2)
                else
                    try self.searchMultiTerm(const_terms[0..num_terms], limit * 2),
                .@"or" => if (num_terms == 1)
                    try self.searchSingleTerm(const_terms[0], limit * 2)
                else
                    try self.searchMultiTermOr(const_terms[0..num_terms], limit * 2),
            };
            candidates_owned = true;
        } else {
            return &[_]ScoredDoc{};
        }

        defer if (candidates_owned) self.allocator.free(candidates);

        // Apply exclusions if any
        if (num_excluded == 0) {
            // No exclusions - sort and return top K
            std.mem.sort(ScoredDoc, candidates, {}, ScoredDoc.lessThan);
            if (candidates.len <= limit) {
                const result = self.allocator.dupe(ScoredDoc, candidates) catch {
                    return FtsError.OutOfMemory;
                };
                return result;
            }
            const result = self.allocator.dupe(ScoredDoc, candidates[0..limit]) catch {
                return FtsError.OutOfMemory;
            };
            return result;
        }

        // Build exclusion set
        var excluded_docs = std.AutoHashMap(DocId, void).init(self.allocator);
        defer excluded_docs.deinit();

        for (const_excluded[0..num_excluded]) |term| {
            const dict_entry = self.dictionary.get(term) catch continue orelse continue;
            if (dict_entry.posting_page == 0) continue;

            var iter = self.posting_store.iterate(dict_entry.posting_page) catch continue;
            defer iter.deinit();

            while (iter.next() catch null) |entry| {
                excluded_docs.put(entry.doc_id, {}) catch continue;
            }
        }

        // Filter results
        var results: ArrayListUnmanaged(ScoredDoc) = .{};
        errdefer results.deinit(self.allocator);

        for (candidates) |doc| {
            if (!excluded_docs.contains(doc.doc_id)) {
                results.append(self.allocator, doc) catch {
                    return FtsError.OutOfMemory;
                };
            }
        }

        // Sort and return top K
        std.mem.sort(ScoredDoc, results.items, {}, ScoredDoc.lessThan);

        const result_slice = results.toOwnedSlice(self.allocator) catch {
            return FtsError.OutOfMemory;
        };

        if (result_slice.len > limit) {
            const trimmed = self.allocator.alloc(ScoredDoc, limit) catch {
                self.allocator.free(result_slice);
                return FtsError.OutOfMemory;
            };
            @memcpy(trimmed, result_slice[0..limit]);
            self.allocator.free(result_slice);
            return trimmed;
        }
        return result_slice;
    }

    /// Phrase storage: array of term arrays (max 8 phrases, max 8 terms each)
    const MAX_PHRASES: usize = 8;
    const MAX_PHRASE_TERMS: usize = 8;

    /// Parse query text into terms, phrases, and exclusions
    /// Phrases are detected by quoted strings: "quick brown fox"
    fn parseQueryTerms(
        self: *Self,
        query_text: []const u8,
        terms_buf: *[32][]u8,
        num_terms: *usize,
        excluded_buf: *[32][]u8,
        num_excluded: *usize,
        phrases_buf: *[MAX_PHRASES][MAX_PHRASE_TERMS][]u8,
        phrase_lens: *[MAX_PHRASES]usize,
        num_phrases: *usize,
    ) FtsError!void {
        num_terms.* = 0;
        num_excluded.* = 0;
        num_phrases.* = 0;

        var i: usize = 0;
        while (i < query_text.len) {
            // Skip whitespace
            while (i < query_text.len and std.ascii.isWhitespace(query_text[i])) {
                i += 1;
            }
            if (i >= query_text.len) break;

            // Check for exclusion prefix
            const is_excluded = query_text[i] == '-';
            if (is_excluded) i += 1;
            if (i >= query_text.len) break;

            // Check for quoted phrase
            if (query_text[i] == '"') {
                i += 1; // Skip opening quote
                if (i >= query_text.len) break;

                // Parse phrase terms until closing quote
                if (num_phrases.* < MAX_PHRASES) {
                    const phrase_idx = num_phrases.*;
                    phrase_lens[phrase_idx] = 0;

                    while (i < query_text.len and query_text[i] != '"') {
                        // Skip whitespace within phrase
                        while (i < query_text.len and query_text[i] != '"' and std.ascii.isWhitespace(query_text[i])) {
                            i += 1;
                        }
                        if (i >= query_text.len or query_text[i] == '"') break;

                        // Find word end
                        const start = i;
                        while (i < query_text.len and query_text[i] != '"' and (std.ascii.isAlphanumeric(query_text[i]) or query_text[i] == '_')) {
                            i += 1;
                        }

                        if (i > start and phrase_lens[phrase_idx] < MAX_PHRASE_TERMS) {
                            const word = query_text[start..i];

                            // Skip if too short
                            if (word.len >= self.config.tokenizer.min_token_length) {
                                // Normalize to lowercase
                                const normalized = self.allocator.alloc(u8, word.len) catch {
                                    return FtsError.OutOfMemory;
                                };
                                for (word, 0..) |c, j| {
                                    normalized[j] = std.ascii.toLower(c);
                                }

                                // Don't filter stop words in phrases (they're positionally significant)
                                phrases_buf[phrase_idx][phrase_lens[phrase_idx]] = normalized;
                                phrase_lens[phrase_idx] += 1;
                            }
                        }
                    }

                    // Skip closing quote
                    if (i < query_text.len and query_text[i] == '"') {
                        i += 1;
                    }

                    // Only count phrase if it has at least 2 terms
                    if (phrase_lens[phrase_idx] >= 2) {
                        num_phrases.* += 1;
                    } else {
                        // Single word "phrase" - add as regular term
                        if (phrase_lens[phrase_idx] == 1) {
                            if (is_excluded) {
                                if (num_excluded.* < 32) {
                                    excluded_buf[num_excluded.*] = phrases_buf[phrase_idx][0];
                                    num_excluded.* += 1;
                                } else {
                                    self.allocator.free(phrases_buf[phrase_idx][0]);
                                }
                            } else {
                                if (num_terms.* < 32) {
                                    terms_buf[num_terms.*] = phrases_buf[phrase_idx][0];
                                    num_terms.* += 1;
                                } else {
                                    self.allocator.free(phrases_buf[phrase_idx][0]);
                                }
                            }
                        }
                    }
                }
                continue;
            }

            // Find word end (regular term)
            const start = i;
            while (i < query_text.len and (std.ascii.isAlphanumeric(query_text[i]) or query_text[i] == '_')) {
                i += 1;
            }

            if (i > start) {
                const word = query_text[start..i];

                // Skip "OR" keyword and stop words
                if (std.ascii.eqlIgnoreCase(word, "OR") or
                    std.ascii.eqlIgnoreCase(word, "AND") or
                    std.ascii.eqlIgnoreCase(word, "NOT"))
                {
                    continue;
                }

                // Skip if too short
                if (word.len < self.config.tokenizer.min_token_length) continue;

                // Normalize to lowercase
                const normalized = self.allocator.alloc(u8, word.len) catch {
                    return FtsError.OutOfMemory;
                };
                for (word, 0..) |c, j| {
                    normalized[j] = std.ascii.toLower(c);
                }

                // Check stop words (language-aware)
                if (self.config.tokenizer.remove_stop_words and
                    stopwords_mod.isStopWord(normalized, self.config.tokenizer.language))
                {
                    self.allocator.free(normalized);
                    continue;
                }

                // Add to appropriate list
                if (is_excluded) {
                    if (num_excluded.* < 32) {
                        excluded_buf[num_excluded.*] = normalized;
                        num_excluded.* += 1;
                    } else {
                        self.allocator.free(normalized);
                    }
                } else {
                    if (num_terms.* < 32) {
                        terms_buf[num_terms.*] = normalized;
                        num_terms.* += 1;
                    } else {
                        self.allocator.free(normalized);
                    }
                }
            }
        }
    }

    /// Search with OR semantics (documents matching any term)
    fn searchMultiTermOr(self: *Self, terms: []const []const u8, limit: u32) FtsError![]ScoredDoc {
        if (terms.len == 0) {
            return &[_]ScoredDoc{};
        }

        // Collect scores for all documents matching any term
        var doc_scores = std.AutoHashMap(DocId, f32).init(self.allocator);
        defer doc_scores.deinit();

        for (terms) |term| {
            const dict_entry = self.dictionary.get(term) catch continue orelse continue;
            if (dict_entry.posting_page == 0) continue;

            var iter = self.posting_store.iterate(dict_entry.posting_page) catch continue;
            defer iter.deinit();

            while (iter.next() catch null) |entry| {
                const doc_length = self.doc_lengths.getLength(entry.doc_id) catch continue;

                const score = self.scorer.scoreTerm(
                    entry.term_freq,
                    dict_entry.doc_freq,
                    doc_length,
                );

                // Accumulate score (OR = sum of matching term scores)
                const result = doc_scores.getOrPut(entry.doc_id) catch {
                    return FtsError.OutOfMemory;
                };
                if (result.found_existing) {
                    result.value_ptr.* += score;
                } else {
                    result.value_ptr.* = score;
                }
            }
        }

        // Collect all results (no term count filter for OR)
        var results: ArrayListUnmanaged(ScoredDoc) = .{};
        errdefer results.deinit(self.allocator);

        var score_iter = doc_scores.iterator();
        while (score_iter.next()) |entry| {
            results.append(self.allocator, .{
                .doc_id = entry.key_ptr.*,
                .score = entry.value_ptr.*,
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
            const trimmed = self.allocator.alloc(ScoredDoc, limit) catch {
                self.allocator.free(result_slice);
                return FtsError.OutOfMemory;
            };
            @memcpy(trimmed, result_slice[0..limit]);
            self.allocator.free(result_slice);
            return trimmed;
        }
        return result_slice;
    }

    // ========================================================================
    // Phrase Search
    // ========================================================================

    /// Search for an exact phrase (terms must appear adjacent in order)
    /// Requires store_positions=true in config
    pub fn searchPhrase(
        self: *Self,
        phrase_text: []const u8,
        limit: u32,
    ) FtsError![]ScoredDoc {
        if (!self.config.store_positions) {
            // Fall back to AND search if positions not stored
            return self.search(phrase_text, limit);
        }

        // Tokenize phrase
        var tok = Tokenizer.init(self.allocator, phrase_text, self.config.tokenizer);
        const tokens = tok.tokenizeAll() catch {
            return FtsError.TokenizerError;
        };
        defer self.allocator.free(tokens);

        if (tokens.len == 0) {
            return &[_]ScoredDoc{};
        }

        // Single word "phrase" is just a regular search
        if (tokens.len == 1) {
            var lower_buf: [64]u8 = undefined;
            if (tokens[0].text.len <= 64) {
                for (tokens[0].text, 0..) |c, i| {
                    lower_buf[i] = std.ascii.toLower(c);
                }
                return self.searchSingleTerm(lower_buf[0..tokens[0].text.len], limit);
            }
            return &[_]ScoredDoc{};
        }

        // Collect normalized terms
        var phrase_terms: [32][]u8 = undefined;
        var num_terms: usize = 0;
        defer {
            for (phrase_terms[0..num_terms]) |t| {
                self.allocator.free(t);
            }
        }

        for (tokens) |token| {
            if (num_terms >= 32) break;
            if (token.text.len > 64) continue;

            const normalized = self.allocator.alloc(u8, token.text.len) catch {
                return FtsError.OutOfMemory;
            };
            for (token.text, 0..) |c, i| {
                normalized[i] = std.ascii.toLower(c);
            }
            phrase_terms[num_terms] = normalized;
            num_terms += 1;
        }

        if (num_terms < 2) {
            return &[_]ScoredDoc{};
        }

        return self.searchPhraseTerms(phrase_terms[0..num_terms], limit);
    }

    /// Search for phrase with pre-tokenized terms
    fn searchPhraseTerms(self: *Self, terms: []const []const u8, limit: u32) FtsError![]ScoredDoc {
        if (terms.len < 2) {
            return &[_]ScoredDoc{};
        }

        // Collect postings for first term (smallest result set optimization)
        var first_term_docs = std.AutoHashMap(DocId, []const u32).init(self.allocator);
        defer {
            var it = first_term_docs.valueIterator();
            while (it.next()) |positions| {
                self.allocator.free(positions.*);
            }
            first_term_docs.deinit();
        }

        const first_entry = self.dictionary.get(terms[0]) catch {
            return FtsError.DictionaryError;
        } orelse {
            return &[_]ScoredDoc{};
        };

        if (first_entry.posting_page == 0) {
            return &[_]ScoredDoc{};
        }

        var iter = self.posting_store.iterate(first_entry.posting_page) catch {
            return FtsError.PostingError;
        };
        defer iter.deinit();

        while (true) {
            const entry_opt = iter.nextWithPositions(self.allocator) catch {
                break;
            };
            if (entry_opt) |entry| {
                if (entry.positions) |pos| {
                    first_term_docs.put(entry.doc_id, pos) catch {
                        self.allocator.free(pos);
                        return FtsError.OutOfMemory;
                    };
                }
            } else {
                break;
            }
        }

        // Now check each subsequent term and verify phrase positions
        var phrase_matches = std.AutoHashMap(DocId, f32).init(self.allocator);
        defer phrase_matches.deinit();

        // Initialize candidates from first term
        var first_iter = first_term_docs.iterator();
        while (first_iter.next()) |entry| {
            phrase_matches.put(entry.key_ptr.*, 0.0) catch {
                return FtsError.OutOfMemory;
            };
        }

        // For each subsequent term, verify position adjacency
        for (terms[1..], 1..) |term, term_idx| {
            const dict_entry = self.dictionary.get(term) catch {
                continue;
            } orelse {
                // Term not found, no phrase matches possible
                phrase_matches.clearAndFree();
                break;
            };

            if (dict_entry.posting_page == 0) {
                phrase_matches.clearAndFree();
                break;
            }

            var term_iter = self.posting_store.iterate(dict_entry.posting_page) catch {
                continue;
            };
            defer term_iter.deinit();

            // Track which docs still match after this term
            var still_matching = std.AutoHashMap(DocId, f32).init(self.allocator);
            defer still_matching.deinit();

            while (true) {
                const entry_opt = term_iter.nextWithPositions(self.allocator) catch {
                    break;
                };
                if (entry_opt) |entry| {
                    defer if (entry.positions) |p| self.allocator.free(p);

                    // Only check docs that matched so far
                    if (!phrase_matches.contains(entry.doc_id)) continue;

                    // Get first term positions for this doc
                    const first_positions = first_term_docs.get(entry.doc_id) orelse continue;
                    const current_positions = entry.positions orelse continue;

                    // Check if any position in current term = first_pos + term_idx
                    var found_adjacent = false;
                    for (first_positions) |first_pos| {
                        const expected_pos = first_pos + @as(u32, @intCast(term_idx));
                        for (current_positions) |cur_pos| {
                            if (cur_pos == expected_pos) {
                                found_adjacent = true;
                                break;
                            }
                        }
                        if (found_adjacent) break;
                    }

                    if (found_adjacent) {
                        // Get score contribution
                        const doc_length = self.doc_lengths.getLength(entry.doc_id) catch 1;
                        const score = self.scorer.scoreTerm(
                            entry.term_freq,
                            dict_entry.doc_freq,
                            doc_length,
                        );
                        const existing = phrase_matches.get(entry.doc_id) orelse 0.0;
                        still_matching.put(entry.doc_id, existing + score) catch {
                            return FtsError.OutOfMemory;
                        };
                    }
                } else {
                    break;
                }
            }

            // Update phrase_matches to only docs that still match
            phrase_matches.clearAndFree();
            var still_iter = still_matching.iterator();
            while (still_iter.next()) |entry| {
                phrase_matches.put(entry.key_ptr.*, entry.value_ptr.*) catch {
                    return FtsError.OutOfMemory;
                };
            }
        }

        // Add score from first term
        var match_iter = phrase_matches.iterator();
        while (match_iter.next()) |entry| {
            const doc_id = entry.key_ptr.*;
            const doc_length = self.doc_lengths.getLength(doc_id) catch 1;
            const first_score = self.scorer.scoreTerm(1, first_entry.doc_freq, doc_length);
            entry.value_ptr.* += first_score;
        }

        // Collect results
        var results: ArrayListUnmanaged(ScoredDoc) = .{};
        errdefer results.deinit(self.allocator);

        var result_iter = phrase_matches.iterator();
        while (result_iter.next()) |entry| {
            results.append(self.allocator, .{
                .doc_id = entry.key_ptr.*,
                .score = entry.value_ptr.*,
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
            const trimmed = self.allocator.alloc(ScoredDoc, limit) catch {
                self.allocator.free(result_slice);
                return FtsError.OutOfMemory;
            };
            @memcpy(trimmed, result_slice[0..limit]);
            self.allocator.free(result_slice);
            return trimmed;
        }
        return result_slice;
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

    var index = FtsIndex.init(allocator, &bp, &dict_tree, &lengths_tree, null, .{});

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

test "fts OR query" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const page_manager = lattice.storage.page_manager;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_fts_or_test.db";
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

    var index = FtsIndex.init(allocator, &bp, &dict_tree, &lengths_tree, null, .{});

    // Index documents with distinct terms
    _ = try index.indexDocument(1, "apple banana cherry");
    _ = try index.indexDocument(2, "banana date elderberry");
    _ = try index.indexDocument(3, "cherry fig grape");
    _ = try index.indexDocument(4, "date apple fig");

    // OR search: "apple banana" should match docs 1, 2, 4
    // doc1 has both, doc2 has banana, doc4 has apple
    const results1 = try index.searchOr("apple banana", 10);
    defer index.freeResults(results1);
    try std.testing.expectEqual(@as(usize, 3), results1.len);

    // AND search: "apple banana" should match only doc1
    const results2 = try index.search("apple banana", 10);
    defer index.freeResults(results2);
    try std.testing.expectEqual(@as(usize, 1), results2.len);
    try std.testing.expectEqual(@as(DocId, 1), results2[0].doc_id);

    // OR with non-existent term: "apple zebra" should match docs 1, 4
    const results3 = try index.searchOr("apple zebra", 10);
    defer index.freeResults(results3);
    try std.testing.expectEqual(@as(usize, 2), results3.len);
}

test "fts NOT query (exclusions)" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const page_manager = lattice.storage.page_manager;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_fts_not_test.db";
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

    var index = FtsIndex.init(allocator, &bp, &dict_tree, &lengths_tree, null, .{});

    // Index documents
    _ = try index.indexDocument(1, "apple banana cherry");
    _ = try index.indexDocument(2, "apple date elderberry");
    _ = try index.indexDocument(3, "apple fig grape");
    _ = try index.indexDocument(4, "banana cherry date");

    // Search "apple" without exclusion: should match 1, 2, 3
    const results1 = try index.search("apple", 10);
    defer index.freeResults(results1);
    try std.testing.expectEqual(@as(usize, 3), results1.len);

    // Search "apple -banana": should match 2, 3 (exclude 1 which has banana)
    const results2 = try index.searchWithMode("apple -banana", .@"and", 10);
    defer index.freeResults(results2);
    try std.testing.expectEqual(@as(usize, 2), results2.len);

    // Verify neither result is doc1
    for (results2) |doc| {
        try std.testing.expect(doc.doc_id != 1);
    }

    // Search "apple -banana -fig": should match only doc2
    const results3 = try index.searchWithMode("apple -banana -fig", .@"and", 10);
    defer index.freeResults(results3);
    try std.testing.expectEqual(@as(usize, 1), results3.len);
    try std.testing.expectEqual(@as(DocId, 2), results3[0].doc_id);
}

test "fts phrase query" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const page_manager = lattice.storage.page_manager;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_fts_phrase_test.db";
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

    // Enable position storage for phrase queries
    var index = FtsIndex.init(allocator, &bp, &dict_tree, &lengths_tree, null, .{
        .store_positions = true,
    });

    // Index documents with specific word ordering
    _ = try index.indexDocument(1, "quick brown fox"); // has "quick brown"
    _ = try index.indexDocument(2, "brown quick dog"); // has both but NOT adjacent as "quick brown"
    _ = try index.indexDocument(3, "the quick brown rabbit"); // has "quick brown"
    _ = try index.indexDocument(4, "very brown and quick"); // has both but NOT adjacent

    // Phrase search "quick brown" should only match docs where words are adjacent
    const results1 = try index.searchPhrase("quick brown", 10);
    defer index.freeResults(results1);

    // Should match doc1 and doc3 (where "quick brown" appears as a phrase)
    try std.testing.expectEqual(@as(usize, 2), results1.len);

    // Verify results are doc1 and doc3
    var found_doc1 = false;
    var found_doc3 = false;
    for (results1) |doc| {
        if (doc.doc_id == 1) found_doc1 = true;
        if (doc.doc_id == 3) found_doc3 = true;
    }
    try std.testing.expect(found_doc1);
    try std.testing.expect(found_doc3);

    // Compare with AND search which should match all 4 docs
    const results2 = try index.search("quick brown", 10);
    defer index.freeResults(results2);
    try std.testing.expectEqual(@as(usize, 4), results2.len);
}

test "fts quoted phrase syntax" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const page_manager = lattice.storage.page_manager;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_fts_quoted_phrase_test.db";
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

    // Enable position storage for phrase queries
    var index = FtsIndex.init(allocator, &bp, &dict_tree, &lengths_tree, null, .{
        .store_positions = true,
    });

    // Index documents
    _ = try index.indexDocument(1, "quick brown fox jumps");
    _ = try index.indexDocument(2, "brown quick dog runs");
    _ = try index.indexDocument(3, "the quick brown rabbit hops");
    _ = try index.indexDocument(4, "lazy brown dog sleeps");

    // Test 1: Quoted phrase "quick brown" via searchWithMode
    // Should match doc1 and doc3 where "quick brown" appears adjacently
    const results1 = try index.searchWithMode("\"quick brown\"", .@"and", 10);
    defer index.freeResults(results1);
    try std.testing.expectEqual(@as(usize, 2), results1.len);

    // Test 2: Quoted phrase with additional term: "quick brown" jumps
    // Should match doc1 (has both phrase AND term "jumps")
    const results2 = try index.searchWithMode("\"quick brown\" jumps", .@"and", 10);
    defer index.freeResults(results2);
    try std.testing.expectEqual(@as(usize, 1), results2.len);
    try std.testing.expectEqual(@as(DocId, 1), results2[0].doc_id);

    // Test 3: Quoted phrase with exclusion: "quick brown" -fox
    // Should match doc3 (has phrase, excludes doc1 with "fox")
    const results3 = try index.searchWithMode("\"quick brown\" -fox", .@"and", 10);
    defer index.freeResults(results3);
    try std.testing.expectEqual(@as(usize, 1), results3.len);
    try std.testing.expectEqual(@as(DocId, 3), results3[0].doc_id);

    // Test 4: Single word in quotes treated as regular term
    // "fox" should match doc1
    const results4 = try index.searchWithMode("\"fox\"", .@"and", 10);
    defer index.freeResults(results4);
    try std.testing.expectEqual(@as(usize, 1), results4.len);
    try std.testing.expectEqual(@as(DocId, 1), results4[0].doc_id);

    // Test 5: OR mode with quoted phrase
    // "quick brown" OR dog should match doc1, doc2, doc3, doc4
    const results5 = try index.searchWithMode("\"quick brown\" dog", .@"or", 10);
    defer index.freeResults(results5);
    try std.testing.expectEqual(@as(usize, 4), results5.len);
}

test "fts fuzzy search" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const page_manager = lattice.storage.page_manager;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_fts_fuzzy_test.db";
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

    var index = FtsIndex.init(allocator, &bp, &dict_tree, &lengths_tree, null, .{});

    // Index documents with specific terms
    _ = try index.indexDocument(1, "database systems and optimization");
    _ = try index.indexDocument(2, "datastore management");
    _ = try index.indexDocument(3, "the quick brown fox");
    _ = try index.indexDocument(4, "advanced algorithms");

    // Test 1: Exact match still works
    const results1 = try index.searchFuzzy("database", .{ .max_distance = 2, .min_term_length = 4 }, 10);
    defer index.freeResults(results1);
    try std.testing.expectEqual(@as(usize, 1), results1.len);
    try std.testing.expectEqual(@as(DocId, 1), results1[0].doc_id);

    // Test 2: Typo "databse" should match "database" (edit distance 1)
    const results2 = try index.searchFuzzy("databse", .{ .max_distance = 2, .min_term_length = 4 }, 10);
    defer index.freeResults(results2);
    try std.testing.expectEqual(@as(usize, 1), results2.len);
    try std.testing.expectEqual(@as(DocId, 1), results2[0].doc_id);

    // Test 3: Typo "quikc" should match "quick" (edit distance 2)
    const results3 = try index.searchFuzzy("quikc", .{ .max_distance = 2, .min_term_length = 4 }, 10);
    defer index.freeResults(results3);
    try std.testing.expectEqual(@as(usize, 1), results3.len);
    try std.testing.expectEqual(@as(DocId, 3), results3[0].doc_id);

    // Test 4: No match for too distant terms
    const results4 = try index.searchFuzzy("zebra", .{ .max_distance = 2, .min_term_length = 4 }, 10);
    defer index.freeResults(results4);
    try std.testing.expectEqual(@as(usize, 0), results4.len);

    // Test 5: Multiple fuzzy matches - "datastore" and "database" both within distance 2 of "databas"
    const results5 = try index.searchFuzzy("databas", .{ .max_distance = 2, .min_term_length = 4 }, 10);
    defer index.freeResults(results5);
    // Should match doc1 (database) - datastore is distance 3 from databas
    try std.testing.expect(results5.len >= 1);
}

test "fts prefix search" {
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const page_manager = lattice.storage.page_manager;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_fts_prefix_test.db";
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

    var index = FtsIndex.init(allocator, &bp, &dict_tree, &lengths_tree, null, .{});

    // Index documents with terms that share prefixes
    _ = try index.indexDocument(1, "database systems");
    _ = try index.indexDocument(2, "datastore management");
    _ = try index.indexDocument(3, "data analysis");
    _ = try index.indexDocument(4, "optimize performance");
    _ = try index.indexDocument(5, "optimization techniques");
    _ = try index.indexDocument(6, "optimizer settings");
    _ = try index.indexDocument(7, "unrelated content");

    // Test 1: Prefix "data*" should match docs 1, 2, 3
    const results1 = try index.searchWithPrefix("data*", .{ .min_prefix_length = 2, .max_expansions = 50 }, 10);
    defer index.freeResults(results1);
    try std.testing.expectEqual(@as(usize, 3), results1.len);

    // Test 2: Prefix "optim*" should match docs 4, 5, 6
    const results2 = try index.searchWithPrefix("optim*", .{ .min_prefix_length = 2, .max_expansions = 50 }, 10);
    defer index.freeResults(results2);
    try std.testing.expectEqual(@as(usize, 3), results2.len);

    // Test 3: Mixed query - regular term + prefix
    // "systems data*" should only match doc 1 (has both "systems" and "database")
    const results3 = try index.searchWithPrefix("systems data*", .{ .min_prefix_length = 2, .max_expansions = 50 }, 10);
    defer index.freeResults(results3);
    try std.testing.expectEqual(@as(usize, 1), results3.len);
    try std.testing.expectEqual(@as(DocId, 1), results3[0].doc_id);

    // Test 4: No match for non-existent prefix
    const results4 = try index.searchWithPrefix("xyz*", .{ .min_prefix_length = 2, .max_expansions = 50 }, 10);
    defer index.freeResults(results4);
    try std.testing.expectEqual(@as(usize, 0), results4.len);

    // Test 5: Prefix too short (below min_prefix_length) returns no results
    const results5 = try index.searchWithPrefix("d*", .{ .min_prefix_length = 2, .max_expansions = 50 }, 10);
    defer index.freeResults(results5);
    try std.testing.expectEqual(@as(usize, 0), results5.len);

    // Test 6: Exact term without wildcard still works
    const results6 = try index.searchWithPrefix("database", .{ .min_prefix_length = 2, .max_expansions = 50 }, 10);
    defer index.freeResults(results6);
    try std.testing.expectEqual(@as(usize, 1), results6.len);
    try std.testing.expectEqual(@as(DocId, 1), results6[0].doc_id);
}
