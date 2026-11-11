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
const scoped_tree = @import("scoped_tree.zig");
const ScopedTree = scoped_tree.ScopedTree;
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

/// The best `limit` scored documents, kept in a bounded min-heap.
///
/// Equal scores rank in the order they were added, which is what a stable
/// descending sort followed by truncation gives, so results match the full sort
/// this replaces exactly.
const TopK = struct {
    items: []Item,
    len: usize = 0,
    limit: usize,
    next_seq: u64 = 0,

    const Item = struct { doc: ScoredDoc, seq: u64 };

    /// `expected` bounds the heap's size when fewer items than `limit` will come.
    fn init(allocator: Allocator, limit: u32, expected: usize) FtsError!TopK {
        const cap = @min(@as(usize, limit), expected);
        return .{
            .items = allocator.alloc(Item, cap) catch return FtsError.OutOfMemory,
            .limit = cap,
        };
    }

    fn deinit(self: *TopK, allocator: Allocator) void {
        allocator.free(self.items);
    }

    /// Whether `a` ranks below `b`: lower score, or equal score added later.
    fn worse(a: Item, b: Item) bool {
        if (a.doc.score != b.doc.score) return a.doc.score < b.doc.score;
        return a.seq > b.seq;
    }

    fn add(self: *TopK, doc: ScoredDoc) void {
        const item = Item{ .doc = doc, .seq = self.next_seq };
        self.next_seq += 1;
        if (self.limit == 0) return;
        if (self.len < self.limit) {
            // Sift up: the root is the worst item kept.
            var i = self.len;
            self.len += 1;
            while (i > 0) {
                const parent = (i - 1) / 2;
                if (!worse(item, self.items[parent])) break;
                self.items[i] = self.items[parent];
                i = parent;
            }
            self.items[i] = item;
            return;
        }
        if (!worse(self.items[0], item)) return;
        self.siftDown(item);
    }

    /// Replace the root with `item` and restore the heap.
    fn siftDown(self: *TopK, item: Item) void {
        var i: usize = 0;
        while (true) {
            const left = 2 * i + 1;
            if (left >= self.len) break;
            var child = left;
            if (left + 1 < self.len and worse(self.items[left + 1], self.items[left])) child = left + 1;
            if (!worse(self.items[child], item)) break;
            self.items[i] = self.items[child];
            i = child;
        }
        self.items[i] = item;
    }

    /// The kept documents, best first. Owned by the caller.
    fn finish(self: *TopK, allocator: Allocator) FtsError![]ScoredDoc {
        if (self.len == 0) return &[_]ScoredDoc{};
        const out = allocator.alloc(ScoredDoc, self.len) catch return FtsError.OutOfMemory;
        // Repeatedly take the worst off the heap into the back of `out`.
        var n = self.len;
        while (n > 0) {
            n -= 1;
            out[n] = self.items[0].doc;
            const last = self.items[self.len - 1];
            self.len -= 1;
            if (self.len > 0) self.siftDown(last);
        }
        return out;
    }
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

    /// The tokenizer settings the stored terms were written with, once read;
    /// see `analysis`.
    analysis_cache: ?TokenizerConfig = null,

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
        return initScoped(allocator, bp, dict_tree, lengths_tree, reverse_tree, config, null);
    }

    /// Initialize an index confined to one declared scope.
    ///
    /// The trees are shared by every declared index, so a scope keeps each one's
    /// terms and documents to itself. Passing null gives the unscoped index,
    /// which is the one built before declarations existed and which still holds
    /// whatever was handed to `indexDocument` directly.
    pub fn initScoped(
        allocator: Allocator,
        bp: *BufferPool,
        dict_tree: *BTree,
        lengths_tree: *BTree,
        reverse_tree: ?*BTree,
        config: FtsConfig,
        scope: ?[scoped_tree.PREFIX_SIZE]u8,
    ) Self {
        const dict_view = if (scope) |p| ScopedTree.scoped(dict_tree, p) else ScopedTree.unscoped(dict_tree);
        const lengths_view = if (scope) |p| ScopedTree.scoped(lengths_tree, p) else ScopedTree.unscoped(lengths_tree);

        return Self{
            .allocator = allocator,
            .config = config,
            .dictionary = Dictionary.init(allocator, dict_view),
            .posting_store = PostingStore.init(allocator, bp),
            .doc_lengths = DocLengthStore.init(allocator, lengths_view),
            .scorer = Bm25Scorer.init(config.bm25),
            .reverse_index = if (reverse_tree) |rt| ReverseIndex.init(
                allocator,
                if (scope) |p| ScopedTree.scoped(rt, p) else ScopedTree.unscoped(rt),
            ) else null,
            .dict_tree = dict_tree,
            .lengths_tree = lengths_tree,
            .reverse_tree = reverse_tree,
        };
    }

    // ========================================================================
    // Analysis
    // ========================================================================

    /// The tokenizer settings this index's terms are written and read with.
    ///
    /// They are recorded in the index when its first document goes in, from
    /// `config.tokenizer`, and from then on win over the config, so a word is
    /// always looked up as the term it was stored as even if the config
    /// changes between opens. An index 0.16.0 built has no record; its terms
    /// came from the ASCII-only tokenizer, so it keeps that tokenizer
    /// (`ascii_only`) and its stored terms stay findable. An empty index has
    /// nothing stored and uses the config as it is.
    pub fn analysis(self: *Self) FtsError!TokenizerConfig {
        if (self.analysis_cache) |cached| return cached;
        const recorded = self.doc_lengths.getAnalysis() catch return FtsError.ScorerError;
        const result = if (recorded) |r|
            decodeAnalysis(self.config.tokenizer, r)
        else if (!(self.doc_lengths.hasDocuments() catch return FtsError.ScorerError))
            freshAnalysis(self.config.tokenizer)
        else
            legacyAnalysis(self.config.tokenizer);
        self.analysis_cache = result;
        return result;
    }

    /// The settings to index a document with. An empty index (a new one, or
    /// one every document has left) takes the config's settings and records
    /// them; otherwise the recorded or legacy settings stand.
    fn analysisForWrite(self: *Self) FtsError!TokenizerConfig {
        if (self.doc_lengths.hasDocuments() catch return FtsError.ScorerError) return self.analysis();
        const fresh = freshAnalysis(self.config.tokenizer);
        const encoded = encodeAnalysis(fresh);
        const recorded = self.doc_lengths.getAnalysis() catch return FtsError.ScorerError;
        if (recorded == null or !std.mem.eql(u8, &recorded.?, &encoded)) {
            self.doc_lengths.putAnalysis(encoded) catch return FtsError.ScorerError;
        }
        self.analysis_cache = fresh;
        return fresh;
    }

    const ANALYSIS_VERSION: u8 = 1;
    const ANALYSIS_ACCENTS: u8 = 1;
    const ANALYSIS_STEMMING: u8 = 2;

    fn freshAnalysis(config: TokenizerConfig) TokenizerConfig {
        var result = config;
        result.ascii_only = false;
        return result;
    }

    fn legacyAnalysis(config: TokenizerConfig) TokenizerConfig {
        var result = config;
        result.ascii_only = true;
        result.remove_accents = false;
        result.use_stemming = false;
        return result;
    }

    /// Version, flags, language, reserved.
    fn encodeAnalysis(config: TokenizerConfig) [scorer_mod.DocLengthStore.ANALYSIS_LEN]u8 {
        var flags: u8 = 0;
        if (config.remove_accents) flags |= ANALYSIS_ACCENTS;
        if (config.use_stemming) flags |= ANALYSIS_STEMMING;
        return .{ ANALYSIS_VERSION, flags, @intFromEnum(config.language), 0 };
    }

    fn decodeAnalysis(config: TokenizerConfig, recorded: [scorer_mod.DocLengthStore.ANALYSIS_LEN]u8) TokenizerConfig {
        var result = freshAnalysis(config);
        // A later version's record is read as far as this one understands it.
        result.remove_accents = recorded[1] & ANALYSIS_ACCENTS != 0;
        result.use_stemming = recorded[1] & ANALYSIS_STEMMING != 0;
        if (std.enums.fromInt(tokenizer_mod.Language, recorded[2])) |language| result.language = language;
        return result;
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
        return self.indexDocumentAnalyzed(doc_id, text, try self.analysisForWrite());
    }

    /// `indexDocument` with the tokenizer settings given rather than the
    /// index's own; tests use it to build an index the way 0.16.0 did.
    fn indexDocumentAnalyzed(
        self: *Self,
        doc_id: DocId,
        text: []const u8,
        tokenizer_config: TokenizerConfig,
    ) FtsError!u32 {
        // 1. Tokenize text
        var tok = Tokenizer.init(self.allocator, text, tokenizer_config);
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
            var lower_buf: [tokenizer_mod.MAX_TERM_LEN]u8 = undefined;
            const normalized = tokenizer_mod.normalizeTerm(tokenizer_config, token.text, &lower_buf) orelse continue;

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
                    .positions = .empty,
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

            // Skip populating the reverse index when the serialized term
            // list would exceed a single btree leaf page. The document
            // stays queryable via the forward posting lists; only future
            // `removeDocument(doc_id)` calls for this specific doc will
            // need to walk the dictionary as a fallback instead of using
            // the reverse-index shortcut. Treating this as fatal used to
            // propagate a bare IoError back to callers that index
            // multi-KiB markdown content, which is precisely the
            // nullclaw onboard-scaffold workload.
            ri.setDocTerms(doc_id, term_slice) catch |err| switch (err) {
                reverse_index_mod.ReverseIndexError.ValueTooLarge => {
                    std.log.warn(
                        "fts: reverse_index too large for doc={d}, terms={d}; " ++
                            "future removeDocument will fall back to dictionary scan",
                        .{ doc_id, num_terms },
                    );
                },
                reverse_index_mod.ReverseIndexError.OutOfMemory => return FtsError.OutOfMemory,
                else => return FtsError.IoError,
            };
        }

        return @intCast(tokens.len);
    }

    /// Empty this index completely.
    ///
    /// Only the entries under this index's own scope go, so the other declared
    /// indexes sharing the same trees are untouched. This is for redeclaring or
    /// rebuilding an index, where walking the documents one at a time would miss
    /// any whose entity no longer exists — exactly the state a crash in the middle
    /// of a write leaves behind.
    ///
    /// The posting pages themselves stay allocated. Nothing points at them once
    /// the dictionary entries naming them are gone, so they are wasted space
    /// rather than wrong data, and freeing them needs page reclamation the store
    /// does not have yet.
    pub fn clearScope(self: *Self) FtsError!void {
        self.dictionary.tree.clear(self.allocator) catch return FtsError.BTreeError;
        self.doc_lengths.tree.clear(self.allocator) catch return FtsError.BTreeError;
        self.doc_lengths.stats = .{ .total_docs = 0, .total_tokens = 0, .avg_doc_length = 0 };
        self.analysis_cache = null;
        if (self.reverse_index) |*ri| {
            ri.tree.clear(self.allocator) catch return FtsError.BTreeError;
        }
    }

    /// Remove a document from the index
    /// If reverse_index is available, properly cleans up posting lists and stats
    pub fn removeDocument(self: *Self, doc_id: DocId) FtsError!void {
        // Track if any dictionary stat updates failed (non-fatal but should be reported)
        var stats_update_failed = false;

        // If we have a reverse index, do proper cleanup
        if (self.reverse_index) |*ri| {
            // 1. Get terms from reverse index
            if (ri.getDocTerms(doc_id) catch {
                return FtsError.IoError;
            }) |terms| {
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
            } else {
                // Reverse index has no entry for this doc. This happens when
                // indexDocument skipped populating it because the serialized
                // term list exceeded a single btree leaf page. Fall back to a
                // full dictionary scan so stale posting entries for doc_id do
                // not survive removal (and resurrect on re-index under the
                // same id).
                self.removeDocumentByDictionaryScan(doc_id, &stats_update_failed) catch |err| {
                    return err;
                };
            }
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

    /// Fallback cleanup path used when the reverse index has no terms for
    /// `doc_id` (indexDocument skipped populating it because the term list
    /// was too large for a single btree leaf). Walks the entire dictionary
    /// and removes any posting entries referencing `doc_id`, keeping
    /// dictionary stats consistent.
    ///
    /// We cannot mutate the dictionary tree while iterating it, so this is
    /// done in two phases: collect a snapshot of (term, posting_page) pairs
    /// first, then apply removals afterwards. The work is O(|dictionary|),
    /// but this path only fires for oversized documents on removal.
    fn removeDocumentByDictionaryScan(
        self: *Self,
        doc_id: DocId,
        stats_update_failed: *bool,
    ) FtsError!void {
        const PostingRef = struct {
            term: []u8,
            posting_page: lattice.core.types.PageId,
        };

        var refs: ArrayListUnmanaged(PostingRef) = .empty;
        defer {
            for (refs.items) |ref| {
                self.allocator.free(ref.term);
            }
            refs.deinit(self.allocator);
        }

        {
            var iter: Dictionary.DictionaryIterator = undefined;
            self.dictionary.iterate(&iter) catch {
                return FtsError.DictionaryError;
            };
            defer iter.deinit();

            while (iter.next() catch {
                return FtsError.DictionaryError;
            }) |item| {
                if (item.entry.posting_page == 0) continue;
                const term_copy = self.allocator.dupe(u8, item.term) catch {
                    return FtsError.OutOfMemory;
                };
                refs.append(self.allocator, .{
                    .term = term_copy,
                    .posting_page = item.entry.posting_page,
                }) catch {
                    self.allocator.free(term_copy);
                    return FtsError.OutOfMemory;
                };
            }
        }

        for (refs.items) |ref| {
            const result = self.posting_store.removeEntry(ref.posting_page, doc_id) catch continue;
            if (!result.found) continue;

            self.dictionary.decrementDocFreq(ref.term) catch {
                stats_update_failed.* = true;
            };
            self.dictionary.subtractTotalFreq(ref.term, result.term_freq) catch {
                stats_update_failed.* = true;
            };
        }
    }

    // ========================================================================
    // Search Operations
    // ========================================================================

    /// Search with a text query
    /// Returns scored documents sorted by relevance (highest first)
    /// Roughly how many documents this query would match.
    ///
    /// The dictionary already records, for every term, how many documents contain
    /// it. Search is AND by default, so the rarest term bounds the result: a query
    /// for two words cannot match more documents than the scarcer of them appears
    /// in. That makes the minimum a real upper bound rather than a guess.
    ///
    /// A term absent from the dictionary gives zero, which is exact — an AND query
    /// naming a word nobody used matches nothing.
    ///
    /// Null means there is nothing to go on: no usable tokens after the analyzer
    /// has run. Callers should treat that as "no opinion" rather than as zero.
    ///
    /// This is a B-tree lookup per term against data the index already keeps, so
    /// it costs microseconds and needs no counting pass.
    pub fn estimateMatches(self: *Self, query_text: []const u8) FtsError!?u32 {
        const tokenizer_config = try self.analysis();
        var tok = Tokenizer.init(self.allocator, query_text, tokenizer_config);
        const tokens = tok.tokenizeAll() catch return FtsError.TokenizerError;
        defer self.allocator.free(tokens);

        var estimate: ?u32 = null;
        for (tokens) |token| {
            // The same normalisation search does, so the terms looked up here are
            // the terms that would actually be searched.
            var lower_buf: [tokenizer_mod.MAX_TERM_LEN]u8 = undefined;
            const normalized = tokenizer_mod.normalizeTerm(tokenizer_config, token.text, &lower_buf) orelse continue;

            const entry = self.dictionary.get(normalized) catch {
                return FtsError.DictionaryError;
            };
            const freq: u32 = if (entry) |e| e.doc_freq else 0;
            estimate = if (estimate) |current| @min(current, freq) else freq;
            if (estimate == 0) return 0;
        }
        return estimate;
    }

    pub fn search(
        self: *Self,
        query_text: []const u8,
        limit: u32,
    ) FtsError![]ScoredDoc {
        // Tokenize query
        const tokenizer_config = try self.analysis();
        var tok = Tokenizer.init(self.allocator, query_text, tokenizer_config);
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
            var lower_buf: [tokenizer_mod.MAX_TERM_LEN]u8 = undefined;
            if (tokenizer_mod.normalizeTerm(tokenizer_config, token.text, &lower_buf)) |normalized| {
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
    ///
    /// Reads the posting list, then looks up every document's length in one
    /// sorted pass over the lengths tree, and keeps the best `limit` in a
    /// bounded heap. Ties keep posting order, as the stable sort this replaces
    /// did.
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

        var list = try self.readPostingsWithLengths(dict_entry.posting_page);
        defer list.deinit(self.allocator);

        var top = try TopK.init(self.allocator, limit, list.postings.len);
        defer top.deinit(self.allocator);

        for (list.postings.items(.doc_id), list.postings.items(.term_freq), list.lengths) |doc_id, term_freq, length| {
            // No length means the document was removed; it cannot be scored.
            const doc_length = length orelse continue;
            top.add(.{
                .doc_id = doc_id,
                .score = self.scorer.scoreTerm(
                    self.doc_lengths.stats,
                    term_freq,
                    dict_entry.doc_freq,
                    doc_length,
                ),
            });
        }
        return top.finish(self.allocator);
    }

    const Posting = struct { doc_id: DocId, term_freq: u32 };

    /// Every (doc_id, term_freq) in a posting list, in list order.
    fn readPostings(self: *Self, posting_page: lattice.core.types.PageId) FtsError!std.MultiArrayList(Posting) {
        var postings: std.MultiArrayList(Posting) = .empty;
        errdefer postings.deinit(self.allocator);
        var iter = self.posting_store.iterate(posting_page) catch return FtsError.PostingError;
        defer iter.deinit();
        while (iter.next() catch return FtsError.PostingError) |entry| {
            postings.append(self.allocator, .{ .doc_id = entry.doc_id, .term_freq = entry.term_freq }) catch {
                return FtsError.OutOfMemory;
            };
        }
        return postings;
    }

    /// Lengths of the given documents, null where a document has none.
    /// Caller frees the result.
    fn lookupLengths(self: *Self, doc_ids: []const DocId) FtsError![]?u32 {
        const lengths = self.allocator.alloc(?u32, doc_ids.len) catch return FtsError.OutOfMemory;
        errdefer self.allocator.free(lengths);
        const order = self.allocator.alloc(u32, doc_ids.len) catch return FtsError.OutOfMemory;
        defer self.allocator.free(order);
        self.doc_lengths.getLengths(doc_ids, lengths, order) catch |err| return switch (err) {
            error.OutOfMemory => FtsError.OutOfMemory,
            else => FtsError.ScorerError,
        };
        return lengths;
    }

    /// A posting list read into memory with each document's length beside
    /// it, looked up in one sorted pass rather than one tree descent per
    /// posting. A null length means the document was removed.
    const PostingsWithLengths = struct {
        postings: std.MultiArrayList(Posting),
        lengths: []?u32,

        fn deinit(self: *PostingsWithLengths, allocator: Allocator) void {
            self.postings.deinit(allocator);
            allocator.free(self.lengths);
        }
    };

    fn readPostingsWithLengths(self: *Self, posting_page: lattice.core.types.PageId) FtsError!PostingsWithLengths {
        var postings = try self.readPostings(posting_page);
        errdefer postings.deinit(self.allocator);
        return .{ .postings = postings, .lengths = try self.lookupLengths(postings.items(.doc_id)) };
    }

    /// Search for multiple terms (AND semantics - documents must contain all terms)
    fn searchMultiTerm(self: *Self, terms: []const []const u8, limit: u32) FtsError![]ScoredDoc {
        return self.searchMultiTermAnd(terms, limit);
    }

    /// Multi-term AND search.
    ///
    /// The rarest term's list supplies the candidates, and every other list is
    /// read through once and matched against them by doc_id. Nothing here relies
    /// on a posting list being in doc_id order, because they are not: an update
    /// removes the document's postings and appends the new ones at the tail, and
    /// a transaction commits its documents in hash order. Lists written that way
    /// by earlier releases are already on disk, so search has to be correct over
    /// them rather than only over lists written from now on. A merge that walks
    /// the lists in step with `skipTo` silently drops any document stored behind
    /// its cursor.
    fn searchMultiTermAnd(self: *Self, terms: []const []const u8, limit: u32) FtsError![]ScoredDoc {
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

            // Every term is required, so one that no document has rules out
            // every document. Skipping it would answer a broader query than the
            // one asked, and disagree with `estimateMatches`, which says zero.
            const dict_entry = (self.dictionary.get(term) catch {
                return FtsError.DictionaryError;
            }) orelse return &[_]ScoredDoc{};
            if (dict_entry.posting_page == 0) return &[_]ScoredDoc{};

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

        // A document the rarest term's list names. `matched` counts the terms
        // found for it, in term order, so a document missing one term can never
        // be counted for a later one. Its term frequencies are kept, one row of
        // `term_freqs` per candidate, and only documents that match every term
        // have their lengths looked up and are scored.
        const Candidate = struct {
            doc_id: DocId,
            matched: u32,
        };

        const num_terms = term_infos.len;
        var candidates: ArrayListUnmanaged(Candidate) = .empty;
        defer candidates.deinit(self.allocator);
        var term_freqs: ArrayListUnmanaged(u32) = .empty;
        defer term_freqs.deinit(self.allocator);
        var slot_by_doc: std.AutoHashMapUnmanaged(DocId, u32) = .empty;
        defer slot_by_doc.deinit(self.allocator);

        const driver = term_infos[0];
        {
            var driver_iter = self.posting_store.iterate(driver.dict_entry.posting_page) catch {
                return FtsError.PostingError;
            };
            defer driver_iter.deinit();

            while (driver_iter.next() catch return FtsError.PostingError) |entry| {
                const slot = slot_by_doc.getOrPut(self.allocator, entry.doc_id) catch {
                    return FtsError.OutOfMemory;
                };
                if (slot.found_existing) continue;
                slot.value_ptr.* = @intCast(candidates.items.len);
                candidates.append(self.allocator, .{
                    .doc_id = entry.doc_id,
                    .matched = 1,
                }) catch return FtsError.OutOfMemory;
                const row = term_freqs.addManyAsSlice(self.allocator, num_terms) catch return FtsError.OutOfMemory;
                row[0] = entry.term_freq;
            }
        }

        var terms_matched: u32 = 1;
        for (term_infos[1..], 1..) |info, term_index| {
            var iter = self.posting_store.iterate(info.dict_entry.posting_page) catch {
                return FtsError.PostingError;
            };
            defer iter.deinit();

            var any_matched = false;
            while (iter.next() catch return FtsError.PostingError) |entry| {
                const slot = slot_by_doc.get(entry.doc_id) orelse continue;
                const candidate = &candidates.items[slot];
                // Missed an earlier term, or already counted for this one.
                if (candidate.matched != terms_matched) continue;
                candidate.matched += 1;
                term_freqs.items[slot * num_terms + term_index] = entry.term_freq;
                any_matched = true;
            }
            terms_matched += 1;
            // Nothing has every term so far, so nothing can have them all.
            if (!any_matched) break;
        }

        // Every match is ranked before the limit applies. Stopping once there
        // were "enough" matches in posting order would keep the first ones
        // stored rather than the best scoring ones.
        var matches: ArrayListUnmanaged(DocId) = .empty;
        defer matches.deinit(self.allocator);
        var match_slots: ArrayListUnmanaged(u32) = .empty;
        defer match_slots.deinit(self.allocator);
        for (candidates.items, 0..) |candidate, slot| {
            if (candidate.matched != terms_matched) continue;
            matches.append(self.allocator, candidate.doc_id) catch return FtsError.OutOfMemory;
            match_slots.append(self.allocator, @intCast(slot)) catch return FtsError.OutOfMemory;
        }

        const lengths = try self.lookupLengths(matches.items);
        defer self.allocator.free(lengths);

        var top = try TopK.init(self.allocator, limit, matches.items.len);
        defer top.deinit(self.allocator);
        for (matches.items, match_slots.items, lengths) |doc_id, slot, length| {
            // No length means the document is gone; it cannot be scored.
            const doc_length = length orelse continue;
            var score: f32 = 0;
            for (term_infos, term_freqs.items[slot * num_terms ..][0..num_terms]) |info, tf| {
                score += self.scorer.scoreTerm(
                    self.doc_lengths.stats,
                    tf,
                    info.dict_entry.doc_freq,
                    doc_length,
                );
            }
            top.add(.{ .doc_id = doc_id, .score = score });
        }
        return top.finish(self.allocator);
    }

    /// Matches text that is not in the index yet against one query, by term
    /// presence.
    ///
    /// The query and the text go through the tokenizer and normalisation that
    /// indexing and search use, so a pending document matches a query exactly
    /// when the indexed one would: every query term must be one of its terms.
    /// The score is the number of distinct query terms, or 0 when any is
    /// missing or the query has no searchable terms. It is not BM25, so it only
    /// decides which pending documents match, not how they rank against
    /// indexed ones. The query is analysed once, so scoring many pending
    /// documents costs only their own tokenizing.
    pub const PresenceMatcher = struct {
        query_arena: std.heap.ArenaAllocator,
        scratch: std.heap.ArenaAllocator,
        config: TokenizerConfig,
        /// Distinct query terms.
        terms: []const []const u8,

        pub fn deinit(self: *PresenceMatcher) void {
            self.scratch.deinit();
            self.query_arena.deinit();
        }

        pub fn score(self: *PresenceMatcher, text: []const u8) FtsError!f32 {
            if (self.terms.len == 0) return 0;
            _ = self.scratch.reset(.retain_capacity);
            const arena = self.scratch.allocator();

            var text_terms = StringHashMap(void).init(arena);
            var tok = Tokenizer.init(arena, text, self.config);
            while (tok.next()) |token| {
                var term_buf: [tokenizer_mod.MAX_TERM_LEN]u8 = undefined;
                const term = tokenizer_mod.normalizeTerm(self.config, token.text, &term_buf) orelse continue;
                if (text_terms.contains(term)) continue;
                const owned = arena.dupe(u8, term) catch return FtsError.OutOfMemory;
                text_terms.put(owned, {}) catch return FtsError.OutOfMemory;
            }
            for (self.terms) |term| {
                if (!text_terms.contains(term)) return 0;
            }
            return @floatFromInt(self.terms.len);
        }
    };

    /// A matcher for `query_text` against text this index would take in now.
    pub fn presenceMatcher(self: *Self, query_text: []const u8) FtsError!PresenceMatcher {
        // Text that goes in now is analysed as indexing would analyse it.
        const tokenizer_config = if (!(self.doc_lengths.hasDocuments() catch return FtsError.ScorerError))
            freshAnalysis(self.config.tokenizer)
        else
            try self.analysis();

        var query_arena = std.heap.ArenaAllocator.init(self.allocator);
        errdefer query_arena.deinit();
        const arena = query_arena.allocator();

        var terms: ArrayListUnmanaged([]const u8) = .empty;
        var tok = Tokenizer.init(arena, query_text, tokenizer_config);
        next_token: while (tok.next()) |token| {
            var term_buf: [tokenizer_mod.MAX_TERM_LEN]u8 = undefined;
            const term = tokenizer_mod.normalizeTerm(tokenizer_config, token.text, &term_buf) orelse continue;
            for (terms.items) |seen| {
                if (std.mem.eql(u8, seen, term)) continue :next_token;
            }
            const owned = arena.dupe(u8, term) catch return FtsError.OutOfMemory;
            terms.append(arena, owned) catch return FtsError.OutOfMemory;
        }
        return .{
            .query_arena = query_arena,
            .scratch = std.heap.ArenaAllocator.init(self.allocator),
            .config = tokenizer_config,
            .terms = terms.items,
        };
    }

    /// `presenceMatcher(query_text).score(text)`, for a single document.
    pub fn termPresenceScore(
        self: *Self,
        query_text: []const u8,
        text: []const u8,
    ) FtsError!f32 {
        var matcher = try self.presenceMatcher(query_text);
        defer matcher.deinit();
        return matcher.score(text);
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
        const tokenizer_config = try self.analysis();
        var tok = Tokenizer.init(self.allocator, query_text, tokenizer_config);
        const tokens = tok.tokenizeAll() catch {
            return FtsError.TokenizerError;
        };
        defer self.allocator.free(tokens);

        if (tokens.len == 0) {
            return &[_]ScoredDoc{};
        }

        // A document's score is the sum over query words of its best match for
        // each word. Taking the best rather than the sum over a word's
        // expansions keeps a document that happens to contain several spellings
        // near one query word ("databse" -> "database", "databases") from
        // outranking one that matches more of the query.
        var doc_scores = std.AutoHashMap(DocId, f32).init(self.allocator);
        defer doc_scores.deinit();
        var word_best = std.AutoHashMap(DocId, f32).init(self.allocator);
        defer word_best.deinit();

        // Process each query term
        for (tokens) |token| {
            // Normalize token
            var lower_buf: [tokenizer_mod.MAX_TERM_LEN]u8 = undefined;
            const normalized = tokenizer_mod.normalizeTerm(tokenizer_config, token.text, &lower_buf) orelse continue;

            word_best.clearRetainingCapacity();

            // Skip short terms for fuzzy matching
            if (normalized.len < fuzzy_config.min_term_length) {
                // Try exact match for short terms
                const results = try self.searchSingleTerm(normalized, std.math.maxInt(u32));
                defer self.freeResults(results);
                for (results) |doc| {
                    word_best.put(doc.doc_id, doc.score) catch return FtsError.OutOfMemory;
                }
            } else {
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

                    var list = try self.readPostingsWithLengths(match.entry.posting_page);
                    defer list.deinit(self.allocator);

                    for (list.postings.items(.doc_id), list.postings.items(.term_freq), list.lengths) |doc_id, term_freq, length| {
                        const doc_length = length orelse continue;

                        // Score with fuzzy penalty
                        const score = self.scorer.scoreTermFuzzy(
                            self.doc_lengths.stats,
                            term_freq,
                            match.entry.doc_freq,
                            doc_length,
                            match.distance,
                            fuzzy_config.max_distance,
                        );

                        const best = word_best.getOrPut(doc_id) catch return FtsError.OutOfMemory;
                        best.value_ptr.* = if (best.found_existing) @max(best.value_ptr.*, score) else score;
                    }
                }
            }

            var best_iter = word_best.iterator();
            while (best_iter.next()) |hit| {
                const total = doc_scores.getOrPut(hit.key_ptr.*) catch return FtsError.OutOfMemory;
                total.value_ptr.* = if (total.found_existing) total.value_ptr.* + hit.value_ptr.* else hit.value_ptr.*;
            }
        }

        // Convert to result array
        var results: ArrayListUnmanaged(ScoredDoc) = .empty;
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
    ///
    /// Every query word is required. Words are split the way the tokenizer
    /// splits them, so "bar," asks for "bar"; words the tokenizer would drop
    /// (too short, stop words) are dropped here too. A prefix shorter than
    /// `min_prefix_length` is a requirement nothing can meet. A document counts
    /// once per query word however many of a prefix's expansions it contains.
    /// Words starting with '-' are ignored.
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

        // Documents matched by the query word being processed, with its score.
        var word_docs = std.AutoHashMap(DocId, f32).init(self.allocator);
        defer word_docs.deinit();

        var total_terms: u32 = 0;

        const tokenizer_config = try self.analysis();
        // Every word, so the checks below can tell which is the prefix.
        var split_config = tokenizer_config;
        split_config.min_token_length = 1;
        split_config.max_token_length = std.math.maxInt(u8);
        split_config.remove_stop_words = false;

        var chunks = std.mem.tokenizeAny(u8, query_text, " \t\r\n");
        while (chunks.next()) |chunk| {
            // Exclusions are not supported by this search.
            if (chunk[0] == '-') continue;

            const wildcard = prefix_mod.hasWildcard(chunk);
            const body = prefix_mod.extractPrefix(chunk);

            var words = Tokenizer.init(self.allocator, body, split_config);
            while (words.next()) |token| {
                const word = token.text;
                // Only the word the '*' is attached to is a prefix.
                const is_prefix = wildcard and token.byte_offset + word.len == body.len;

                // A prefix is folded but not stemmed: a stem is not a prefix
                // of the stems of longer words.
                var lower_buf: [tokenizer_mod.MAX_TERM_LEN]u8 = undefined;
                var fold_config = tokenizer_config;
                fold_config.use_stemming = false;
                const folded = tokenizer_mod.normalizeTerm(fold_config, word, &lower_buf) orelse {
                    if (is_prefix) return &[_]ScoredDoc{};
                    continue;
                };

                word_docs.clearRetainingCapacity();
                if (is_prefix) {
                    const normalized = folded;
                    total_terms += 1;
                    if (normalized.len < prefix_config.min_prefix_length) return &[_]ScoredDoc{};

                    const prefix_matches = prefix_mod.expandPrefixTerms(
                        self.allocator,
                        normalized,
                        &self.dictionary,
                        prefix_config,
                    ) catch {
                        return FtsError.DictionaryError;
                    };
                    defer prefix_mod.freeMatches(self.allocator, prefix_matches);

                    for (prefix_matches) |match| {
                        try self.addTermScores(match.entry, &word_docs);
                    }
                } else {
                    if (word.len < tokenizer_config.min_token_length or
                        word.len > tokenizer_config.max_token_length) continue;
                    if (tokenizer_config.remove_stop_words and
                        stopwords_mod.isStopWord(folded, tokenizer_config.language))
                    {
                        continue;
                    }
                    total_terms += 1;

                    var term_buf: [tokenizer_mod.MAX_TERM_LEN]u8 = undefined;
                    const normalized = tokenizer_mod.normalizeTerm(tokenizer_config, word, &term_buf) orelse continue;

                    const dict_entry = (self.dictionary.get(normalized) catch {
                        return FtsError.DictionaryError;
                    }) orelse return &[_]ScoredDoc{};
                    try self.addTermScores(dict_entry, &word_docs);
                }

                // A required word nothing matched rules out every document.
                if (word_docs.count() == 0) return &[_]ScoredDoc{};

                var it = word_docs.iterator();
                while (it.next()) |hit| {
                    const score = doc_scores.getOrPut(hit.key_ptr.*) catch return FtsError.OutOfMemory;
                    score.value_ptr.* = if (score.found_existing) score.value_ptr.* + hit.value_ptr.* else hit.value_ptr.*;
                    const count = doc_term_counts.getOrPut(hit.key_ptr.*) catch return FtsError.OutOfMemory;
                    count.value_ptr.* = if (count.found_existing) count.value_ptr.* + 1 else 1;
                }
            }
        }

        // Convert to result array (AND semantics: require all terms)
        var results: ArrayListUnmanaged(ScoredDoc) = .empty;
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

    /// Add one dictionary term's BM25 score to every document in its postings.
    fn addTermScores(self: *Self, entry: DictionaryEntry, scores: *std.AutoHashMap(DocId, f32)) FtsError!void {
        if (entry.posting_page == 0) return;
        var list = try self.readPostingsWithLengths(entry.posting_page);
        defer list.deinit(self.allocator);
        for (list.postings.items(.doc_id), list.postings.items(.term_freq), list.lengths) |doc_id, term_freq, length| {
            const doc_length = length orelse continue;
            const score = self.scorer.scoreTerm(
                self.doc_lengths.stats,
                term_freq,
                entry.doc_freq,
                doc_length,
            );
            const slot = scores.getOrPut(doc_id) catch return FtsError.OutOfMemory;
            slot.value_ptr.* = if (slot.found_existing) slot.value_ptr.* + score else score;
        }
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
        var num_const_terms: usize = num_terms;
        // Without stored positions a phrase cannot be checked for adjacency,
        // so in AND mode it asks for its words, as `searchPhrase` does. Dropping
        // it instead would answer a phrase-only query with nothing at all.
        if (num_phrases > 0 and !self.config.store_positions and mode == .@"and") {
            for (0..num_phrases) |p| {
                for (phrases_buf[p][0..phrase_lens[p]]) |word| {
                    if (num_const_terms >= const_terms.len) break;
                    const duplicate = for (const_terms[0..num_const_terms]) |t| {
                        if (std.mem.eql(u8, t, word)) break true;
                    } else false;
                    if (duplicate) continue;
                    const_terms[num_const_terms] = word;
                    num_const_terms += 1;
                }
            }
        }
        var const_excluded: [32][]const u8 = undefined;
        for (excluded_buf[0..num_excluded], 0..) |t, i| {
            const_excluded[i] = t;
        }

        // Each sub-search below is narrowed afterwards: by another phrase, by
        // the plain terms, or by the exclusions. Cutting one short beforehand
        // ranks and truncates a set that is not yet the answer, so a document
        // that would have survived the narrowing can be dropped for scoring
        // below others that do not. Only a lone term search with nothing to
        // filter it may stop at `limit`.
        const unbounded = std.math.maxInt(u32);
        const narrowed = num_excluded > 0 or (num_phrases > 0 and self.config.store_positions);
        const term_limit: u32 = if (narrowed) unbounded else limit;

        // Get initial candidates based on terms and phrases
        var candidates: []ScoredDoc = undefined;
        var candidates_owned = false;

        if (num_phrases > 0 and self.config.store_positions) {
            // Search phrases first (most restrictive)
            var const_phrase: [MAX_PHRASE_TERMS][]const u8 = undefined;
            for (phrases_buf[0][0..phrase_lens[0]], 0..) |t, i| {
                const_phrase[i] = t;
            }
            candidates = try self.searchPhraseTerms(const_phrase[0..phrase_lens[0]], unbounded);
            candidates_owned = true;

            // Intersect with additional phrases
            for (1..num_phrases) |p| {
                var next_phrase: [MAX_PHRASE_TERMS][]const u8 = undefined;
                for (phrases_buf[p][0..phrase_lens[p]], 0..) |t, i| {
                    next_phrase[i] = t;
                }
                const phrase_results = try self.searchPhraseTerms(next_phrase[0..phrase_lens[p]], unbounded);
                defer self.allocator.free(phrase_results);

                // Build set of phrase matches
                var phrase_docs = std.AutoHashMap(DocId, f32).init(self.allocator);
                defer phrase_docs.deinit();
                for (phrase_results) |doc| {
                    phrase_docs.put(doc.doc_id, doc.score) catch continue;
                }

                // Filter candidates to those matching this phrase
                var filtered: ArrayListUnmanaged(ScoredDoc) = .empty;
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
                        try self.searchSingleTerm(const_terms[0], unbounded)
                    else
                        try self.searchMultiTerm(const_terms[0..num_terms], unbounded),
                    .@"or" => if (num_terms == 1)
                        try self.searchSingleTerm(const_terms[0], unbounded)
                    else
                        try self.searchMultiTermOr(const_terms[0..num_terms], unbounded),
                };
                defer self.allocator.free(term_results);

                // Build set of term matches
                var term_docs = std.AutoHashMap(DocId, f32).init(self.allocator);
                defer term_docs.deinit();
                for (term_results) |doc| {
                    term_docs.put(doc.doc_id, doc.score) catch continue;
                }

                // Filter phrase candidates by term matches (AND mode) or merge (OR mode)
                var filtered: ArrayListUnmanaged(ScoredDoc) = .empty;
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
        } else if (num_const_terms > 0) {
            // No phrases (or positions not stored), just use term search
            candidates = switch (mode) {
                .@"and" => if (num_const_terms == 1)
                    try self.searchSingleTerm(const_terms[0], term_limit)
                else
                    try self.searchMultiTerm(const_terms[0..num_const_terms], term_limit),
                .@"or" => if (num_const_terms == 1)
                    try self.searchSingleTerm(const_terms[0], term_limit)
                else
                    try self.searchMultiTermOr(const_terms[0..num_const_terms], term_limit),
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
        var results: ArrayListUnmanaged(ScoredDoc) = .empty;
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
        const tokenizer_config = try self.analysis();

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

                    // The words between the quotes go through the tokenizer the
                    // index was built with, so a phrase loses the stop words and
                    // short words indexing dropped. Those never took a position,
                    // so keeping them would ask for a gap the index never has
                    // and "Bank of America" would never match.
                    const body_start = i;
                    while (i < query_text.len and query_text[i] != '"') i += 1;
                    var tok = Tokenizer.init(self.allocator, query_text[body_start..i], tokenizer_config);
                    while (tok.next()) |token| {
                        if (phrase_lens[phrase_idx] >= MAX_PHRASE_TERMS) break;
                        var term_buf: [tokenizer_mod.MAX_TERM_LEN]u8 = undefined;
                        const term = tokenizer_mod.normalizeTerm(tokenizer_config, token.text, &term_buf) orelse continue;
                        const normalized = self.allocator.dupe(u8, term) catch {
                            return FtsError.OutOfMemory;
                        };
                        phrases_buf[phrase_idx][phrase_lens[phrase_idx]] = normalized;
                        phrase_lens[phrase_idx] += 1;
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

            // A run up to whitespace or a quote is split into words by the
            // tokenizer, as indexing splits text, so "bread," asks for
            // "bread" and "e-mail" for "e" and "mail" (both required: '-'
            // excludes only at the start of the run). A leading '-' applies
            // to every word of the run.
            const start = i;
            while (i < query_text.len and !std.ascii.isWhitespace(query_text[i]) and query_text[i] != '"') {
                i += 1;
            }

            var words = Tokenizer.init(self.allocator, query_text[start..i], tokenizer_config);
            while (words.next()) |token| {
                // Operators are not search terms.
                if (std.ascii.eqlIgnoreCase(token.text, "OR") or
                    std.ascii.eqlIgnoreCase(token.text, "AND") or
                    std.ascii.eqlIgnoreCase(token.text, "NOT"))
                {
                    continue;
                }

                var term_buf: [tokenizer_mod.MAX_TERM_LEN]u8 = undefined;
                const term = tokenizer_mod.normalizeTerm(tokenizer_config, token.text, &term_buf) orelse continue;
                const normalized = self.allocator.dupe(u8, term) catch {
                    return FtsError.OutOfMemory;
                };

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
            // OR = sum of matching term scores
            try self.addTermScores(dict_entry, &doc_scores);
        }

        // Collect all results (no term count filter for OR)
        var results: ArrayListUnmanaged(ScoredDoc) = .empty;
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
        const tokenizer_config = try self.analysis();
        var tok = Tokenizer.init(self.allocator, phrase_text, tokenizer_config);
        const tokens = tok.tokenizeAll() catch {
            return FtsError.TokenizerError;
        };
        defer self.allocator.free(tokens);

        if (tokens.len == 0) {
            return &[_]ScoredDoc{};
        }

        // Single word "phrase" is just a regular search
        if (tokens.len == 1) {
            var lower_buf: [tokenizer_mod.MAX_TERM_LEN]u8 = undefined;
            const term = tokenizer_mod.normalizeTerm(tokenizer_config, tokens[0].text, &lower_buf) orelse return &[_]ScoredDoc{};
            return self.searchSingleTerm(term, limit);
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
            var term_buf: [tokenizer_mod.MAX_TERM_LEN]u8 = undefined;
            const term = tokenizer_mod.normalizeTerm(tokenizer_config, token.text, &term_buf) orelse continue;

            const normalized = self.allocator.dupe(u8, term) catch {
                return FtsError.OutOfMemory;
            };
            phrase_terms[num_terms] = normalized;
            num_terms += 1;
        }

        if (num_terms < 2) {
            return &[_]ScoredDoc{};
        }

        return self.searchPhraseTerms(phrase_terms[0..num_terms], limit);
    }

    /// Search for phrase with pre-tokenized terms
    ///
    /// A document matches when some occurrence of the first term is followed by
    /// each later term at the next positions in turn. The candidate start
    /// positions are narrowed term by term, so every term has to line up behind
    /// the same start; checking each term against any start independently would
    /// let "a b c" match "a b x a x c".
    fn searchPhraseTerms(self: *Self, terms: []const []const u8, limit: u32) FtsError![]ScoredDoc {
        if (terms.len < 2) {
            return &[_]ScoredDoc{};
        }

        // Per document: the first term's positions that can still start the
        // phrase (the first `live` of `starts`), and the score so far.
        const Candidate = struct {
            starts: []u32,
            live: usize,
            score: f32,
        };
        var candidates = std.AutoHashMap(DocId, Candidate).init(self.allocator);
        defer {
            var it = candidates.valueIterator();
            while (it.next()) |c| self.allocator.free(c.starts);
            candidates.deinit();
        }

        const first_entry = (self.dictionary.get(terms[0]) catch {
            return FtsError.DictionaryError;
        }) orelse return &[_]ScoredDoc{};
        if (first_entry.posting_page == 0) return &[_]ScoredDoc{};

        {
            var iter = self.posting_store.iterate(first_entry.posting_page) catch {
                return FtsError.PostingError;
            };
            defer iter.deinit();

            while (iter.nextWithPositions(self.allocator) catch return FtsError.PostingError) |entry| {
                const positions = @constCast(entry.positions orelse continue);
                const doc_length = self.doc_lengths.getLength(entry.doc_id) catch {
                    self.allocator.free(positions);
                    continue;
                };
                const slot = candidates.getOrPut(entry.doc_id) catch {
                    self.allocator.free(positions);
                    return FtsError.OutOfMemory;
                };
                if (slot.found_existing) {
                    self.allocator.free(positions);
                    continue;
                }
                slot.value_ptr.* = .{
                    .starts = positions,
                    .live = positions.len,
                    .score = self.scorer.scoreTerm(
                        self.doc_lengths.stats,
                        entry.term_freq,
                        first_entry.doc_freq,
                        doc_length,
                    ),
                };
            }
        }

        // Narrow the starts by each later term, which must sit `offset` places
        // after a start. A candidate this term's list does not name, or whose
        // starts all fail, is out.
        for (terms[1..], 1..) |term, offset| {
            if (candidates.count() == 0) break;

            const dict_entry = (self.dictionary.get(term) catch {
                return FtsError.DictionaryError;
            }) orelse return &[_]ScoredDoc{};
            if (dict_entry.posting_page == 0) return &[_]ScoredDoc{};

            var seen = std.AutoHashMap(DocId, void).init(self.allocator);
            defer seen.deinit();

            var term_iter = self.posting_store.iterate(dict_entry.posting_page) catch {
                return FtsError.PostingError;
            };
            defer term_iter.deinit();

            while (term_iter.nextWithPositions(self.allocator) catch return FtsError.PostingError) |entry| {
                defer if (entry.positions) |p| self.allocator.free(p);
                const candidate = candidates.getPtr(entry.doc_id) orelse continue;
                const positions = entry.positions orelse continue;
                if (seen.contains(entry.doc_id)) continue;

                var kept: usize = 0;
                for (candidate.starts[0..candidate.live]) |start| {
                    const wanted = start + @as(u32, @intCast(offset));
                    if (std.sort.binarySearch(u32, positions, wanted, orderU32) != null) {
                        candidate.starts[kept] = start;
                        kept += 1;
                    }
                }
                candidate.live = kept;
                if (kept == 0) continue;

                seen.put(entry.doc_id, {}) catch return FtsError.OutOfMemory;
                const doc_length = self.doc_lengths.getLength(entry.doc_id) catch 1;
                candidate.score += self.scorer.scoreTerm(
                    self.doc_lengths.stats,
                    entry.term_freq,
                    dict_entry.doc_freq,
                    doc_length,
                );
            }

            // Drop every candidate this term did not keep.
            var drop: ArrayListUnmanaged(DocId) = .empty;
            defer drop.deinit(self.allocator);
            var it = candidates.iterator();
            while (it.next()) |c| {
                if (!seen.contains(c.key_ptr.*)) {
                    drop.append(self.allocator, c.key_ptr.*) catch return FtsError.OutOfMemory;
                }
            }
            for (drop.items) |doc_id| {
                const removed = candidates.fetchRemove(doc_id).?;
                self.allocator.free(removed.value.starts);
            }
        }

        // Collect results
        var results: ArrayListUnmanaged(ScoredDoc) = .empty;
        errdefer results.deinit(self.allocator);

        var result_iter = candidates.iterator();
        while (result_iter.next()) |entry| {
            results.append(self.allocator, .{
                .doc_id = entry.key_ptr.*,
                .score = entry.value_ptr.score,
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

    fn orderU32(a: u32, b: u32) std.math.Order {
        return std.math.order(a, b);
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

test "fts indexDocument survives large (>=9 KiB) documents" {
    // Regression: a ~9 KiB document tripped an integer-overflow panic
    // somewhere in the tokenize→posting→dictionary pipeline when called
    // from nullclaw's memory engine (SOUL.md → AGENTS.md scaffold).
    // This test indexes progressively larger synthetic documents and
    // asserts that each completes cleanly so the panic surfaces in CI.
    const allocator = std.testing.allocator;

    const vfs = lattice.storage.vfs;
    const page_manager = lattice.storage.page_manager;

    var posix_vfs = vfs.PosixVfs.init(allocator);
    const vfs_impl = posix_vfs.vfs();

    const db_path = "/tmp/lattice_fts_large_doc_test.db";
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

    // Build a synthetic document whose token vocabulary and total token
    // count both scale with `word_count`, so any cumulative-state bug in
    // posting lists, dictionary BTree inserts, or skip-pointer rollover
    // becomes visible.
    var doc: std.ArrayListUnmanaged(u8) = .empty;
    defer doc.deinit(allocator);

    const sizes = [_]usize{ 256, 1024, 4096, 9242, 16384 };
    var doc_id: u64 = 1;
    for (sizes) |target_bytes| {
        doc.clearRetainingCapacity();
        var seed: usize = 0;
        while (doc.items.len < target_bytes) : (seed += 1) {
            // Vocabulary grows with seed (`wordXXXX`), so each run adds
            // fresh dictionary entries plus repeats of earlier words.
            var word_buf: [32]u8 = undefined;
            const word = std.fmt.bufPrint(&word_buf, "word{d} ", .{seed}) catch unreachable;
            try doc.appendSlice(allocator, word);
        }
        const n = try index.indexDocument(doc_id, doc.items[0..@min(doc.items.len, target_bytes)]);
        try std.testing.expect(n > 0);
        doc_id += 1;
    }

    const stats = index.getStats();
    try std.testing.expect(stats.total_docs == sizes.len);
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

/// An index over a scratch file, with a reverse index so documents can be
/// removed and re-indexed the way the database does on update.
const AndSearchFixture = struct {
    posix_vfs: lattice.storage.vfs.PosixVfs,
    pm: lattice.storage.page_manager.PageManager,
    bp: BufferPool,
    dict_tree: BTree,
    lengths_tree: BTree,
    reverse_tree: BTree,
    index: FtsIndex,
    db_path: []const u8,

    fn create(allocator: Allocator, db_path: []const u8) !*AndSearchFixture {
        return createWithPool(allocator, db_path, 256);
    }

    fn createWithPool(allocator: Allocator, db_path: []const u8, frames: usize) !*AndSearchFixture {
        const self = try allocator.create(AndSearchFixture);
        errdefer allocator.destroy(self);
        self.db_path = db_path;
        self.posix_vfs = lattice.storage.vfs.PosixVfs.init(allocator);
        const vfs_impl = self.posix_vfs.vfs();
        vfs_impl.delete(db_path) catch {};
        self.pm = try lattice.storage.page_manager.PageManager.init(allocator, vfs_impl, db_path, .{ .create = true });
        errdefer self.pm.deinit();
        self.bp = try BufferPool.init(allocator, &self.pm, frames * 4096);
        errdefer self.bp.deinit();
        self.dict_tree = try BTree.init(allocator, &self.bp);
        self.lengths_tree = try BTree.init(allocator, &self.bp);
        self.reverse_tree = try BTree.init(allocator, &self.bp);
        self.index = FtsIndex.init(allocator, &self.bp, &self.dict_tree, &self.lengths_tree, &self.reverse_tree, .{});
        return self;
    }

    fn destroy(self: *AndSearchFixture, allocator: Allocator) void {
        self.bp.deinit();
        self.pm.deinit();
        self.posix_vfs.vfs().delete(self.db_path) catch {};
        allocator.destroy(self);
    }

    /// What the database does when an indexed property changes.
    fn replace(self: *AndSearchFixture, doc_id: DocId, text: []const u8) !void {
        try self.index.removeDocument(doc_id);
        _ = try self.index.indexDocument(doc_id, text);
    }

    fn postingDocIds(self: *AndSearchFixture, allocator: Allocator, term: []const u8) ![]DocId {
        const entry = (try self.index.dictionary.get(term)).?;
        var ids: ArrayListUnmanaged(DocId) = .empty;
        errdefer ids.deinit(allocator);
        var iter = try self.index.posting_store.iterate(entry.posting_page);
        defer iter.deinit();
        while (try iter.next()) |posting| try ids.append(allocator, posting.doc_id);
        return ids.toOwnedSlice(allocator);
    }
};

fn resultsContain(results: []const ScoredDoc, doc_id: DocId) bool {
    for (results) |result| {
        if (result.doc_id == doc_id) return true;
    }
    return false;
}

test "fts AND search finds a document re-indexed after an update" {
    const allocator = std.testing.allocator;
    const fx = try AndSearchFixture.create(allocator, "/tmp/lattice_fts_and_update_test.db");
    defer fx.destroy(allocator);

    _ = try fx.index.indexDocument(1, "banana");
    _ = try fx.index.indexDocument(2, "apple banana");
    _ = try fx.index.indexDocument(3, "apple");
    try fx.replace(1, "apple banana");

    // An update appends the re-indexed document at the tail, so the lists are
    // no longer in doc_id order. Databases written by 0.16.0 already hold lists
    // like this, so search has to cope with them rather than assume order.
    const apple = try fx.postingDocIds(allocator, "apple");
    defer allocator.free(apple);
    try std.testing.expectEqualSlices(DocId, &[_]DocId{ 2, 3, 1 }, apple);

    const results = try fx.index.search("apple banana", 10);
    defer fx.index.freeResults(results);
    try std.testing.expectEqual(@as(usize, 2), results.len);
    try std.testing.expect(resultsContain(results, 1));
    try std.testing.expect(resultsContain(results, 2));
}

test "fts AND search over unordered multi-page posting lists matches a scan" {
    const allocator = std.testing.allocator;
    const fx = try AndSearchFixture.create(allocator, "/tmp/lattice_fts_and_unordered_test.db");
    defer fx.destroy(allocator);

    // Enough documents that "alpha" spans more than one posting page and every
    // page carries skip pointers.
    const doc_count = 3000;
    var has_alpha = [_]bool{false} ** (doc_count + 1);
    var has_beta = [_]bool{false} ** (doc_count + 1);

    var text_buf: [64]u8 = undefined;
    for (1..doc_count + 1) |i| {
        has_alpha[i] = i % 2 == 0;
        has_beta[i] = i % 3 == 0;
        const text = try std.fmt.bufPrint(&text_buf, "{s} {s} filler", .{
            if (has_alpha[i]) "alpha" else "gamma",
            if (has_beta[i]) "beta" else "delta",
        });
        _ = try fx.index.indexDocument(@intCast(i), text);
    }

    // Rewrite a spread of documents, flipping which terms they hold, so both
    // lists end up with re-added documents far out of order.
    var i: usize = 1;
    while (i <= doc_count) : (i += 7) {
        has_alpha[i] = !has_alpha[i];
        has_beta[i] = i % 5 != 0;
        const text = try std.fmt.bufPrint(&text_buf, "{s} {s} filler", .{
            if (has_alpha[i]) "alpha" else "gamma",
            if (has_beta[i]) "beta" else "delta",
        });
        try fx.replace(@intCast(i), text);
    }

    const results = try fx.index.search("alpha beta", std.math.maxInt(u32));
    defer fx.index.freeResults(results);

    var expected: usize = 0;
    for (1..doc_count + 1) |doc| {
        if (has_alpha[doc] and has_beta[doc]) {
            expected += 1;
            try std.testing.expect(resultsContain(results, @intCast(doc)));
        }
    }
    try std.testing.expectEqual(expected, results.len);
}

test "fts AND search with a term no document contains matches nothing" {
    const allocator = std.testing.allocator;
    const fx = try AndSearchFixture.create(allocator, "/tmp/lattice_fts_and_unknown_term_test.db");
    defer fx.destroy(allocator);

    _ = try fx.index.indexDocument(1, "apple pie");
    _ = try fx.index.indexDocument(2, "apple banana");

    // Search is AND, so a word that is in no document rules every one out.
    // Dropping it instead would answer a different, broader query.
    for ([_][]const u8{ "apple zebra", "zebra apple", "apple pie zebra" }) |query| {
        const results = try fx.index.search(query, 10);
        defer fx.index.freeResults(results);
        try std.testing.expectEqual(@as(usize, 0), results.len);
    }

    // A term whose every document was removed has a dictionary entry but no
    // postings left, which has to mean the same thing.
    try fx.index.removeDocument(1);
    const results = try fx.index.search("apple pie", 10);
    defer fx.index.freeResults(results);
    try std.testing.expectEqual(@as(usize, 0), results.len);
}

test "fts AND search ranks every match before applying the limit" {
    const allocator = std.testing.allocator;
    const fx = try AndSearchFixture.create(allocator, "/tmp/lattice_fts_and_ranking_test.db");
    defer fx.destroy(allocator);

    // Every document has both terms; the last one in doc_id order is the
    // shortest, so BM25 ranks it first.
    _ = try fx.index.indexDocument(1, "apple banana cherry grape lemon mango olive");
    _ = try fx.index.indexDocument(2, "apple banana cherry grape lemon mango");
    _ = try fx.index.indexDocument(3, "apple banana cherry grape lemon");
    _ = try fx.index.indexDocument(4, "apple banana cherry grape");
    _ = try fx.index.indexDocument(5, "apple banana");

    const all = try fx.index.search("apple banana", std.math.maxInt(u32));
    defer fx.index.freeResults(all);
    try std.testing.expectEqual(@as(usize, 5), all.len);
    try std.testing.expectEqual(@as(DocId, 5), all[0].doc_id);

    for (1..5) |limit| {
        const top = try fx.index.search("apple banana", @intCast(limit));
        defer fx.index.freeResults(top);
        try std.testing.expectEqual(limit, top.len);
        for (top, all[0..limit]) |got, want| {
            try std.testing.expectEqual(want.doc_id, got.doc_id);
            try std.testing.expectEqual(want.score, got.score);
        }
    }
}

test "fts searchWithMode applies exclusions before the limit" {
    const allocator = std.testing.allocator;
    const fx = try AndSearchFixture.create(allocator, "/tmp/lattice_fts_mode_exclusion_limit_test.db");
    defer fx.destroy(allocator);

    // The three short documents outscore the long one for "apple", and every
    // one of them has the excluded word. The long one is the only answer.
    _ = try fx.index.indexDocument(1, "apple banana");
    _ = try fx.index.indexDocument(2, "apple banana");
    _ = try fx.index.indexDocument(3, "apple banana");
    _ = try fx.index.indexDocument(4, "apple cherry grape lemon mango olive peach");

    for ([_]QueryMode{ .@"and", .@"or" }) |mode| {
        const results = try fx.index.searchWithMode("apple -banana", mode, 1);
        defer fx.index.freeResults(results);
        try std.testing.expectEqual(@as(usize, 1), results.len);
        try std.testing.expectEqual(@as(DocId, 4), results[0].doc_id);
    }
}

test "fts searchWithMode intersects a phrase with terms before the limit" {
    const allocator = std.testing.allocator;
    const fx = try AndSearchFixture.create(allocator, "/tmp/lattice_fts_mode_phrase_limit_test.db");
    defer fx.destroy(allocator);
    fx.index.config.store_positions = true;

    // Five short documents hold the phrase and outscore the long one, which is
    // the only document that also holds "zebra".
    for (1..6) |id| _ = try fx.index.indexDocument(@intCast(id), "quick brown");
    _ = try fx.index.indexDocument(6, "quick brown cherry grape lemon mango olive zebra");

    const results = try fx.index.searchWithMode("\"quick brown\" zebra", .@"and", 1);
    defer fx.index.freeResults(results);
    try std.testing.expectEqual(@as(usize, 1), results.len);
    try std.testing.expectEqual(@as(DocId, 6), results[0].doc_id);
}

test "fts searchWithMode treats a phrase as its words when positions are not stored" {
    const allocator = std.testing.allocator;
    const fx = try AndSearchFixture.create(allocator, "/tmp/lattice_fts_mode_phrase_no_positions_test.db");
    defer fx.destroy(allocator);

    _ = try fx.index.indexDocument(1, "quick brown fox");
    _ = try fx.index.indexDocument(2, "quick red fox");

    const results = try fx.index.searchWithMode("\"quick brown\"", .@"and", 10);
    defer fx.index.freeResults(results);
    try std.testing.expectEqual(@as(usize, 1), results.len);
    try std.testing.expectEqual(@as(DocId, 1), results[0].doc_id);
}

/// Text with `word` repeated `count` times, separated by spaces.
fn repeatedWords(allocator: Allocator, word: []const u8, count: usize) ![]u8 {
    var text: ArrayListUnmanaged(u8) = .empty;
    errdefer text.deinit(allocator);
    for (0..count) |i| {
        if (i > 0) try text.append(allocator, ' ');
        try text.appendSlice(allocator, word);
    }
    return text.toOwnedSlice(allocator);
}

test "fts posting entry with more positions than a page holds is stored" {
    const allocator = std.testing.allocator;
    const fx = try AndSearchFixture.create(allocator, "/tmp/lattice_fts_many_positions_test.db");
    defer fx.destroy(allocator);
    fx.index.config.store_positions = true;

    // 3000 occurrences encode to about 6 KB of positions: past the 512-byte
    // buffer the entry used to be encoded into, and past a whole page.
    const text = try repeatedWords(allocator, "echo", 3000);
    defer allocator.free(text);
    _ = try fx.index.indexDocument(1, text);
    _ = try fx.index.indexDocument(2, "echo chamber");
    _ = try fx.index.indexDocument(3, "echo chamber");

    const results = try fx.index.search("echo", 10);
    defer fx.index.freeResults(results);
    try std.testing.expectEqual(@as(usize, 3), results.len);

    const phrase = try fx.index.searchPhrase("echo chamber", 10);
    defer fx.index.freeResults(phrase);
    try std.testing.expectEqual(@as(usize, 2), phrase.len);
    try std.testing.expect(resultsContain(phrase, 2));
    try std.testing.expect(resultsContain(phrase, 3));

    // The first positions are kept, so a phrase near the start still matches.
    const echoes = try fx.index.searchPhrase("echo echo", 10);
    defer fx.index.freeResults(echoes);
    try std.testing.expectEqual(@as(usize, 1), echoes.len);
    try std.testing.expectEqual(@as(DocId, 1), echoes[0].doc_id);
}

test "fts removing from a page with skip pointers and a long entry" {
    const allocator = std.testing.allocator;
    const fx = try AndSearchFixture.create(allocator, "/tmp/lattice_fts_skip_rebuild_long_entry_test.db");
    defer fx.destroy(allocator);
    fx.index.config.store_positions = true;

    // One entry of about 400 bytes among enough short ones that the page has
    // skip pointers, so removing any entry rebuilds them and has to measure
    // the long one.
    const text = try repeatedWords(allocator, "delta", 200);
    defer allocator.free(text);
    _ = try fx.index.indexDocument(1, text);
    for (2..200) |id| _ = try fx.index.indexDocument(@intCast(id), "delta");

    try fx.index.removeDocument(150);
    const results = try fx.index.search("delta", std.math.maxInt(u32));
    defer fx.index.freeResults(results);
    try std.testing.expectEqual(@as(usize, 198), results.len);
    try std.testing.expect(!resultsContain(results, 150));
}

test "fts posting page keeps one encoding when store_positions changes" {
    const allocator = std.testing.allocator;
    const fx = try AndSearchFixture.create(allocator, "/tmp/lattice_fts_positions_flip_test.db");
    defer fx.destroy(allocator);

    // A database opened first without positions and later with them appends
    // entries of both kinds to the same pages.
    _ = try fx.index.indexDocument(1, "kilo lima");
    _ = try fx.index.indexDocument(2, "kilo kilo lima");
    fx.index.config.store_positions = true;
    _ = try fx.index.indexDocument(3, "kilo kilo kilo lima");
    fx.index.config.store_positions = false;
    _ = try fx.index.indexDocument(4, "kilo lima");

    var iter = try fx.index.posting_store.iterate((try fx.index.dictionary.get("kilo")).?.posting_page);
    defer iter.deinit();
    var seen: [4]struct { doc: DocId, tf: u32 } = undefined;
    var n: usize = 0;
    while (try iter.next()) |posting| : (n += 1) {
        try std.testing.expect(n < seen.len);
        seen[n] = .{ .doc = posting.doc_id, .tf = posting.term_freq };
    }
    try std.testing.expectEqual(@as(usize, 4), n);
    for (seen, [_]u32{ 1, 2, 3, 1 }, 1..) |got, tf, doc| {
        try std.testing.expectEqual(@as(DocId, @intCast(doc)), got.doc);
        try std.testing.expectEqual(tf, got.tf);
    }
}

test "fts phrase terms must all follow the same occurrence of the first" {
    const allocator = std.testing.allocator;
    const fx = try AndSearchFixture.create(allocator, "/tmp/lattice_fts_phrase_same_start_test.db");
    defer fx.destroy(allocator);
    fx.index.config.store_positions = true;

    // Each later word follows some "alpha", but never the same one.
    _ = try fx.index.indexDocument(1, "alpha bravo xray alpha xray charlie");
    _ = try fx.index.indexDocument(2, "xray alpha bravo charlie");

    const results = try fx.index.searchPhrase("alpha bravo charlie", 10);
    defer fx.index.freeResults(results);
    try std.testing.expectEqual(@as(usize, 1), results.len);
    try std.testing.expectEqual(@as(DocId, 2), results[0].doc_id);

    const mode = try fx.index.searchWithMode("\"alpha bravo charlie\"", .@"and", 10);
    defer fx.index.freeResults(mode);
    try std.testing.expectEqual(@as(usize, 1), mode.len);
    try std.testing.expectEqual(@as(DocId, 2), mode[0].doc_id);
}

test "fts quoted phrase with a stop word matches as indexed" {
    const allocator = std.testing.allocator;
    const fx = try AndSearchFixture.create(allocator, "/tmp/lattice_fts_phrase_stop_word_test.db");
    defer fx.destroy(allocator);
    fx.index.config.store_positions = true;

    _ = try fx.index.indexDocument(1, "Bank of America branch");
    _ = try fx.index.indexDocument(2, "America bank holiday");

    const results = try fx.index.searchWithMode("\"bank of america\"", .@"and", 10);
    defer fx.index.freeResults(results);
    try std.testing.expectEqual(@as(usize, 1), results.len);
    try std.testing.expectEqual(@as(DocId, 1), results[0].doc_id);
}

test "fts prefix search counts a document once per query word" {
    const allocator = std.testing.allocator;
    const fx = try AndSearchFixture.create(allocator, "/tmp/lattice_fts_prefix_count_test.db");
    defer fx.destroy(allocator);

    // Doc 1 has two words under "optim" and no "zebra"; it must not stand in
    // for a document holding both query words.
    _ = try fx.index.indexDocument(1, "optimize optimization");
    _ = try fx.index.indexDocument(2, "zebra optimizer");

    for ([_][]const u8{ "zebra optim*", "optim* zebra" }) |query| {
        const results = try fx.index.searchWithPrefix(query, .{}, 10);
        defer fx.index.freeResults(results);
        try std.testing.expectEqual(@as(usize, 1), results.len);
        try std.testing.expectEqual(@as(DocId, 2), results[0].doc_id);
    }
}

test "fts prefix search splits words the way the tokenizer does" {
    const allocator = std.testing.allocator;
    const fx = try AndSearchFixture.create(allocator, "/tmp/lattice_fts_prefix_split_test.db");
    defer fx.destroy(allocator);

    _ = try fx.index.indexDocument(1, "database systems");
    _ = try fx.index.indexDocument(2, "datastore management");

    // Punctuation is not part of a word, and a stop word asks for nothing.
    const results = try fx.index.searchWithPrefix("the systems, data*", .{}, 10);
    defer fx.index.freeResults(results);
    try std.testing.expectEqual(@as(usize, 1), results.len);
    try std.testing.expectEqual(@as(DocId, 1), results[0].doc_id);
}

test "fts prefix and fuzzy matches keep their terms after the walk" {
    const allocator = std.testing.allocator;
    // A pool small enough that the dictionary's leaves are evicted while a
    // search reads posting pages.
    const fx = try AndSearchFixture.createWithPool(allocator, "/tmp/lattice_fts_expansion_terms_test.db", 24);
    defer fx.destroy(allocator);

    var text_buf: [64]u8 = undefined;
    for (0..400) |i| {
        const text = try std.fmt.bufPrint(&text_buf, "optim{d:0>4} filler{d:0>4}", .{ i, i });
        _ = try fx.index.indexDocument(@intCast(i + 1), text);
    }

    const prefix_matches = try prefix_mod.expandPrefixTerms(allocator, "optim", &fx.index.dictionary, .{ .max_expansions = 1000 });
    defer prefix_mod.freeMatches(allocator, prefix_matches);
    const fuzzy_matches = try fuzzy_mod.expandFuzzyTerms(allocator, "optim0001", &fx.index.dictionary, 1);
    defer fuzzy_mod.freeMatches(allocator, fuzzy_matches);

    // Read every posting page to push the dictionary leaves out of the pool.
    const churn = try fx.index.search("filler0000", 10);
    defer fx.index.freeResults(churn);
    for (0..400) |i| {
        const term = try std.fmt.bufPrint(&text_buf, "filler{d:0>4}", .{i});
        const r = try fx.index.search(term, 1);
        fx.index.freeResults(r);
    }

    try std.testing.expectEqual(@as(usize, 400), prefix_matches.len);
    for (prefix_matches) |m| try std.testing.expect(std.mem.startsWith(u8, m.term, "optim"));
    try std.testing.expect(fuzzy_matches.len >= 10);
    for (fuzzy_matches) |m| try std.testing.expect(std.mem.startsWith(u8, m.term, "optim"));
}

test "fts fuzzy search scores a query word by its best match" {
    const allocator = std.testing.allocator;
    const fx = try AndSearchFixture.create(allocator, "/tmp/lattice_fts_fuzzy_best_test.db");
    defer fx.destroy(allocator);

    // "databse" is one edit from "database" and two from "databases"; doc 1
    // holds both and must not score above its exact match for one of them.
    _ = try fx.index.indexDocument(1, "databases database");
    _ = try fx.index.indexDocument(2, "unrelated words");
    _ = try fx.index.indexDocument(3, "more unrelated words");

    const exact = try fx.index.search("database", 10);
    defer fx.index.freeResults(exact);
    try std.testing.expectEqual(@as(usize, 1), exact.len);

    const fuzzy = try fx.index.searchFuzzy("databse", .{ .max_distance = 3, .min_term_length = 3 }, 10);
    defer fx.index.freeResults(fuzzy);
    try std.testing.expectEqual(@as(usize, 1), fuzzy.len);
    try std.testing.expectEqual(@as(DocId, 1), fuzzy[0].doc_id);
    try std.testing.expect(fuzzy[0].score <= exact[0].score);
}

test "fts term presence matches whole terms the way search does" {
    const allocator = std.testing.allocator;
    const fx = try AndSearchFixture.create(allocator, "/tmp/lattice_fts_term_presence_test.db");
    defer fx.destroy(allocator);
    const index = &fx.index;

    try std.testing.expectEqual(@as(f32, 0), try index.termPresenceScore("cat", "concatenate strings"));
    try std.testing.expectEqual(@as(f32, 1), try index.termPresenceScore("Cat", "the cat sat"));
    // Punctuation is not part of a term, and stop words ask for nothing.
    try std.testing.expectEqual(@as(f32, 1), try index.termPresenceScore("bread,", "about bread"));
    try std.testing.expectEqual(@as(f32, 1), try index.termPresenceScore("the bread", "about bread"));
    try std.testing.expectEqual(@as(f32, 0), try index.termPresenceScore("bread butter", "about bread"));
    try std.testing.expectEqual(@as(f32, 2), try index.termPresenceScore("bread butter bread", "butter on bread"));
}

/// What a single-term search should return: every posting scored with its
/// length looked up one at a time, stably sorted, then cut to `limit`.
fn referenceSearch(allocator: Allocator, index: *FtsIndex, terms: []const []const u8, limit: usize) ![]ScoredDoc {
    var scores: std.AutoArrayHashMapUnmanaged(DocId, struct { score: f32, matched: usize }) = .empty;
    defer scores.deinit(allocator);
    var entries: [8]DictionaryEntry = undefined;
    for (terms, 0..) |term, t| entries[t] = (try index.dictionary.get(term)).?;
    // Score in the order search does: rarest term first, ties by query order.
    var order: [8]usize = undefined;
    for (0..terms.len) |t| order[t] = t;
    std.mem.sort(usize, order[0..terms.len], entries[0..terms.len], struct {
        fn lessThan(e: []const DictionaryEntry, a: usize, b: usize) bool {
            return e[a].doc_freq < e[b].doc_freq;
        }
    }.lessThan);
    for (order[0..terms.len]) |t| {
        var iter = try index.posting_store.iterate(entries[t].posting_page);
        defer iter.deinit();
        while (try iter.next()) |p| {
            const len = index.doc_lengths.getLength(p.doc_id) catch continue;
            const s = index.scorer.scoreTerm(index.doc_lengths.stats, p.term_freq, entries[t].doc_freq, len);
            const slot = try scores.getOrPut(allocator, p.doc_id);
            if (!slot.found_existing) slot.value_ptr.* = .{ .score = 0, .matched = 0 };
            slot.value_ptr.score += s;
            slot.value_ptr.matched += 1;
        }
    }
    var out: ArrayListUnmanaged(ScoredDoc) = .empty;
    errdefer out.deinit(allocator);
    var it = scores.iterator();
    while (it.next()) |e| {
        if (e.value_ptr.matched == terms.len) try out.append(allocator, .{ .doc_id = e.key_ptr.*, .score = e.value_ptr.score });
    }
    std.mem.sort(ScoredDoc, out.items, {}, ScoredDoc.lessThan);
    _ = limit;
    return out.toOwnedSlice(allocator);
}

/// `got` must be the first `limit` of `want` up to the order of documents
/// that tie on score: the same scores in the same order, and each document
/// one that `want` scores the same.
fn expectSameRanking(want: []const ScoredDoc, got: []const ScoredDoc, limit: usize) !void {
    try std.testing.expectEqual(@min(want.len, limit), got.len);
    for (want[0..got.len], got) |x, y| try std.testing.expectEqual(x.score, y.score);
    var seen = std.AutoHashMap(DocId, void).init(std.testing.allocator);
    defer seen.deinit();
    for (got) |y| {
        try std.testing.expect(!(try seen.getOrPut(y.doc_id)).found_existing);
        var found = false;
        for (want) |x| {
            if (x.doc_id == y.doc_id) {
                try std.testing.expectEqual(x.score, y.score);
                found = true;
                break;
            }
        }
        try std.testing.expect(found);
    }
}

test "fts batched scoring and bounded ranking match a full scan and sort" {
    const allocator = std.testing.allocator;
    const fx = try AndSearchFixture.create(allocator, "/tmp/lattice_fts_topk_equivalence_test.db");
    defer fx.destroy(allocator);

    // Two scoped indexes over the same trees with the same doc ids, so a
    // lookup that strayed out of its scope would read the other's lengths.
    var a = FtsIndex.initScoped(allocator, &fx.bp, &fx.dict_tree, &fx.lengths_tree, &fx.reverse_tree, .{}, .{ 0, 1, 0, 1 });
    var b = FtsIndex.initScoped(allocator, &fx.bp, &fx.dict_tree, &fx.lengths_tree, &fx.reverse_tree, .{}, .{ 0, 1, 0, 2 });

    var prng = std.Random.DefaultPrng.init(7);
    const rnd = prng.random();
    var text_buf: [256]u8 = undefined;
    for (1..2500) |id| {
        var w: std.Io.Writer = .fixed(&text_buf);
        // Few distinct lengths, so many documents tie on score.
        const words = 1 + rnd.uintLessThan(usize, 4);
        for (0..words) |_| try w.print("{s} ", .{([_][]const u8{ "red", "green", "blue", "cyan" })[rnd.uintLessThan(usize, 4)]});
        _ = try a.indexDocument(@intCast(id * 7919 % 100_003), w.buffered());
        _ = try b.indexDocument(@intCast(id * 7919 % 100_003), "red red red red red red red red red");
    }
    // Updates put documents out of order in the posting lists.
    for (1..400) |id| {
        const doc: DocId = @intCast(id * 7919 % 100_003);
        try a.removeDocument(doc);
        _ = try a.indexDocument(doc, if (id % 2 == 0) "red green" else "blue");
    }

    const queries = [_][]const []const u8{
        &.{"red"},  &.{"green"},          &.{"blue"},
        &.{"cyan"}, &.{ "red", "green" }, &.{ "green", "blue", "cyan" },
    };
    for (queries) |terms| {
        var text: [64]u8 = undefined;
        var w: std.Io.Writer = .fixed(&text);
        for (terms) |t| try w.print("{s} ", .{t});
        for ([_]u32{ 1, 3, 10, 100, std.math.maxInt(u32) }) |limit| {
            const got = try a.search(w.buffered(), limit);
            defer a.freeResults(got);
            const want = try referenceSearch(allocator, &a, terms, limit);
            defer allocator.free(want);
            try expectSameRanking(want, got, limit);
        }
    }
}

test "fts a new index finds non-ASCII words whole, however they are cased or accented" {
    const allocator = std.testing.allocator;
    const fx = try AndSearchFixture.create(allocator, "/tmp/lattice_fts_unicode_words_test.db");
    defer fx.destroy(allocator);

    _ = try fx.index.indexDocument(1, "Herr Müller wohnt in München");
    _ = try fx.index.indexDocument(2, "Frau Zöller wohnt in Köln");
    _ = try fx.index.indexDocument(3, "Встреча в Москве");

    const Case = struct { query: []const u8, want: []const DocId };
    const cases = [_]Case{
        .{ .query = "Müller", .want = &.{1} },
        .{ .query = "MULLER", .want = &.{1} },
        .{ .query = "muenchen munchen", .want = &.{} },
        .{ .query = "münchen", .want = &.{1} },
        // 0.16.0 stored "ller" for both names, so each found the other.
        .{ .query = "ller", .want = &.{} },
        .{ .query = "москве", .want = &.{3} },
    };
    for (cases) |c| {
        const got = try fx.index.search(c.query, 10);
        defer fx.index.freeResults(got);
        try std.testing.expectEqual(c.want.len, got.len);
        for (c.want, got) |w, g| try std.testing.expectEqual(w, g.doc_id);
    }

    // The same analysis reaches every query form.
    const fuzzy = try fx.index.searchFuzzy("Mûller", .{ .max_distance = 1 }, 10);
    defer fx.index.freeResults(fuzzy);
    try std.testing.expectEqual(@as(usize, 1), fuzzy.len);
    const prefix = try fx.index.searchWithPrefix("MÜN*", .{}, 10);
    defer fx.index.freeResults(prefix);
    try std.testing.expectEqual(@as(usize, 1), prefix.len);
    const mode = try fx.index.searchWithMode("Köln -Müller", .@"and", 10);
    defer fx.index.freeResults(mode);
    try std.testing.expectEqual(@as(usize, 1), mode.len);
    try std.testing.expectEqual(@as(DocId, 2), mode[0].doc_id);
    try std.testing.expectEqual(@as(f32, 1), try fx.index.termPresenceScore("MÜLLER", "müller"));
}

test "fts an index 0.16.0 built keeps its ASCII tokenizer until it is emptied" {
    const allocator = std.testing.allocator;
    const fx = try AndSearchFixture.create(allocator, "/tmp/lattice_fts_legacy_analysis_test.db");
    defer fx.destroy(allocator);

    // Written as 0.16.0 wrote it: ASCII-only terms and no recorded analysis.
    const legacy = FtsIndex.legacyAnalysis(fx.index.config.tokenizer);
    _ = try fx.index.indexDocumentAnalyzed(1, "Herr Müller", legacy);
    _ = try fx.index.indexDocumentAnalyzed(2, "Frau Zöller", legacy);

    // Reopened by this version.
    var reopened = FtsIndex.init(allocator, &fx.bp, &fx.dict_tree, &fx.lengths_tree, &fx.reverse_tree, .{});
    try std.testing.expect((try reopened.analysis()).ascii_only);
    // Its stored terms are still found, by the same queries as before.
    {
        const got = try reopened.search("Müller", 10);
        defer reopened.freeResults(got);
        try std.testing.expectEqual(@as(usize, 2), got.len);
    }
    // New documents go in the same way, so old and new stay consistent.
    _ = try reopened.indexDocument(3, "Herr Müller");
    {
        const got = try reopened.search("Müller", 10);
        defer reopened.freeResults(got);
        try std.testing.expectEqual(@as(usize, 3), got.len);
    }

    // Once every document has left, the next one records this version's
    // analysis and the index splits words properly from then on.
    for ([_]DocId{ 1, 2, 3 }) |doc| try reopened.removeDocument(doc);
    _ = try reopened.indexDocument(4, "Herr Müller");
    _ = try reopened.indexDocument(5, "Frau Zöller");
    var again = FtsIndex.init(allocator, &fx.bp, &fx.dict_tree, &fx.lengths_tree, &fx.reverse_tree, .{});
    try std.testing.expect(!(try again.analysis()).ascii_only);
    const got = try again.search("Müller", 10);
    defer again.freeResults(got);
    try std.testing.expectEqual(@as(usize, 1), got.len);
    try std.testing.expectEqual(@as(DocId, 4), got[0].doc_id);
}

test "fts the recorded analysis wins over a changed config" {
    const allocator = std.testing.allocator;
    const fx = try AndSearchFixture.create(allocator, "/tmp/lattice_fts_recorded_analysis_test.db");
    defer fx.destroy(allocator);

    var stemming = FtsIndex.init(allocator, &fx.bp, &fx.dict_tree, &fx.lengths_tree, &fx.reverse_tree, .{
        .tokenizer = .{ .use_stemming = true },
    });
    _ = try stemming.indexDocument(1, "she runs daily");

    // Opened with stemming off: the stored term is "run", and the query must
    // still be stemmed to find it.
    var plain = FtsIndex.init(allocator, &fx.bp, &fx.dict_tree, &fx.lengths_tree, &fx.reverse_tree, .{});
    try std.testing.expect((try plain.analysis()).use_stemming);
    const got = try plain.search("running", 10);
    defer plain.freeResults(got);
    try std.testing.expectEqual(@as(usize, 1), got.len);

    // And highlighting with the index's analysis marks the word search matched.
    var term_buf: [tokenizer_mod.MAX_TERM_LEN]u8 = undefined;
    const term = tokenizer_mod.normalizeTerm(try plain.analysis(), "running", &term_buf).?;
    const highlight = @import("highlight.zig");
    const matches = try highlight.findMatches(allocator, "she runs daily", &.{term}, try plain.analysis());
    defer allocator.free(matches);
    try std.testing.expectEqual(@as(usize, 1), matches.len);
    try std.testing.expectEqual(@as(u32, 4), matches[0].start);
}

test "fts query words split on punctuation and non-ASCII without stalling" {
    const allocator = std.testing.allocator;
    const fx = try AndSearchFixture.create(allocator, "/tmp/lattice_fts_mode_punctuation_test.db");
    defer fx.destroy(allocator);

    _ = try fx.index.indexDocument(1, "bread and butter");
    _ = try fx.index.indexDocument(2, "bread and jam");

    // A comma, or a byte the query parser did not treat as a word or a
    // separator, used to leave it looping on the same character.
    const got = try fx.index.searchWithMode("bread, butter", .@"and", 10);
    defer fx.index.freeResults(got);
    try std.testing.expectEqual(@as(usize, 1), got.len);
    try std.testing.expectEqual(@as(DocId, 1), got[0].doc_id);

    const accented = try fx.index.searchWithMode("bread café", .@"or", 10);
    defer fx.index.freeResults(accented);
    try std.testing.expectEqual(@as(usize, 2), accented.len);
}

test "fts an index with documents but no stored stats is not taken for empty" {
    const allocator = std.testing.allocator;
    const fx = try AndSearchFixture.create(allocator, "/tmp/lattice_fts_no_stats_analysis_test.db");
    defer fx.destroy(allocator);

    // Built as 0.14 built it: ASCII terms, and neither stats nor analysis.
    const legacy = FtsIndex.legacyAnalysis(fx.index.config.tokenizer);
    _ = try fx.index.indexDocumentAnalyzed(1, "Herr Müller", legacy);
    _ = try fx.index.indexDocumentAnalyzed(2, "Frau Zöller", legacy);
    try fx.lengths_tree.delete(&[_]u8{ 0x00, 's', 't', 'a', 't', 's' });

    var reopened = FtsIndex.init(allocator, &fx.bp, &fx.dict_tree, &fx.lengths_tree, &fx.reverse_tree, .{});
    try std.testing.expectEqual(@as(u64, 0), reopened.doc_lengths.stats.total_docs);
    try std.testing.expect((try reopened.analysis()).ascii_only);
    // A new document is written the way the stored ones were, not recorded
    // as a fresh index over terms it cannot read.
    _ = try reopened.indexDocument(3, "Herr Müller");
    var again = FtsIndex.init(allocator, &fx.bp, &fx.dict_tree, &fx.lengths_tree, &fx.reverse_tree, .{});
    try std.testing.expect((try again.analysis()).ascii_only);
    const got = try again.search("ller", 10);
    defer again.freeResults(got);
    try std.testing.expectEqual(@as(usize, 3), got.len);
}
