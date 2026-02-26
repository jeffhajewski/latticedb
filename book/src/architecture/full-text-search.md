# Full-Text Search (BM25)

This document explains how Lattice's full-text search works, from text input to ranked results.

## Overview

Full-text search allows you to find documents containing specific words or phrases, ranked by relevance. Lattice implements the **BM25** (Best Match 25) ranking algorithm, the same algorithm used by Elasticsearch and other production search engines.

The FTS system consists of five components:

```
┌─────────────┐     ┌────────────┐     ┌──────────────┐     ┌─────────┐
│  Tokenizer  │ ──► │ Dictionary │ ──► │ Posting List │ ──► │ Scorer  │
│             │     │  (B+Tree)  │     │   (Pages)    │     │ (BM25)  │
└─────────────┘     └────────────┘     └──────────────┘     └─────────┘
      │                   │                   │                  │
      ▼                   ▼                   ▼                  ▼
 "hello world"      "hello" → 1        doc_ids with         ranked
  → ["hello",       "world" → 2        term frequencies     results
     "world"]
```

## 1. Tokenizer

The tokenizer breaks text into searchable tokens.

### What It Does

Given input text:
```
"The quick brown fox jumps over the lazy dog"
```

The tokenizer produces:
```
["quick", "brown", "fox", "jumps", "lazy", "dog"]
```

Notice "The", "the", and "over" are missing—they're **stop words**.

### How It Works

```
Input: "Hello, World! This is a TEST."
        │
        ▼
┌─────────────────────────────────────┐
│ 1. Scan for word boundaries         │
│    Split on non-alphanumeric chars  │
│    "Hello" "World" "This" "is" "a" "TEST"
└─────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────┐
│ 2. Apply length filter              │
│    min_length=2, max_length=64      │
│    "Hello" "World" "This" "is" "TEST"
│    ("a" removed - too short)        │
└─────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────┐
│ 3. Normalize to lowercase           │
│    "hello" "world" "this" "is" "test"
└─────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────┐
│ 4. Filter stop words                │
│    "hello" "world" "test"           │
│    ("this", "is" removed)           │
└─────────────────────────────────────┘
        │
        ▼
Output: [Token{text="hello", position=0},
         Token{text="world", position=1},
         Token{text="test",  position=2}]
```

### Stop Words

Stop words are common words that add little search value. Lattice supports stop word filtering for **11 languages**:

| Language | Example Stop Words |
|----------|-------------------|
| English | the, and, is, a, to, of, in, that, it |
| German | der, die, das, und, ist, in, zu, den |
| French | le, la, les, de, et, en, que, un |
| Spanish | el, la, los, de, en, que, y, es |
| Italian | il, la, lo, i, di, e, che, un |
| Portuguese | o, a, os, de, e, que, em, um |
| Dutch | de, het, een, en, van, in, is |
| Swedish | och, i, att, det, en, som, av |
| Norwegian | og, i, det, er, en, at, til |
| Danish | og, i, at, det, en, er, til |
| Finnish | ja, on, ei, ole, oli, se, han |
| Russian | и, в, не, на, что, он, с, как |

Set the language in your tokenizer config:

```zig
var tokenizer = Tokenizer.init(allocator, text, .{
    .remove_stop_words = true,
    .language = .german,  // Use German stop words
});
```

### Configuration

```zig
pub const TokenizerConfig = struct {
    min_token_length: u8 = 2,      // Skip tokens shorter than this
    max_token_length: u8 = 64,     // Skip tokens longer than this
    lowercase: bool = true,         // Convert to lowercase
    remove_accents: bool = true,    // Remove diacritics (planned)
    remove_stop_words: bool = true, // Filter common words
    use_stemming: bool = false,     // Apply Porter stemmer
    language: Language = .english,  // Language for stop words
};
```

### Porter Stemmer

When `use_stemming` is enabled, tokens are reduced to their root forms:

```
Input Token    →  Stemmed
─────────────────────────
"running"      →  "run"
"connected"    →  "connect"
"optimization" →  "optim"
"databases"    →  "databas"
```

**Why stem?** Stemming improves recall by matching morphological variants:
- Query "run" matches documents containing "running", "runs", "runner"
- Query "connect" matches "connected", "connecting", "connection"

**Note:** Currently only English stemming is supported. For other languages, words are returned unchanged when stemming is enabled. Future versions may add Snowball stemmers for additional languages.

Use `normalizeAndStem()` or `normalizeAndStemWithLanguage()` for manual stemming:

```zig
var buf: [64]u8 = undefined;

// English stemming
const en_stemmed = tokenizer_mod.normalizeAndStemWithLanguage("RUNNING", &buf, true, .english);
// en_stemmed = "run"

// German (no stemmer available, returns lowercased)
const de_stemmed = tokenizer_mod.normalizeAndStemWithLanguage("RUNNING", &buf, true, .german);
// de_stemmed = "running"
```

---

## 2. Dictionary

The dictionary maps tokens to integer IDs and tracks statistics.

### What It Does

```
Token          →  TokenId  DocFreq  PostingPage
─────────────────────────────────────────────────
"hello"        →     1        5         42
"world"        →     2        3         43
"database"     →     3       12         44
"search"       →     4        8         45
```

### Why TokenIds?

Storing the full token string everywhere would waste space. Instead:
- Dictionary stores: `"database"` → `TokenId 3`
- Posting lists store: `TokenId 3` (4 bytes instead of 8)
- Reduced I/O and memory usage

### Storage Format

The dictionary uses a B+Tree with:
- **Key**: Token string (e.g., `"hello"`)
- **Value**: DictionaryEntry (24 bytes)

```
DictionaryEntry (24 bytes, extern struct):
┌──────────────┬──────────────┬──────────────┬──────────────┬──────────────┐
│  total_freq  │  token_id    │   doc_freq   │ posting_page │  _padding    │
│   (8 bytes)  │  (4 bytes)   │  (4 bytes)   │  (4 bytes)   │  (4 bytes)   │
└──────────────┴──────────────┴──────────────┴──────────────┴──────────────┘
```

Fields are ordered largest-first to minimize internal padding (u64 requires 8-byte alignment).

| Field | Type | Description |
|-------|------|-------------|
| `total_freq` | u64 | Total occurrences across all documents |
| `token_id` | u32 | Unique identifier (1 to ~4 billion) |
| `doc_freq` | u32 | Number of documents containing this token |
| `posting_page` | PageId | First page of the posting list |
| `_padding` | u32 | Explicit padding for 8-byte struct alignment |

### Operations

**getOrCreate(token)** - Get existing TokenId or create new one:
```
Input: "hello"
        │
        ▼
┌─────────────────────────────────────┐
│ B+Tree lookup for "hello"           │
│                                     │
│ Found? → Return existing token_id   │
│                                     │
│ Not found? →                        │
│   1. Assign next_token_id (e.g., 5) │
│   2. Insert into B+Tree             │
│   3. Return 5                       │
└─────────────────────────────────────┘
```

---

## 3. Posting Lists

A posting list stores which documents contain a specific token.

### What It Does

For the token "database" (TokenId 3):
```
Posting List for "database":
┌────────┬───────────┐
│ DocId  │ TermFreq  │
├────────┼───────────┤
│   15   │     3     │  ← Document 15 contains "database" 3 times
│   42   │     1     │  ← Document 42 contains "database" 1 time
│   89   │     7     │  ← Document 89 contains "database" 7 times
│  156   │     2     │
│  203   │     1     │
│  ...   │    ...    │
└────────┴───────────┘
```

### Page Layout

Each posting list page is 4096 bytes:

```
Posting Page (4096 bytes):
┌─────────────────────────────────────────────────────────────┐
│ PageHeader (8 bytes)                                        │
│   page_type = FTS_POSTING                                   │
├─────────────────────────────────────────────────────────────┤
│ PostingPageHeader (20 bytes)                                │
│   token_id:        u32  - Which token this list is for      │
│   num_entries:     u32  - Number of postings in this page   │
│   next_page:       u32  - Overflow page (0 = none)          │
│   num_skip_ptrs:   u16  - Number of skip pointers           │
│   flags:           u16  - 0x01 = has positions              │
│   data_start:      u32  - Byte offset where data begins     │
├─────────────────────────────────────────────────────────────┤
│ Skip Pointers (16 bytes each, optional)                     │
│   [doc_id, byte_offset, entry_count] × N                    │
├─────────────────────────────────────────────────────────────┤
│ Posting Data (variable length, varint encoded)              │
│   [doc_id: varint] [term_freq: varint]                      │
│   [doc_id: varint] [term_freq: varint]                      │
│   ...                                                       │
└─────────────────────────────────────────────────────────────┘
```

### PostingPageHeader Explained

The `PostingPageHeader` is metadata at the start of each posting page:

| Field | Bytes | Purpose |
|-------|-------|---------|
| `token_id` | 4 | Identifies which token this posting list belongs to |
| `num_entries` | 4 | How many (doc_id, term_freq) pairs are in this page |
| `next_page` | 4 | PageId of overflow page if list doesn't fit (0 = no overflow) |
| `num_skip_pointers` | 2 | Count of skip pointers for fast seeking |
| `flags` | 2 | Bit flags: 0x01 = positions stored for phrase queries |
| `data_start` | 4 | Byte offset where posting entries begin (after skip pointers) |

### Varint Encoding

Document IDs and frequencies are stored using **variable-length integers** (varints) for compression:

```
Value           Binary              Varint Bytes     Savings
─────────────────────────────────────────────────────────────
127             0111 1111           [0x7F]           1 byte (vs 8)
128             1000 0000           [0x80, 0x01]     2 bytes
16383           0011 1111 1111 1111 [0xFF, 0x7F]     2 bytes
1,000,000       ...                 [0xC0, 0x84, 0x3D] 3 bytes
```

**Encoding algorithm:**
```
while value >= 0x80:
    output byte = (value & 0x7F) | 0x80   // Low 7 bits + continuation flag
    value = value >> 7
output byte = value                        // Final byte (no continuation)
```

**Example:** Encoding 300 (binary: 1 0010 1100)
```
300 = 0b100101100

Step 1: 300 >= 128, so:
        output[0] = (300 & 0x7F) | 0x80 = 0x2C | 0x80 = 0xAC
        300 >> 7 = 2

Step 2: 2 < 128, so:
        output[1] = 2 = 0x02

Result: [0xAC, 0x02] (2 bytes instead of 8)
```

### Overflow Pages

When a posting list exceeds one page, it chains to overflow pages:

```
Page 42 (first page)          Page 87 (overflow)
┌────────────────────┐        ┌────────────────────┐
│ Header             │        │ Header             │
│   next_page = 87 ──┼───────►│   next_page = 0    │
│   num_entries = 200│        │   num_entries = 50 │
├────────────────────┤        ├────────────────────┤
│ Entries 1-200      │        │ Entries 201-250    │
└────────────────────┘        └────────────────────┘
```

### Skip Pointers

Skip pointers enable O(log n) seeking within posting lists, dramatically speeding up multi-term AND queries.

**Structure:**
```
SkipPointer (16 bytes):
┌────────────┬─────────────┬─────────────┐
│  doc_id    │ byte_offset │ entry_count │
│  (8 bytes) │  (4 bytes)  │  (4 bytes)  │
└────────────┴─────────────┴─────────────┘
```

**How they work:**

Skip pointers are created every 128 entries (SKIP_INTERVAL). They record:
- `doc_id`: The document ID at that entry
- `byte_offset`: Where to jump in the posting data
- `entry_count`: Number of entries before this pointer

```
Posting list with 500 entries:

Skip Pointers:
  [0] doc_id=1280, offset=512, count=128   ← entry 128
  [1] doc_id=2560, offset=1024, count=256  ← entry 256
  [2] doc_id=3840, offset=1536, count=384  ← entry 384

Seeking to doc_id 3000:
  1. Binary search skip pointers: find [1] (doc_id=2560 < 3000)
  2. Jump to offset 1024
  3. Linear scan from entry 256 to find doc_id 3000

Result: Skipped 256 entries instead of scanning all 300
```

**Multi-term intersection optimization:**

For AND queries like "database optimization":
1. Sort terms by doc_freq (smallest first)
2. Iterate through smallest posting list
3. For each doc_id, use `skipTo()` on other lists
4. Skip pointers let large lists jump ahead efficiently

```
Query: "the database" (AND)

"the":      10,000 documents
"database":    100 documents  ← Driver (smallest)

Without skip pointers: 10,000 + 100 = 10,100 iterations
With skip pointers:    100 + ~100 seeks = ~200 operations
```

---

## 4. Document Length Storage

BM25 scoring requires knowing each document's length. This is stored in a separate B+Tree:

```
DocId  →  Length (tokens)
────────────────────────
   1   →      150
   2   →       45
   3   →      892
  ...  →      ...
```

**Why store lengths?**

BM25 penalizes long documents to prevent them from dominating results just because they contain more words. A 10,000-word document mentioning "database" once is less relevant than a 100-word document mentioning it once.

**Statistics tracked:**
- `total_docs`: Total documents indexed
- `total_tokens`: Sum of all document lengths
- `avg_doc_length`: Average tokens per document (for normalization)

---

## 5. BM25 Scoring

BM25 calculates a relevance score for each document.

### The Formula

```
Score(D, Q) = Σ IDF(term) × TF_norm(term, D)
              for each term in query Q
```

Where:

**IDF (Inverse Document Frequency):**
```
IDF(term) = log((N - df + 0.5) / (df + 0.5) + 1)

N  = total number of documents
df = number of documents containing this term
```

Rare terms get higher IDF scores. If "quantum" appears in 5 of 10,000 documents, it's more significant than "the" appearing in 9,500.

**TF_norm (Normalized Term Frequency):**
```
TF_norm = (tf × (k1 + 1)) / (tf + k1 × (1 - b + b × (dl / avgdl)))

tf    = term frequency in this document
k1    = saturation parameter (default 1.2)
b     = length normalization (default 0.75)
dl    = document length
avgdl = average document length
```

### Parameters

| Parameter | Default | Effect |
|-----------|---------|--------|
| `k1` | 1.2 | Controls term frequency saturation. Higher = more weight to repeated terms |
| `b` | 0.75 | Length normalization. 0 = no normalization, 1 = full normalization |

### Scoring Example

```
Corpus: 1000 documents, average length 200 tokens
Query: "database optimization"

Document 42:
  - Length: 150 tokens
  - "database" appears 3 times
  - "optimization" appears 1 time

Term: "database"
  - doc_freq = 50 (appears in 50 docs)
  - IDF = log((1000 - 50 + 0.5) / (50 + 0.5) + 1) = log(19.82) ≈ 2.99

  - tf = 3
  - dl/avgdl = 150/200 = 0.75
  - TF_norm = (3 × 2.2) / (3 + 1.2 × (1 - 0.75 + 0.75 × 0.75))
            = 6.6 / (3 + 1.2 × 0.8125)
            = 6.6 / 3.975
            ≈ 1.66

  - Score for "database" = 2.99 × 1.66 ≈ 4.96

Term: "optimization"
  - doc_freq = 10 (rarer term)
  - IDF = log((1000 - 10 + 0.5) / (10 + 0.5) + 1) ≈ 4.55

  - tf = 1
  - TF_norm = (1 × 2.2) / (1 + 1.2 × 0.8125) ≈ 1.12

  - Score for "optimization" = 4.55 × 1.12 ≈ 5.10

Total Score for Document 42 = 4.96 + 5.10 = 10.06
```

---

## 6. Search Flow

### Single-Term Search

```
Query: "database"
        │
        ▼
┌─────────────────────────────────────┐
│ 1. Tokenize query                   │
│    → ["database"]                   │
└─────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────┐
│ 2. Dictionary lookup                │
│    "database" → TokenId 3           │
│                 doc_freq = 50       │
│                 posting_page = 44   │
└─────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────┐
│ 3. Iterate posting list (page 44)  │
│    For each (doc_id, term_freq):    │
│      - Get doc_length               │
│      - Calculate BM25 score         │
│      - Add to results               │
└─────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────┐
│ 4. Sort by score descending         │
│    Return top K results             │
└─────────────────────────────────────┘
```

### Multi-Term Search (AND Semantics)

```
Query: "database optimization"
        │
        ▼
┌─────────────────────────────────────┐
│ 1. Tokenize query                   │
│    → ["database", "optimization"]   │
└─────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────┐
│ 2. For each term, iterate posting   │
│    list and accumulate scores       │
│                                     │
│    doc_scores[doc_id] += score      │
│    doc_term_count[doc_id] += 1      │
└─────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────┐
│ 3. Filter: keep only docs where     │
│    term_count == num_query_terms    │
│    (AND semantics)                  │
└─────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────┐
│ 4. Sort by accumulated score        │
│    Return top K results             │
└─────────────────────────────────────┘
```

### OR Search

OR search returns documents matching **any** query term:

```zig
// Returns docs containing "mysql" OR "postgres" OR both
const results = try fts.searchOr("mysql postgres", 10);

// Or use explicit mode selection
const results = try fts.searchWithMode("mysql postgres", .@"or", 10);
```

```
Query: "mysql postgres" (OR mode)
        │
        ▼
┌─────────────────────────────────────┐
│ 1. Tokenize query                   │
│    → ["mysql", "postgres"]          │
└─────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────┐
│ 2. For each term, iterate posting   │
│    list and accumulate scores       │
│                                     │
│    doc_scores[doc_id] += score      │
│    (no term count filtering)        │
└─────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────┐
│ 3. Sort by accumulated score        │
│    Docs with both terms rank higher │
└─────────────────────────────────────┘
```

### NOT Search (Exclusions)

Prefix terms with `-` to exclude documents containing them:

```zig
// Find "database" docs that don't mention "mysql"
const results = try fts.searchWithMode("database -mysql", .@"and", 10);

// Multiple exclusions
const results = try fts.searchWithMode("database -mysql -oracle", .@"and", 10);
```

```
Query: "database -mysql"
        │
        ▼
┌─────────────────────────────────────┐
│ 1. Parse query                      │
│    terms = ["database"]             │
│    excluded = ["mysql"]             │
└─────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────┐
│ 2. Search for positive terms        │
│    → candidate documents            │
└─────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────┐
│ 3. Build exclusion set              │
│    (all doc_ids containing "mysql") │
└─────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────┐
│ 4. Filter candidates                │
│    Remove docs in exclusion set     │
└─────────────────────────────────────┘
```

### Phrase Search

Phrase search finds documents where terms appear **adjacent and in order**:

```zig
// Enable position storage in config
var fts = FtsIndex.init(allocator, &bp, &dict_tree, &lengths_tree, .{
    .store_positions = true,
});

// Search for exact phrase
const results = try fts.searchPhrase("quick brown fox", 10);
```

```
Query: "quick brown fox" (phrase)

Document 1: "The quick brown fox jumps"     ← MATCHES (adjacent: pos 1,2,3)
Document 2: "A quick red brown fox"         ← NO MATCH (not adjacent)
Document 3: "The brown quick fox"           ← NO MATCH (wrong order)
```

**How it works:**

```
┌─────────────────────────────────────┐
│ 1. Get posting lists with positions │
│                                     │
│    "quick": doc1[pos=1], doc2[pos=1]│
│    "brown": doc1[pos=2], doc2[pos=3]│
│    "fox":   doc1[pos=3], doc2[pos=4]│
└─────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────┐
│ 2. For each candidate document:     │
│    Check position adjacency         │
│                                     │
│    doc1: quick@1, brown@2, fox@3    │
│          1+1=2 ✓, 1+2=3 ✓ → MATCH   │
│                                     │
│    doc2: quick@1, brown@3, fox@4    │
│          1+1=2 ≠ 3 → NO MATCH       │
└─────────────────────────────────────┘
```

**Note:** Phrase queries require `store_positions = true` in FtsConfig. Without positions, `searchPhrase()` falls back to AND semantics.

### Quoted Phrase Syntax

You can also use quoted strings in regular `search()` and `searchWithMode()` calls to automatically detect phrase queries:

```zig
// These are equivalent:
const results1 = try fts.searchPhrase("quick brown", 10);
const results2 = try fts.searchWithMode("\"quick brown\"", .@"and", 10);
```

**Combining phrases with terms and exclusions:**

```zig
// Phrase + additional term (AND mode)
// Matches documents with "quick brown" phrase AND term "jumps"
const results = try fts.searchWithMode("\"quick brown\" jumps", .@"and", 10);

// Phrase + exclusion
// Matches documents with "quick brown" phrase but NOT containing "fox"
const results = try fts.searchWithMode("\"quick brown\" -fox", .@"and", 10);

// Multiple phrases
// Matches documents with both phrases
const results = try fts.searchWithMode("\"quick brown\" \"lazy dog\"", .@"and", 10);

// Phrase with OR mode
// Matches documents with "quick brown" phrase OR term "rabbit"
const results = try fts.searchWithMode("\"quick brown\" rabbit", .@"or", 10);
```

**Single-word quotes:**

Single words in quotes are treated as regular terms (since a one-word phrase is just a term):

```zig
// These are equivalent:
const results1 = try fts.search("database", 10);
const results2 = try fts.searchWithMode("\"database\"", .@"and", 10);
```

### Fuzzy Search

Fuzzy search finds documents even when query terms contain typos. It uses Levenshtein (edit) distance to match terms within a configurable threshold.

```zig
const fuzzy = @import("fts/fuzzy.zig");

// Search with typo tolerance (max 2 edits)
const results = try fts.searchFuzzy("databse", .{
    .max_distance = 2,      // Allow up to 2 edits
    .min_term_length = 4,   // Only fuzzy-match terms >= 4 chars
}, 10);
defer fts.freeResults(results);
// Matches documents containing "database" (edit distance 1)
```

**How it works:**

```
Query: "databse" (typo)
  ↓
Scan dictionary for terms within edit distance 2:
  - "database" (distance=1) ✓
  - "datastore" (distance=3) ✗
  ↓
Search matching terms with distance penalty:
  - Score("database") × penalty(1) = Score × 0.75
  ↓
Return ranked results
```

**Distance penalty formula:**

```
penalty = 1.0 - (distance / max_distance)²

Examples (max_distance = 2):
- distance 0: 1.00 (exact match)
- distance 1: 0.75 (25% penalty)
- distance 2: 0.00 (filtered out)
```

**Levenshtein distance** counts the minimum single-character edits needed:
- **Insertion**: "helo" → "hello" (distance 1)
- **Deletion**: "hello" → "helo" (distance 1)
- **Substitution**: "hello" → "hallo" (distance 1)

**Note:** Transpositions ("recieve" → "receive") count as 2 edits in standard Levenshtein.

### Prefix/Wildcard Search

Prefix search finds documents containing terms that start with a given prefix. Use `*` as a suffix wildcard.

```zig
const prefix = @import("fts/prefix.zig");

// Search for terms starting with "optim" (matches "optimize", "optimization", "optimizer")
const results = try fts.searchWithPrefix("optim*", .{
    .min_prefix_length = 2,   // Minimum prefix length (prevents "a*" explosion)
    .max_expansions = 50,     // Maximum terms to expand
}, 10);
defer fts.freeResults(results);
```

**How it works:**

```
Query: "optim*"
  ↓
Calculate range bounds: ["optim", "optin")
  ↓
B+Tree range scan to find matching terms:
  - "optimization" ✓
  - "optimize" ✓
  - "optimizer" ✓
  ↓
Search all matching terms (like OR query)
  ↓
Return ranked results
```

**Combining prefix with regular terms:**

```zig
// AND semantics: documents must contain "systems" AND a term starting with "data"
const results = try fts.searchWithPrefix("systems data*", .{}, 10);
// Matches documents with both "systems" and "database"/"datastore"/"data"
```

**Constraints:**
- Suffix wildcards only (`optim*`), not prefix wildcards (`*tion`)
- No middle wildcards (`da*base`)
- Minimum prefix length configurable (default 2 chars)
- Maximum expansions capped to prevent explosion

### Search Highlighting

Search highlighting returns text snippets with matched terms marked, enabling UI display of search results with context.

```zig
const highlight = @import("fts/highlight.zig");

const text = "The database stores data efficiently for optimal performance.";
const query_terms = [_][]const u8{ "database", "data" };

const result = try highlight.highlight(
    allocator,
    text,
    &query_terms,
    .{ .use_stemming = false },  // TokenizerConfig
    .{
        .context_chars = 80,      // Characters of context around matches
        .max_snippets = 3,        // Maximum snippets to return
        .merge_distance = 40,     // Merge snippets closer than this
        .prefix_marker = "<em>",  // Marker before matched terms
        .suffix_marker = "</em>", // Marker after matched terms
        .ellipsis = "...",        // Added when text is truncated
    },
);
defer highlight.freeResult(allocator, result);

// result.snippets[0].text = "The <em>database</em> stores <em>data</em> efficiently..."
// result.total_matches = 2
```

**How it works:**

```
Query terms: ["database", "data"]
Document: "The database stores data efficiently for optimal performance."
  ↓
Re-tokenize document with same config:
  - "database" at offset 4 → stems to "databas" or "database"
  - "data" at offset 20 → stems to "data"
  ↓
Match against query terms (stemmed comparison):
  - Match found at positions 4-12 and 20-24
  ↓
Group matches into snippets with context:
  - Single snippet with both matches (close together)
  ↓
Insert markers around matches:
  - "The <em>database</em> stores <em>data</em> efficiently..."
```

**With stemming:**

```zig
const text = "The runners were running fast in the marathon.";
const query_terms = [_][]const u8{"run"};  // Already stemmed query

const result = try highlight.highlight(
    allocator,
    text,
    &query_terms,
    .{ .use_stemming = true },  // Enable stemming
    .{ .prefix_marker = "**", .suffix_marker = "**" },
);
// "running" stems to "run" → match
// "runners" stems to "runner" → no match
// result.snippets[0].text = "The runners were **running** fast..."
```

**Key design decisions:**
- **Re-tokenization approach**: Document text is re-tokenized at query time (no storage overhead)
- **Stemmed matching**: Query terms are matched against stemmed document tokens
- **Original text preserved**: Markers wrap the original text, not stemmed forms
- **Configurable markers**: Default `<em>`/`</em>` but customizable for any format
- **Snippet merging**: Close matches combined into single snippets

**Finding matches only (without snippets):**

```zig
const matches = try highlight.findMatches(
    allocator,
    text,
    &query_terms,
    .{ .use_stemming = false },
);
defer highlight.freeMatches(allocator, matches);

for (matches) |match| {
    // match.start = byte offset of match start
    // match.end = byte offset of match end (exclusive)
    const matched_text = text[match.start..match.end];
}
```

---

## 7. Indexing Flow

### indexDocument(doc_id, text)

```
Input: doc_id=42, text="The quick database optimization guide"
        │
        ▼
┌─────────────────────────────────────┐
│ 1. Tokenize text                    │
│    → ["quick", "database",          │
│        "optimization", "guide"]     │
└─────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────┐
│ 2. Count term frequencies           │
│    term_freqs = {                   │
│      "quick": 1,                    │
│      "database": 1,                 │
│      "optimization": 1,             │
│      "guide": 1                     │
│    }                                │
└─────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────┐
│ 3. For each term:                   │
│    a. getOrCreate in dictionary     │
│    b. Create posting page if needed │
│    c. Append posting entry          │
│    d. Increment doc_freq            │
└─────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────┐
│ 4. Store document length            │
│    doc_lengths[42] = 4              │
│    Update avg_doc_length            │
└─────────────────────────────────────┘
        │
        ▼
Output: 4 (number of tokens indexed)
```

---

## 8. Data Structures Summary

### In-Memory

| Structure | Purpose |
|-----------|---------|
| `Tokenizer` | Streaming text tokenization |
| `PostingIterator` | Iterate posting list entries |
| `Bm25Scorer` | Calculate relevance scores |

### On-Disk (B+Trees)

| B+Tree | Key | Value |
|--------|-----|-------|
| Dictionary | Token string | DictionaryEntry (24 bytes) |
| DocLengths | DocId (8 bytes) | Length (4 bytes) |

### On-Disk (Pages)

| Page Type | Content |
|-----------|---------|
| `FTS_POSTING` | Posting list entries (varint encoded) |

---

## 9. Limitations and Future Work

### Current Limitations

1. **English-only stemming** - Porter stemmer for English only (other languages skip stemming)

### Implemented Features

1. **Phrase queries** - `searchPhrase("quick brown fox")` with position verification
2. **Boolean queries** - AND (default), OR (`searchOr`), NOT (`-term` syntax)
3. **Porter stemming** - `use_stemming=true` reduces words to roots (English)
4. **Position indexing** - `store_positions=true` for phrase queries
5. **Skip pointers** - O(log n) posting list intersection for multi-term queries
6. **Quoted phrase syntax** - Parse `"exact phrase"` in regular search() calls
7. **Fuzzy search** - Levenshtein distance for typo tolerance via `searchFuzzy()`
8. **Multi-language stop words** - 11 languages supported (EN, DE, FR, ES, IT, PT, NL, SV, NO, DA, FI, RU)
9. **Prefix/wildcard search** - `searchWithPrefix("optim*")` expands to matching terms
10. **Search highlighting** - `highlight()` returns snippets with matched terms marked
11. **Document deletion** - `removeDocument()` with reverse index properly cleans posting lists and stats

### Planned Features

1. **Multi-language stemmers** - Snowball stemmers for German, French, etc.

---

## 10. Usage Example

```zig
const std = @import("std");
const lattice = @import("lattice");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    // Initialize storage (simplified)
    var bp = try BufferPool.init(allocator, &pm, 64 * 4096);
    var dict_tree = try BTree.init(allocator, &bp);
    var lengths_tree = try BTree.init(allocator, &bp);

    // Create FTS index with phrase query support
    var fts = FtsIndex.init(allocator, &bp, &dict_tree, &lengths_tree, .{
        .store_positions = true,  // Enable phrase queries
    });

    // Index documents
    _ = try fts.indexDocument(1, "Introduction to database systems");
    _ = try fts.indexDocument(2, "Advanced database optimization techniques");
    _ = try fts.indexDocument(3, "Web development with JavaScript");
    _ = try fts.indexDocument(4, "Database performance and MySQL tuning");

    // Basic AND search (all terms must match)
    const results1 = try fts.search("database optimization", 10);
    defer fts.freeResults(results1);
    // Returns doc 2 (contains both terms)

    // OR search (any term matches)
    const results2 = try fts.searchOr("mysql postgres", 10);
    defer fts.freeResults(results2);
    // Returns doc 4 (contains "mysql")

    // NOT search (exclusions with -prefix)
    const results3 = try fts.searchWithMode("database -mysql", .@"and", 10);
    defer fts.freeResults(results3);
    // Returns docs 1, 2 (contain "database" but not "mysql")

    // Phrase search (exact sequence)
    const results4 = try fts.searchPhrase("database systems", 10);
    defer fts.freeResults(results4);
    // Returns doc 1 (has "database systems" as adjacent phrase)

    // Quoted phrase syntax (alternative to searchPhrase)
    // Phrase "database optimization" + term "advanced"
    const results5 = try fts.searchWithMode("\"database optimization\" advanced", .@"and", 10);
    defer fts.freeResults(results5);
    // Returns doc 2 (has phrase "database optimization" AND term "advanced")

    // Fuzzy search (typo tolerance)
    const results6 = try fts.searchFuzzy("databse", .{
        .max_distance = 2,
        .min_term_length = 4,
    }, 10);
    defer fts.freeResults(results6);
    // Returns docs with "database" (edit distance 1 from "databse")

    for (results1) |result| {
        std.debug.print("Doc {}: score {d:.2}\n", .{ result.doc_id, result.score });
    }
}
```

---

## 11. Serialization Pattern

All on-disk structures use `extern struct` with compile-time size assertions:

```zig
/// Good: Self-documenting, compile-time verified
pub const PostingPageHeader = extern struct {
    token_id: TokenId,
    num_entries: u32,
    next_page: PageId,
    num_skip_pointers: u16,
    flags: u16,
    data_start: u32,

    comptime {
        std.debug.assert(@sizeOf(PostingPageHeader) == 20);
    }

    pub fn read(data: []const u8) PostingPageHeader {
        return std.mem.bytesAsValue(PostingPageHeader, data[OFFSET..][0..@sizeOf(PostingPageHeader)]).*;
    }

    pub fn write(self: *const PostingPageHeader, data: []u8) void {
        @memcpy(data[OFFSET..][0..@sizeOf(PostingPageHeader)], std.mem.asBytes(self));
    }
};
```

**Why this pattern?**

1. **No magic numbers** - Field layout is defined by the struct, not manual offsets
2. **Compile-time verification** - `comptime` block catches size mismatches immediately
3. **Self-documenting** - Struct fields serve as documentation
4. **Consistent with codebase** - WAL, PageHeader, FileHeader all use this pattern

**Alignment considerations:**

When structs contain `u64` fields, use explicit padding and order fields largest-first:

```zig
pub const DictionaryEntry = extern struct {
    total_freq: u64,   // 8 bytes - largest first (requires 8-byte alignment)
    token_id: u32,     // 4 bytes
    doc_freq: u32,     // 4 bytes
    posting_page: u32, // 4 bytes
    _padding: u32 = 0, // 4 bytes - explicit trailing padding

    comptime {
        std.debug.assert(@sizeOf(DictionaryEntry) == 24);
    }
};
```

---

## 12. File Reference

| File | Purpose |
|------|---------|
| `src/fts/tokenizer.zig` | Text tokenization, normalization, language config |
| `src/fts/stopwords.zig` | Multi-language stop word lists (11 languages) |
| `src/fts/dictionary.zig` | Token → TokenId mapping via B+Tree, range iteration |
| `src/fts/posting.zig` | Posting list storage with varint encoding, entry removal |
| `src/fts/scorer.zig` | BM25 scoring, document length storage |
| `src/fts/index.zig` | Main FtsIndex coordinator, boolean/phrase/fuzzy/prefix search |
| `src/fts/stemmer.zig` | Porter stemmer algorithm (English), language routing |
| `src/fts/fuzzy.zig` | Levenshtein distance, fuzzy term expansion |
| `src/fts/prefix.zig` | Prefix/wildcard search, upper bound calculation |
| `src/fts/highlight.zig` | Search result highlighting, snippet extraction |
| `src/fts/reverse_index.zig` | doc_id → terms mapping for document deletion |
