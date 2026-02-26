# B+Tree

## What It Is

A B+Tree is a self-balancing tree structure optimized for disk-based storage. It provides O(log N) lookups, inserts, and deletes, with efficient range scans.

## Why B+Tree?

### The Problem with Binary Trees

A binary search tree with 1 million entries is ~20 levels deep (log2(1M) ≈ 20). Each level requires a disk read. That's 20 disk reads per lookup!

### B+Tree: Wide and Shallow

A B+Tree has many keys per node (hundreds), making it very shallow:

```
Binary Tree (1M entries):          B+Tree (1M entries):
        depth ~20                       depth ~3

          ○                               ○
         / \                         /    |    \
        ○   ○                       ○     ○     ○
       /\   /\                    / | \ / | \ / | \
      ○ ○ ○  ○                   ○  ○  ○ ○  ○  ○ ○ ○
     ... (20 levels)             (leaf level)

    20 disk reads                   3 disk reads!
```

With 100 keys per node: log100(1M) ≈ 3 levels.

### Data Only in Leaves

B+Trees store all data in leaf nodes. Internal nodes only contain keys for routing:

```
                    ┌─────────────────────┐
                    │   [30]  [60]  [90]  │  ◄── Internal: keys only
                    └───┬──────┬──────┬───┘
                       ╱       │       ╲
        ┌─────────────┘        │        └─────────────┐
        ▼                      ▼                      ▼
┌───────────────┐    ┌───────────────┐    ┌───────────────┐
│ 10:val 20:val │◄──►│ 30:val 50:val │◄──►│ 60:val 80:val │
└───────────────┘    └───────────────┘    └───────────────┘
        ▲                                         ▲
        └─── Leaves: keys AND values ─────────────┘
             Linked for range scans
```

## Our Implementation: Direct Page Manipulation

We don't create "node objects" in memory. Instead, we read/write bytes directly in page buffers. The "node" is just a lens over page bytes.

```
Traditional approach:                Our approach:

┌─────────────┐                     ┌─────────────┐
│ Page bytes  │                     │ Page bytes  │
└──────┬──────┘                     └──────┬──────┘
       │ deserialize                       │
       ▼                                   │ direct access
┌─────────────┐                            │
│ Node struct │                            │
│ - keys[]    │                            ▼
│ - values[]  │                     Read/write at
│ - children[]│                     calculated offsets
└──────┬──────┘
       │ serialize
       ▼
┌─────────────┐
│ Page bytes  │
└─────────────┘
```

No intermediate objects. No serialization overhead.

## Page Layout

### Leaf Node

```
┌────────────────────────────────────────────────────────────────┐
│                         Leaf Page (4KB)                         │
├────────────────────────────────────────────────────────────────┤
│ Bytes 0-7:   Page Header (checksum, type=leaf, flags)          │
├────────────────────────────────────────────────────────────────┤
│ Bytes 8-9:   Entry count (u16)                                 │
│ Bytes 10-13: Prev leaf page (u32)                              │
│ Bytes 14-17: Next leaf page (u32)                              │
│ Bytes 18-19: Free space offset (u16)                           │
├────────────────────────────────────────────────────────────────┤
│ Slot 0: key_offset, key_len, val_offset, val_len (8 bytes)     │
│ Slot 1: key_offset, key_len, val_offset, val_len (8 bytes)     │
│ Slot 2: ...                                                    │
│         ...                                                    │
│                    [free space grows down ↓]                   │
│                                                                │
│                    [entries grow up ↑]                         │
│                                                                │
│ "value2"                                                       │
│ "key2"                                                         │
│ "value1"                                                       │
│ "key1"                                                         │
└────────────────────────────────────────────────────────────────┘
                                                          Byte 4095
```

**Variable-length entries**: Keys and values are stored at the end of the page, growing upward. Slots at the front point to them.

### Internal Node

```
┌────────────────────────────────────────────────────────────────┐
│                       Internal Page (4KB)                       │
├────────────────────────────────────────────────────────────────┤
│ Bytes 0-7:   Page Header (checksum, type=internal, flags)      │
├────────────────────────────────────────────────────────────────┤
│ Bytes 8-9:   Key count (u16)                                   │
│ Bytes 10-13: Rightmost child (u32)                             │
│ Bytes 14-15: Free space offset (u16)                           │
├────────────────────────────────────────────────────────────────┤
│ Slot 0: child_page, key_offset, key_len (8 bytes)              │
│ Slot 1: child_page, key_offset, key_len (8 bytes)              │
│         ...                                                    │
│                    [free space]                                │
│                                                                │
│ "separator_key2"                                               │
│ "separator_key1"                                               │
└────────────────────────────────────────────────────────────────┘
```

## Point Lookup

Finding a value by key:

```zig
pub fn get(self: *Self, key: []const u8) !?[]const u8 {
    // 1. Start at root
    var page_id = self.root_page;

    while (true) {
        // 2. Fetch page from buffer pool
        const frame = try self.bp.fetchPage(page_id, .shared);
        defer self.bp.unpinPage(frame, false);

        const page_type = getPageType(frame.data);

        if (page_type == .btree_leaf) {
            // 3. Binary search in leaf
            return LeafNode.search(frame.data, key, self.comparator);
        } else {
            // 4. Binary search for child pointer, descend
            page_id = InternalNode.findChild(frame.data, key, self.comparator);
        }
    }
}
```

**Example lookup for key "dog":**

```
                        Root (Internal)
                    ┌─────────────────────┐
                    │  [cat]     [fish]   │
                    └───┬──────────┬──────┘
                       ╱           ╲
     "dog" > "cat"    ╱             ╲
     "dog" < "fish"  ╱               ╲
                    ▼
             ┌─────────────────┐
             │ cow:v1  dog:v2  │ ◄── Found! Return v2
             └─────────────────┘
```

## Insertion

### Simple Case: Space in Leaf

```zig
pub fn insert(self: *Self, key: []const u8, value: []const u8) !void {
    // 1. Find the leaf
    const leaf_page = try self.findLeaf(key);

    // 2. Fetch with exclusive latch
    const frame = try self.bp.fetchPage(leaf_page, .exclusive);
    defer self.bp.unpinPage(frame, true);

    // 3. Check if there's space
    if (LeafNode.hasSpace(frame.data, key.len, value.len)) {
        // 4. Insert directly
        LeafNode.insert(frame.data, key, value, self.comparator);
    } else {
        // 5. Need to split
        try self.splitLeafAndInsert(frame, key, value);
    }
}
```

### Splitting a Leaf

When a leaf is full:

```
Before split:
┌─────────────────────────────────────┐
│  a:1  b:2  c:3  d:4  e:5  [FULL]   │
└─────────────────────────────────────┘
                 │
                 │ Insert "f:6" - no room!
                 ▼
After split:
┌──────────────────────┐    ┌──────────────────────┐
│  a:1  b:2  c:3       │◄──►│  d:4  e:5  f:6       │
└──────────────────────┘    └──────────────────────┘
         │                            │
         └───────────┬────────────────┘
                     │
                     ▼
              "d" promoted to parent
```

```zig
fn splitLeaf(self: *Self, frame: *BufferFrame, key: []const u8, value: []const u8) !void {
    // 1. Allocate new page
    const new_page = try self.pm.allocatePage();
    const new_frame = try self.bp.fetchPage(new_page, .exclusive);

    // 2. Find split point (middle)
    const entry_count = LeafNode.getCount(frame.data);
    const split_point = entry_count / 2;

    // 3. Move upper half to new page
    LeafNode.moveEntries(frame.data, new_frame.data, split_point, entry_count);

    // 4. Insert new key into appropriate page
    if (compare(key, split_key) < 0) {
        LeafNode.insert(frame.data, key, value);
    } else {
        LeafNode.insert(new_frame.data, key, value);
    }

    // 5. Update sibling pointers
    LeafNode.setNext(frame.data, new_page);
    LeafNode.setPrev(new_frame.data, frame.page_id);

    // 6. Get separator key and insert into parent
    const separator = LeafNode.getFirstKey(new_frame.data);
    try self.insertIntoParent(frame.page_id, separator, new_page);
}
```

### Growing the Tree

When the root splits, we create a new root:

```
Before:
          ┌─────────────┐
          │  Root (full)│
          └─────────────┘

After root split:
          ┌─────────────┐
          │  New Root   │  ◄── New level!
          │    [50]     │
          └──────┬──────┘
                ╱ ╲
    ┌──────────┘   └──────────┐
    ▼                         ▼
┌─────────────┐      ┌─────────────┐
│  Old Root   │      │  New Page   │
│  (left)     │      │  (right)    │
└─────────────┘      └─────────────┘
```

The tree grows at the top, not the bottom. All leaves stay at the same depth.

## Range Scan

Thanks to linked leaves, range scans are efficient:

```zig
pub fn range(self: *Self, start: ?[]const u8, end: ?[]const u8) !Iterator {
    // 1. Find starting leaf
    const start_page = if (start) |k|
        try self.findLeaf(k)
    else
        try self.findLeftmostLeaf();

    return Iterator{
        .btree = self,
        .current_page = start_page,
        .current_slot = 0,
        .end_key = end,
    };
}
```

**Iterator walks the leaf chain:**

```
Start key: "dog"     End key: "hamster"

   ┌─────────────────────────────────────┐
   │           Internal nodes            │
   └─────────────────────────────────────┘
                     │
                     ▼
┌─────────────┐   ┌─────────────┐   ┌─────────────┐
│ cat│cow│    │──►│ dog│elk│fox│──►│goat│ham│    │
└─────────────┘   └─────────────┘   └─────────────┘
                    ▲                   ▲
                    │ start             │ end
                    └───────────────────┘
                          scan range
```

No need to traverse internal nodes for each entry - just follow the links.

## Concurrency

We use **latch crabbing** (simplified):

```
Lookup (read-only):
    1. Latch root (shared)
    2. Latch child (shared)
    3. Unlatch root
    4. Latch grandchild (shared)
    5. Unlatch child
    ... continue to leaf

Insert (may modify):
    1. Latch root (exclusive)
    2. If child is safe (won't split), latch child and unlatch root
    3. Continue down
```

A "safe" node is one with enough space that it won't split (for insert) or won't underflow (for delete).

## The BTree Struct

```zig
pub const BTree = struct {
    bp: *BufferPool,           // Page cache
    root_page: PageId,         // Current root
    comparator: KeyComparator, // For ordering
    page_size: u32,            // Usually 4096
    allocator: Allocator,

    // ...methods
};
```

This is just ~40 bytes of metadata. The actual data lives in pages on disk.

## Key Design Decisions

### Variable-Length Keys and Values

We store the actual bytes, not fixed-size slots. This supports:
- Short keys efficiently (no wasted space)
- Long keys (up to page size)
- Any binary data

### Separator Keys in Internal Nodes

Internal nodes store separator keys, not full key-value pairs. This maximizes fanout (keys per internal node).

### No Deletion (Yet)

Our implementation doesn't handle deletes with rebalancing. For a knowledge graph database, we can use soft deletes (mark as deleted) and periodic compaction.

### Page-Based, Not Block-Based

Each node is exactly one page. No spanning, no compression across pages. Simple and predictable.

## Example: Full Insert Sequence

Insert "zebra" into a tree:

```
Step 1: Start at root
┌─────────────────────────────────────────────┐
│  Root (Internal)                            │
│  [lion]           [rabbit]                  │
│    ↓                 ↓                      │
│  page 2           page 3                    │→ page 4 (rightmost)
└─────────────────────────────────────────────┘
"zebra" > "rabbit" → go right to page 4

Step 2: Descend to leaf (page 4)
┌─────────────────────────────────────────────┐
│  Leaf (Page 4)                              │
│  snake:v1  tiger:v2  wolf:v3                │
│  [has space for zebra]                      │
└─────────────────────────────────────────────┘

Step 3: Insert into leaf
┌─────────────────────────────────────────────┐
│  Leaf (Page 4)                              │
│  snake:v1  tiger:v2  wolf:v3  zebra:v4     │
└─────────────────────────────────────────────┘
Done! No splits needed.
```

## Performance Characteristics

| Operation | Average Case | Worst Case |
|-----------|--------------|------------|
| Lookup    | O(log N)     | O(log N)   |
| Insert    | O(log N)     | O(log N)   |
| Range(k)  | O(log N + k) | O(log N + k) |

Where N is the number of entries and k is the number of entries in range.

Disk I/O per operation: ~3-4 reads for trees up to billions of entries.
