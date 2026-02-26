# Graph Storage

## The Property Graph Model

Lattice implements a **Labeled Property Graph** - the same model used by Neo4j, Amazon Neptune, and other graph databases. It consists of:

```
Node:
  - id: unique identifier (u64)
  - labels: set of strings (e.g., "Person", "Employee")
  - properties: key-value pairs (e.g., name: "Alice", age: 30)

Edge:
  - source: node id
  - target: node id
  - type: string (e.g., "KNOWS", "WORKS_AT")
  - properties: key-value pairs (e.g., since: 2020)
```

This model is expressive enough to represent almost any domain while remaining simple to query and traverse.

## The Storage Challenge

How do you store a graph in a B+Tree (which is fundamentally a key-value store)?

The key insight: **decompose the graph into multiple B+Trees, each optimized for a specific access pattern.**

```
┌─────────────────────────────────────────────────────────────────┐
│                      Graph Storage Layer                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐           │
│  │   SYMBOLS    │  │    NODES     │  │    EDGES     │           │
│  │   B+Tree     │  │   B+Tree     │  │   B+Tree     │           │
│  │              │  │              │  │              │           │
│  │ string → id  │  │ node_id →    │  │ composite    │           │
│  │ id → string  │  │   NodeData   │  │ key → data   │           │
│  └──────────────┘  └──────────────┘  └──────────────┘           │
│                                                                  │
│  ┌──────────────┐                                               │
│  │ LABEL_INDEX  │                                               │
│  │   B+Tree     │                                               │
│  │              │                                               │
│  │ (label,node) │                                               │
│  │    → ∅       │                                               │
│  └──────────────┘                                               │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## String Interning (Symbol Table)

Graphs have lots of repeated strings: label names, property keys, edge types. Storing "Person" thousands of times wastes space. Instead, we **intern** strings.

```
String Interning:

  "Person"  ──────►  1000
  "Employee" ─────►  1001
  "name"    ──────►  1002
  "KNOWS"   ──────►  1003

  Now instead of storing "Person" (6 bytes),
  we store 1000 (2 bytes)
```

The Symbol Table uses two B+Trees:

```
SYMBOLS (forward):           SYMBOLS_REVERSE:
  "Person"   → 1000           1000 → "Person"
  "Employee" → 1001           1001 → "Employee"
  "name"     → 1002           1002 → "name"
  "KNOWS"    → 1003           1003 → "KNOWS"
```

Symbol IDs are u16 (0-65535):
- 0: Reserved (null)
- 1-999: Reserved for system use
- 1000-65535: User-defined symbols

### API

```zig
var symbols = SymbolTable.init(allocator, &forward_tree, &reverse_tree);

// Intern a string (creates if not exists, returns existing if present)
const person_id = try symbols.intern("Person");  // 1000

// Lookup without creating
const id = try symbols.lookup("Person");  // 1000
// or
try symbols.lookup("Unknown");  // SymbolError.NotFound

// Resolve ID back to string
const name = try symbols.resolve(person_id);  // "Person"
defer symbols.freeString(name);  // Must free allocated string
```

## Node Storage

Nodes are stored in a B+Tree with simple u64 keys:

```
NODES B+Tree:
  Key: node_id (u64, little-endian)
  Value: NodeData (serialized)
```

### NodeData Format

```
┌────────────────────────────────────────────────────────────┐
│ num_labels: u16                                            │
│ labels: [symbol_id: u16] × num_labels                     │
│ num_properties: u16                                        │
│ properties: [PropertyEntry] × num_properties              │
└────────────────────────────────────────────────────────────┘

PropertyEntry:
┌────────────────────────────────────────────────────────────┐
│ key_id: u16 (interned string)                              │
│ value_type: u8                                             │
│ value_data: variable                                       │
└────────────────────────────────────────────────────────────┘

Value Types:
  0 = Null
  1 = Bool (1 byte: 0 or 1)
  2 = Int64 (8 bytes, little-endian)
  3 = Float64 (8 bytes, IEEE 754)
  4 = String (u32 length + bytes)
  5 = Bytes (u32 length + bytes)
  6 = List (TODO)
  7 = Map (TODO)
```

### API

```zig
var store = NodeStore.init(allocator, &nodes_tree);

// Create a node
const labels = [_]SymbolId{ person_id, employee_id };
const properties = [_]Property{
    .{ .key_id = name_id, .value = .{ .string_val = "Alice" } },
    .{ .key_id = age_id, .value = .{ .int_val = 30 } },
};
const node_id = try store.create(&labels, &properties);

// Get a node
var node = try store.get(node_id);
defer node.deinit(allocator);

// Check existence
if (store.exists(node_id)) { ... }

// Update a node
try store.update(node_id, &new_labels, &new_properties);

// Delete a node
try store.delete(node_id);
```

## Edge Storage

Edges are more complex because we need efficient traversal in **both directions**:
- "Find all people Alice knows" (outgoing)
- "Find all people who know Alice" (incoming)

### The Double-Write Pattern

For edge `(Alice)-[:KNOWS]->(Bob)`, we store **two entries**:

```
Entry 1 (Outgoing from Alice):
  Key: (Alice, OUTGOING, KNOWS, Bob)
  Value: edge properties

Entry 2 (Incoming to Bob):
  Key: (Bob, INCOMING, KNOWS, Alice)
  Value: edge properties (same data)
```

This doubles write cost but enables O(1) traversal in either direction.

### Key Format

```
Edge Key (19 bytes, big-endian for lexicographic ordering):
┌──────────────────────────────────────────────────────────┐
│ source_id: u64    │ direction: u8 │ type_id: u16 │ target_id: u64 │
│   (8 bytes)       │   (1 byte)    │  (2 bytes)   │   (8 bytes)    │
└──────────────────────────────────────────────────────────┘

Direction:
  0 = Outgoing (source → target)
  1 = Incoming (target ← source)
```

Big-endian encoding ensures keys sort correctly for range scans:
- All edges from node X are contiguous
- Within that, all outgoing edges are together
- Within that, edges of same type are together

### Why This Key Order?

The key `(source, direction, type, target)` is optimized for common queries:

```
Query: "All outgoing edges from Alice"
  Scan: (Alice, 0, *, *)
  Keys are contiguous!

Query: "All KNOWS edges from Alice"
  Scan: (Alice, 0, KNOWS, *)
  Even more specific prefix!

Query: "Does Alice know Bob?"
  Point lookup: (Alice, 0, KNOWS, Bob)
  Direct key access!
```

### API

```zig
var store = EdgeStore.init(allocator, &edges_tree);

// Create an edge
const properties = [_]Property{
    .{ .key_id = since_id, .value = .{ .int_val = 2020 } },
};
try store.create(alice_id, bob_id, knows_id, &properties);

// Get an edge
var edge = try store.get(alice_id, bob_id, knows_id);
defer edge.deinit(allocator);

// Check existence
if (store.exists(alice_id, bob_id, knows_id)) { ... }

// Delete an edge (removes both outgoing and incoming entries)
try store.delete(alice_id, bob_id, knows_id);

// Iterate all outgoing edges from a node
var iter = try store.getOutgoing(alice_id);
defer iter.deinit();
while (try iter.next()) |edge| {
    defer edge.deinit(allocator);
    // Process edge...
}

// Iterate incoming edges
var incoming = try store.getIncoming(bob_id);
defer incoming.deinit();

// Filter by edge type
var knows_edges = try store.getOutgoingByType(alice_id, knows_id);
defer knows_edges.deinit();

// Count edges without allocating
const out_count = try store.countOutgoing(alice_id);
const in_count = try store.countIncoming(bob_id);
```

## Label Index

For queries like `MATCH (n:Person)`, we need to find all nodes with a given label efficiently. The Label Index provides this.

```
LABEL_INDEX B+Tree:
  Key: (label_id: u16, node_id: u64) - big-endian
  Value: empty (existence only)
```

### How It Works

```
When creating node Alice with labels [Person, Employee]:
  Insert: (Person, Alice) → ∅
  Insert: (Employee, Alice) → ∅

Query "all Person nodes":
  Range scan: (Person, 0) to (Person, MAX)
  Returns: Alice, Bob, Carol, ...
```

### API

```zig
var index = LabelIndex.init(allocator, &label_tree);

// Add labels when creating a node
try index.addLabels(&[_]SymbolId{ person_id, employee_id }, node_id);

// Check if node has a label
if (index.hasLabel(person_id, node_id)) { ... }

// Remove a label from a node
try index.remove(person_id, node_id);

// Get all nodes with a label (allocates result slice)
const person_nodes = try index.getNodesByLabel(person_id);
defer allocator.free(person_nodes);
for (person_nodes) |node_id| {
    // Process each Person node...
}

// Lazy iteration (memory-efficient for large result sets)
var iter = try index.iterNodesByLabel(person_id);
defer iter.deinit();
while (try iter.next()) |node_id| {
    // Process one node at a time...
}

// Count nodes without allocating
const person_count = try index.countNodesByLabel(person_id);
```

## Putting It Together

A complete graph operation involves multiple B+Trees:

```
Creating node Alice:Person with name="Alice":

1. Symbol Table:
   - intern("Person") → 1000
   - intern("name") → 1001

2. Node Store:
   - Allocate node_id = 1
   - Serialize: [1 label: 1000][1 prop: 1001="Alice"]
   - Insert: 1 → serialized_data

3. Label Index:
   - Insert: (1000, 1) → ∅


Creating edge (Alice)-[:KNOWS]->(Bob):

1. Symbol Table:
   - intern("KNOWS") → 1002

2. Edge Store:
   - Insert: (1, OUT, 1002, 2) → properties
   - Insert: (2, IN, 1002, 1) → properties
```

## Performance Characteristics

| Operation | Complexity | Notes |
|-----------|------------|-------|
| Create node | O(log n) | One B+Tree insert + label index inserts |
| Get node | O(log n) | Single B+Tree lookup |
| Delete node | O(log n) | One B+Tree delete |
| Create edge | O(log n) | Two B+Tree inserts (double-write) |
| Get edge | O(log n) | Single B+Tree lookup |
| Delete edge | O(log n) | Two B+Tree deletes (double-delete) |
| Check label | O(log n) | Single B+Tree lookup |
| Remove label | O(log n) | Single B+Tree delete |
| All nodes with label | O(log n + k) | Range scan, k = result count |
| All edges from node | O(log n + k) | Range scan, k = edge count |

Where n = total items in the respective B+Tree.

## Current Limitations

1. **No MVCC**: All operations see latest data (no snapshot isolation yet)
2. **Property updates overwrite**: No partial property updates
3. **No underflow handling**: Deleted entries leave wasted space until compaction

## Future Enhancements

1. **Property indexes**: Secondary indexes on property values
2. **Edge type index**: Fast lookup by edge type across all nodes
3. **MVCC integration**: Transaction-aware reads and writes
4. **B+Tree underflow handling**: Merge/redistribute pages after deletions

## Summary

| Component | B+Tree Key | Purpose |
|-----------|------------|---------|
| Symbol Table (forward) | string | String → ID mapping |
| Symbol Table (reverse) | symbol_id | ID → String mapping |
| Node Store | node_id | Node data storage |
| Edge Store | (src, dir, type, tgt) | Edge data + traversal |
| Label Index | (label_id, node_id) | Label-based queries |

The graph storage layer transforms a B+Tree (a key-value store) into a full property graph database through careful key design and the double-write pattern for edges.
