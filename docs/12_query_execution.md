# Query Execution

## Overview

Lattice executes Cypher queries using the **Volcano iterator model**—a pull-based execution engine where operators form a tree and data flows upward one row at a time. This design enables lazy evaluation, memory efficiency, and composable query plans.

```
┌─────────────────────────────────────────────────────────────────┐
│                    Query Execution Pipeline                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  "MATCH (p:Person) WHERE p.age > 30 RETURN p.name LIMIT 10"     │
│                              │                                   │
│                              ▼                                   │
│                    ┌─────────────────┐                          │
│                    │      Lexer      │  Tokens                  │
│                    └────────┬────────┘                          │
│                              ▼                                   │
│                    ┌─────────────────┐                          │
│                    │     Parser      │  AST                     │
│                    └────────┬────────┘                          │
│                              ▼                                   │
│                    ┌─────────────────┐                          │
│                    │    Semantic     │  Validated AST           │
│                    │    Analyzer     │  + Variable bindings     │
│                    └────────┬────────┘                          │
│                              ▼                                   │
│                    ┌─────────────────┐                          │
│                    │     Planner     │  Operator Tree           │
│                    └────────┬────────┘                          │
│                              ▼                                   │
│                    ┌─────────────────┐                          │
│                    │    Executor     │  Result Rows             │
│                    └─────────────────┘                          │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## The Volcano Iterator Model

Every operator implements three methods:

```zig
pub const Operator = struct {
    vtable: *const VTable,
    ptr: *anyopaque,

    pub const VTable = struct {
        open:  fn (*anyopaque, *ExecutionContext) OperatorError!void,
        next:  fn (*anyopaque, *ExecutionContext) OperatorError!?*Row,
        close: fn (*anyopaque, *ExecutionContext) void,
        deinit: fn (*anyopaque, Allocator) void,
    };
};
```

- **open()**: Initialize the operator, acquire resources (iterators, memory)
- **next()**: Return the next row, or `null` if exhausted
- **close()**: Release resources (unpin pages, close iterators)
- **deinit()**: Free the operator itself

### Pull-Based Execution

Data flows **upward** through the operator tree. The root operator "pulls" from its children:

```
        ┌─────────┐
        │  Limit  │  ◄── Executor calls next() here
        └────┬────┘
             │ pulls from
        ┌────┴────┐
        │ Project │
        └────┬────┘
             │ pulls from
        ┌────┴────┐
        │ Filter  │
        └────┬────┘
             │ pulls from
        ┌────┴────┐
        │LabelScan│  ◄── Reads from storage
        └─────────┘
```

When the executor calls `root.next()`:
1. Limit calls Project.next()
2. Project calls Filter.next()
3. Filter calls LabelScan.next()
4. LabelScan reads from B+Tree, returns row
5. Filter evaluates predicate—if false, pulls another row
6. Project evaluates expressions, transforms row
7. Limit checks count, returns row (or null if limit reached)

### Lazy Evaluation

Work happens only when `next()` is called. For `LIMIT 10`, we might scan millions of nodes but only process 10 matching rows. Unused rows are never materialized.

## Rows and Slots

A `Row` is a fixed-size tuple flowing between operators:

```zig
pub const Row = struct {
    slots: [16]SlotValue,      // Variable bindings
    distances: [16]f32,        // Vector search distances
    scores: [16]f32,           // FTS relevance scores
    populated: u16,            // Bitmask of populated slots
};
```

### Slot Values

Each slot holds one of:

```zig
pub const SlotValue = union(enum) {
    empty: void,               // Unset
    node_ref: NodeId,          // Reference to a node (just the ID)
    edge_ref: EdgeId,          // Reference to an edge
    property: PropertyValue,   // Actual value (int, string, bool, etc.)
};
```

### Why Slots Instead of Named Variables?

Performance. Looking up `slots[3]` is O(1). The planner assigns each variable to a numbered slot:

```cypher
MATCH (p:Person)-[r:KNOWS]->(f:Person)
       │          │          │
     slot 0    slot 1     slot 2
```

The execution context maintains the mapping from names to slots for expression evaluation.

### Lazy Materialization

Rows store **references** (NodeId, EdgeId), not full objects. Properties are fetched on-demand during expression evaluation. This avoids loading data that's never accessed.

## Operators

### Scan Operators

**AllNodesScan**: Iterates every node in the database.

```
open():  Create B+Tree iterator (null start key = first entry)
next():  Read entry, extract NodeId from key, set output slot
close(): Release iterator, unpin pages
```

**LabelScan**: Iterates nodes with a specific label.

```
open():  Create LabelIndex iterator for the label
next():  Get next NodeId from index, set output slot
close(): Release iterator
```

Both produce rows with a single slot containing a node reference.

### Filter Operator

Evaluates a predicate and passes through only matching rows.

```zig
fn next(self: *Filter, ctx: *ExecutionContext) !?*Row {
    while (true) {
        const row = try self.input.next(ctx) orelse return null;

        const result = try self.evaluator.evaluate(self.predicate, row, ctx);
        if (result.isTruthy()) {
            return row;
        }
        // Predicate failed, try next row
    }
}
```

Supports short-circuit evaluation for `AND`/`OR`.

### Project Operator

Transforms rows by evaluating expressions for each output column.

```cypher
RETURN p.name, p.age + 1, "literal"
       │        │          │
    expr[0]  expr[1]    expr[2]
```

```zig
fn next(self: *Project, ctx: *ExecutionContext) !?*Row {
    const input_row = try self.input.next(ctx) orelse return null;

    self.output_row.clear();
    for (self.items) |item| {
        const value = try self.evaluator.evaluate(item.expr, input_row, ctx);
        self.output_row.setSlot(item.output_slot, resultToSlotValue(value));
    }
    return self.output_row;
}
```

### Expand Operator

Traverses edges from input nodes. This implements pattern matching like `(a)-[r]->(b)`.

```
State:
    source_slot      = slot containing the source node
    target_slot      = slot to write target node
    edge_slot        = slot to write edge (optional)
    edge_iterator    = current iterator over edges
    current_input    = row being expanded

next():
    loop:
        if edge_iterator.next() returns edge:
            output_row = copy input row
            output_row[target_slot] = edge.target
            output_row[edge_slot] = edge (if requested)
            return output_row

        // Exhausted edges for current input, get next input
        current_input = input.next()
        if null: return null

        source = current_input[source_slot]
        edge_iterator = edgeStore.getOutgoing(source)
```

This is a **one-to-many** operator: one input row can produce multiple output rows.

Supports three directions:
- `outgoing`: `(a)-[r]->(b)` — traverse outgoing edges
- `incoming`: `(a)<-[r]-(b)` — traverse incoming edges
- `both`: `(a)-[r]-(b)` — traverse both directions

### Limit and Skip

**Limit**: Returns at most N rows.

```zig
fn next(self: *Limit, ctx: *ExecutionContext) !?*Row {
    if (self.returned >= self.count) return null;
    const row = try self.input.next(ctx) orelse return null;
    self.returned += 1;
    return row;
}
```

**Skip**: Discards the first N rows.

```zig
fn next(self: *Skip, ctx: *ExecutionContext) !?*Row {
    while (self.skipped < self.count) {
        _ = try self.input.next(ctx) orelse return null;
        self.skipped += 1;
    }
    return try self.input.next(ctx);
}
```

### Sort Operator (Blocking)

Sort must see all input before producing any output—it's a **blocking operator**.

```zig
fn open(self: *Sort, ctx: *ExecutionContext) !void {
    try self.input.open(ctx);

    // Materialize all input rows
    while (try self.input.next(ctx)) |row| {
        try self.rows.append(row.*);
    }

    // Sort in memory
    self.sortRows();
}

fn next(self: *Sort, ctx: *ExecutionContext) !?*Row {
    if (self.index >= self.rows.items.len) return null;
    defer self.index += 1;
    return &self.rows.items[self.index];
}
```

This breaks the streaming model but is necessary for ordering.

### Vector Search Operator

Performs k-NN search using the HNSW index.

```zig
fn open(self: *VectorSearch, ctx: *ExecutionContext) !void {
    // Execute search upfront
    self.results = try self.index.search(self.query_vector, self.k, null);
}

fn next(self: *VectorSearch, ctx: *ExecutionContext) !?*Row {
    while (self.current < self.results.len) {
        const result = self.results[self.current];
        self.current += 1;

        // Apply distance threshold
        if (self.threshold) |t| {
            if (result.distance > t) continue;
        }

        self.row.setSlot(self.output_slot, .{ .node_ref = result.node_id });
        self.row.setDistance(self.output_slot, result.distance);
        return self.row;
    }
    return null;
}
```

The distance is stored in the row for use in expressions or sorting.

### FTS Search Operator

Performs full-text search with BM25 scoring.

```zig
fn open(self: *FtsSearch, ctx: *ExecutionContext) !void {
    self.results = try self.index.search(self.query_text, self.limit);
}

fn next(self: *FtsSearch, ctx: *ExecutionContext) !?*Row {
    // Similar to VectorSearch but with scores instead of distances
    // ...
    self.row.setScore(self.output_slot, result.score);
    return self.row;
}
```

### Mutation Operators

Mutation operators modify the graph structure. Unlike read operators, they have side effects.

**CreateNode**: Creates new nodes with labels and properties.

```zig
fn next(self: *CreateNode, ctx: *ExecutionContext) !?*Row {
    const node_id = try ctx.storage.createNode(self.labels);
    for (self.properties) |prop| {
        const value = try self.evaluator.evaluate(prop.value, null, ctx);
        try ctx.storage.setProperty(node_id, prop.key, value);
    }
    self.output_row.setSlot(self.output_slot, .{ .node_ref = node_id });
    return &self.output_row;
}
```

**DeleteNode**: Removes nodes from the graph. With DETACH, also removes connected edges.

```zig
fn next(self: *DeleteNode, ctx: *ExecutionContext) !?*Row {
    const row = try self.input.next(ctx) orelse return null;
    const node_id = row.slots[self.target_slot].node_ref;

    if (self.detach) {
        try ctx.storage.detachDelete(node_id);
    } else {
        try ctx.storage.deleteNode(node_id);
    }
    return row;  // Pass through for chaining
}
```

**SetProperty**: Updates properties on nodes or edges.

```cypher
MATCH (n:Person {name: "Alice"}) SET n.age = 31, n.city = "NYC"
```

```zig
fn next(self: *SetProperty, ctx: *ExecutionContext) !?*Row {
    const row = try self.input.next(ctx) orelse return null;
    const target = row.slots[self.target_slot];
    const value = try self.evaluator.evaluate(self.value_expr, row, ctx);

    // SET n.prop = NULL removes the property
    if (value == .null_val) {
        try ctx.storage.removeProperty(target, self.property_name);
    } else {
        try ctx.storage.setProperty(target, self.property_name, value);
    }
    return row;
}
```

**SetLabels**: Adds labels to nodes.

```cypher
MATCH (n:Person {name: "Alice"}) SET n:Admin:Verified
```

**RemoveProperty**: Explicitly removes properties.

```cypher
MATCH (n:Person {name: "Alice"}) REMOVE n.city
```

**RemoveLabel**: Removes labels from nodes.

```cypher
MATCH (n:Person {name: "Alice"}) REMOVE n:Verified
```

## Expression Evaluation

The expression evaluator handles predicates and projections.

```zig
pub fn evaluate(expr: *const Expression, row: *const Row, ctx: *ExecutionContext) !EvalResult {
    return switch (expr.*) {
        .literal => |lit| evaluateLiteral(lit),
        .variable => |v| evaluateVariable(v, row, ctx),
        .property_access => |pa| evaluatePropertyAccess(pa, row, ctx),
        .binary => |b| evaluateBinary(b, row, ctx),
        .unary => |u| evaluateUnary(u, row, ctx),
        .function_call => |f| evaluateFunction(f, row, ctx),
        // ...
    };
}
```

### Supported Operations

**Comparison**: `=`, `<>`, `<`, `<=`, `>`, `>=`

**Logical**: `AND`, `OR`, `NOT`, `XOR` (with short-circuit evaluation)

**Arithmetic**: `+`, `-`, `*`, `/`, `%`, `^`

**String**: `CONTAINS`, `STARTS WITH`, `ENDS WITH`

**Null handling**: `IS NULL`, `IS NOT NULL`

**Functions**: `id()`, `coalesce()`, `abs()`, `size()`, `toInteger()`, `toFloat()`

**Vector Search**: `<=>` (vector distance operator)
```cypher
-- k-NN search with distance threshold
MATCH (c:Chunk) WHERE c.embedding <=> $query < 0.5 RETURN c
```

**Full-Text Search**: `@@` (FTS match operator)
```cypher
-- BM25-scored full-text search
MATCH (d:Doc) WHERE d.text @@ $search RETURN d
MATCH (d:Doc) WHERE d.text @@ "neural networks" RETURN d
```

These operators are recognized by the planner and converted to specialized search operators that use the HNSW and FTS indexes directly, rather than scanning all nodes.

### Type Coercion

Numeric operations promote integers to floats when mixed:
```
42 + 3.14  →  45.14 (float)
```

Null propagates through most operations:
```
null + 1   →  null
null = null → true (special case for equality)
```

## The Query Planner

The planner transforms an AST into an operator tree by walking the query clauses.

### Planning Algorithm

```zig
pub fn plan(query: *Query) !Operator {
    var current: ?Operator = null;

    for (query.clauses) |clause| {
        current = switch (clause) {
            .match => try planMatch(clause.match, current),
            .where => try planWhere(clause.where, current),
            .create => try planCreate(clause.create, current),
            .delete => try planDelete(clause.delete, current),
            .set => try planSet(clause.set, current),
            .remove => try planRemove(clause.remove, current),
            .return_ => try planReturn(clause.return_, current),
            .order_by => try planOrderBy(clause.order_by, current),
            .limit => try planLimit(clause.limit, current),
            .skip => try planSkip(clause.skip, current),
        };
    }

    return current.?;
}
```

### Example: Simple Query

```cypher
MATCH (p:Person) WHERE p.age > 30 RETURN p.name LIMIT 10
```

Produces:

```
Limit(count=10)
  └── Project([p.name → slot 0])
        └── Filter(p.age > 30)
              └── LabelScan(label="Person", output=slot 0)
```

### Example: Edge Traversal

```cypher
MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name
```

Produces:

```
Project([a.name → slot 0, b.name → slot 1])
  └── Expand(source=0, target=1, type="KNOWS", dir=outgoing)
        └── LabelScan(label="Person", output=slot 0)
```

### Variable Binding

The planner allocates slots for variables:

| Variable | Slot | Kind |
|----------|------|------|
| a        | 0    | node |
| b        | 1    | node |
| r        | 2    | edge |

These bindings are registered in the ExecutionContext for expression evaluation.

## Execution Context

Runtime state for query execution:

```zig
pub const ExecutionContext = struct {
    allocator: Allocator,
    row_arena: ArenaAllocator,          // Fast row allocation
    parameters: StringHashMap(Value),   // Query parameters ($name)
    variables: StringHashMap(u8),       // Variable → slot mapping
};
```

### Row Arena

Rows are allocated from an arena that's reset between queries. This avoids per-row allocation overhead:

```zig
// During query execution
const row = try ctx.allocRow();  // Fast bump allocation

// After query completes
ctx.resetRowArena();  // Frees all rows at once
```

### Query Parameters

Parameters allow safe value injection:

```cypher
MATCH (p:Person) WHERE p.age > $min_age RETURN p
```

```zig
try ctx.setParameter("min_age", .{ .int_val = 30 });
```

## Operator Composition

Operators are composable—any operator can wrap any other:

```
// Filter wrapping Expand wrapping LabelScan
Filter
  └── Expand
        └── LabelScan

// Limit wrapping Sort wrapping Filter
Limit
  └── Sort
        └── Filter
              └── ...
```

This enables flexible query plans without special-casing combinations.

## Memory Management

### Operator Lifecycle

```
1. Planner creates operators (heap allocated)
2. Executor calls open() on root (cascades down)
3. Executor calls next() repeatedly (rows flow up)
4. Executor calls close() on root (cascades down)
5. Executor calls deinit() on root (frees operators bottom-up)
```

### Iterator Cleanup

Operators holding B+Tree or index iterators **must** release them in `close()`:

```zig
fn close(self: *LabelScan, ctx: *ExecutionContext) void {
    if (self.iterator) |*iter| {
        iter.deinit();  // Unpins pages in buffer pool
        self.iterator = null;
    }
}
```

Failing to do this leaks pinned pages.

## Future Optimizations

The current planner uses simple heuristics. Future improvements could include:

### Predicate Pushdown

Move filters closer to scans:
```
Before: Filter(a.x > 10) → Expand → LabelScan
After:  Expand → Filter(a.x > 10) → LabelScan
```

### Index Selection

Choose between AllNodesScan vs LabelScan vs property index based on selectivity.

### Join Ordering

For multi-pattern queries, order joins by estimated cardinality.

### Vectorized Execution

Process rows in batches instead of one-at-a-time for better cache utilization.

## Database Query API

The `Database.query()` method provides the primary interface for executing Cypher queries. It orchestrates all components of the query pipeline.

### Basic Usage

```zig
// Open database
var db = try Database.open(allocator, "mydb.ltdb", .{ .create = true });
defer db.close();

// Create some data
_ = try db.createNode(&[_][]const u8{"Person"});
_ = try db.createNode(&[_][]const u8{"Person"});

// Execute a query
var result = try db.query("MATCH (n:Person) RETURN n");
defer result.deinit();  // Always clean up results

// Process results
std.debug.print("Found {} rows\n", .{result.rowCount()});
for (result.rows) |row| {
    for (row.values) |val| {
        switch (val) {
            .node_id => |id| std.debug.print("Node: {}\n", .{id}),
            .string_val => |s| std.debug.print("String: {s}\n", .{s}),
            .int_val => |i| std.debug.print("Int: {}\n", .{i}),
            .null_val => std.debug.print("null\n", .{}),
            else => {},
        }
    }
}
```

### Result Types

```zig
/// A single value in a query result
pub const ResultValue = union(enum) {
    null_val: void,      // NULL
    bool_val: bool,      // true/false
    int_val: i64,        // integers
    float_val: f64,      // floating point
    string_val: []const u8,  // strings
    node_id: NodeId,     // node reference
};

/// A row in a query result
pub const ResultRow = struct {
    values: []ResultValue,  // One value per column
};

/// Complete query result
pub const QueryResult = struct {
    columns: [][]const u8,  // Column names from RETURN clause
    rows: []ResultRow,      // Result rows
    allocator: Allocator,

    pub fn deinit(self: *QueryResult) void;  // Free all memory
    pub fn rowCount(self: *const QueryResult) usize;
    pub fn columnCount(self: *const QueryResult) usize;
};
```

### Error Handling

The `query()` method returns `QueryError` on failure:

```zig
pub const QueryError = error{
    ParseError,      // Syntax error in Cypher
    SemanticError,   // Undefined variable, type mismatch
    PlanError,       // Query cannot be planned
    ExecutionError,  // Runtime error during execution
    OutOfMemory,
};
```

Example error handling:

```zig
const result = db.query("MATCH (n RETURN n");  // Missing )
if (result) |r| {
    defer r.deinit();
    // process results
} else |err| switch (err) {
    QueryError.ParseError => std.debug.print("Syntax error\n", .{}),
    QueryError.SemanticError => std.debug.print("Semantic error\n", .{}),
    else => std.debug.print("Query failed: {}\n", .{err}),
}
```

### Internal Pipeline

When you call `db.query(cypher)`, the following happens:

```
1. Parser.init(allocator, cypher)
   └── parser.parse() → AST (or ParseError)

2. SemanticAnalyzer.init(allocator)
   └── analyzer.analyze(ast) → AnalysisResult (or SemanticError)

3. QueryPlanner.init(allocator, storage_context)
   └── planner.plan(ast, analysis) → Operator tree (or PlanError)

4. ExecutionContext.init(allocator)
   └── Register variable bindings from planner

5. execute(allocator, root_operator, context)
   └── Pull rows through operator tree → executor.QueryResult

6. convertResult(exec_result, planner)
   └── Transform to user-friendly QueryResult
```

The `StorageContext` connects the planner to database storage:

```zig
const storage_ctx = StorageContext{
    .node_tree = &self.node_tree,       // B+Tree for nodes
    .label_index = &self.label_index,   // Label → NodeId index
    .edge_store = &self.edge_store,     // Edge storage
    .symbol_table = &self.symbol_table, // String interning
};
```

### Memory Management

- **QueryResult owns all memory**: Call `deinit()` when done
- **Strings are copied**: Safe to use after query components are freed
- **Row arena**: Internal rows use arena allocation, reset between queries

## Files

| File | Purpose |
|------|---------|
| `src/storage/database.zig` | Database.query() API, QueryResult types |
| `src/query/executor.zig` | Operator interface, Row, ExecutionContext |
| `src/query/expression.zig` | Expression evaluator |
| `src/query/planner.zig` | AST to operator tree transformation |
| `src/query/operators/scan.zig` | AllNodesScan, LabelScan |
| `src/query/operators/filter.zig` | Filter operator |
| `src/query/operators/project.zig` | Project operator |
| `src/query/operators/expand.zig` | Edge traversal |
| `src/query/operators/vector.zig` | HNSW k-NN search |
| `src/query/operators/fts.zig` | Full-text search |
| `src/query/operators/limit.zig` | Limit, Skip, Sort |
| `src/query/operators/mutation.zig` | CREATE, DELETE, SET, REMOVE |
