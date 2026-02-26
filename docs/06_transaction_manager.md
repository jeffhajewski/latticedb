# Transaction Manager

## What is a Transaction?

A transaction is a **logical unit of work** - a group of operations that should either all succeed or all fail together.

```
Without transactions:                With transactions:

1. Debit Alice $100    ✓            BEGIN
2. ─── CRASH ───                    1. Debit Alice $100
3. Credit Bob $100     ✗            2. Credit Bob $100
                                    COMMIT
Result: Alice lost $100,
Bob got nothing                     Result: Either both happen,
                                    or neither happens
```

## The ACID Properties

Transactions guarantee four properties:

| Property | Meaning | How We Achieve It |
|----------|---------|-------------------|
| **A**tomicity | All or nothing | WAL + rollback |
| **C**onsistency | Valid state to valid state | Application logic |
| **I**solation | Transactions don't interfere | Timestamps + MVCC |
| **D**urability | Committed = permanent | WAL + fsync |

## The Core Data Structures

### Transaction (User-Facing Handle)

This is what the application sees:

```zig
pub const Transaction = struct {
    id: u64,              // Unique identifier (1, 2, 3, ...)
    state: TxnState,      // active, committed, or aborted
    mode: TxnMode,        // read_only or read_write
    isolation: IsolationLevel,  // snapshot, read_committed, serializable
    start_ts: u64,        // When transaction started (for visibility)
    commit_ts: u64,       // When committed (0 until commit)
};
```

### TxnEntry (Internal State)

The manager keeps more detailed state internally:

```zig
const TxnEntry = struct {
    txn: Transaction,           // The user-facing handle
    last_lsn: u64,              // LSN of most recent operation
    begin_lsn: u64,             // LSN of TXN_BEGIN record
    savepoints: ArrayList,      // Stack of savepoints
    undo_log: ArrayList,        // Operations to reverse on abort
};
```

### TxnManager (The Coordinator)

```zig
pub const TxnManager = struct {
    allocator: Allocator,
    wal: *WalManager,                        // For durability
    active_txns: HashMap(u64, TxnEntry),     // All running transactions
    next_txn_id: u64,                        // Counter for IDs
    current_ts: u64,                         // Global timestamp clock
    committed_count: u64,                    // Statistics
    aborted_count: u64,
    mutex: Mutex,                            // Thread safety
};
```

## Transaction Lifecycle

### 1. BEGIN

When you start a transaction:

```
Application                    TxnManager                         WAL
    │                              │                               │
    │  begin(read_write, snapshot) │                               │
    ├─────────────────────────────►│                               │
    │                              │                               │
    │                              │  1. Lock mutex                │
    │                              │  2. Assign txn_id = 1         │
    │                              │  3. Assign start_ts = 1       │
    │                              │  4. appendRecord(TXN_BEGIN)   │
    │                              ├──────────────────────────────►│
    │                              │                     lsn = 1   │
    │                              │◄──────────────────────────────┤
    │                              │                               │
    │                              │  5. Create TxnEntry           │
    │                              │     last_lsn = 1              │
    │                              │  6. Store in active_txns      │
    │                              │  7. Unlock mutex              │
    │                              │                               │
    │   Transaction { id=1, ... }  │                               │
    │◄─────────────────────────────┤                               │
```

The `start_ts` is crucial for isolation - it determines what data this transaction can "see".

### 2. Operations

Each modification goes through `logOperation`:

```
Application                    TxnManager                         WAL
    │                              │                               │
    │  logOperation(INSERT, data)  │                               │
    ├─────────────────────────────►│                               │
    │                              │                               │
    │                              │  1. Check txn.canWrite()      │
    │                              │  2. Get TxnEntry              │
    │                              │  3. appendRecord(INSERT,      │
    │                              │       txn_id=1,               │
    │                              │       prev_lsn=1,  ◄── last_lsn
    │                              │       payload=data)           │
    │                              ├──────────────────────────────►│
    │                              │                     lsn = 2   │
    │                              │◄──────────────────────────────┤
    │                              │                               │
    │                              │  4. Update last_lsn = 2       │
    │                              │                               │
    │              lsn = 2         │                               │
    │◄─────────────────────────────┤                               │
```

Notice how `prev_lsn` points to the previous operation. This builds a **backward chain**.

### 3. COMMIT

Commit makes everything permanent:

```
Application                    TxnManager                         WAL           Disk
    │                              │                               │              │
    │  commit(&txn)                │                               │              │
    ├─────────────────────────────►│                               │              │
    │                              │                               │              │
    │                              │  1. Check txn.state == active │              │
    │                              │  2. appendRecord(TXN_COMMIT,  │              │
    │                              │       prev_lsn=last_lsn)      │              │
    │                              ├──────────────────────────────►│              │
    │                              │                               │              │
    │                              │  3. wal.sync()                │              │
    │                              ├──────────────────────────────►│   fsync()   │
    │                              │                               ├─────────────►│
    │                              │                               │   durable!  │
    │                              │                               │◄─────────────┤
    │                              │◄──────────────────────────────┤              │
    │                              │                               │              │
    │                              │  4. Assign commit_ts          │              │
    │                              │  5. Set txn.state = committed │              │
    │                              │  6. Remove from active_txns   │              │
    │                              │  7. Increment committed_count │              │
    │                              │                               │              │
    │              OK              │                               │              │
    │◄─────────────────────────────┤                               │              │
```

**The critical point**: We only return success to the application AFTER `fsync()` completes. This is the durability guarantee.

### 4. ABORT

Abort discards everything:

```
Application                    TxnManager                         WAL
    │                              │                               │
    │  abort(&txn)                 │                               │
    ├─────────────────────────────►│                               │
    │                              │                               │
    │                              │  1. appendRecord(TXN_ABORT)   │
    │                              ├──────────────────────────────►│
    │                              │                               │
    │                              │  2. wal.sync()                │
    │                              │                               │
    │                              │  3. Set txn.state = aborted   │
    │                              │  4. Clean up TxnEntry         │
    │                              │  5. Remove from active_txns   │
    │                              │                               │
    │              OK              │                               │
    │◄─────────────────────────────┤                               │
```

We log TXN_ABORT so that during crash recovery, we know this transaction was intentionally aborted.

## The prev_lsn Chain

Every record points back to the previous record **from the same transaction**. This creates a linked list through the WAL:

```
┌─────────────────────────────────────────────────────────────────┐
│                         WAL                                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  LSN 1: TXN_BEGIN  (txn=1, prev_lsn=0)  ◄─────────────┐         │
│                                                        │         │
│  LSN 2: INSERT     (txn=1, prev_lsn=1)  ──────────────┘         │
│                              ▲                                   │
│                              │                                   │
│  LSN 3: TXN_BEGIN  (txn=2, prev_lsn=0)  ◄───────┐               │
│                                                  │               │
│  LSN 4: UPDATE     (txn=1, prev_lsn=2)  ────────┼───┐           │
│                              ▲                   │   │           │
│                              │                   │   │           │
│  LSN 5: DELETE     (txn=2, prev_lsn=3)  ────────┘   │           │
│                                                      │           │
│  LSN 6: TXN_COMMIT (txn=1, prev_lsn=4)  ─────────────┘          │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘

Transaction 1's chain: 6 → 4 → 2 → 1
Transaction 2's chain: 5 → 3
```

**Why is this useful?**

1. **Rollback**: To abort transaction 1, follow 6→4→2→1, undoing each operation
2. **Recovery**: After crash, find uncommitted transactions by following chains
3. **No scanning**: Don't need to scan entire WAL to find a transaction's operations

## Savepoints: Partial Rollback

Sometimes you want to undo part of a transaction, not all of it:

```
BEGIN;
    INSERT user Alice;           -- LSN 2
    SAVEPOINT before_orders;     -- Remember this point
    INSERT order #1;             -- LSN 4
    INSERT order #2;             -- LSN 5
    -- Oops, orders were wrong
    ROLLBACK TO before_orders;   -- Undo LSN 4, 5
    INSERT order #3;             -- LSN 7
COMMIT;
```

### How Savepoints Work

```
                    Savepoint Stack                 Undo Log
                    ┌─────────────────┐            ┌─────────────┐
                    │ "before_orders" │            │ INSERT #1   │ ← position 1
tm.savepoint()  ──► │   lsn: 3        │            │ INSERT #2   │ ← position 2
                    │   undo_pos: 0   │──────────► └─────────────┘
                    └─────────────────┘                  ▲
                                                         │
                                                    truncate here
                                                    on rollback
```

When we rollback to savepoint:
1. Find the savepoint by name
2. Log `SAVEPOINT_ROLLBACK` to WAL
3. Truncate undo_log to `undo_position`
4. Remove savepoints created after this one

## Timestamps and Isolation

Every transaction gets two timestamps:

```
start_ts:  Assigned at BEGIN - determines what data is visible
commit_ts: Assigned at COMMIT - marks when changes become visible to others
```

### Snapshot Isolation Example

```
Timeline:
─────────────────────────────────────────────────────────────────►
     │           │           │           │           │
   ts=1        ts=2        ts=3        ts=4        ts=5
     │           │           │           │           │
   Txn A       Txn A       Txn B       Txn A       Txn B
   BEGIN      INSERT      BEGIN      COMMIT       reads
 start_ts=1    x=100     start_ts=3  commit_ts=4    x
```

**Question**: What does Txn B see when it reads x?

With **snapshot isolation**: Txn B started at ts=3, before Txn A committed at ts=4. So Txn B does NOT see x=100. It sees whatever x was before Txn A.

This is why we track `start_ts` and `commit_ts` - they determine visibility.

## Read-Only Transactions

Read-only transactions are special:

```zig
var txn = try tm.begin(.read_only, .snapshot);

// This fails:
const result = tm.logOperation(&txn, .insert, "data");
// Error: TxnError.ReadOnly
```

Why have read-only transactions?
1. **No WAL writes** - faster, no disk I/O for reads
2. **No locks needed** - can always proceed
3. **Never abort due to conflicts** - just reads a snapshot
4. **Helps garbage collection** - we know this txn won't modify old versions

## Thread Safety

The TxnManager uses a mutex to protect shared state:

```zig
pub fn begin(self: *Self, ...) TxnError!Transaction {
    self.mutex.lock();         // ← Only one thread at a time
    defer self.mutex.unlock(); // ← Released when function exits

    // Safe to modify next_txn_id, active_txns, etc.
    ...
}
```

This is a simple approach. The mutex is held briefly (microseconds), and the WAL I/O dominates latency anyway.

## Statistics

The manager tracks statistics for monitoring:

```zig
const stats = tm.getStats();

stats.active_count     // Currently running transactions
stats.committed_count  // Total successful commits
stats.aborted_count    // Total aborts
stats.oldest_active_id // Oldest running transaction
stats.current_ts       // Current timestamp
```

`oldest_active_id` is important for garbage collection - we can't clean up any data that this transaction might still need to see.

## API Summary

```zig
var tm = TxnManager.init(allocator, &wal);

// Start a transaction
var txn = try tm.begin(.read_write, .snapshot);

// Log operations
_ = try tm.logOperation(&txn, .insert, payload);
_ = try tm.logOperation(&txn, .update, payload);

// Create savepoint
try tm.savepoint(&txn, "before_danger");

// More operations
_ = try tm.logOperation(&txn, .delete, payload);

// Rollback to savepoint if needed
try tm.rollbackToSavepoint(&txn, "before_danger");

// Commit or abort
try tm.commit(&txn);  // or tm.abort(&txn)
```

## Integration

```
┌─────────────────────────────────────────────────────────────────┐
│                        Application                               │
└───────────────────────────────┬─────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                      TxnManager                                  │
│  • Manages transaction lifecycle                                 │
│  • Maintains prev_lsn chains                                     │
│  • Tracks active transactions                                    │
└───────────────────────────────┬─────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                       WalManager                                 │
│  • Durability through logging                                    │
│  • fsync on commit                                               │
└───────────────────────────────┬─────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                    BufferPool + BTree                            │
│  • Actual data storage                                           │
│  • Page management                                               │
│  • (Will check txn visibility for MVCC)                          │
└─────────────────────────────────────────────────────────────────┘
```

The Transaction Manager is the **coordinator** - it doesn't store data itself, but ensures that all operations follow ACID rules by orchestrating the WAL, tracking state, and enforcing invariants.
