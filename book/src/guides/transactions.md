# Transactions and Durability

LatticeDB provides ACID transactions with snapshot isolation.

## Transaction Model

- **Read transactions** see a consistent snapshot of the database. Multiple read transactions can run concurrently.
- **Write transactions** can modify the database. Only one write transaction can commit at a time (single-writer model).
- **Snapshot isolation** — each transaction sees the database as it was when the transaction began. Other transactions' uncommitted changes are invisible.

## Using Transactions

### Python

```python
# Read transaction
with db.read() as txn:
    node = txn.get_node(node_id)
    edges = txn.get_outgoing_edges(node_id)
    # Transaction automatically completes when context exits

# Write transaction
with db.write() as txn:
    node = txn.create_node(labels=["Person"], properties={"name": "Alice"})
    txn.commit()
    # If commit() is not called, the transaction is rolled back
```

### TypeScript

```typescript
// Read transaction
await db.read(async (txn) => {
  const node = await txn.getNode(nodeId);
  const edges = await txn.getOutgoingEdges(nodeId);
});

// Write transaction
await db.write(async (txn) => {
  const node = await txn.createNode({
    labels: ["Person"],
    properties: { name: "Alice" },
  });
  // Transaction auto-commits on success, rolls back on error
});
```

### C

```c
// Begin a read transaction
lattice_txn* txn;
lattice_begin(db, LATTICE_TXN_READ_ONLY, &txn);
// ... read operations ...
lattice_commit(txn);

// Begin a write transaction
lattice_begin(db, LATTICE_TXN_READ_WRITE, &txn);
// ... write operations ...
lattice_commit(txn);  // or lattice_rollback(txn);
```

## Queries and Transactions

`db.query()` automatically creates the appropriate transaction:

- Read queries (`MATCH ... RETURN`) use a read transaction
- Write queries (`CREATE`, `SET`, `DELETE`) use a write transaction

```python
# Implicit read transaction
results = db.query("MATCH (n:Person) RETURN n.name")

# Implicit write transaction
db.query("CREATE (n:Person {name: 'Alice'})")
```

## Durability

LatticeDB uses a write-ahead log (WAL) for crash recovery:

1. Changes are written to the WAL before being applied to data pages
2. Commit means the WAL record is on disk (a fast sequential write)
3. Data pages are written lazily during checkpointing
4. On crash, the WAL is replayed to recover committed transactions

This means committed data survives process crashes and power failures.

## Concurrency

- Readers never block writers
- Writers never block readers
- Multiple readers can execute simultaneously
- Write transactions are serialized at commit time

This makes LatticeDB well-suited for read-heavy workloads typical of RAG and search applications.
