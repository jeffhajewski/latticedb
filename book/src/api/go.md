# Go

The Go binding wraps the C API through cgo. Everything is a normal Go value:
properties are `any`, errors are ordinary errors you can compare with
`errors.Is`, and transactions are closures so you cannot forget to finish one.

## Installing

```bash
go get github.com/jeffhajewski/latticedb/bindings/go
```

Because it uses cgo, building needs the LatticeDB shared library available to
the linker. Installing a release package puts it somewhere pkg-config can find,
which is what the default build expects. If you are working inside a checkout of
the repository, build the library first and use the `repolocal` build tag, which
points the linker at `zig-out/lib`:

```bash
zig build
go run -tags repolocal .
```

## Opening a database

```go
import latticedb "github.com/jeffhajewski/latticedb/bindings/go"

db, err := latticedb.Open("graph.lattice", latticedb.OpenOptions{Create: true})
if err != nil {
    log.Fatal(err)
}
defer db.Close()
```

`OpenOptions` covers the things you have to decide when the file is created:

| Field | What it does |
|-------|--------------|
| `Create` | Create the file if it is not there |
| `ReadOnly` | Open without the ability to write |
| `CacheSizeMB` | How much memory to keep pages in |
| `PageSize` | Page size in bytes |
| `EnableVectors` | Turn on the vector index |
| `VectorDimensions` | How many numbers are in each vector |
| `EnableAdjacencyCache` | Keep an in-memory map of connections to speed up traversal |
| `DisableWAL` | Open without write-ahead logging |
| `DisableLock` | Open without taking a lock on the file |

```go
db, err := latticedb.Open(":memory:", latticedb.OpenOptions{})
```

### In-memory databases

Pass `:memory:` as the path and the database has no files behind it. Nothing is
written to disk and nothing survives closing it, which suits a scratch database, a
test, or one you pulled out of object storage and will hand back as bytes.

It behaves like any other database — transactions, the write-ahead log, and
serialization all work. The differences are that it disappears when closed, and
that nothing locks it, since no other process can reach it.

A database can only be open in one process at a time. Opening takes a lock on the
file, so a second process gets an error whose `Code` is `ErrorDatabaseLocked`
rather than quietly corrupting your data. A read-only handle shares the lock with
other readers, but is still refused while a writer holds the database, because
what it would read is a stale file that a checkpoint may be rewriting underneath
it. Set `DisableLock` only on filesystems where locking does not work; it does not
make concurrent access safe, it removes the thing that was going to tell you it
was not.

Three of these need a word of explanation. `DisableWAL` and `DisableLock` are
phrased as negatives because a Go `bool` cannot tell "the caller left this alone"
apart from "the caller set it to false", and both of those features default to
on. Set them when you genuinely want them off. There is also an older
`EnableVector` field kept for
compatibility; new code should use `EnableVectors`.

## Writing

`Update` runs a function inside a write transaction. Return `nil` and it
commits; return an error and it rolls back and hands you the error:

```go
err = db.Update(func(tx *latticedb.Tx) error {
    alice, err := tx.CreateNode(latticedb.CreateNodeOptions{
        Labels:     []string{"Person"},
        Properties: map[string]latticedb.Value{"name": "Alice", "email": "alice@example.com"},
    })
    if err != nil {
        return err
    }

    bob, err := tx.CreateNode(latticedb.CreateNodeOptions{
        Labels:     []string{"Person"},
        Properties: map[string]latticedb.Value{"name": "Bob"},
    })
    if err != nil {
        return err
    }

    _, err = tx.CreateEdge(alice.ID, bob.ID, "KNOWS", latticedb.CreateEdgeOptions{
        Properties: map[string]latticedb.Value{"since": int64(2020)},
    })
    return err
})
```

`Value` is an alias for `any`, so you pass Go values straight through. Strings,
`int64`, `float64`, `bool`, `[]byte`, and `[]float32` for vectors all work. Note
`int64` rather than `int`, since the stored type is explicitly 64-bit.

Only one write transaction can be open at a time. A second one fails
immediately with `ErrorLockTimeout` rather than waiting, so if several
goroutines write, they need to take turns. See
[One writer at a time](../guides/transactions.md#one-writer-at-a-time).

## Reading

`View` is the read-only counterpart:

```go
err = db.View(func(tx *latticedb.Tx) error {
    name, ok, err := tx.GetProperty(nodeID, "name")
    if err != nil {
        return err
    }
    if ok {
        fmt.Println(name)   // Alice
    }
    return nil
})
```

Property reads return a value, whether it was there, and an error. The middle
return is what separates "this property is not set" from "this property is set
to something empty", which a zero value alone could not tell you.

If you would rather manage the transaction yourself, `BeginRead`, `BeginWrite`,
`Commit`, and `Rollback` are available. `Update` and `View` are safer, because
they cannot leave a transaction open on an early return.

## Queries

```go
result, err := db.Query(`MATCH (p:Person) WHERE p.email = "bob@example.com" RETURN p.name`, nil)
```

The second argument is parameters, and using them is better than building query
strings:

```go
result, err := db.Query(
    "MATCH (p:Person) WHERE p.email = $email RETURN p.name",
    map[string]latticedb.Value{"email": "bob@example.com"},
)
```

`Tx.Query` runs a query inside a transaction you already have open, so it sees
that transaction's own uncommitted changes.

## Traversal

```go
edges, err := tx.GetOutgoingEdges(nodeID)
edges, err := tx.GetIncomingEdges(nodeID)
```

When you only care about one kind of relationship, filter by type and bound the
result, which stops collection early instead of gathering everything and
discarding most of it:

```go
edges, err := tx.GetOutgoingEdgesByType(nodeID, "KNOWS", 100)
```

## Property indexes

```go
err := db.CreateNodePropertyIndex("Person", "email")

err = db.View(func(tx *latticedb.Tx) error {
    ids, err := tx.FindNodesByLabelProperty("Person", "email", "alice@example.com", 10)
    // ids -> [1]
    return err
})
```

The limit is required and has to be greater than zero. Looking up a property
with no index behind it returns an error rather than quietly scanning. See
[Property Indexes](../guides/property-indexes.md).

Edges use `CreateEdgePropertyIndex` and `FindEdgesByTypeProperty`.

## Vector search

```go
results, err := db.VectorSearch(queryVector, latticedb.VectorSearchOptions{
    K:        10,
    EfSearch: 64,
})
```

`K` is how many neighbours you want. `EfSearch` trades speed for accuracy: higher
values search more of the index and find more of the true nearest neighbours.
[Benchmarks](../performance/benchmarks.md) shows the measured effect at
different settings.

Store a vector on a node with `tx.SetVector(nodeID, "embedding", vector)`, and
load many at once with `tx.BatchInsertVectors`.

## Full-text search

```go
results, err := db.FTSSearch("Document", "text", "graph database", latticedb.FTSSearchOptions{Limit: 20})

results, err = db.FTSSearchFuzzy("Document", "text", "databse", latticedb.FTSSearchOptions{
    Limit:         20,
    MaxDistance:   2,
    MinTermLength: 4,
})
```

Declare an index over the property holding the text with
`db.CreateNodeFTSIndex(label, property)`, or `db.CreateEdgeFTSIndex(edgeType,
property)` for a relationship. Writing that property keeps it current,
and searching a label and property with no declared index is an error rather than
an empty result.

Fuzzy search tolerates misspellings. `MaxDistance` is how many single-character
edits away a word may be, and `MinTermLength` stops short words being matched
loosely, where one edit can turn any three-letter word into any other.

## Durable streams

```go
err = db.Update(func(tx *latticedb.Tx) error {
    return tx.PublishStream("events", "signup", "alice joined")
})

records, err := db.ReadStream("events", 0, 10, 0)
for _, r := range records {
    fmt.Println(r.Sequence, r.Kind, r.Payload)
    // 1 signup alice joined
}
```

The arguments to `ReadStream` are the stream name, the sequence you last saw,
how many records you want, and how long to wait in milliseconds when there is
nothing new.

Reading does not record your position, on purpose: if it did, a crash between
reading and handling a record would lose it. Save the position yourself once the
work is done, inside the transaction that did the work:

```go
err = db.Update(func(tx *latticedb.Tx) error {
    // ... handle the records ...
    return tx.SetStreamOffset("events", "billing-worker", lastSequence)
})

sequence, exists, err := db.GetStreamOffset("events", "billing-worker")
```

`Changes` reads the built-in stream of graph mutations, so you can react to
writes without publishing anything yourself. `TrimStream` discards records every
consumer has passed; nothing trims automatically.

## Errors

```go
var latticeErr *latticedb.Error
if errors.As(err, &latticeErr) {
    if latticeErr.Code == latticedb.ErrorLockTimeout {
        // somebody else is writing
    }
}
```

Query failures come back as `QueryError`, which carries where in the query text
the problem is, so you can point at it rather than just reporting that something
was wrong.

## Where to go next

- [Quick Start](../getting-started/quickstart.md) for the same ideas in other languages
- [Cypher Overview](../cypher/overview.md) for the query language
- [Transactions and Durability](../guides/transactions.md) for the one-writer rule
