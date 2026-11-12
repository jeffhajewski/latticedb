# Property Indexes

By default, finding a node by one of its properties means looking at every node
with that label and checking. That is fine for a few thousand nodes and slow for
a few million. A property index gives LatticeDB a direct route from a value to
the nodes that hold it.

You create one for a specific label and property, such as every `Person` node's
`email`. From then on, looking up a person by email goes straight to the answer
instead of scanning.

## Creating one

Indexes belong to the database, not to a transaction, so you create them on the
database handle:

```python
db.create_node_property_index("Person", "email")
```

```typescript
await db.createNodePropertyIndex("Person", "email");
```

```go
err := db.CreateNodePropertyIndex("Person", "email")
```

```c
lattice_node_property_index_create(db, "Person", "email");
```

Creating an index scans the nodes that already exist so that the ones already in
the database are included. On a large database that takes a moment, and it is
work you only pay once.

Edges work the same way, using the edge type where a node would use its label:

```python
db.create_edge_property_index("REVIEWED", "year")
```

## Looking things up

Once the index exists, ask for nodes by value:

```python
with db.read() as txn:
    ids = txn.find_nodes_by_label_property("Person", "email", "alice@example.com", limit=10)
    # ids -> [1]
    name = txn.get_property(ids[0], "name")
    # name -> "Alice"
```

You get node IDs back rather than whole nodes, which keeps the lookup cheap when
you only need to know what matched. Read the properties you actually want with
`get_property`.

The same call in the other languages:

```typescript
const ids = await txn.findNodesByLabelProperty("Person", "email", "alice@example.com", 10);
```

```go
ids, err := tx.FindNodesByLabelProperty("Person", "email", "alice@example.com", 10)
```

```c
lattice_nodes_find_by_label_property(
    txn, "Person", "email", &value, /* limit */ 10, &node_ids, &count);
```

Edges use `find_edges_by_type_property`, `findEdgesByTypeProperty`,
`FindEdgesByTypeProperty`, or `lattice_edges_find_by_type_property`:

```python
with db.read() as txn:
    edge_ids = txn.find_edges_by_type_property("REVIEWED", "year", 2024, limit=10)
```

### Every lookup is limited

Every lookup takes a limit, and it has to be greater than zero. Go, Java and C
require it; Python and TypeScript default it to 100, so leaving it out returns at
most 100 IDs:

```python
txn.find_nodes_by_label_property("Person", "email", "alice@example.com", limit=0)
# ValueError: limit must be positive
```

This is deliberate. An unbounded lookup on a common value could return a very
large list. Pass a larger limit when a value can match more than 100 records.

### Asking for an index that does not exist

If you look up a property with no index behind it, the call fails rather than
quietly scanning instead:

```python
with db.read() as txn:
    txn.find_nodes_by_label_property("Person", "email", "alice@example.com", limit=10)
# LatticeUnsupportedError: Unsupported operation or value type
```

This one surprises people, so it is worth saying plainly why it works this way.
A lookup that silently fell back to a full scan would still return the right
answer, so nothing would look broken. You would simply get scan performance
while believing you had index performance, and you would find out under load.
Failing tells you immediately.

The same happens after you drop an index. Nothing else changes, but that lookup
stops working:

```python
db.drop_node_property_index("Person", "email")
```

Creating an index that already exists is also an error, `LatticeAlreadyExistsError`,
rather than a silent no-op.

## Cypher uses your indexes automatically

You do not have to change your queries. When the planner sees a query it can
answer through an index, it uses it. Both of these forms qualify:

```cypher
MATCH (p:Person {email: "bob@example.com"}) RETURN p.name
```

```cypher
MATCH (p:Person) WHERE p.email = "carol@example.com" RETURN p.name
```

Writing the comparison the other way round works too, so `WHERE "bob@example.com" = p.email`
is recognised just the same.

An `AND` still qualifies, because every row has to satisfy both sides. The
planner uses the index for the part it can and checks the rest normally:

```cypher
MATCH (p:Person)
WHERE p.email = "bob@example.com" AND p.team = "platform"
RETURN p.name
```

An `OR` does not qualify, and this is the interesting case. A row only has to
satisfy one side, so narrowing to either branch would throw away rows that match
the other. The query still returns the correct answer; it just gets there by
scanning:

```cypher
MATCH (p:Person)
WHERE p.email = "bob@example.com" OR p.email = "carol@example.com"
RETURN p.name
```

The rule of thumb is that an index can help when a condition must be true, and
cannot when it is only one of several ways to match.

## Keeping up with changes

You do not have to maintain anything. Once an index exists, it is updated by
ordinary writes, whether those go through a transaction or directly:

```python
with db.write() as txn:
    txn.create_node(labels=["Person"], properties={"name": "Dan", "email": "dan@example.com"})
    txn.commit()

with db.read() as txn:
    txn.find_nodes_by_label_property("Person", "email", "dan@example.com", limit=10)
    # -> [4]
```

Indexes are stored in the database file, so they survive closing and reopening,
and they are rebuilt during recovery if the process stops unexpectedly. Creating
or dropping an index is written to the file before the call returns, so a crash
right after it cannot undo it.

## Creating an index needs the database to itself

Creating or dropping an index is refused while a write transaction is open:

```python
with db.write() as txn:
    txn.create_node(labels=["Person"], properties={"name": "Erin"})
    db.create_node_property_index("Person", "name")
    # LatticeLockTimeoutError: Lock timeout
```

Do index work when nothing else is in flight, usually at startup or during a
migration rather than in the middle of request handling. This is the same
one-writer rule that applies to transactions; see
[One writer at a time](./transactions.md#one-writer-at-a-time).

## Choosing what to index

An index costs you something. It uses space in the file, it makes creation
slower the first time because of the initial scan, and it adds a little work to
every write that touches the indexed property. That is a good trade for a lookup
your application does constantly, and a bad one for a property you query once a
month.

Some things worth knowing before you add one:

- **Index for lookups you actually perform.** An index on a property nothing
  looks up is pure cost.
- **These are equality indexes.** They answer "which nodes have exactly this
  value". They do not help with ranges, sorting, or partial text matching. For
  finding text inside a longer string, you want
  [full-text search](./full-text-search.md) instead.
- **High-variety properties benefit most.** An index on `email`, where almost
  every value is unique, narrows millions of nodes to one. An index on a
  property with three possible values only narrows to a third, which a scan
  would have managed nearly as fast.
- **Index the pair, not the property.** An index covers one label and property
  together. Indexing `email` on `Person` does nothing for `email` on `Company`.

## Where to go next

- [Performance Tuning](./performance-tuning.md) for the other things that affect query speed
- [Full-Text Search](./full-text-search.md) for matching words inside text
- [Working with Embeddings](./embeddings.md) for finding things by similarity rather than equality
