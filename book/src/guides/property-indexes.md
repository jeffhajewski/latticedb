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

### Ranges, prefixes and sorting

An index keeps its values in order, so it also answers comparisons and prefixes.
With an index on `Person(age)` and one on `Person(name)`:

```cypher
MATCH (p:Person) WHERE p.age >= 30 AND p.age < 40 RETURN p.name, p.age
```

```
┌─────────┬───────┐
│ p.name  │ p.age │
├─────────┼───────┤
│ "Alice" │ 34    │
│ "Carol" │ 38    │
└─────────┴───────┘
```

```cypher
MATCH (p:Person) WHERE p.name STARTS WITH 'Al' RETURN p.name
```

```
┌─────────┐
│ p.name  │
├─────────┤
│ "Alan"  │
│ "Alice" │
└─────────┘
```

`<`, `<=`, `>` and `>=`, written either way round, and `STARTS WITH` qualify.
Several comparisons on one property joined by `AND` become one range. The
comparisons the index answers are not checked again row by row, except where
the index cannot decide on its own: a string bound longer than 128 bytes.
An equality on an indexed property is preferred to a range; without statistics,
a range on an indexed property is always read through the index, which measured
no slower than a scan even when the range holds every node.

A query that sorts by an indexed property and stops after a few rows reads the
index in order and stops as soon as it has them, rather than sorting every node:

```cypher
MATCH (p:Person) RETURN p.name, p.age ORDER BY p.age LIMIT 3
```

```
┌─────────┬───────┐
│ p.name  │ p.age │
├─────────┼───────┤
│ "Bob"   │ 29    │
│ "Alice" │ 34    │
│ "Carol" │ 38    │
└─────────┴───────┘
```

Descending works the same way, and the index puts nodes without the property
where `ORDER BY` puts nulls, last ascending and first descending:

```cypher
MATCH (p:Person) RETURN p.name, p.age ORDER BY p.age DESC LIMIT 3
```

```
┌─────────┬───────┐
│ p.name  │ p.age │
├─────────┼───────┤
│ "Dan"   │ null  │
│ "Alan"  │ 41    │
│ "Carol" │ 38    │
└─────────┴───────┘
```

That applies to a single `MATCH` pattern, with or without a `WHERE`, whose
`RETURN` does not aggregate or use `DISTINCT`, sorted first by the property,
either way, with a `LIMIT`. Rows with equal values come in the same order as
without the index. A `WHERE` with a vector or full-text search keeps the
ordinary sort, since the search returns its rows nearest or best first.

On 200,000 `:Person` nodes (a release build, median of seven runs):

| query | without an index | with one |
|---|---:|---:|
| `WHERE p.age > 97` (4,000 rows) | 48 ms | 1.4 ms |
| `WHERE p.score >= 500 AND p.score < 505` (978 rows) | 54 ms | 0.3 ms |
| `WHERE p.name STARTS WITH 'Jo'` (293 rows) | 48 ms | 0.10 ms |
| `ORDER BY p.score LIMIT 10` | 46 ms | 0.01 ms |
| `ORDER BY p.score DESC LIMIT 10` | 46 ms | 0.01 ms |
| `WHERE p.age >= 50 RETURN count(p)` (half the nodes) | 49 ms | 3.4 ms |
| `WHERE p.age >= 0` (every node) | 114 ms | 88 ms |

A relationship type's index answers the same comparisons, and equality, for a
pattern that starts from the relationship: a single relationship type, with
nothing known about the node it starts from. With an index on `KNOWS(since)`:

```cypher
MATCH (a)-[r:KNOWS]->(b) WHERE r.since >= 2020 RETURN a.name, b.name, r.since ORDER BY r.since
```

```
┌─────────┬─────────┬─────────┐
│ a.name  │ b.name  │ r.since │
├─────────┼─────────┼─────────┤
│ "Alice" │ "Carol" │ 2021    │
│ "Bob"   │ "Alan"  │ 2023    │
└─────────┴─────────┴─────────┘
```

An inline map, `-[r:KNOWS {since: 2021}]->`, works the same way. Reading a
relationship through the index costs about four times what walking every
relationship costs per relationship, so when the range holds a fifth of them or
more, the query walks them instead. On 1,000,000 `:KNOWS` relationships with
`w` spread over 0 to 9,999:

| query | without an index | with one |
|---|---:|---:|
| `WHERE r.w = 1234` (115 rows) | 177 ms | 0.09 ms |
| `WHERE r.w > 9990` (944 rows) | 176 ms | 0.7 ms |
| `WHERE r.w >= 100 AND r.w < 200` (about 10,000 rows) | 224 ms | 9.5 ms |
| `WHERE r.name STARTS WITH 'Jo'` (1,447 rows) | 183 ms | 1.2 ms |
| `WHERE r.w >= 5000` (half, walked) | 280 ms | 280 ms |

Values of different types are kept in the order `ORDER BY` puts them in, and
integers and floats compare by exact value, as in the rest of Cypher: `3` and
`3.0` are one value, and `9007199254740993` is greater than
`9007199254740992.0`.

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
every write that touches the indexed property, and to every node created with
the label but without the property, which the index holds too so that it can
sort them. That is a good trade for a lookup
your application does constantly, and a bad one for a property you query once a
month.

Some things worth knowing before you add one:

- **Index for lookups you actually perform.** An index on a property nothing
  looks up is pure cost.
- **Indexes answer equality, ranges and prefixes** (on relationships, for a
  pattern that starts from the relationship type), not text inside a longer
  string (`CONTAINS`, `ENDS WITH`). For finding words in text, you want
  [full-text search](./full-text-search.md) instead.
- **High-variety properties benefit most.** An index on `email`, where almost
  every value is unique, narrows millions of nodes to one. An index on a
  property with three possible values only narrows to a third, which a scan
  would have managed nearly as fast.
- **Index the pair, not the property.** An index covers one label and property
  together. Indexing `email` on `Person` does nothing for `email` on `Company`.

## Indexes and older releases

Creating a property index with this release marks the file as needing format 4
to open, so LatticeDB 0.16.0 refuses it rather than misreading the index; a
file without one stays readable by 0.16.0. Dropping the index does not undo
the mark.

An index that 0.16.0 created keeps working for equality, but is not used for
ranges, prefixes or sorting until it is rebuilt. `lattice info` lists each
index with its encoding: `digest` is the 0.16.0 one, `ordered_dense` (a
label's) and `ordered` (a relationship type's) are this release's. The
`encoding` column of `CALL db.indexes()` says the same:

```bash
lattice info people.lattice
```

```text
...
Index:        :Person(age)  digest
```

`lattice compact --upgrade-indexes` rebuilds every index in an older encoding
in place, keeping its label or type and property, and then compacts as usual:

```bash
lattice compact people.lattice --upgrade-indexes
```

```text
Compacted database: people.lattice
  Indexes rebuilt: 1
  Pages before:    1036
  Pages after:     1036
  Pages removed:   0
  Bytes reclaimed: 0
```

From Zig, `Database.upgradePropertyIndexes()` does the same and returns how
many it rebuilt. Either way it reads every node of the label (or relationship
of the type) once, as creating the index does, in one step that a crash
undoes completely, and it marks the file as needing format 4 as creating an
index does.

## Where to go next

- [Performance Tuning](./performance-tuning.md) for the other things that affect query speed
- [Full-Text Search](./full-text-search.md) for matching words inside text
- [Working with Embeddings](./embeddings.md) for finding things by similarity rather than equality
