# Knowledge Graph Modeling

This guide covers data modeling patterns for knowledge graphs in LatticeDB.

## Basic Patterns

### Entities and Relationships

Model real-world entities as nodes and their relationships as edges:

```python
with db.write() as txn:
    alice = txn.create_node(labels=["Person"], properties={"name": "Alice"})
    company = txn.create_node(labels=["Company"], properties={"name": "Acme Corp"})
    txn.create_edge(alice.id, company.id, "WORKS_AT")
    txn.commit()
```

### Labels as Types

Use labels to categorize nodes. A node can have multiple labels:

```python
txn.create_node(labels=["Person", "Employee", "Manager"], properties={"name": "Alice"})
```

Query by label:

```cypher
MATCH (m:Manager) RETURN m.name
```

## Document Graph

A common pattern for document management:

```
Document -[:AUTHORED_BY]-> Person
Document -[:ABOUT]-> Topic
Chunk -[:PART_OF]-> Document
Chunk -[:NEXT]-> Chunk
```

```python
with db.write() as txn:
    doc = txn.create_node(labels=["Document"], properties={"title": "Paper Title"})
    author = txn.create_node(labels=["Person"], properties={"name": "Alice"})
    topic = txn.create_node(labels=["Topic"], properties={"name": "Machine Learning"})

    txn.create_edge(doc.id, author.id, "AUTHORED_BY")
    txn.create_edge(doc.id, topic.id, "ABOUT")

    # Create a chain of chunks
    chunks = []
    for i, text in enumerate(chunk_texts):
        chunk = txn.create_node(labels=["Chunk"], properties={"text": text, "position": i})
        txn.create_edge(chunk.id, doc.id, "PART_OF")
        if chunks:
            txn.create_edge(chunks[-1].id, chunk.id, "NEXT")
        chunks.append(chunk)

    txn.commit()
```

## Multi-Hop Queries

### Finding Collaborators

```cypher
-- Direct collaborators
MATCH (a:Person {name: "Alice"})-[:COLLABORATES_WITH]->(b:Person)
RETURN b.name

-- Collaborators of collaborators
MATCH (a:Person {name: "Alice"})-[:COLLABORATES_WITH*2]->(c:Person)
RETURN DISTINCT c.name
```

### Path Queries

```cypher
-- How are two people connected?
MATCH (a:Person {name: "Alice"})-[*1..4]->(b:Person {name: "Dave"})
RETURN a.name, b.name
```

### Aggregating Over Relationships

```cypher
-- Authors with the most documents about ML
MATCH (p:Person)<-[:AUTHORED_BY]-(d:Document)-[:ABOUT]->(t:Topic {name: "Machine Learning"})
RETURN p.name, count(d) AS papers
ORDER BY papers DESC
LIMIT 10
```

## Social Network

```
Person -[:KNOWS]-> Person
Person -[:FOLLOWS]-> Person
Person -[:POSTED]-> Post
Post -[:TAGGED]-> Topic
```

```cypher
-- Find friends who share interests
MATCH (me:Person {name: "Alice"})-[:KNOWS]->(friend:Person)
MATCH (me)-[:FOLLOWS]->(topic:Topic)<-[:FOLLOWS]-(friend)
RETURN friend.name, collect(topic.name) AS shared_interests
```

## Modeling Tips

- **Use descriptive edge types.** `AUTHORED_BY` is more useful than `RELATED_TO`.
- **Keep properties simple.** Store complex data as separate nodes with edges rather than large property values.
- **Use labels for querying.** Labels enable efficient scans via the label index.
- **Model for your queries.** Think about what traversals you need and structure edges accordingly.
- **Use MERGE for idempotent imports.** When loading data from multiple sources, `MERGE` prevents duplicate nodes.
