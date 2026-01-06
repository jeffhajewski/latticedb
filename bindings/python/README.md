# Lattice Python Bindings

Python bindings for [LatticeDB](https://github.com/latticedb/latticedb), an embedded knowledge graph database for AI/RAG applications.

## Installation

### From Source

```bash
# Build the native library first
cd /path/to/latticedb
zig build shared

# Install the Python package
cd bindings/python
pip install -e .
```

## Quick Start

```python
from lattice import Database

# Create a new database
with Database("mydb.ltdb", create=True) as db:
    # Write transaction
    with db.write() as txn:
        # Create nodes with properties
        alice = txn.create_node(
            labels=["Person"],
            properties={"name": "Alice", "age": 30}
        )
        bob = txn.create_node(
            labels=["Person"],
            properties={"name": "Bob", "age": 25}
        )

        # Create relationships
        txn.create_edge(alice.id, bob.id, "KNOWS")

        txn.commit()

    # Query with Cypher
    result = db.query("MATCH (n:Person) RETURN n.name")
    for row in result:
        print(row)  # {'n': 'Alice'}, {'n': 'Bob'}

    # Query with parameters (safe from injection)
    result = db.query(
        "MATCH (n:Person) WHERE n.name = $name RETURN n",
        parameters={"name": "Alice"}
    )
```

## API Reference

### Database

```python
Database(
    path: str | Path,
    *,
    create: bool = False,        # Create if doesn't exist
    read_only: bool = False,     # Open in read-only mode
    cache_size_mb: int = 100,    # Page cache size
    enable_vector: bool = False, # Enable vector storage
    vector_dimensions: int = 128 # Vector dimensions
)
```

#### Methods

- `open()` - Open the database connection
- `close()` - Close the database connection
- `read()` - Start a read-only transaction (context manager)
- `write()` - Start a read-write transaction (context manager)
- `query(cypher: str, parameters: dict = None)` - Execute a Cypher query with optional parameters

### Transaction

#### Read Operations

- `is_read_only` - True if read-only transaction
- `is_active` - True if transaction is still active
- `get_node(node_id: int)` - Get a node by ID, returns `Node` or `None`
- `get_property(node_id: int, key: str)` - Get a property value, returns value or `None`

#### Write Operations

- `create_node(labels: list[str], properties: dict = None)` - Create a node with labels and optional properties
- `delete_node(node_id: int)` - Delete a node
- `set_property(node_id: int, key: str, value)` - Set a property on a node
- `set_vector(node_id: int, key: str, vector: np.ndarray)` - Set a vector embedding
- `create_edge(source_id: int, target_id: int, edge_type: str)` - Create an edge
- `delete_edge(source_id: int, target_id: int, edge_type: str)` - Delete an edge
- `commit()` - Commit the transaction
- `rollback()` - Rollback the transaction

### Querying

#### Basic Queries

```python
# Simple match
result = db.query("MATCH (n:Person) RETURN n")

# Return properties
result = db.query("MATCH (n:Person) RETURN n.name")

# With WHERE clause
result = db.query("MATCH (n:Person) WHERE n.age > 25 RETURN n.name")
```

#### Parameterized Queries

Use parameters to safely pass values into queries:

```python
# String parameter
result = db.query(
    "MATCH (n:Person) WHERE n.name = $name RETURN n",
    parameters={"name": "Alice"}
)

# Integer parameter
result = db.query(
    "MATCH (n:Person) WHERE n.age = $age RETURN n.name",
    parameters={"age": 30}
)

# Multiple parameters
result = db.query(
    "MATCH (n:Person) WHERE n.name = $name AND n.age > $min_age RETURN n",
    parameters={"name": "Alice", "min_age": 20}
)
```

#### Working with Results

```python
result = db.query("MATCH (n:Person) RETURN n.name")

# Get column names
print(result.columns)  # ['n']

# Iterate rows
for row in result:
    print(row)  # {'n': 'Alice'}

# Get all rows as list
rows = list(result)

# Get row count
print(len(result))
```

### Reading Node Data

```python
with db.read() as txn:
    # Get a node by ID
    node = txn.get_node(node_id)
    if node:
        print(f"ID: {node.id}")
        print(f"Labels: {node.labels}")

    # Get individual properties
    name = txn.get_property(node_id, "name")
    age = txn.get_property(node_id, "age")

    # Returns None if property doesn't exist
    unknown = txn.get_property(node_id, "nonexistent")  # None
```

### Vector Operations

To use vector embeddings, enable vector storage when opening the database:

```python
import numpy as np

with Database("mydb.ltdb", create=True, enable_vector=True, vector_dimensions=384) as db:
    # Store vectors
    with db.write() as txn:
        node1 = txn.create_node(labels=["Document"])
        txn.set_property(node1.id, "title", "Introduction to ML")
        embedding1 = np.random.rand(384).astype(np.float32)
        txn.set_vector(node1.id, "embedding", embedding1)

        node2 = txn.create_node(labels=["Document"])
        txn.set_property(node2.id, "title", "Deep Learning Guide")
        embedding2 = np.random.rand(384).astype(np.float32)
        txn.set_vector(node2.id, "embedding", embedding2)

        txn.commit()

    # Search for similar vectors (HNSW approximate nearest neighbor)
    query_vector = np.random.rand(384).astype(np.float32)
    results = db.vector_search(query_vector, k=10, ef_search=64)

    for result in results:
        print(f"Node {result.node_id}: distance={result.distance:.4f}")
```

#### Vector Search Parameters

- `vector`: Query vector (numpy array of float32)
- `k`: Number of nearest neighbors to return (default: 10)
- `ef_search`: HNSW exploration factor - higher values are slower but more accurate (default: 64)

## Supported Property Types

- `None` - Null value
- `bool` - Boolean
- `int` - 64-bit integer
- `float` - 64-bit float
- `str` - UTF-8 string
- `bytes` - Binary data

## Error Handling

The library raises typed exceptions:

```python
from lattice import LatticeError, LatticeNotFoundError, LatticeIOError

try:
    with Database("nonexistent.ltdb") as db:
        pass
except LatticeNotFoundError:
    print("Database not found")
except LatticeIOError:
    print("I/O error")
except LatticeError as e:
    print(f"Error: {e}")
```

## Requirements

- Python 3.9+
- NumPy (optional, for vector operations)
- The native LatticeDB library (`liblattice.dylib` / `liblattice.so`)

## Building from Source

See [CONTRIBUTING.md](../../CONTRIBUTING.md) for build instructions.

## License

Same license as LatticeDB.
