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
        # Create nodes
        alice = txn.create_node(labels=["Person"])
        bob = txn.create_node(labels=["Person"])

        # Set properties
        txn.set_property(alice.id, "name", "Alice")
        txn.set_property(alice.id, "age", 30)
        txn.set_property(bob.id, "name", "Bob")

        # Create relationships
        txn.create_edge(alice.id, bob.id, "KNOWS")

        txn.commit()

    # Query with Cypher
    result = db.query("MATCH (n:Person) RETURN n")
    for row in result:
        print(row)
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
- `query(cypher: str)` - Execute a Cypher query

### Transaction

#### Read Operations

- `is_read_only` - True if read-only transaction
- `is_active` - True if transaction is still active

#### Write Operations

- `create_node(labels: list[str])` - Create a node with labels
- `delete_node(node_id: int)` - Delete a node
- `set_property(node_id: int, key: str, value)` - Set a property on a node
- `create_edge(source_id: int, target_id: int, edge_type: str)` - Create an edge
- `delete_edge(source_id: int, target_id: int, edge_type: str)` - Delete an edge
- `commit()` - Commit the transaction
- `rollback()` - Rollback the transaction

### Vector Operations

To use vector embeddings, enable vector storage when opening the database:

```python
import numpy as np

with Database("mydb.ltdb", create=True, enable_vector=True, vector_dimensions=384) as db:
    with db.write() as txn:
        node = txn.create_node(labels=["Document"])

        # Store a vector embedding
        embedding = np.random.rand(384).astype(np.float32)
        txn.set_vector(node.id, "embedding", embedding)

        txn.commit()
```

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
