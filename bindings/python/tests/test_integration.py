"""
Integration tests for Lattice Python bindings.

These tests require the native library to be built and available.
They are skipped if the library is not found.
"""

import pytest

from lattice import (
    Database,
    LatticeError,
    LatticeNotFoundError,
    library_available,
)

# Skip all tests in this module if the native library is not available
pytestmark = pytest.mark.skipif(
    not library_available(),
    reason="Native library not found - build with 'zig build' first",
)


class TestDatabaseLifecycle:
    """Tests for database open/close operations."""

    def test_open_close(self, tmp_path):
        """Test basic database open and close."""
        db_path = tmp_path / "test.db"
        db = Database(db_path, create=True)

        assert not db.is_open
        db.open()
        assert db.is_open
        assert db_path.exists()

        db.close()
        assert not db.is_open

    def test_context_manager(self, tmp_path):
        """Test database as context manager."""
        db_path = tmp_path / "test.db"

        with Database(db_path, create=True) as db:
            assert db.is_open

        assert not db.is_open

    def test_open_nonexistent_without_create(self, tmp_path):
        """Test opening nonexistent database without create flag raises error."""
        db_path = tmp_path / "nonexistent.db"

        with pytest.raises(LatticeError):
            with Database(db_path, create=False) as db:
                pass

    def test_reopen_existing(self, tmp_path):
        """Test reopening an existing database."""
        db_path = tmp_path / "test.db"

        # Create database
        with Database(db_path, create=True) as db:
            assert db.is_open

        # Reopen without create flag
        with Database(db_path, create=False) as db:
            assert db.is_open

    def test_read_only_mode(self, tmp_path):
        """Test opening database in read-only mode."""
        db_path = tmp_path / "test.db"

        # Create database first
        with Database(db_path, create=True) as db:
            pass

        # Open in read-only mode
        with Database(db_path, read_only=True) as db:
            assert db.is_open
            # Should not be able to get a write transaction
            with pytest.raises(RuntimeError, match="read-only"):
                db.write()


class TestTransactions:
    """Tests for transaction operations."""

    def test_read_transaction(self, tmp_path):
        """Test read-only transaction."""
        db_path = tmp_path / "test.db"

        with Database(db_path, create=True) as db:
            with db.read() as txn:
                assert txn.is_read_only
                assert txn.is_active

            # Transaction should be rolled back after exit
            assert not txn.is_active

    def test_write_transaction_commit(self, tmp_path):
        """Test write transaction with explicit commit."""
        db_path = tmp_path / "test.db"

        with Database(db_path, create=True) as db:
            with db.write() as txn:
                assert not txn.is_read_only
                assert txn.is_active
                txn.commit()
                assert not txn.is_active

    def test_write_transaction_rollback(self, tmp_path):
        """Test write transaction with explicit rollback."""
        db_path = tmp_path / "test.db"

        with Database(db_path, create=True) as db:
            with db.write() as txn:
                assert txn.is_active
                txn.rollback()
                assert not txn.is_active

    def test_transaction_auto_rollback(self, tmp_path):
        """Test that uncommitted transactions are rolled back on exit."""
        db_path = tmp_path / "test.db"

        with Database(db_path, create=True) as db:
            with db.write() as txn:
                # Don't commit - should auto-rollback
                pass

            assert not txn.is_active


class TestNodeOperations:
    """Tests for node CRUD operations."""

    def test_create_node(self, tmp_path):
        """Test creating a node."""
        db_path = tmp_path / "test.db"

        with Database(db_path, create=True) as db:
            with db.write() as txn:
                node = txn.create_node(labels=["Person"])
                assert node.id > 0
                assert "Person" in node.labels
                txn.commit()

    def test_create_node_with_properties(self, tmp_path):
        """Test creating a node with properties."""
        db_path = tmp_path / "test.db"

        with Database(db_path, create=True) as db:
            with db.write() as txn:
                node = txn.create_node(
                    labels=["Person"],
                    properties={"name": "Alice", "age": 30},
                )
                assert node.id > 0
                assert node.properties["name"] == "Alice"
                assert node.properties["age"] == 30
                txn.commit()

    def test_delete_node(self, tmp_path):
        """Test deleting a node."""
        db_path = tmp_path / "test.db"

        with Database(db_path, create=True) as db:
            with db.write() as txn:
                node = txn.create_node(labels=["Person"])
                node_id = node.id
                txn.delete_node(node_id)
                txn.commit()

    def test_set_property(self, tmp_path):
        """Test setting a property on a node."""
        db_path = tmp_path / "test.db"

        with Database(db_path, create=True) as db:
            with db.write() as txn:
                node = txn.create_node(labels=["Person"])
                txn.set_property(node.id, "name", "Bob")
                txn.set_property(node.id, "active", True)
                txn.set_property(node.id, "score", 3.14)
                txn.commit()

    def test_cannot_write_in_read_transaction(self, tmp_path):
        """Test that write operations fail in read-only transaction."""
        db_path = tmp_path / "test.db"

        with Database(db_path, create=True) as db:
            with db.read() as txn:
                with pytest.raises(RuntimeError, match="read-only"):
                    txn.create_node(labels=["Person"])

    def test_get_node(self, tmp_path):
        """Test retrieving a node by ID."""
        db_path = tmp_path / "test.db"

        with Database(db_path, create=True) as db:
            with db.write() as txn:
                created = txn.create_node(labels=["Person"])
                node_id = created.id
                txn.commit()

            # Retrieve in a new transaction
            with db.read() as txn:
                node = txn.get_node(node_id)
                assert node is not None
                assert node.id == node_id
                assert "Person" in node.labels

    def test_get_node_not_found(self, tmp_path):
        """Test that get_node returns None for nonexistent node."""
        db_path = tmp_path / "test.db"

        with Database(db_path, create=True) as db:
            with db.read() as txn:
                node = txn.get_node(99999)
                assert node is None

    def test_get_property(self, tmp_path):
        """Test retrieving a property from a node."""
        db_path = tmp_path / "test.db"

        with Database(db_path, create=True) as db:
            with db.write() as txn:
                node = txn.create_node(labels=["Person"])
                txn.set_property(node.id, "name", "Alice")
                txn.set_property(node.id, "age", 30)
                txn.set_property(node.id, "active", True)
                node_id = node.id
                txn.commit()

            # Retrieve in a new transaction
            with db.read() as txn:
                assert txn.get_property(node_id, "name") == "Alice"
                assert txn.get_property(node_id, "age") == 30
                assert txn.get_property(node_id, "active") is True

    def test_get_property_not_found(self, tmp_path):
        """Test that get_property returns None for nonexistent property."""
        db_path = tmp_path / "test.db"

        with Database(db_path, create=True) as db:
            with db.write() as txn:
                node = txn.create_node(labels=["Person"])
                node_id = node.id
                txn.commit()

            with db.read() as txn:
                assert txn.get_property(node_id, "nonexistent") is None


class TestEdgeOperations:
    """Tests for edge CRUD operations."""

    def test_create_edge(self, tmp_path):
        """Test creating an edge between nodes."""
        db_path = tmp_path / "test.db"

        with Database(db_path, create=True) as db:
            with db.write() as txn:
                alice = txn.create_node(labels=["Person"])
                bob = txn.create_node(labels=["Person"])

                edge = txn.create_edge(alice.id, bob.id, "KNOWS")
                assert edge.id > 0
                assert edge.source_id == alice.id
                assert edge.target_id == bob.id
                assert edge.edge_type == "KNOWS"
                txn.commit()

    def test_delete_edge(self, tmp_path):
        """Test deleting an edge."""
        db_path = tmp_path / "test.db"

        with Database(db_path, create=True) as db:
            with db.write() as txn:
                alice = txn.create_node(labels=["Person"])
                bob = txn.create_node(labels=["Person"])
                edge = txn.create_edge(alice.id, bob.id, "KNOWS")

                txn.delete_edge(alice.id, bob.id, "KNOWS")
                txn.commit()

    def test_get_outgoing_edges(self, tmp_path):
        """Test getting outgoing edges from a node."""
        db_path = tmp_path / "test.db"

        with Database(db_path, create=True) as db:
            with db.write() as txn:
                alice = txn.create_node(labels=["Person"])
                bob = txn.create_node(labels=["Person"])
                charlie = txn.create_node(labels=["Person"])

                txn.create_edge(alice.id, bob.id, "KNOWS")
                txn.create_edge(alice.id, charlie.id, "LIKES")
                alice_id = alice.id
                bob_id = bob.id
                charlie_id = charlie.id
                txn.commit()

            with db.read() as txn:
                edges = txn.get_outgoing_edges(alice_id)
                assert len(edges) == 2

                # Check edge details
                targets = {e.target_id for e in edges}
                assert bob_id in targets
                assert charlie_id in targets

                edge_types = {e.edge_type for e in edges}
                assert "KNOWS" in edge_types
                assert "LIKES" in edge_types

                # All edges should have alice as source
                for edge in edges:
                    assert edge.source_id == alice_id

    def test_get_incoming_edges(self, tmp_path):
        """Test getting incoming edges to a node."""
        db_path = tmp_path / "test.db"

        with Database(db_path, create=True) as db:
            with db.write() as txn:
                alice = txn.create_node(labels=["Person"])
                bob = txn.create_node(labels=["Person"])
                charlie = txn.create_node(labels=["Person"])

                txn.create_edge(alice.id, charlie.id, "KNOWS")
                txn.create_edge(bob.id, charlie.id, "LIKES")
                alice_id = alice.id
                bob_id = bob.id
                charlie_id = charlie.id
                txn.commit()

            with db.read() as txn:
                edges = txn.get_incoming_edges(charlie_id)
                assert len(edges) == 2

                # Check edge details
                sources = {e.source_id for e in edges}
                assert alice_id in sources
                assert bob_id in sources

                edge_types = {e.edge_type for e in edges}
                assert "KNOWS" in edge_types
                assert "LIKES" in edge_types

                # All edges should have charlie as target
                for edge in edges:
                    assert edge.target_id == charlie_id

    def test_get_edges_empty(self, tmp_path):
        """Test getting edges when none exist."""
        db_path = tmp_path / "test.db"

        with Database(db_path, create=True) as db:
            with db.write() as txn:
                node = txn.create_node(labels=["Person"])
                node_id = node.id
                txn.commit()

            with db.read() as txn:
                outgoing = txn.get_outgoing_edges(node_id)
                incoming = txn.get_incoming_edges(node_id)
                assert len(outgoing) == 0
                assert len(incoming) == 0


class TestQueries:
    """Tests for Cypher query execution."""

    def test_simple_match_query(self, tmp_path):
        """Test a simple MATCH query."""
        db_path = tmp_path / "test.db"

        with Database(db_path, create=True) as db:
            # Create some data
            with db.write() as txn:
                txn.create_node(labels=["Person"])
                txn.create_node(labels=["Person"])
                txn.commit()

            # Query the data
            result = db.query("MATCH (n:Person) RETURN n")
            assert len(result) >= 2

    def test_query_with_properties(self, tmp_path):
        """Test query returning properties."""
        db_path = tmp_path / "test.db"

        with Database(db_path, create=True) as db:
            with db.write() as txn:
                txn.create_node(
                    labels=["Person"],
                    properties={"name": "Alice"},
                )
                txn.commit()

            result = db.query("MATCH (n:Person) RETURN n.name")
            rows = list(result)
            assert len(rows) >= 1

    def test_empty_result(self, tmp_path):
        """Test query with no results."""
        db_path = tmp_path / "test.db"

        with Database(db_path, create=True) as db:
            result = db.query("MATCH (n:NonexistentLabel) RETURN n")
            assert len(result) == 0

    def test_query_with_parameters(self, tmp_path):
        """Test query with bound parameters."""
        db_path = tmp_path / "test.db"

        with Database(db_path, create=True) as db:
            with db.write() as txn:
                txn.create_node(
                    labels=["Person"],
                    properties={"name": "Alice", "age": 30},
                )
                txn.create_node(
                    labels=["Person"],
                    properties={"name": "Bob", "age": 25},
                )
                txn.commit()

            # Query with string parameter
            result = db.query(
                "MATCH (n:Person) WHERE n.name = $name RETURN n.name",
                parameters={"name": "Alice"},
            )
            rows = list(result)
            assert len(rows) == 1
            # Column name is 'n' (variable name), value is the property
            assert rows[0]["n"] == "Alice"

            # Query with integer parameter
            result = db.query(
                "MATCH (n:Person) WHERE n.age = $age RETURN n.name",
                parameters={"age": 25},
            )
            rows = list(result)
            assert len(rows) == 1
            assert rows[0]["n"] == "Bob"

    def test_query_result_iteration(self, tmp_path):
        """Test iterating over query results."""
        db_path = tmp_path / "test.db"

        with Database(db_path, create=True) as db:
            with db.write() as txn:
                for i in range(3):
                    txn.create_node(labels=["Item"])
                txn.commit()

            result = db.query("MATCH (n:Item) RETURN n")
            count = 0
            for row in result:
                count += 1
            assert count >= 3


class TestVectorOperations:
    """Tests for vector operations."""

    def test_set_vector(self, tmp_path):
        """Test setting a vector on a node."""
        pytest.importorskip("numpy")
        import numpy as np

        db_path = tmp_path / "test.db"

        # Must enable vector storage with correct dimensions
        with Database(db_path, create=True, enable_vector=True, vector_dimensions=4) as db:
            with db.write() as txn:
                node = txn.create_node(labels=["Document"])
                vector = np.array([0.1, 0.2, 0.3, 0.4], dtype=np.float32)
                txn.set_vector(node.id, "embedding", vector)
                txn.commit()

    def test_vector_search(self, tmp_path):
        """Test vector similarity search."""
        pytest.importorskip("numpy")
        import numpy as np

        db_path = tmp_path / "test.db"

        with Database(db_path, create=True, enable_vector=True, vector_dimensions=4) as db:
            # Create nodes with vectors
            with db.write() as txn:
                node1 = txn.create_node(labels=["Document"])
                txn.set_property(node1.id, "name", "doc1")
                txn.set_vector(node1.id, "embedding", np.array([1.0, 0.0, 0.0, 0.0], dtype=np.float32))

                node2 = txn.create_node(labels=["Document"])
                txn.set_property(node2.id, "name", "doc2")
                txn.set_vector(node2.id, "embedding", np.array([0.9, 0.1, 0.0, 0.0], dtype=np.float32))

                node3 = txn.create_node(labels=["Document"])
                txn.set_property(node3.id, "name", "doc3")
                txn.set_vector(node3.id, "embedding", np.array([0.0, 0.0, 1.0, 0.0], dtype=np.float32))

                txn.commit()

            # Search for vectors similar to [1.0, 0.0, 0.0, 0.0]
            query = np.array([1.0, 0.0, 0.0, 0.0], dtype=np.float32)
            results = db.vector_search(query, k=2)

            # Should return at least 2 results
            assert len(results) >= 2

            # First result should be node1 (exact match)
            assert results[0].node_id == node1.id
            assert results[0].distance < 0.01  # Very close to 0

            # Second result should be node2 (close to query)
            assert results[1].node_id == node2.id

    def test_vector_search_empty(self, tmp_path):
        """Test vector search on empty database returns empty results."""
        pytest.importorskip("numpy")
        import numpy as np

        db_path = tmp_path / "test.db"

        with Database(db_path, create=True, enable_vector=True, vector_dimensions=4) as db:
            query = np.array([1.0, 0.0, 0.0, 0.0], dtype=np.float32)
            results = db.vector_search(query, k=10)

            # Should return empty list
            assert len(results) == 0


class TestFtsOperations:
    """Tests for full-text search operations."""

    def test_fts_index_and_search(self, tmp_path):
        """Test indexing and searching text."""
        db_path = tmp_path / "test.db"

        with Database(db_path, create=True) as db:
            # Create nodes and index text
            with db.write() as txn:
                node1 = txn.create_node(labels=["Document"])
                txn.set_property(node1.id, "title", "Introduction to Machine Learning")
                txn.fts_index(node1.id, "Machine learning is a subset of artificial intelligence")

                node2 = txn.create_node(labels=["Document"])
                txn.set_property(node2.id, "title", "Deep Learning Guide")
                txn.fts_index(node2.id, "Deep learning uses neural networks for complex tasks")

                node3 = txn.create_node(labels=["Document"])
                txn.set_property(node3.id, "title", "Python Programming")
                txn.fts_index(node3.id, "Python is a popular programming language")

                txn.commit()

            # Search for "machine learning"
            results = db.fts_search("machine learning", limit=10)

            # Should find at least one result
            assert len(results) >= 1

            # First result should be the machine learning document
            assert results[0].node_id == node1.id
            assert results[0].score > 0

    def test_fts_search_no_results(self, tmp_path):
        """Test FTS search with no matching documents."""
        db_path = tmp_path / "test.db"

        with Database(db_path, create=True) as db:
            with db.write() as txn:
                node = txn.create_node(labels=["Document"])
                txn.fts_index(node.id, "The quick brown fox")
                txn.commit()

            # Search for something not in the index
            results = db.fts_search("elephant zebra", limit=10)

            # Should return empty list
            assert len(results) == 0

    def test_fts_search_empty_database(self, tmp_path):
        """Test FTS search on empty database."""
        db_path = tmp_path / "test.db"

        with Database(db_path, create=True) as db:
            results = db.fts_search("anything", limit=10)

            # Should return empty list
            assert len(results) == 0


class TestUtilities:
    """Tests for utility functions."""

    def test_version(self):
        """Test version() returns a version string."""
        from lattice import version

        ver = version()
        assert isinstance(ver, str)
        assert len(ver) > 0
        # Should be in semver format (e.g., "0.1.0")
        parts = ver.split(".")
        assert len(parts) >= 2

    def test_node_exists(self, tmp_path):
        """Test node_exists() method."""
        db_path = tmp_path / "test.db"

        with Database(db_path, create=True) as db:
            with db.write() as txn:
                # Create a node
                node = txn.create_node(labels=["Person"])

                # Node should exist
                assert txn.node_exists(node.id) is True

                # Non-existent node should not exist
                assert txn.node_exists(999999) is False

                txn.commit()

            # Check in read transaction too
            with db.read() as txn:
                assert txn.node_exists(node.id) is True
                assert txn.node_exists(999999) is False

    def test_node_exists_after_delete(self, tmp_path):
        """Test node_exists() returns False after node deletion."""
        db_path = tmp_path / "test.db"

        with Database(db_path, create=True) as db:
            with db.write() as txn:
                node = txn.create_node(labels=["Person"])
                node_id = node.id
                txn.commit()

            with db.write() as txn:
                # Node should exist before deletion
                assert txn.node_exists(node_id) is True

                # Delete the node
                txn.delete_node(node_id)

                # Node should not exist after deletion
                assert txn.node_exists(node_id) is False

                txn.commit()
