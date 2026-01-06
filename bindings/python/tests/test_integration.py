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

    @pytest.mark.skip(reason="Property operations not yet implemented in C API")
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

    @pytest.mark.skip(reason="Property operations not yet implemented in C API")
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

    @pytest.mark.skip(reason="Query on empty label returns error instead of empty result")
    def test_empty_result(self, tmp_path):
        """Test query with no results."""
        db_path = tmp_path / "test.db"

        with Database(db_path, create=True) as db:
            result = db.query("MATCH (n:NonexistentLabel) RETURN n")
            assert len(result) == 0

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

    @pytest.mark.skip(reason="Vector operations not yet implemented in C API")
    def test_set_vector(self, tmp_path):
        """Test setting a vector on a node."""
        pytest.importorskip("numpy")
        import numpy as np

        db_path = tmp_path / "test.db"

        with Database(db_path, create=True) as db:
            with db.write() as txn:
                node = txn.create_node(labels=["Document"])
                vector = np.array([0.1, 0.2, 0.3, 0.4], dtype=np.float32)
                txn.set_vector(node.id, "embedding", vector)
                txn.commit()
