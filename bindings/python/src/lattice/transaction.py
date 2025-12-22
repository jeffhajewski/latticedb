"""
Transaction class for Lattice Python bindings.
"""

from typing import TYPE_CHECKING, Any, Dict, List, Optional

import numpy as np
from numpy.typing import NDArray

from lattice.types import Edge, Node, PropertyValue

if TYPE_CHECKING:
    from lattice.database import Database


class Transaction:
    """
    A database transaction.

    Transactions provide atomic, isolated access to the database.
    Use as a context manager to ensure proper cleanup.

    Example:
        with db.write() as txn:
            node = txn.create_node(labels=["Person"])
            txn.commit()
    """

    def __init__(self, db: "Database", *, read_only: bool = False) -> None:
        """
        Create a new transaction.

        Args:
            db: The database connection.
            read_only: Whether this is a read-only transaction.
        """
        self._db = db
        self._read_only = read_only
        self._handle: Optional[Any] = None
        self._committed = False
        self._rolled_back = False

    def __enter__(self) -> "Transaction":
        """Context manager entry - begins the transaction."""
        self._begin()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit - rolls back if not committed."""
        if not self._committed and not self._rolled_back:
            self.rollback()

    def _begin(self) -> None:
        """Begin the transaction."""
        # TODO: Call lattice_begin via ctypes
        self._handle = object()  # Placeholder

    def commit(self) -> None:
        """Commit the transaction."""
        if self._committed:
            raise RuntimeError("Transaction already committed")
        if self._rolled_back:
            raise RuntimeError("Transaction already rolled back")
        # TODO: Call lattice_commit via ctypes
        self._committed = True

    def rollback(self) -> None:
        """Rollback the transaction."""
        if self._committed:
            raise RuntimeError("Transaction already committed")
        if self._rolled_back:
            return
        # TODO: Call lattice_rollback via ctypes
        self._rolled_back = True

    def create_node(
        self,
        *,
        labels: Optional[List[str]] = None,
        properties: Optional[Dict[str, PropertyValue]] = None,
    ) -> Node:
        """
        Create a new node.

        Args:
            labels: Node labels.
            properties: Node properties.

        Returns:
            The created node.
        """
        if self._read_only:
            raise RuntimeError("Cannot create node in read-only transaction")
        # TODO: Call lattice_node_create via ctypes
        node = Node(
            id=0,  # Placeholder
            labels=labels or [],
            properties=properties or {},
        )
        return node

    def delete_node(self, node_id: int) -> None:
        """
        Delete a node.

        Args:
            node_id: ID of the node to delete.
        """
        if self._read_only:
            raise RuntimeError("Cannot delete node in read-only transaction")
        # TODO: Call lattice_node_delete via ctypes

    def get_node(self, node_id: int) -> Optional[Node]:
        """
        Get a node by ID.

        Args:
            node_id: The node ID.

        Returns:
            The node, or None if not found.
        """
        # TODO: Implement node lookup
        return None

    def set_property(
        self,
        node_id: int,
        key: str,
        value: PropertyValue,
    ) -> None:
        """
        Set a property on a node.

        Args:
            node_id: The node ID.
            key: Property key.
            value: Property value.
        """
        if self._read_only:
            raise RuntimeError("Cannot set property in read-only transaction")
        # TODO: Call lattice_node_set_property via ctypes

    def set_vector(
        self,
        node_id: int,
        key: str,
        vector: NDArray[np.float32],
    ) -> None:
        """
        Set a vector embedding on a node.

        Args:
            node_id: The node ID.
            key: Vector property key.
            vector: Vector data as float32 array.
        """
        if self._read_only:
            raise RuntimeError("Cannot set vector in read-only transaction")
        # TODO: Call lattice_node_set_vector via ctypes

    def create_edge(
        self,
        source_id: int,
        target_id: int,
        edge_type: str,
        *,
        properties: Optional[Dict[str, PropertyValue]] = None,
    ) -> Edge:
        """
        Create an edge between two nodes.

        Args:
            source_id: Source node ID.
            target_id: Target node ID.
            edge_type: Edge type/label.
            properties: Edge properties.

        Returns:
            The created edge.
        """
        if self._read_only:
            raise RuntimeError("Cannot create edge in read-only transaction")
        # TODO: Call lattice_edge_create via ctypes
        edge = Edge(
            id=0,  # Placeholder
            source_id=source_id,
            target_id=target_id,
            edge_type=edge_type,
            properties=properties or {},
        )
        return edge

    def delete_edge(self, edge_id: int) -> None:
        """
        Delete an edge.

        Args:
            edge_id: ID of the edge to delete.
        """
        if self._read_only:
            raise RuntimeError("Cannot delete edge in read-only transaction")
        # TODO: Call lattice_edge_delete via ctypes

    @property
    def is_read_only(self) -> bool:
        """Return True if this is a read-only transaction."""
        return self._read_only

    @property
    def is_active(self) -> bool:
        """Return True if the transaction is still active."""
        return not self._committed and not self._rolled_back
