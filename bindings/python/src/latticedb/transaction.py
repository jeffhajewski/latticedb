"""
Transaction class for Lattice Python bindings.
"""

import ctypes
from ctypes import byref, c_uint64, c_void_p
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from latticedb._bindings import (
    LATTICE_ERROR_NOT_FOUND,
    LATTICE_OK,
    LATTICE_TXN_READ_ONLY,
    LATTICE_TXN_READ_WRITE,
    LatticeValue,
    check_error,
    get_lib,
    python_to_value,
    value_to_python,
)
from latticedb.types import Edge, Node, PropertyValue

if TYPE_CHECKING:
    import numpy as np
    from numpy.typing import NDArray

    from latticedb.database import Database


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
        if self._db._handle is None:
            raise RuntimeError("Database is not open")

        lib = get_lib()
        txn_ptr = c_void_p()
        mode = LATTICE_TXN_READ_ONLY if self._read_only else LATTICE_TXN_READ_WRITE
        code = lib._lib.lattice_begin(self._db._handle, mode, byref(txn_ptr))
        check_error(code)
        self._handle = txn_ptr

    def commit(self) -> None:
        """Commit the transaction."""
        if self._committed:
            raise RuntimeError("Transaction already committed")
        if self._rolled_back:
            raise RuntimeError("Transaction already rolled back")
        if self._handle is None:
            raise RuntimeError("Transaction not started")

        lib = get_lib()
        code = lib._lib.lattice_commit(self._handle)
        self._handle = None
        self._committed = True
        check_error(code)

    def rollback(self) -> None:
        """Rollback the transaction."""
        if self._committed:
            raise RuntimeError("Transaction already committed")
        if self._rolled_back:
            return
        if self._handle is None:
            self._rolled_back = True
            return

        lib = get_lib()
        code = lib._lib.lattice_rollback(self._handle)
        self._handle = None
        self._rolled_back = True
        check_error(code)

    def create_node(
        self,
        *,
        labels: Optional[List[str]] = None,
        properties: Optional[Dict[str, PropertyValue]] = None,
    ) -> Node:
        """
        Create a new node.

        Args:
            labels: Node labels (currently only first label is used).
            properties: Node properties.

        Returns:
            The created node.
        """
        if self._read_only:
            raise RuntimeError("Cannot create node in read-only transaction")
        if self._handle is None:
            raise RuntimeError("Transaction not started")

        lib = get_lib()
        node_id = c_uint64()

        # C API only supports one label at creation time
        label = labels[0] if labels else ""
        code = lib._lib.lattice_node_create(
            self._handle,
            label.encode("utf-8"),
            byref(node_id),
        )
        check_error(code)

        node = Node(
            id=node_id.value,
            labels=labels or [],
            properties={},
        )

        # Set properties if provided
        if properties:
            for key, value in properties.items():
                self.set_property(node.id, key, value)
                node.properties[key] = value

        return node

    def delete_node(self, node_id: int) -> None:
        """
        Delete a node.

        Args:
            node_id: ID of the node to delete.
        """
        if self._read_only:
            raise RuntimeError("Cannot delete node in read-only transaction")
        if self._handle is None:
            raise RuntimeError("Transaction not started")

        lib = get_lib()
        code = lib._lib.lattice_node_delete(self._handle, node_id)
        check_error(code)

    def get_node(self, node_id: int) -> Optional[Node]:
        """
        Get a node by ID.

        Args:
            node_id: The node ID.

        Returns:
            The node, or None if not found.
        """
        if self._handle is None:
            raise RuntimeError("Transaction not started")

        lib = get_lib()

        # Check if node exists
        exists = ctypes.c_bool()
        code = lib._lib.lattice_node_exists(self._handle, node_id, ctypes.byref(exists))
        check_error(code)

        if not exists.value:
            return None

        # Get labels
        labels_ptr = ctypes.c_char_p()
        code = lib._lib.lattice_node_get_labels(
            self._handle, node_id, ctypes.byref(labels_ptr)
        )
        if code == LATTICE_ERROR_NOT_FOUND:
            return None
        check_error(code)

        # Parse comma-separated labels
        labels: List[str] = []
        if labels_ptr.value:
            labels_str = labels_ptr.value.decode("utf-8")
            if labels_str:
                labels = labels_str.split(",")
            # Free the allocated string
            lib._lib.lattice_free_string(labels_ptr)

        return Node(
            id=node_id,
            labels=labels,
            properties={},  # Properties can be fetched with get_property()
        )

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
        if self._handle is None:
            raise RuntimeError("Transaction not started")

        lib = get_lib()
        c_value = LatticeValue()
        # Keep reference to any allocated data until C call completes
        _ref = python_to_value(value, c_value)
        code = lib._lib.lattice_node_set_property(
            self._handle,
            node_id,
            key.encode("utf-8"),
            byref(c_value),
        )
        del _ref  # Now safe to release
        check_error(code)

    def get_property(
        self,
        node_id: int,
        key: str,
    ) -> Optional[PropertyValue]:
        """
        Get a property from a node.

        Args:
            node_id: The node ID.
            key: Property key.

        Returns:
            The property value, or None if not found.
        """
        if self._handle is None:
            raise RuntimeError("Transaction not started")

        lib = get_lib()
        c_value = LatticeValue()
        code = lib._lib.lattice_node_get_property(
            self._handle,
            node_id,
            key.encode("utf-8"),
            byref(c_value),
        )

        if code == LATTICE_ERROR_NOT_FOUND:
            return None
        check_error(code)
        return value_to_python(c_value)

    def set_vector(
        self,
        node_id: int,
        key: str,
        vector: "NDArray[np.float32]",
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
        if self._handle is None:
            raise RuntimeError("Transaction not started")

        import numpy as np

        lib = get_lib()
        # Ensure vector is float32 and contiguous
        vec = np.ascontiguousarray(vector, dtype=np.float32)
        vec_ptr = vec.ctypes.data_as(ctypes.POINTER(ctypes.c_float))
        code = lib._lib.lattice_node_set_vector(
            self._handle,
            node_id,
            key.encode("utf-8"),
            vec_ptr,
            len(vec),
        )
        check_error(code)

    def fts_index(
        self,
        node_id: int,
        text: str,
    ) -> None:
        """
        Index text for full-text search on a node.

        Args:
            node_id: The node ID to associate with the text.
            text: The text content to index.
        """
        if self._read_only:
            raise RuntimeError("Cannot index text in read-only transaction")
        if self._handle is None:
            raise RuntimeError("Transaction not started")

        lib = get_lib()
        text_bytes = text.encode("utf-8")
        code = lib._lib.lattice_fts_index(
            self._handle,
            node_id,
            text_bytes,
            len(text_bytes),
        )
        check_error(code)

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
            properties: Edge properties (not yet supported).

        Returns:
            The created edge.
        """
        if self._read_only:
            raise RuntimeError("Cannot create edge in read-only transaction")
        if self._handle is None:
            raise RuntimeError("Transaction not started")

        lib = get_lib()
        edge_id = c_uint64()
        code = lib._lib.lattice_edge_create(
            self._handle,
            source_id,
            target_id,
            edge_type.encode("utf-8"),
            byref(edge_id),
        )
        check_error(code)

        # Note: Edge properties not yet supported in C API
        edge = Edge(
            id=edge_id.value,
            source_id=source_id,
            target_id=target_id,
            edge_type=edge_type,
            properties=properties or {},
        )
        return edge

    def delete_edge(self, source_id: int, target_id: int, edge_type: str) -> None:
        """
        Delete an edge between two nodes.

        Args:
            source_id: ID of the source node.
            target_id: ID of the target node.
            edge_type: Type of the edge to delete.
        """
        if self._read_only:
            raise RuntimeError("Cannot delete edge in read-only transaction")
        if self._handle is None:
            raise RuntimeError("Transaction not started")

        lib = get_lib()
        code = lib._lib.lattice_edge_delete(
            self._handle, source_id, target_id, edge_type.encode("utf-8")
        )
        check_error(code)

    def get_outgoing_edges(self, node_id: int) -> List[Edge]:
        """
        Get all outgoing edges from a node.

        Args:
            node_id: The source node ID.

        Returns:
            List of edges originating from the node.
        """
        if self._handle is None:
            raise RuntimeError("Transaction not started")

        lib = get_lib()
        result_ptr = c_void_p()
        code = lib._lib.lattice_edge_get_outgoing(
            self._handle, node_id, byref(result_ptr)
        )
        check_error(code)

        return self._collect_edge_results(result_ptr, lib)

    def get_incoming_edges(self, node_id: int) -> List[Edge]:
        """
        Get all incoming edges to a node.

        Args:
            node_id: The target node ID.

        Returns:
            List of edges pointing to the node.
        """
        if self._handle is None:
            raise RuntimeError("Transaction not started")

        lib = get_lib()
        result_ptr = c_void_p()
        code = lib._lib.lattice_edge_get_incoming(
            self._handle, node_id, byref(result_ptr)
        )
        check_error(code)

        return self._collect_edge_results(result_ptr, lib)

    def _collect_edge_results(self, result_ptr: c_void_p, lib: Any) -> List[Edge]:
        """
        Helper to collect edges from a result handle.

        Args:
            result_ptr: The edge result handle.
            lib: The library instance.

        Returns:
            List of Edge objects.
        """
        edges: List[Edge] = []
        try:
            count = lib._lib.lattice_edge_result_count(result_ptr)
            for i in range(count):
                source = c_uint64()
                target = c_uint64()
                edge_type_ptr = ctypes.c_char_p()
                edge_type_len = ctypes.c_uint32()
                code = lib._lib.lattice_edge_result_get(
                    result_ptr,
                    i,
                    byref(source),
                    byref(target),
                    byref(edge_type_ptr),
                    byref(edge_type_len),
                )
                check_error(code)
                edge_type = edge_type_ptr.value[:edge_type_len.value].decode("utf-8") if edge_type_ptr.value else ""
                edges.append(Edge(
                    id=0,  # Edge ID not available from traversal
                    source_id=source.value,
                    target_id=target.value,
                    edge_type=edge_type,
                    properties={},
                ))
        finally:
            lib._lib.lattice_edge_result_free(result_ptr)
        return edges

    def node_exists(self, node_id: int) -> bool:
        """
        Check if a node exists.

        Args:
            node_id: The node ID to check.

        Returns:
            True if the node exists, False otherwise.
        """
        if self._handle is None:
            raise RuntimeError("Transaction not started")

        lib = get_lib()
        exists = ctypes.c_bool()
        code = lib._lib.lattice_node_exists(self._handle, node_id, ctypes.byref(exists))
        check_error(code)
        return exists.value

    @property
    def is_read_only(self) -> bool:
        """Return True if this is a read-only transaction."""
        return self._read_only

    @property
    def is_active(self) -> bool:
        """Return True if the transaction is still active."""
        return not self._committed and not self._rolled_back
