"""
Transaction class for Lattice Python bindings.
"""

import ctypes
import warnings
from ctypes import byref, c_uint64, c_void_p
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from latticedb._bindings import (
    LATTICE_ERROR_NOT_FOUND,
    LATTICE_TXN_READ_ONLY,
    LATTICE_TXN_READ_WRITE,
    LATTICE_VALUE_NULL,
    LatticeNodeId,
    LatticeValue,
    NodeWithVector,
    check_error,
    check_query_error,
    get_lib,
    python_to_value,
    value_to_python,
)
from latticedb.types import Edge, FtsSearchResult, Node, PropertyValue, QueryResult, VectorSearchResult

if TYPE_CHECKING:
    import numpy as np
    from numpy.typing import NDArray

    from latticedb.database import Database


def _is_numpy_array(value: Any) -> bool:
    """Check if a value is a numpy array without importing numpy."""
    return type(value).__module__ == "numpy" and type(value).__name__ == "ndarray"


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
        check_error(code)
        self._handle = None
        self._committed = True

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

    def publish_stream(
        self,
        stream: str,
        payload: PropertyValue,
        *,
        kind: str = "message",
    ) -> None:
        """Publish a durable stream record in this write transaction."""
        if self._read_only:
            raise RuntimeError("Cannot publish stream records in read-only transaction")
        if self._handle is None:
            raise RuntimeError("Transaction not started")

        lib = get_lib()
        stream_bytes = stream.encode("utf-8")
        kind_bytes = kind.encode("utf-8") if kind else b""
        c_value = LatticeValue()
        _ref = python_to_value(payload, c_value)
        code = lib._lib.lattice_stream_publish(
            self._handle,
            stream_bytes,
            len(stream_bytes),
            kind_bytes if kind_bytes else None,
            len(kind_bytes),
            byref(c_value),
        )
        del _ref
        check_error(code)

    def set_stream_offset(self, stream: str, consumer: str, sequence: int) -> None:
        """Commit a durable consumer offset in this write transaction."""
        if self._read_only:
            raise RuntimeError("Cannot set stream offsets in read-only transaction")
        if self._handle is None:
            raise RuntimeError("Transaction not started")

        lib = get_lib()
        stream_bytes = stream.encode("utf-8")
        consumer_bytes = consumer.encode("utf-8")
        code = lib._lib.lattice_stream_set_offset(
            self._handle,
            stream_bytes,
            len(stream_bytes),
            consumer_bytes,
            len(consumer_bytes),
            sequence,
        )
        check_error(code)

    def trim_stream(self, stream: str, through_sequence: int) -> None:
        """Delete stream records through ``through_sequence`` in this transaction."""
        if self._read_only:
            raise RuntimeError("Cannot trim streams in read-only transaction")
        if self._handle is None:
            raise RuntimeError("Transaction not started")

        lib = get_lib()
        stream_bytes = stream.encode("utf-8")
        code = lib._lib.lattice_stream_trim(
            self._handle,
            stream_bytes,
            len(stream_bytes),
            through_sequence,
        )
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
            labels: Node labels.
            properties: Node properties.
        """
        if self._read_only:
            raise RuntimeError("Cannot create node in read-only transaction")
        if self._handle is None:
            raise RuntimeError("Transaction not started")

        lib = get_lib()
        node_id = c_uint64()

        label_bytes = labels[0].encode("utf-8") if labels and len(labels) == 1 else None
        code = lib._lib.lattice_node_create(
            self._handle,
            label_bytes,
            byref(node_id),
        )
        check_error(code)

        if labels and len(labels) > 1:
            for label in labels:
                code = lib._lib.lattice_node_add_label(
                    self._handle,
                    node_id.value,
                    label.encode("utf-8"),
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

        Note: The returned node's ``properties`` will be empty. Use
        :meth:`get_property` to fetch individual properties by key.

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
        try:
            return value_to_python(c_value)
        finally:
            lib._lib.lattice_value_free(byref(c_value))

    def query(
        self,
        cypher: str,
        parameters: Optional[Dict[str, PropertyValue]] = None,
    ) -> QueryResult:
        """
        Execute a Cypher query inside this transaction.

        Args:
            cypher: The Cypher query string.
            parameters: Optional query parameters.

        Returns:
            Query results scoped to this transaction's snapshot.
        """
        if self._handle is None:
            raise RuntimeError("Transaction not started")
        if self._db._handle is None:
            raise RuntimeError("Database is not open")

        lib = get_lib()
        query_ptr = c_void_p()
        result_ptr = c_void_p()

        try:
            code = lib._lib.lattice_query_prepare(
                self._db._handle,
                cypher.encode("utf-8"),
                byref(query_ptr),
            )
            check_error(code)

            if parameters:
                for name, value in parameters.items():
                    if _is_numpy_array(value):
                        import numpy as np

                        vec = np.ascontiguousarray(value, dtype=np.float32)
                        vec_ptr = vec.ctypes.data_as(ctypes.POINTER(ctypes.c_float))
                        code = lib._lib.lattice_query_bind_vector(
                            query_ptr,
                            name.encode("utf-8"),
                            vec_ptr,
                            len(vec),
                        )
                        check_error(code)
                    else:
                        c_value = LatticeValue()
                        _ref = python_to_value(value, c_value)
                        code = lib._lib.lattice_query_bind(
                            query_ptr,
                            name.encode("utf-8"),
                            byref(c_value),
                        )
                        del _ref
                        check_error(code)

            code = lib._lib.lattice_query_execute(
                query_ptr,
                self._handle,
                byref(result_ptr),
            )
            check_query_error(code, query_ptr)

            column_count = lib._lib.lattice_result_column_count(result_ptr)
            columns = []
            for i in range(column_count):
                name_ptr = lib._lib.lattice_result_column_name(result_ptr, i)
                if name_ptr:
                    columns.append(name_ptr.decode("utf-8"))
                else:
                    columns.append(f"column_{i}")

            rows: List[Dict[str, Any]] = []
            while lib._lib.lattice_result_next(result_ptr):
                row: Dict[str, Any] = {}
                for i, col_name in enumerate(columns):
                    c_value = LatticeValue()
                    c_value.type = LATTICE_VALUE_NULL
                    code = lib._lib.lattice_result_get(result_ptr, i, byref(c_value))
                    check_error(code)
                    row[col_name] = value_to_python(c_value)
                rows.append(row)

            return QueryResult(columns=columns, _rows=rows)
        finally:
            if result_ptr.value:
                lib._lib.lattice_result_free(result_ptr)
            if query_ptr.value:
                lib._lib.lattice_query_free(query_ptr)

    def get_nodes_by_label(self, label: str) -> List[int]:
        """
        Return every node id that currently carries ``label`` in this transaction.
        """
        if self._handle is None:
            raise RuntimeError("Transaction not started")

        lib = get_lib()
        label_bytes = label.encode("utf-8")
        ids_ptr = ctypes.POINTER(LatticeNodeId)()
        count = ctypes.c_size_t(0)

        code = lib._lib.lattice_get_nodes_by_label_txn(
            self._handle,
            label_bytes,
            len(label_bytes),
            byref(ids_ptr),
            byref(count),
        )
        check_error(code)

        try:
            if count.value == 0 or not ids_ptr:
                return []
            return [ids_ptr[i] for i in range(count.value)]
        finally:
            if ids_ptr:
                lib._lib.lattice_free_node_ids(ids_ptr, count.value)

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

    def batch_insert_vectors(
        self,
        label: str,
        vectors: "NDArray[np.float32]",
    ) -> List[int]:
        """Insert N vector-bearing nodes in a single FFI call.

        Args:
            label: Label for all nodes.
            vectors: 2D array shape (N, dimensions), dtype float32.

        Returns:
            List of N node IDs.
        """
        if self._read_only:
            raise RuntimeError("Cannot insert in read-only transaction")
        if self._handle is None:
            raise RuntimeError("Transaction not started")

        import numpy as np

        # Validate input
        if vectors.ndim != 2:
            raise ValueError(f"Expected 2D array, got {vectors.ndim}D")

        vec = np.ascontiguousarray(vectors, dtype=np.float32)
        n, dims = vec.shape
        if n == 0:
            return []
        label_bytes = label.encode("utf-8")

        # Build C array of NodeWithVector specs
        specs = (NodeWithVector * n)()
        for i in range(n):
            specs[i].label = label_bytes
            specs[i].vector = vec[i].ctypes.data_as(ctypes.POINTER(ctypes.c_float))
            specs[i].dimensions = dims

        # Allocate output arrays
        node_ids = (LatticeNodeId * n)()
        count_out = ctypes.c_uint32(0)

        lib = get_lib()
        code = lib._lib.lattice_batch_insert(
            self._handle,
            specs,
            n,
            node_ids,
            ctypes.byref(count_out),
        )
        check_error(code)

        created = count_out.value
        if created < n:
            raise RuntimeError(
                f"Batch insert partially failed: {created}/{n} nodes created. "
                "Transaction should be rolled back."
            )

        return [node_ids[i] for i in range(created)]

    def batch_insert(
        self,
        label: str,
        vectors: "NDArray[np.float32]",
    ) -> List[int]:
        """Deprecated compatibility alias for :meth:`batch_insert_vectors`."""
        warnings.warn(
            "Transaction.batch_insert(...) is deprecated; use batch_insert_vectors(...). Earliest removal is v0.6.0.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.batch_insert_vectors(label, vectors)

    def vector_search(
        self,
        vector: "NDArray[np.float32]",
        *,
        k: int = 10,
        ef_search: int = 64,
    ) -> List[VectorSearchResult]:
        """
        Search for similar vectors within this transaction.
        """
        if self._handle is None:
            raise RuntimeError("Transaction not started")

        import numpy as np

        if not isinstance(vector, np.ndarray):
            vector = np.array(vector, dtype=np.float32)
        elif vector.dtype != np.float32:
            vector = vector.astype(np.float32)
        if not vector.flags["C_CONTIGUOUS"]:
            vector = np.ascontiguousarray(vector)

        lib = get_lib()
        result_ptr = c_void_p()
        code = lib._lib.lattice_vector_search_txn(
            self._handle,
            vector.ctypes.data_as(ctypes.POINTER(ctypes.c_float)),
            len(vector),
            k,
            ef_search,
            byref(result_ptr),
        )
        check_error(code)

        try:
            count = lib._lib.lattice_vector_result_count(result_ptr)
            results: List[VectorSearchResult] = []
            for i in range(count):
                node_id = LatticeNodeId()
                distance = ctypes.c_float()
                code = lib._lib.lattice_vector_result_get(
                    result_ptr, i, byref(node_id), byref(distance)
                )
                check_error(code)
                results.append(
                    VectorSearchResult(node_id=node_id.value, distance=distance.value)
                )
            return results
        finally:
            if result_ptr.value:
                lib._lib.lattice_vector_result_free(result_ptr)

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

    def fts_search(
        self,
        query: str,
        *,
        limit: int = 10,
    ) -> List[FtsSearchResult]:
        """
        Full-text search within this transaction.
        """
        if self._handle is None:
            raise RuntimeError("Transaction not started")

        lib = get_lib()
        result_ptr = c_void_p()
        query_bytes = query.encode("utf-8")

        code = lib._lib.lattice_fts_search_txn(
            self._handle,
            query_bytes,
            len(query_bytes),
            limit,
            byref(result_ptr),
        )
        check_error(code)

        try:
            count = lib._lib.lattice_fts_result_count(result_ptr)
            results: List[FtsSearchResult] = []
            for i in range(count):
                node_id = LatticeNodeId()
                score = ctypes.c_float()
                code = lib._lib.lattice_fts_result_get(
                    result_ptr, i, byref(node_id), byref(score)
                )
                check_error(code)
                results.append(FtsSearchResult(node_id=node_id.value, score=score.value))
            return results
        finally:
            if result_ptr.value:
                lib._lib.lattice_fts_result_free(result_ptr)

    def fts_search_fuzzy(
        self,
        query: str,
        *,
        limit: int = 10,
        max_distance: int = 0,
        min_term_length: int = 0,
    ) -> List[FtsSearchResult]:
        """
        Fuzzy full-text search within this transaction.
        """
        if self._handle is None:
            raise RuntimeError("Transaction not started")

        lib = get_lib()
        result_ptr = c_void_p()
        query_bytes = query.encode("utf-8")

        code = lib._lib.lattice_fts_search_fuzzy_txn(
            self._handle,
            query_bytes,
            len(query_bytes),
            limit,
            max_distance,
            min_term_length,
            byref(result_ptr),
        )
        check_error(code)

        try:
            count = lib._lib.lattice_fts_result_count(result_ptr)
            results: List[FtsSearchResult] = []
            for i in range(count):
                node_id = LatticeNodeId()
                score = ctypes.c_float()
                code = lib._lib.lattice_fts_result_get(
                    result_ptr, i, byref(node_id), byref(score)
                )
                check_error(code)
                results.append(FtsSearchResult(node_id=node_id.value, score=score.value))
            return results
        finally:
            if result_ptr.value:
                lib._lib.lattice_fts_result_free(result_ptr)

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

        edge = Edge(
            id=edge_id.value,
            source_id=source_id,
            target_id=target_id,
            edge_type=edge_type,
            properties={},
        )

        if properties:
            for key, value in properties.items():
                self.set_edge_property(edge.id, key, value)
                edge.properties[key] = value

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

    def set_edge_property(
        self,
        edge_id: int,
        key: str,
        value: PropertyValue,
    ) -> None:
        """
        Set a property on an edge.

        Args:
            edge_id: The stable edge ID.
            key: Property key.
            value: Property value.
        """
        if self._read_only:
            raise RuntimeError("Cannot set edge property in read-only transaction")
        if self._handle is None:
            raise RuntimeError("Transaction not started")

        lib = get_lib()
        c_value = LatticeValue()
        _ref = python_to_value(value, c_value)
        code = lib._lib.lattice_edge_set_property(
            self._handle,
            edge_id,
            key.encode("utf-8"),
            byref(c_value),
        )
        del _ref
        check_error(code)

    def get_edge_property(
        self,
        edge_id: int,
        key: str,
    ) -> Optional[PropertyValue]:
        """
        Get a property from an edge.

        Args:
            edge_id: The stable edge ID.
            key: Property key.

        Returns:
            The property value, or None if not found.
        """
        if self._handle is None:
            raise RuntimeError("Transaction not started")

        lib = get_lib()
        c_value = LatticeValue()
        code = lib._lib.lattice_edge_get_property(
            self._handle,
            edge_id,
            key.encode("utf-8"),
            byref(c_value),
        )

        if code == LATTICE_ERROR_NOT_FOUND:
            return None
        check_error(code)
        try:
            return value_to_python(c_value)
        finally:
            lib._lib.lattice_value_free(byref(c_value))

    def remove_edge_property(self, edge_id: int, key: str) -> None:
        """
        Remove a property from an edge.

        Args:
            edge_id: The stable edge ID.
            key: Property key.
        """
        if self._read_only:
            raise RuntimeError("Cannot remove edge property in read-only transaction")
        if self._handle is None:
            raise RuntimeError("Transaction not started")

        lib = get_lib()
        code = lib._lib.lattice_edge_remove_property(
            self._handle,
            edge_id,
            key.encode("utf-8"),
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
        if code == LATTICE_ERROR_NOT_FOUND:
            return []
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
        if code == LATTICE_ERROR_NOT_FOUND:
            return []
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
                edge_id = c_uint64()
                code = lib._lib.lattice_edge_result_get_id(
                    result_ptr,
                    i,
                    byref(edge_id),
                )
                check_error(code)

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
                    id=edge_id.value,
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
