"""
Database class for Lattice Python bindings.
"""

import ctypes
import warnings
from ctypes import POINTER, byref, c_size_t, c_ubyte, c_void_p
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

if TYPE_CHECKING:
    import numpy as np
    from numpy.typing import NDArray

from latticedb._bindings import (
    LATTICE_TXN_READ_ONLY,
    LATTICE_TXN_READ_WRITE,
    LATTICE_VALUE_NULL,
    LatticeNodeId,
    LatticeValue,
    OpenOptions,
    OpenOptionsV2,
    OpenOptionsV3,
    OpenOptionsV4,
    LATTICE_OK,
    LATTICE_ERROR_IO,
    LATTICE_ERROR_UNSUPPORTED,
    LatticeUnsupportedError,
    check_error,
    check_query_error,
    get_lib,
    python_to_value,
    value_to_python,
)
from latticedb.transaction import Transaction
from latticedb.types import QueryResult, VectorSearchResult, FtsSearchResult, StreamRecord


def _is_numpy_array(value: Any) -> bool:
    """Check if a value is a numpy array without importing numpy."""
    return type(value).__module__ == "numpy" and type(value).__name__ == "ndarray"


class Database:
    """
    Lattice database connection.

    Example:
        with Database("knowledge.lattice", create=True) as db:
            with db.write() as txn:
                node = txn.create_node(labels=["Person"], properties={"name": "Alice"})
                txn.commit()
    """

    def __init__(
        self,
        path: Union[str, Path],
        *,
        create: bool = False,
        read_only: bool = False,
        cache_size_mb: int = 100,
        enable_wal: bool = True,
        enable_adjacency_cache: bool = False,
        enable_vectors: Optional[bool] = None,
        enable_vector: Optional[bool] = None,
        vector_dimensions: int = 128,
        lock: bool = True,
    ) -> None:
        """
        Open a Lattice database.

        Args:
            path: Path to the database file.
            create: Create the database if it doesn't exist.
            read_only: Open in read-only mode.
            cache_size_mb: Size of the page cache in megabytes.
            enable_wal: Enable WAL-backed transactions.
            enable_adjacency_cache: Enable the in-memory graph adjacency cache.
            enable_vectors: Enable vector storage and indexing.
            enable_vector: Compatibility alias for ``enable_vectors``.
            vector_dimensions: Dimension of vectors when vector support is enabled.
            lock: Take a lock on the file so two processes cannot tread on each
                other. A read-write handle takes it exclusively and a read-only
                handle shares it, so opening raises
                :class:`LatticeDatabaseLockedError` rather than waiting. Turn this
                off only where locking does not work, such as some network
                filesystems; it does not make concurrent access safe.
        """
        self._path = Path(path)
        self._create = create
        self._read_only = read_only
        self._cache_size_mb = cache_size_mb
        self._enable_wal = enable_wal
        self._enable_adjacency_cache = enable_adjacency_cache
        if enable_vector is not None:
            warnings.warn(
                "Database(..., enable_vector=...) is deprecated; use enable_vectors=.... Earliest removal is v0.6.0.",
                DeprecationWarning,
                stacklevel=2,
            )
        self._enable_vectors = (
            enable_vectors if enable_vectors is not None else bool(enable_vector)
        )
        self._vector_dimensions = vector_dimensions
        self._lock = lock
        self._handle: Optional[Any] = None
        self._closed = False
        # Set by deserialize when it points the database at a caller's buffer
        # rather than copying it. Holding the reference here is what keeps those
        # bytes alive for as long as the database can read them.
        self._borrowed: Optional[bytes] = None

    def serialize(self) -> bytes:
        """Return the whole database as bytes.

        The result is a database file. Write it anywhere and it opens, or hand it
        to :func:`deserialize`. This is what makes it practical to keep many
        small databases in object storage: upload the bytes with whatever client
        you already use.

        Pending writes are folded in first, so the bytes need no write-ahead log
        beside them. Raises if a transaction is open, because bytes captured
        while writes land underneath them are torn.
        """
        if self._handle is None:
            raise RuntimeError("Database is not open")

        lib = get_lib()
        if not getattr(lib, "_has_serialize", False):
            raise LatticeUnsupportedError(
                "the native library is too old to support serialize"
            )

        out = POINTER(c_ubyte)()
        length = c_size_t(0)
        code = lib._lib.lattice_serialize(self._handle, byref(out), byref(length))
        check_error(code)
        try:
            return bytes(bytearray(out[: length.value]))
        finally:
            lib._lib.lattice_free_bytes(out, length)

    def __enter__(self) -> "Database":
        """Context manager entry."""
        self.open()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.close()

    def open(self) -> None:
        """Open the database connection."""
        if self._handle is not None:
            return

        lib = get_lib()
        db_ptr = c_void_p()
        if getattr(lib, "_has_lattice_open_v4", False):
            opts_v4 = OpenOptionsV4(
                struct_size=ctypes.sizeof(OpenOptionsV4),
                create=self._create,
                read_only=self._read_only,
                cache_size_mb=self._cache_size_mb,
                page_size=4096,
                enable_vector=self._enable_vectors,
                vector_dimensions=self._vector_dimensions,
                enable_wal=self._enable_wal,
                enable_adjacency_cache=self._enable_adjacency_cache,
                lock=self._lock,
            )
            code = lib._lib.lattice_open_v4(
                str(self._path).encode("utf-8"),
                byref(opts_v4),
                byref(db_ptr),
            )
        elif getattr(lib, "_has_lattice_open_v3", False):
            opts_v3 = OpenOptionsV3(
                struct_size=ctypes.sizeof(OpenOptionsV3),
                create=self._create,
                read_only=self._read_only,
                cache_size_mb=self._cache_size_mb,
                page_size=4096,
                enable_vector=self._enable_vectors,
                vector_dimensions=self._vector_dimensions,
                enable_wal=self._enable_wal,
                enable_adjacency_cache=self._enable_adjacency_cache,
            )
            code = lib._lib.lattice_open_v3(
                str(self._path).encode("utf-8"),
                byref(opts_v3),
                byref(db_ptr),
            )
        elif getattr(lib, "_has_lattice_open_v2", False):
            if self._enable_adjacency_cache:
                raise LatticeUnsupportedError(
                    "lattice_open_v3 is required to enable the adjacency cache",
                    LATTICE_ERROR_UNSUPPORTED,
                )
            opts_v2 = OpenOptionsV2(
                struct_size=ctypes.sizeof(OpenOptionsV2),
                create=self._create,
                read_only=self._read_only,
                cache_size_mb=self._cache_size_mb,
                page_size=4096,
                enable_vector=self._enable_vectors,
                vector_dimensions=self._vector_dimensions,
                enable_wal=self._enable_wal,
            )
            code = lib._lib.lattice_open_v2(
                str(self._path).encode("utf-8"),
                byref(opts_v2),
                byref(db_ptr),
            )
        else:
            if self._enable_adjacency_cache:
                raise LatticeUnsupportedError(
                    "lattice_open_v3 is required to enable the adjacency cache",
                    LATTICE_ERROR_UNSUPPORTED,
                )
            if not self._enable_wal:
                raise LatticeUnsupportedError(
                    "lattice_open_v2 is required to disable WAL",
                    LATTICE_ERROR_UNSUPPORTED,
                )
            opts_v1 = OpenOptions(
                create=self._create,
                read_only=self._read_only,
                cache_size_mb=self._cache_size_mb,
                page_size=4096,
                enable_vector=self._enable_vectors,
                vector_dimensions=self._vector_dimensions,
            )
            code = lib._lib.lattice_open(
                str(self._path).encode("utf-8"),
                byref(opts_v1),
                byref(db_ptr),
            )
        check_error(code)
        self._handle = db_ptr

    def close(self) -> None:
        """Close the database connection."""
        if self._closed or self._handle is None:
            return

        lib = get_lib()
        code = lib._lib.lattice_close(self._handle)
        if code in (LATTICE_OK, LATTICE_ERROR_IO):
            self._handle = None
            self._closed = True
        check_error(code)

    def read(self) -> Transaction:
        """
        Begin a read-only transaction.

        Returns:
            A read-only transaction context manager.
        """
        return Transaction(self, read_only=True)

    def write(self) -> Transaction:
        """
        Begin a read-write transaction.

        Returns:
            A read-write transaction context manager.
        """
        if self._read_only:
            raise RuntimeError("Cannot write to a read-only database")
        return Transaction(self, read_only=False)

    def query(
        self,
        cypher: str,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> QueryResult:
        """
        Execute a Cypher query.

        Args:
            cypher: The Cypher query string.
            parameters: Query parameters. Supports scalar types, bytes, numpy
                vectors, nested lists, and string-keyed dicts.

        Returns:
            Query results.

        Example:
            # Scalar parameters
            result = db.query(
                "MATCH (n:Person) WHERE n.name = $name RETURN n",
                parameters={"name": "Alice"}
            )

            # Vector parameters (requires numpy)
            import numpy as np
            query_vec = np.random.rand(128).astype(np.float32)
            result = db.query(
                "MATCH (n:Document) WHERE n.embedding <=> $vec < 0.5 RETURN n",
                parameters={"vec": query_vec}
            )
        """
        if self._handle is None:
            raise RuntimeError("Database is not open")

        lib = get_lib()
        query_ptr = c_void_p()
        txn_ptr = c_void_p()
        result_ptr = c_void_p()

        try:
            # Prepare the query
            code = lib._lib.lattice_query_prepare(
                self._handle,
                cypher.encode("utf-8"),
                byref(query_ptr),
            )
            check_error(code)

            # Bind parameters if provided
            if parameters:
                for name, value in parameters.items():
                    # Check if value is a numpy array (vector parameter)
                    if _is_numpy_array(value):
                        import numpy as np
                        # Ensure vector is float32 and contiguous
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
                        # Keep reference to any allocated data until C call completes
                        _ref = python_to_value(value, c_value)
                        code = lib._lib.lattice_query_bind(
                            query_ptr,
                            name.encode("utf-8"),
                            byref(c_value),
                        )
                        del _ref  # Now safe to release
                        check_error(code)

            # Begin a read-only transaction for the query
            # A read-only transaction cannot run CREATE, SET, DELETE, MERGE, or
            # REMOVE, and a read-write one takes the single writer slot, which
            # would stop concurrent reads. Ask the query which it needs.
            writes = bool(lib._lib.lattice_query_writes(query_ptr))
            code = lib._lib.lattice_begin(
                self._handle,
                LATTICE_TXN_READ_WRITE if writes else LATTICE_TXN_READ_ONLY,
                byref(txn_ptr),
            )
            check_error(code)

            # Execute the query
            code = lib._lib.lattice_query_execute(
                query_ptr,
                txn_ptr,
                byref(result_ptr),
            )
            check_query_error(code, query_ptr)

            # Collect column names
            column_count = lib._lib.lattice_result_column_count(result_ptr)
            columns = []
            for i in range(column_count):
                name_ptr = lib._lib.lattice_result_column_name(result_ptr, i)
                if name_ptr:
                    columns.append(name_ptr.decode("utf-8"))
                else:
                    columns.append(f"column_{i}")

            # Collect all rows
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

            # Every value has been copied into Python objects by this point, so
            # the result can be released before the transaction is resolved.
            lib._lib.lattice_result_free(result_ptr)
            result_ptr = c_void_p()

            if writes:
                # The rollback in `finally` would otherwise discard the write.
                code = lib._lib.lattice_commit(txn_ptr)
                txn_ptr = c_void_p()
                check_error(code)

            return QueryResult(columns=columns, _rows=rows)

        finally:
            # Clean up in reverse order. A transaction still open here either
            # was read-only or belongs to a query that failed, so rolling back
            # is right in both cases.
            if result_ptr.value:
                lib._lib.lattice_result_free(result_ptr)
            if txn_ptr.value:
                lib._lib.lattice_rollback(txn_ptr)
            if query_ptr.value:
                lib._lib.lattice_query_free(query_ptr)

    def get_nodes_by_label(self, label: str) -> List[int]:
        """
        Return every node id that currently carries ``label``.

        An unknown label is not an error and returns an empty list.
        """
        if self._handle is None:
            raise RuntimeError("Database is not open")

        lib = get_lib()
        label_bytes = label.encode("utf-8")
        ids_ptr = ctypes.POINTER(LatticeNodeId)()
        count = ctypes.c_size_t(0)

        code = lib._lib.lattice_get_nodes_by_label(
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

    def create_node_property_index(self, label: str, property_key: str) -> None:
        """Create an explicit equality index for a node label/property pair."""
        if self._handle is None:
            raise RuntimeError("Database is not open")
        code = get_lib()._lib.lattice_node_property_index_create(
            self._handle,
            label.encode("utf-8"),
            property_key.encode("utf-8"),
        )
        check_error(code)

    def drop_node_property_index(self, label: str, property_key: str) -> None:
        """Drop an explicit node property index."""
        if self._handle is None:
            raise RuntimeError("Database is not open")
        code = get_lib()._lib.lattice_node_property_index_drop(
            self._handle,
            label.encode("utf-8"),
            property_key.encode("utf-8"),
        )
        check_error(code)

    def create_edge_property_index(self, edge_type: str, property_key: str) -> None:
        """Create an explicit equality index for an edge type/property pair."""
        if self._handle is None:
            raise RuntimeError("Database is not open")
        code = get_lib()._lib.lattice_edge_property_index_create(
            self._handle,
            edge_type.encode("utf-8"),
            property_key.encode("utf-8"),
        )
        check_error(code)

    def drop_edge_property_index(self, edge_type: str, property_key: str) -> None:
        """Drop an explicit edge property index."""
        if self._handle is None:
            raise RuntimeError("Database is not open")
        code = get_lib()._lib.lattice_edge_property_index_drop(
            self._handle,
            edge_type.encode("utf-8"),
            property_key.encode("utf-8"),
        )
        check_error(code)

    def read_stream(
        self,
        stream: str,
        *,
        after_sequence: int = 0,
        limit: int = 100,
        timeout_ms: int = 0,
    ) -> List[StreamRecord]:
        """Read durable stream records after ``after_sequence``."""
        if self._handle is None:
            raise RuntimeError("Database is not open")

        lib = get_lib()
        stream_bytes = stream.encode("utf-8")
        batch_ptr = c_void_p()
        code = lib._lib.lattice_stream_read(
            self._handle,
            stream_bytes,
            len(stream_bytes),
            after_sequence,
            limit,
            timeout_ms,
            byref(batch_ptr),
        )
        check_error(code)

        try:
            count = lib._lib.lattice_stream_batch_count(batch_ptr)
            records: List[StreamRecord] = []
            for i in range(count):
                sequence = ctypes.c_uint64()
                kind_ptr = ctypes.POINTER(ctypes.c_char)()
                kind_len = ctypes.c_size_t()
                payload_ptr = ctypes.POINTER(LatticeValue)()
                code = lib._lib.lattice_stream_batch_get(
                    batch_ptr,
                    i,
                    byref(sequence),
                    byref(kind_ptr),
                    byref(kind_len),
                    byref(payload_ptr),
                )
                check_error(code)
                kind = ctypes.string_at(kind_ptr, kind_len.value).decode("utf-8")
                payload = value_to_python(payload_ptr.contents)
                records.append(StreamRecord(sequence=sequence.value, kind=kind, payload=payload))
            return records
        finally:
            if batch_ptr.value:
                lib._lib.lattice_stream_batch_free(batch_ptr)

    def get_stream_offset(self, stream: str, consumer: str) -> Optional[int]:
        """Return a durable consumer offset, or None if it has not been set."""
        if self._handle is None:
            raise RuntimeError("Database is not open")

        lib = get_lib()
        stream_bytes = stream.encode("utf-8")
        consumer_bytes = consumer.encode("utf-8")
        exists = ctypes.c_bool()
        sequence = ctypes.c_uint64()
        code = lib._lib.lattice_stream_get_offset(
            self._handle,
            stream_bytes,
            len(stream_bytes),
            consumer_bytes,
            len(consumer_bytes),
            byref(exists),
            byref(sequence),
        )
        check_error(code)
        return sequence.value if exists.value else None

    def changes(
        self,
        *,
        after_sequence: int = 0,
        limit: int = 100,
        timeout_ms: int = 0,
    ) -> List[StreamRecord]:
        """Read graph changefeed records from ``__lattice_changes``."""
        return self.read_stream(
            "__lattice_changes",
            after_sequence=after_sequence,
            limit=limit,
            timeout_ms=timeout_ms,
        )

    def vector_search(
        self,
        vector: "NDArray[np.float32]",
        *,
        k: int = 10,
        ef_search: int = 64,
    ) -> List[VectorSearchResult]:
        """
        Search for similar vectors using HNSW index.

        Args:
            vector: Query vector (numpy array of float32).
            k: Number of results to return.
            ef_search: HNSW ef parameter for search (0 for default).

        Returns:
            List of search results with node IDs and distances, sorted by similarity.
        """
        if self._handle is None:
            raise RuntimeError("Database is not open")

        import numpy as np

        # Ensure vector is float32 contiguous array
        if not isinstance(vector, np.ndarray):
            vector = np.array(vector, dtype=np.float32)
        elif vector.dtype != np.float32:
            vector = vector.astype(np.float32)
        if not vector.flags["C_CONTIGUOUS"]:
            vector = np.ascontiguousarray(vector)

        lib = get_lib()
        result_ptr = c_void_p()

        # Call the C API
        code = lib._lib.lattice_vector_search(
            self._handle,
            vector.ctypes.data_as(ctypes.POINTER(ctypes.c_float)),
            len(vector),
            k,
            ef_search,
            byref(result_ptr),
        )
        check_error(code)

        try:
            # Get result count
            count = lib._lib.lattice_vector_result_count(result_ptr)

            # Collect results
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
            # Free the result handle
            if result_ptr.value:
                lib._lib.lattice_vector_result_free(result_ptr)

    def create_node_fts_index(self, label: str, prop: str) -> None:
        """
        Declare a full-text index over one label/property pair.

        The property holds the text. Declaring the index reads it from every node
        already carrying the label, and writes maintain it from then on. Only
        string properties are indexed.

        Args:
            label: Node label the index covers.
            prop: Property holding the text.
        """
        if self._handle is None:
            raise RuntimeError("Database is not open")
        lib = get_lib()
        code = lib._lib.lattice_node_fts_index_create(
            self._handle, label.encode("utf-8"), prop.encode("utf-8")
        )
        check_error(code)

    def drop_node_fts_index(self, label: str, prop: str) -> None:
        """Remove a declared full-text index and everything it stored."""
        if self._handle is None:
            raise RuntimeError("Database is not open")
        lib = get_lib()
        code = lib._lib.lattice_node_fts_index_drop(
            self._handle, label.encode("utf-8"), prop.encode("utf-8")
        )
        check_error(code)

    def has_node_fts_index(self, label: str, prop: str) -> bool:
        """Whether a full-text index is declared for this label and property."""
        if self._handle is None:
            raise RuntimeError("Database is not open")
        lib = get_lib()
        exists = ctypes.c_bool(False)
        code = lib._lib.lattice_node_fts_index_exists(
            self._handle, label.encode("utf-8"), prop.encode("utf-8"), byref(exists)
        )
        check_error(code)
        return bool(exists.value)

    def create_edge_fts_index(self, edge_type: str, prop: str) -> None:
        """
        Declare a full-text index over one relationship type/property pair.

        A Cypher `-[x:TYPE]->` pattern with `x.property @@ "query"` searches it.
        """
        if self._handle is None:
            raise RuntimeError("Database is not open")
        lib = get_lib()
        code = lib._lib.lattice_edge_fts_index_create(
            self._handle, edge_type.encode("utf-8"), prop.encode("utf-8")
        )
        check_error(code)

    def drop_edge_fts_index(self, edge_type: str, prop: str) -> None:
        """Remove a declared relationship full-text index."""
        if self._handle is None:
            raise RuntimeError("Database is not open")
        lib = get_lib()
        code = lib._lib.lattice_edge_fts_index_drop(
            self._handle, edge_type.encode("utf-8"), prop.encode("utf-8")
        )
        check_error(code)

    def has_edge_fts_index(self, edge_type: str, prop: str) -> bool:
        """Whether an index is declared for this relationship type and property."""
        if self._handle is None:
            raise RuntimeError("Database is not open")
        lib = get_lib()
        exists = ctypes.c_bool(False)
        code = lib._lib.lattice_edge_fts_index_exists(
            self._handle, edge_type.encode("utf-8"), prop.encode("utf-8"), byref(exists)
        )
        check_error(code)
        return bool(exists.value)

    def fts_search(
        self,
        label: str,
        prop: str,
        query: str,
        *,
        limit: int = 10,
    ) -> List[FtsSearchResult]:
        """
        Full-text search of one declared index, using BM25 scoring.

        Args:
            label: Node label whose index to search.
            prop: Property whose index to search.
            query: Search query text.
            limit: Maximum number of results to return.

        Returns:
            List of search results with node IDs and BM25 scores, sorted by relevance.

        Raises:
            LatticeError: If no index is declared for this label and property. That
                is deliberately not an empty result: a mistyped property name and a
                query that found nothing are different situations.
        """
        if self._handle is None:
            raise RuntimeError("Database is not open")

        lib = get_lib()
        result_ptr = c_void_p()

        # Encode query to bytes
        query_bytes = query.encode("utf-8")

        # Call the C API
        code = lib._lib.lattice_fts_search(
            self._handle,
            label.encode("utf-8"),
            prop.encode("utf-8"),
            query_bytes,
            len(query_bytes),
            limit,
            byref(result_ptr),
        )
        check_error(code)

        try:
            # Get result count
            count = lib._lib.lattice_fts_result_count(result_ptr)

            # Collect results
            results: List[FtsSearchResult] = []
            for i in range(count):
                node_id = LatticeNodeId()
                score = ctypes.c_float()
                code = lib._lib.lattice_fts_result_get(
                    result_ptr, i, byref(node_id), byref(score)
                )
                check_error(code)
                results.append(
                    FtsSearchResult(node_id=node_id.value, score=score.value)
                )

            return results
        finally:
            # Free the result handle
            if result_ptr.value:
                lib._lib.lattice_fts_result_free(result_ptr)

    def fts_search_fuzzy(
        self,
        label: str,
        prop: str,
        query: str,
        *,
        limit: int = 10,
        max_distance: int = 0,
        min_term_length: int = 0,
    ) -> List[FtsSearchResult]:
        """
        Fuzzy full-text search of one declared index, with typo tolerance.

        Args:
            label: Node label whose index to search.
            prop: Property whose index to search.
            query: Search query text.
            limit: Maximum number of results to return.
            max_distance: Maximum edit distance for fuzzy matching (0 = default 2).
            min_term_length: Minimum term length to apply fuzzy matching (0 = default 4).

        Returns:
            List of search results with node IDs and BM25 scores, sorted by relevance.
        """
        if self._handle is None:
            raise RuntimeError("Database is not open")

        lib = get_lib()
        result_ptr = c_void_p()

        # Encode query to bytes
        query_bytes = query.encode("utf-8")

        # Call the C API
        code = lib._lib.lattice_fts_search_fuzzy(
            self._handle,
            label.encode("utf-8"),
            prop.encode("utf-8"),
            query_bytes,
            len(query_bytes),
            limit,
            max_distance,
            min_term_length,
            byref(result_ptr),
        )
        check_error(code)

        try:
            # Get result count
            count = lib._lib.lattice_fts_result_count(result_ptr)

            # Collect results
            results: List[FtsSearchResult] = []
            for i in range(count):
                node_id = LatticeNodeId()
                score = ctypes.c_float()
                code = lib._lib.lattice_fts_result_get(
                    result_ptr, i, byref(node_id), byref(score)
                )
                check_error(code)
                results.append(
                    FtsSearchResult(node_id=node_id.value, score=score.value)
                )

            return results
        finally:
            # Free the result handle
            if result_ptr.value:
                lib._lib.lattice_fts_result_free(result_ptr)

    def cache_clear(self) -> None:
        """
        Clear the query cache.

        Removes all cached parsed queries, forcing re-parsing on next execution.
        """
        if self._handle is None:
            raise RuntimeError("Database is not open")

        lib = get_lib()
        code = lib._lib.lattice_query_cache_clear(self._handle)
        check_error(code)

    def cache_stats(self) -> Dict[str, Any]:
        """
        Get query cache statistics.

        Returns:
            Dictionary with 'entries' (int), 'hits' (int), and 'misses' (int).
        """
        if self._handle is None:
            raise RuntimeError("Database is not open")

        lib = get_lib()
        entries = ctypes.c_uint32()
        hits = ctypes.c_uint64()
        misses = ctypes.c_uint64()
        code = lib._lib.lattice_query_cache_stats(
            self._handle,
            byref(entries),
            byref(hits),
            byref(misses),
        )
        check_error(code)
        return {
            "entries": entries.value,
            "hits": hits.value,
            "misses": misses.value,
        }

    @property
    def path(self) -> Path:
        """Return the database file path."""
        return self._path

    @property
    def is_open(self) -> bool:
        """Return True if the database is open."""
        return self._handle is not None and not self._closed


def deserialize(
    data: bytes,
    *,
    cache_size_mb: int = 100,
    enable_wal: bool = True,
    enable_adjacency_cache: bool = False,
    enable_vectors: bool = False,
    vector_dimensions: int = 128,
    lock: bool = True,
    copy: bool = True,
) -> Database:
    """Open a database from bytes produced by :meth:`Database.serialize`.

    Pair this with your own object storage client to keep many small databases
    in a bucket:

    .. code-block:: python

        blob = s3.get_object(Bucket=b, Key=k)["Body"].read()
        db = latticedb.deserialize(blob)
        db.query("CREATE (n:Note {text: 'hello'})")
        s3.put_object(Bucket=b, Key=k, Body=db.serialize(), IfMatch=etag)

    The bytes are copied, so you may discard ``data`` as soon as this returns.
    Changes made afterwards do not travel back to it; call
    :meth:`Database.serialize` again to get the new bytes.

    Pass ``copy=False`` to point at ``data`` instead of duplicating it, which
    halves what a freshly loaded database costs in memory. Each page becomes a
    copy of its own the first time it is written, so reading a database and
    changing a little of it keeps one copy of nearly all of it, and ``data``
    itself is never modified. The returned database keeps a reference to ``data``
    for as long as it is open, so there is nothing for you to keep alive by hand.

    Passing ``IfMatch`` above is worth the trouble. Two workers that read the
    same object, change it, and write it back will otherwise silently overwrite
    each other, and nothing reports an error when they do.
    """
    lib = get_lib()
    if not getattr(lib, "_has_serialize", False):
        raise LatticeUnsupportedError(
            "the native library is too old to support deserialize"
        )

    opts = OpenOptionsV4(
        struct_size=ctypes.sizeof(OpenOptionsV4),
        create=False,
        read_only=False,
        cache_size_mb=cache_size_mb,
        page_size=4096,
        enable_vector=enable_vectors,
        vector_dimensions=vector_dimensions,
        enable_wal=enable_wal,
        enable_adjacency_cache=enable_adjacency_cache,
        lock=lock,
    )

    db_ptr = c_void_p()
    if copy:
        owned = (c_ubyte * len(data)).from_buffer_copy(data)
        code = lib._lib.lattice_deserialize(owned, len(data), byref(opts), byref(db_ptr))
    else:
        if not getattr(lib, "_has_deserialize_borrowed", False):
            raise LatticeUnsupportedError(
                "the native library is too old to deserialize without copying"
            )
        # Going through c_char_p points at the caller's buffer rather than
        # duplicating it, which is the whole point of this branch. from_buffer
        # refuses immutable bytes, and from_buffer_copy would copy.
        borrowed = ctypes.cast(ctypes.c_char_p(data), POINTER(c_ubyte))
        code = lib._lib.lattice_deserialize_borrowed(
            borrowed, len(data), byref(opts), byref(db_ptr)
        )
    check_error(code)

    # The handle is already open, so the wrapper is built around it rather than
    # being asked to open a path of its own.
    db = Database.__new__(Database)
    db._path = Path("<deserialized>")
    db._create = False
    db._read_only = False
    db._cache_size_mb = cache_size_mb
    db._enable_wal = enable_wal
    db._enable_adjacency_cache = enable_adjacency_cache
    db._enable_vectors = enable_vectors
    db._vector_dimensions = vector_dimensions
    db._lock = lock
    db._handle = db_ptr
    db._closed = False
    # Borrowed bytes have to outlive the database. Holding the reference here is
    # what makes that true without asking the caller to think about it.
    db._borrowed = None if copy else data
    return db
