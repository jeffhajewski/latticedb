"""
Database class for Lattice Python bindings.
"""

import ctypes
from ctypes import byref, c_void_p
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

if TYPE_CHECKING:
    import numpy as np
    from numpy.typing import NDArray

from lattice._bindings import (
    LATTICE_TXN_READ_ONLY,
    LATTICE_VALUE_NULL,
    LatticeFtsResult,
    LatticeNodeId,
    LatticeValue,
    LatticeVectorResult,
    OpenOptions,
    check_error,
    get_lib,
    python_to_value,
    value_to_python,
)
from lattice.transaction import Transaction
from lattice.types import Node, QueryResult, VectorSearchResult, FtsSearchResult


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
        enable_vector: bool = False,
        vector_dimensions: int = 128,
    ) -> None:
        """
        Open a Lattice database.

        Args:
            path: Path to the database file.
            create: Create the database if it doesn't exist.
            read_only: Open in read-only mode.
            cache_size_mb: Size of the page cache in megabytes.
            enable_vector: Enable vector storage for embeddings.
            vector_dimensions: Dimension of vectors (required if enable_vector=True).
        """
        self._path = Path(path)
        self._create = create
        self._read_only = read_only
        self._cache_size_mb = cache_size_mb
        self._enable_vector = enable_vector
        self._vector_dimensions = vector_dimensions
        self._handle: Optional[Any] = None
        self._closed = False

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
        opts = OpenOptions(
            create=self._create,
            read_only=self._read_only,
            cache_size_mb=self._cache_size_mb,
            page_size=4096,
            enable_vector=self._enable_vector,
            vector_dimensions=self._vector_dimensions,
        )
        db_ptr = c_void_p()
        code = lib._lib.lattice_open(
            str(self._path).encode("utf-8"),
            byref(opts),
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
            parameters: Query parameters. Supports scalar types (None, bool, int,
                float, str, bytes) and numpy arrays for vector parameters.

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
            code = lib._lib.lattice_begin(
                self._handle,
                LATTICE_TXN_READ_ONLY,
                byref(txn_ptr),
            )
            check_error(code)

            # Execute the query
            code = lib._lib.lattice_query_execute(
                query_ptr,
                txn_ptr,
                byref(result_ptr),
            )
            check_error(code)

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
                    if code == 0:  # LATTICE_OK
                        row[col_name] = value_to_python(c_value)
                    else:
                        row[col_name] = None
                rows.append(row)

            return QueryResult(columns=columns, _rows=rows)

        finally:
            # Clean up in reverse order
            if result_ptr.value:
                lib._lib.lattice_result_free(result_ptr)
            if txn_ptr.value:
                lib._lib.lattice_rollback(txn_ptr)
            if query_ptr.value:
                lib._lib.lattice_query_free(query_ptr)

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

    def fts_search(
        self,
        query: str,
        *,
        limit: int = 10,
    ) -> List[FtsSearchResult]:
        """
        Full-text search using BM25 scoring.

        Args:
            query: Search query text.
            limit: Maximum number of results to return.

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
        code = lib._lib.lattice_fts_search(
            self._handle,
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

    @property
    def path(self) -> Path:
        """Return the database file path."""
        return self._path

    @property
    def is_open(self) -> bool:
        """Return True if the database is open."""
        return self._handle is not None and not self._closed
