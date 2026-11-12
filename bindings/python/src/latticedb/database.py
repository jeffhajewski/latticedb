"""
Database class for Lattice Python bindings.
"""

import ctypes
import warnings
from ctypes import POINTER, byref, c_size_t, c_ubyte, c_void_p
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

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
    OpenOptionsV5,
    LATTICE_CHANGEFEED_RETENTION_AGE,
    LATTICE_CHANGEFEED_RETENTION_ALL,
    LATTICE_CHANGEFEED_RETENTION_LAST,
    LATTICE_CHANGEFEED_RETENTION_OFF,
    LATTICE_FULL_FSYNC_OFF,
    LATTICE_FULL_FSYNC_ON,
    LATTICE_SYNC_MODE_FULL,
    LATTICE_SYNC_MODE_NORMAL,
    LATTICE_VECTOR_METRIC_COSINE,
    LATTICE_VECTOR_METRIC_DOT_PRODUCT,
    LATTICE_VECTOR_METRIC_EUCLIDEAN,
    LATTICE_CHECKPOINT_FULL,
    LATTICE_CHECKPOINT_PASSIVE,
    LATTICE_CHECKPOINT_TRUNCATE,
    LatticeCheckpointStats,
    LATTICE_OK,
    LATTICE_ERROR_IO,
    LATTICE_ERROR_UNSUPPORTED,
    LatticeChangesTrimmedError,
    LatticeNotFoundError,
    LatticeUnsupportedError,
    check_error,
    check_query_error,
    get_lib,
    python_to_value,
    value_to_python,
)
from latticedb.transaction import Transaction
from latticedb.types import (
    ChangefeedStatus,
    CheckpointStats,
    QueryResult,
    VectorSearchResult,
    FtsSearchResult,
    StreamRecord,
)


_SYNC_MODES = {"full": LATTICE_SYNC_MODE_FULL, "normal": LATTICE_SYNC_MODE_NORMAL}
_VECTOR_METRICS = {
    "cosine": LATTICE_VECTOR_METRIC_COSINE,
    "euclidean": LATTICE_VECTOR_METRIC_EUCLIDEAN,
    "dot_product": LATTICE_VECTOR_METRIC_DOT_PRODUCT,
}
_CHECKPOINT_MODES = {
    "passive": LATTICE_CHECKPOINT_PASSIVE,
    "full": LATTICE_CHECKPOINT_FULL,
    "truncate": LATTICE_CHECKPOINT_TRUNCATE,
}
_MAX_U64 = 2**64 - 1


def _sync_mode_code(sync_mode: Optional[str]) -> int:
    if sync_mode is None:
        return 0
    try:
        return _SYNC_MODES[sync_mode]
    except KeyError:
        raise ValueError(
            f"sync_mode must be 'full' or 'normal', not {sync_mode!r}"
        ) from None


# Duration suffixes of an "age:" policy, longest-named first so "ms" is not
# read as "s".
_DURATION_UNITS = (("ms", 1), ("s", 1000), ("m", 60_000), ("h", 3_600_000), ("d", 86_400_000))
_U64_MAX = (1 << 64) - 1


def _changefeed_retention_code(policy: Optional[str]) -> Tuple[int, int]:
    """Turn a policy such as ``"last:1000"`` into the C API's mode and value."""
    if policy is None:
        return (0, 0)
    if not isinstance(policy, str):
        raise TypeError(f"changefeed_retention must be a string, not {type(policy).__name__}")
    lowered = policy.lower()
    if lowered == "all":
        return (LATTICE_CHANGEFEED_RETENTION_ALL, 0)
    if lowered == "off":
        return (LATTICE_CHANGEFEED_RETENTION_OFF, 0)
    if policy.startswith("last:"):
        digits = policy[5:]
        if digits.isdigit() and 0 < int(digits) <= _U64_MAX:
            return (LATTICE_CHANGEFEED_RETENTION_LAST, int(digits))
    elif policy.startswith("age:"):
        text = policy[4:]
        for suffix, ms in _DURATION_UNITS:
            number = text[: -len(suffix)]
            if text.endswith(suffix) and number.isdigit():
                value = int(number) * ms
                if 0 < value <= _U64_MAX:
                    return (LATTICE_CHANGEFEED_RETENTION_AGE, value)
                break
    raise ValueError(
        "changefeed_retention must be 'all', 'off', 'last:<events>' or "
        f"'age:<duration>' (such as 'age:7d', 'age:12h', 'age:500ms'), not {policy!r}"
    )


def _changefeed_retention_text(mode: int, value: int) -> str:
    if mode == LATTICE_CHANGEFEED_RETENTION_OFF:
        return "off"
    if mode == LATTICE_CHANGEFEED_RETENTION_LAST:
        return f"last:{value}"
    if mode == LATTICE_CHANGEFEED_RETENTION_AGE:
        for suffix, ms in reversed(_DURATION_UNITS[1:]):
            if value % ms == 0:
                return f"age:{value // ms}{suffix}"
        return f"age:{value}ms"
    return "all"


def _vector_metric_code(vector_metric: Optional[str]) -> int:
    if vector_metric is None:
        return 0
    try:
        return _VECTOR_METRICS[vector_metric]
    except KeyError:
        raise ValueError(
            "vector_metric must be 'cosine', 'euclidean' or 'dot_product', "
            f"not {vector_metric!r}"
        ) from None


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
        cache_size_mb: Optional[int] = None,
        enable_wal: bool = True,
        enable_adjacency_cache: bool = False,
        enable_vectors: Optional[bool] = None,
        enable_vector: Optional[bool] = None,
        vector_dimensions: int = 128,
        lock: bool = True,
        sync_mode: Optional[str] = None,
        full_fsync: Optional[bool] = None,
        vector_metric: Optional[str] = None,
        vector_m: Optional[int] = None,
        vector_ef_construction: Optional[int] = None,
        changefeed_retention: Optional[str] = None,
    ) -> None:
        """
        Open a Lattice database.

        Args:
            path: Path to the database file.
            create: Create the database if it doesn't exist.
            read_only: Open in read-only mode.
            cache_size_mb: Size of the page cache in megabytes. Unset, the
                ``LATTICE_BUFFER_POOL_MB`` environment variable decides if it is
                set, else 100; a size given here wins over the variable.
            enable_wal: Log every commit, so it survives a crash. Without the
                log, transactions work the same and changes reach the file
                when it is next flushed (at close, for one).
            enable_adjacency_cache: Accepted for compatibility. Queries and the
                other methods here read edges from storage either way, so it
                does not change their speed or results.
            enable_vectors: Create a vector index if the database has none. A
                database that already has one opens with it whatever this says.
            enable_vector: Compatibility alias for ``enable_vectors``.
            vector_dimensions: Dimension of vectors when ``enable_vectors`` is set.
                It must match the index the file records, if it has one, or
                opening raises :class:`LatticeConfigMismatchError`; pass ``0``
                to take the file's dimensions.
            lock: Take a lock on the file so two processes cannot tread on each
                other. A read-write handle takes it exclusively and a read-only
                handle shares it, so opening raises
                :class:`LatticeDatabaseLockedError` rather than waiting. Turn this
                off only where locking does not work, such as some network
                filesystems; it does not make concurrent access safe.
            sync_mode: When a commit waits for the disk. ``"full"`` syncs the
                log on every commit, so a commit that has returned survives a
                power cut. ``"normal"`` hands the commit to the operating
                system and returns: it survives the process being killed, but a
                power cut or OS crash can lose the most recent commits. Unset
                uses the ``LATTICE_SYNC_MODE`` environment variable, else full.
            full_fsync: On macOS, whether syncs ask the drive to flush its cache
                (``F_FULLFSYNC``). No effect elsewhere. Unset uses the
                ``LATTICE_FULL_FSYNC`` environment variable, else off.
            vector_metric: How a new vector index measures distance:
                ``"cosine"`` (``1 - cos``, vectors stored at unit length),
                ``"euclidean"`` (L2 distance) or ``"dot_product"`` (negative
                dot product). Unset means the file's metric, or cosine for a new
                index.
            vector_m: HNSW connections per node for a new index, 2..32. Unset
                means the file's, or 16.
            vector_ef_construction: HNSW candidate list size while building a
                new index, 1..4096. Unset means the file's, or 200.
            changefeed_retention: How much of the changefeed (see
                :meth:`changes`) to keep: ``"all"``, ``"off"``,
                ``"last:<events>"`` or ``"age:<duration>"`` with a duration
                such as ``"500ms"``, ``"90s"``, ``"30m"``, ``"12h"`` or
                ``"7d"``. The file records its policy. Unset keeps the
                file's, which is ``"all"`` unless something set another (for
                a new database, the ``LATTICE_CHANGEFEED_RETENTION``
                environment variable). A value set here replaces the file's
                policy when the database is opened for writing; a read-only
                open never changes it. Retention drops the oldest events at
                checkpoints; :meth:`trim_changefeed` applies it at once.

        The vector settings are fixed once the index exists: any that is set
        must match the file, or opening raises
        :class:`LatticeConfigMismatchError`. ``sync_mode``, ``full_fsync``,
        ``vector_metric``, ``vector_m`` and ``vector_ef_construction`` need a
        native library with ``lattice_open_v5``, and ``changefeed_retention``
        one with ``lattice_changefeed_status``; setting one against an older
        library raises :class:`LatticeUnsupportedError`.
        """
        self._path = Path(path)
        self._create = create
        self._read_only = read_only
        self._cache_size_mb = 0 if cache_size_mb is None else cache_size_mb
        self._enable_wal = enable_wal
        self._enable_adjacency_cache = enable_adjacency_cache
        if enable_vector is not None:
            warnings.warn(
                "Database(..., enable_vector=...) is deprecated; use enable_vectors=....",
                DeprecationWarning,
                stacklevel=2,
            )
        self._enable_vectors = (
            enable_vectors if enable_vectors is not None else bool(enable_vector)
        )
        self._vector_dimensions = vector_dimensions
        self._lock = lock
        self._sync_mode = _sync_mode_code(sync_mode)
        if full_fsync is None:
            self._full_fsync = 0
        else:
            self._full_fsync = LATTICE_FULL_FSYNC_ON if full_fsync else LATTICE_FULL_FSYNC_OFF
        self._vector_metric = _vector_metric_code(vector_metric)
        self._vector_m = vector_m or 0
        self._vector_ef_construction = vector_ef_construction or 0
        self._changefeed_retention = _changefeed_retention_code(changefeed_retention)
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
            # One copy; slicing the POINTER would build a list of ints first.
            return ctypes.string_at(out, length.value)
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
        if self._changefeed_retention[0] and not getattr(lib, "_has_changefeed_status", False):
            raise LatticeUnsupportedError(
                "the native library is too old to support changefeed_retention",
                LATTICE_ERROR_UNSUPPORTED,
            )
        if getattr(lib, "_has_lattice_open_v5", False):
            opts_v5 = OpenOptionsV5(
                struct_size=ctypes.sizeof(OpenOptionsV5),
                create=self._create,
                read_only=self._read_only,
                cache_size_mb=self._cache_size_mb,
                page_size=4096,
                enable_vector=self._enable_vectors,
                vector_dimensions=self._vector_dimensions,
                enable_wal=self._enable_wal,
                enable_adjacency_cache=self._enable_adjacency_cache,
                lock=self._lock,
                sync_mode=self._sync_mode,
                full_fsync=self._full_fsync,
                vector_metric=self._vector_metric,
                vector_m=self._vector_m,
                vector_ef_construction=self._vector_ef_construction,
                changefeed_retention=self._changefeed_retention[0],
                changefeed_retention_value=self._changefeed_retention[1],
            )
            code = lib._lib.lattice_open_v5(
                str(self._path).encode("utf-8"),
                byref(opts_v5),
                byref(db_ptr),
            )
            check_error(code)
            self._handle = db_ptr
            return

        if (
            self._sync_mode
            or self._full_fsync
            or self._vector_metric
            or self._vector_m
            or self._vector_ef_construction
        ):
            raise LatticeUnsupportedError(
                "lattice_open_v5 is required for sync_mode, full_fsync, vector_metric, "
                "vector_m and vector_ef_construction",
                LATTICE_ERROR_UNSUPPORTED,
            )
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
                # Only a successful commit ends the transaction: after a failed
                # one it is still open and holds the writer slot, so leave it
                # for `finally` to roll back.
                code = lib._lib.lattice_commit(txn_ptr)
                if code == LATTICE_OK:
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
        """Read graph changefeed records from ``__lattice_changes``.

        Returns up to ``limit`` records with a sequence greater than
        ``after_sequence``, oldest first; ``after_sequence=0`` reads from the
        oldest event held. With ``timeout_ms`` it waits that long for a
        record when there is none yet.

        Raises :class:`LatticeChangesTrimmedError` (a
        :class:`LatticeNotFoundError`) when retention or a trim has dropped
        the events after ``after_sequence``: the reader has missed changes.
        Raises :class:`LatticeUnsupportedError` when the retention policy is
        ``"off"``.
        """
        try:
            return self.read_stream(
                "__lattice_changes",
                after_sequence=after_sequence,
                limit=limit,
                timeout_ms=timeout_ms,
            )
        except LatticeNotFoundError as exc:
            raise LatticeChangesTrimmedError(
                f"the changefeed no longer holds the events after sequence {after_sequence}: "
                "its retention policy or a trim dropped them",
                exc.code,
            ) from None
        except LatticeUnsupportedError as exc:
            raise LatticeUnsupportedError(
                "the changefeed is off (changefeed_retention='off')", exc.code
            ) from None

    def changefeed_status(self) -> ChangefeedStatus:
        """Return the changefeed's retention policy and the events it holds."""
        if self._handle is None:
            raise RuntimeError("Database is not open")
        lib = get_lib()
        if not getattr(lib, "_has_changefeed_status", False):
            raise LatticeUnsupportedError(
                "the native library is too old to report the changefeed's status",
                LATTICE_ERROR_UNSUPPORTED,
            )
        mode = ctypes.c_uint8()
        value = ctypes.c_uint64()
        oldest = ctypes.c_uint64()
        last = ctypes.c_uint64()
        code = lib._lib.lattice_changefeed_status(
            self._handle, byref(mode), byref(value), byref(oldest), byref(last)
        )
        check_error(code)
        return ChangefeedStatus(
            retention=_changefeed_retention_text(mode.value, value.value),
            oldest_sequence=oldest.value or None,
            last_sequence=last.value,
        )

    def trim_changefeed(self) -> int:
        """Apply the changefeed's retention policy in full now.

        Checkpoints apply it a bounded number of events at a time; this
        drops everything the policy no longer keeps and makes that durable.
        Returns the number of events dropped. Raises
        :class:`LatticeLockTimeoutError` while a write transaction is open.
        """
        if self._handle is None:
            raise RuntimeError("Database is not open")
        lib = get_lib()
        if not getattr(lib, "_has_changefeed_status", False):
            raise LatticeUnsupportedError(
                "the native library is too old to trim the changefeed",
                LATTICE_ERROR_UNSUPPORTED,
            )
        removed = ctypes.c_uint64()
        check_error(lib._lib.lattice_changefeed_trim(self._handle, byref(removed)))
        return removed.value

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

    def checkpoint(self, mode: str = "truncate") -> CheckpointStats:
        """Fold the log (the ``-wal`` file) into the database file now.

        A commit is durable once it is in the log; a checkpoint writes the
        pages the log's commits changed into the database file and can then
        empty the log.

        Args:
            mode: ``"passive"`` writes the changed pages unless one is in use,
                in which case it writes nothing and reports 0 pages; the log is
                left in place. ``"full"`` writes every changed page and leaves
                the log in place. ``"truncate"`` (the default, and what automatic
                checkpoints do) is ``"full"`` and then empties the log; it is the
                only mode that bounds the log's size.

        Returns:
            A :class:`CheckpointStats` saying what the checkpoint did.

        Raises:
            ValueError: ``mode`` is not one of the three above.
            LatticeReadOnlyError: The database was opened read-only.
            LatticeLockTimeoutError: A transaction already has records in the
                log, so a checkpoint would cut its commit off from them.
            LatticeIOError: Writing failed; the log still holds every commit.
            LatticeUnsupportedError: The native library has no
                ``lattice_checkpoint``.
        """
        try:
            mode_code = _CHECKPOINT_MODES[mode]
        except (KeyError, TypeError):
            raise ValueError(
                f"mode must be 'passive', 'full' or 'truncate', not {mode!r}"
            ) from None
        if self._handle is None:
            raise RuntimeError("Database is not open")

        lib = get_lib()
        if not getattr(lib, "_has_checkpoint", False):
            raise LatticeUnsupportedError(
                "the native library is too old to support checkpoint"
            )

        stats = LatticeCheckpointStats()
        code = lib._lib.lattice_checkpoint(self._handle, mode_code, byref(stats))
        check_error(code)
        return CheckpointStats(
            pages_flushed=int(stats.pages_flushed),
            duration_ns=int(stats.duration_ns),
            checkpoint_lsn=int(stats.checkpoint_lsn),
            wal_truncated=bool(stats.wal_truncated),
        )

    def set_auto_checkpoint(self, max_wal_frames: int, min_interval_ms: int = 0) -> None:
        """Set when commits checkpoint from now on.

        A commit runs a ``"truncate"`` checkpoint once the log holds
        ``max_wal_frames`` frames (one frame is one page, usually 4096 bytes),
        and no sooner than ``min_interval_ms`` after the last automatic one.
        ``max_wal_frames=0`` switches automatic checkpoints off: the log grows
        until :meth:`checkpoint` runs, though closing still folds it into the
        file. Without this call the default is 1000 frames, or the
        ``LATTICE_AUTO_CHECKPOINT_FRAMES`` environment variable (0 for none);
        this call wins over the variable. It has no effect on a read-only
        database.

        A larger interval means fewer checkpoints, each a longer pause for the
        commit that runs it, a longer log on disk and more to replay after a
        crash. A smaller one means shorter pauses, more often.

        Raises:
            TypeError: An argument is not an ``int``.
            ValueError: An argument is negative or does not fit in 64 bits.
            LatticeUnsupportedError: The native library has no
                ``lattice_set_auto_checkpoint``.
        """
        args = (("max_wal_frames", max_wal_frames), ("min_interval_ms", min_interval_ms))
        for name, value in args:
            if isinstance(value, bool) or not isinstance(value, int):
                raise TypeError(f"{name} must be an int, not {type(value).__name__}")
            if value < 0 or value > _MAX_U64:
                raise ValueError(f"{name} must be between 0 and 2**64 - 1, not {value}")
        if self._handle is None:
            raise RuntimeError("Database is not open")

        lib = get_lib()
        if not getattr(lib, "_has_set_auto_checkpoint", False):
            raise LatticeUnsupportedError(
                "the native library is too old to support set_auto_checkpoint"
            )

        code = lib._lib.lattice_set_auto_checkpoint(self._handle, max_wal_frames, min_interval_ms)
        check_error(code)

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
    cache_size_mb: Optional[int] = None,
    enable_wal: bool = True,
    enable_adjacency_cache: bool = False,
    enable_vectors: bool = False,
    vector_dimensions: int = 128,
    lock: bool = True,
    sync_mode: Optional[str] = None,
    full_fsync: Optional[bool] = None,
    vector_metric: Optional[str] = None,
    vector_m: Optional[int] = None,
    vector_ef_construction: Optional[int] = None,
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

    The options mean what they mean to :class:`Database`. The vector settings
    are fixed once the index exists, so bytes with an index open with the one
    they record: any vector option that is set must match it, or this raises
    :class:`LatticeConfigMismatchError`. ``enable_vectors`` creates an index in
    bytes that have none, configured by the vector options. ``sync_mode``,
    ``full_fsync``, ``vector_metric``, ``vector_m`` and
    ``vector_ef_construction`` need a native library with
    ``lattice_deserialize_v5``; setting one against an older library raises
    :class:`LatticeUnsupportedError`.

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
    if not copy and not getattr(lib, "_has_deserialize_borrowed", False):
        raise LatticeUnsupportedError(
            "the native library is too old to deserialize without copying"
        )

    sync_mode_code = _sync_mode_code(sync_mode)
    if full_fsync is None:
        full_fsync_code = 0
    else:
        full_fsync_code = LATTICE_FULL_FSYNC_ON if full_fsync else LATTICE_FULL_FSYNC_OFF
    vector_metric_code = _vector_metric_code(vector_metric)
    vector_m_value = vector_m or 0
    vector_ef_construction_value = vector_ef_construction or 0
    cache_size = 0 if cache_size_mb is None else cache_size_mb

    if copy:
        # One copy, owned here for the length of the call.
        buffer: Any = (c_ubyte * len(data)).from_buffer_copy(data)
    else:
        # Going through c_char_p points at the caller's buffer rather than
        # duplicating it, which is the whole point of this branch. from_buffer
        # refuses immutable bytes, and from_buffer_copy would copy.
        buffer = ctypes.cast(ctypes.c_char_p(data), POINTER(c_ubyte))

    db_ptr = c_void_p()
    has_v5 = getattr(
        lib, "_has_deserialize_v5" if copy else "_has_deserialize_borrowed_v5", False
    )
    if has_v5:
        opts_v5 = OpenOptionsV5(
            struct_size=ctypes.sizeof(OpenOptionsV5),
            create=False,
            read_only=False,
            cache_size_mb=cache_size,
            page_size=4096,
            enable_vector=enable_vectors,
            vector_dimensions=vector_dimensions,
            enable_wal=enable_wal,
            enable_adjacency_cache=enable_adjacency_cache,
            lock=lock,
            sync_mode=sync_mode_code,
            full_fsync=full_fsync_code,
            vector_metric=vector_metric_code,
            vector_m=vector_m_value,
            vector_ef_construction=vector_ef_construction_value,
        )
        fn = lib._lib.lattice_deserialize_v5 if copy else lib._lib.lattice_deserialize_borrowed_v5
        code = fn(buffer, len(data), byref(opts_v5), byref(db_ptr))
    else:
        if (
            sync_mode_code
            or full_fsync_code
            or vector_metric_code
            or vector_m_value
            or vector_ef_construction_value
        ):
            raise LatticeUnsupportedError(
                "lattice_deserialize_v5 is required for sync_mode, full_fsync, "
                "vector_metric, vector_m and vector_ef_construction",
                LATTICE_ERROR_UNSUPPORTED,
            )
        opts = OpenOptionsV4(
            struct_size=ctypes.sizeof(OpenOptionsV4),
            create=False,
            read_only=False,
            cache_size_mb=cache_size,
            page_size=4096,
            enable_vector=enable_vectors,
            vector_dimensions=vector_dimensions,
            enable_wal=enable_wal,
            enable_adjacency_cache=enable_adjacency_cache,
            lock=lock,
        )
        fn = lib._lib.lattice_deserialize if copy else lib._lib.lattice_deserialize_borrowed
        code = fn(buffer, len(data), byref(opts), byref(db_ptr))
    check_error(code)

    # The handle is already open, so the wrapper is built around it rather than
    # being asked to open a path of its own.
    db = Database.__new__(Database)
    db._path = Path("<deserialized>")
    db._create = False
    db._read_only = False
    db._cache_size_mb = cache_size
    db._enable_wal = enable_wal
    db._enable_adjacency_cache = enable_adjacency_cache
    db._enable_vectors = enable_vectors
    db._vector_dimensions = vector_dimensions
    db._lock = lock
    db._sync_mode = sync_mode_code
    db._full_fsync = full_fsync_code
    db._vector_metric = vector_metric_code
    db._vector_m = vector_m_value
    db._vector_ef_construction = vector_ef_construction_value
    db._handle = db_ptr
    db._closed = False
    # Borrowed bytes have to outlive the database. Holding the reference here is
    # what makes that true without asking the caller to think about it.
    db._borrowed = None if copy else data
    return db
