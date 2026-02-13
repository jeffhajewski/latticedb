"""
Low-level ctypes bindings to liblattice.

This module provides the raw FFI interface to the Lattice C API.
Users should use the high-level Python API instead.
"""

import ctypes
import os
import sys
from ctypes import (
    POINTER,
    Structure,
    Union,
    c_bool,
    c_char_p,
    c_double,
    c_int,
    c_int64,
    c_size_t,
    c_uint16,
    c_uint32,
    c_uint64,
    c_void_p,
)
from pathlib import Path
from typing import Optional


def _get_lib_name() -> str:
    """Get the library filename for the current platform."""
    return {
        "darwin": "liblattice.dylib",
        "linux": "liblattice.so",
        "win32": "lattice.dll",
    }.get(sys.platform, "liblattice.so")


def _find_library() -> Optional[Path]:
    """Find the lattice shared library.

    Search order:
    1. LATTICE_LIB_PATH environment variable (explicit path)
    2. Package lib directory (bundled in wheel)
    3. Development build directory (zig-out/lib)
    4. Homebrew paths (macOS)
    5. System library paths
    """
    lib_name = _get_lib_name()

    # 1. Check environment variable for explicit path
    env_path = os.environ.get("LATTICE_LIB_PATH")
    if env_path:
        path = Path(env_path)
        if path.exists():
            return path
        # If env var is a directory, look for lib inside
        if path.is_dir():
            lib_path = path / lib_name
            if lib_path.exists():
                return lib_path

    # 2. Package lib directory (for bundled wheels)
    package_lib = Path(__file__).parent / "lib" / lib_name
    if package_lib.exists():
        return package_lib

    # 3. Development: relative to bindings (zig-out/lib)
    dev_path = Path(__file__).parent.parent.parent.parent.parent / "zig-out" / "lib" / lib_name
    if dev_path.exists():
        return dev_path

    # 4. System paths
    system_paths = [
        Path("/usr/local/lib"),
        Path("/usr/lib"),
        Path.home() / ".local" / "lib",
    ]

    # Add Homebrew paths on macOS
    if sys.platform == "darwin":
        system_paths.insert(0, Path("/opt/homebrew/lib"))
        system_paths.insert(1, Path("/usr/local/opt/latticedb/lib"))

    for path in system_paths:
        lib_path = path / lib_name
        if lib_path.exists():
            return lib_path

    return None


# Error codes
LATTICE_OK = 0
LATTICE_ERROR = -1
LATTICE_ERROR_IO = -2
LATTICE_ERROR_CORRUPTION = -3
LATTICE_ERROR_NOT_FOUND = -4
LATTICE_ERROR_ALREADY_EXISTS = -5
LATTICE_ERROR_INVALID_ARG = -6
LATTICE_ERROR_TXN_ABORTED = -7
LATTICE_ERROR_LOCK_TIMEOUT = -8
LATTICE_ERROR_READ_ONLY = -9
LATTICE_ERROR_FULL = -10
LATTICE_ERROR_VERSION_MISMATCH = -11
LATTICE_ERROR_CHECKSUM = -12
LATTICE_ERROR_OUT_OF_MEMORY = -13


# Exception classes
class LatticeError(Exception):
    """Base exception for all Lattice errors."""

    def __init__(self, message: str, code: int = LATTICE_ERROR):
        super().__init__(message)
        self.code = code


class LatticeIOError(LatticeError):
    """I/O error during database operations."""

    pass


class LatticeCorruptionError(LatticeError):
    """Database corruption detected."""

    pass


class LatticeNotFoundError(LatticeError):
    """Requested item not found."""

    pass


class LatticeAlreadyExistsError(LatticeError):
    """Item already exists."""

    pass


class LatticeInvalidArgError(LatticeError):
    """Invalid argument provided."""

    pass


class LatticeTxnAbortedError(LatticeError):
    """Transaction was aborted."""

    pass


class LatticeLockTimeoutError(LatticeError):
    """Lock acquisition timed out."""

    pass


class LatticeReadOnlyError(LatticeError):
    """Cannot write to read-only database/transaction."""

    pass


class LatticeFullError(LatticeError):
    """Database is full."""

    pass


class LatticeVersionMismatchError(LatticeError):
    """Database version mismatch."""

    pass


class LatticeChecksumError(LatticeError):
    """Checksum verification failed."""

    pass


class LatticeOutOfMemoryError(LatticeError):
    """Out of memory."""

    pass


# Map error codes to exception classes
_ERROR_MAP = {
    LATTICE_ERROR: LatticeError,
    LATTICE_ERROR_IO: LatticeIOError,
    LATTICE_ERROR_CORRUPTION: LatticeCorruptionError,
    LATTICE_ERROR_NOT_FOUND: LatticeNotFoundError,
    LATTICE_ERROR_ALREADY_EXISTS: LatticeAlreadyExistsError,
    LATTICE_ERROR_INVALID_ARG: LatticeInvalidArgError,
    LATTICE_ERROR_TXN_ABORTED: LatticeTxnAbortedError,
    LATTICE_ERROR_LOCK_TIMEOUT: LatticeLockTimeoutError,
    LATTICE_ERROR_READ_ONLY: LatticeReadOnlyError,
    LATTICE_ERROR_FULL: LatticeFullError,
    LATTICE_ERROR_VERSION_MISMATCH: LatticeVersionMismatchError,
    LATTICE_ERROR_CHECKSUM: LatticeChecksumError,
    LATTICE_ERROR_OUT_OF_MEMORY: LatticeOutOfMemoryError,
}


# Transaction modes
LATTICE_TXN_READ_ONLY = 0
LATTICE_TXN_READ_WRITE = 1

# Value types
LATTICE_VALUE_NULL = 0
LATTICE_VALUE_BOOL = 1
LATTICE_VALUE_INT = 2
LATTICE_VALUE_FLOAT = 3
LATTICE_VALUE_STRING = 4
LATTICE_VALUE_BYTES = 5
LATTICE_VALUE_VECTOR = 6
LATTICE_VALUE_LIST = 7
LATTICE_VALUE_MAP = 8


class StringValue(Structure):
    _fields_ = [
        ("ptr", c_char_p),
        ("len", c_size_t),
    ]


class BytesValue(Structure):
    _fields_ = [
        ("ptr", POINTER(ctypes.c_uint8)),
        ("len", c_size_t),
    ]


class VectorValue(Structure):
    _fields_ = [
        ("ptr", POINTER(ctypes.c_float)),
        ("dimensions", c_uint32),
    ]


class ValueData(Union):
    _fields_ = [
        ("bool_val", c_bool),
        ("int_val", c_int64),
        ("float_val", c_double),
        ("string_val", StringValue),
        ("bytes_val", BytesValue),
        ("vector_val", VectorValue),
    ]


class LatticeValue(Structure):
    _fields_ = [
        ("type", c_int),
        ("data", ValueData),
    ]


class NodeWithVector(Structure):
    """Corresponds to lattice_node_with_vector in lattice.h."""

    _fields_ = [
        ("label", c_char_p),
        ("vector", POINTER(ctypes.c_float)),
        ("dimensions", c_uint32),
    ]


class OpenOptions(Structure):
    _fields_ = [
        ("create", c_bool),
        ("read_only", c_bool),
        ("cache_size_mb", c_uint32),
        ("page_size", c_uint32),
        ("enable_vector", c_bool),
        ("vector_dimensions", c_uint16),
    ]


# Type aliases
LatticeDatabase = c_void_p
LatticeTxn = c_void_p
LatticeQuery = c_void_p
LatticeResult = c_void_p
LatticeVectorResult = c_void_p
LatticeFtsResult = c_void_p
LatticeEdgeResult = c_void_p
LatticeEmbeddingClient = c_void_p
LatticeNodeId = c_uint64
LatticeEdgeId = c_uint64


class EmbeddingConfig(Structure):
    _fields_ = [
        ("endpoint", c_char_p),
        ("model", c_char_p),
        ("api_format", c_int),
        ("api_key", c_char_p),
        ("timeout_ms", c_uint32),
    ]


class LatticeLib:
    """Wrapper for the Lattice C library."""

    def __init__(self) -> None:
        lib_path = _find_library()
        if lib_path is None:
            raise RuntimeError(
                "Could not find liblattice. "
                "Make sure it is built and in the library search path."
            )

        self._lib = ctypes.CDLL(str(lib_path))
        self._setup_functions()

    def _setup_functions(self) -> None:
        """Set up function signatures."""
        # lattice_open
        self._lib.lattice_open.argtypes = [
            c_char_p,
            POINTER(OpenOptions),
            POINTER(LatticeDatabase),
        ]
        self._lib.lattice_open.restype = c_int

        # lattice_close
        self._lib.lattice_close.argtypes = [LatticeDatabase]
        self._lib.lattice_close.restype = c_int

        # lattice_begin
        self._lib.lattice_begin.argtypes = [
            LatticeDatabase,
            c_int,
            POINTER(LatticeTxn),
        ]
        self._lib.lattice_begin.restype = c_int

        # lattice_commit
        self._lib.lattice_commit.argtypes = [LatticeTxn]
        self._lib.lattice_commit.restype = c_int

        # lattice_rollback
        self._lib.lattice_rollback.argtypes = [LatticeTxn]
        self._lib.lattice_rollback.restype = c_int

        # lattice_node_create
        self._lib.lattice_node_create.argtypes = [
            LatticeTxn,
            c_char_p,
            POINTER(LatticeNodeId),
        ]
        self._lib.lattice_node_create.restype = c_int

        # lattice_node_delete
        self._lib.lattice_node_delete.argtypes = [LatticeTxn, LatticeNodeId]
        self._lib.lattice_node_delete.restype = c_int

        # lattice_node_set_property
        self._lib.lattice_node_set_property.argtypes = [
            LatticeTxn,
            LatticeNodeId,
            c_char_p,
            POINTER(LatticeValue),
        ]
        self._lib.lattice_node_set_property.restype = c_int

        # lattice_node_get_property
        self._lib.lattice_node_get_property.argtypes = [
            LatticeTxn,
            LatticeNodeId,
            c_char_p,
            POINTER(LatticeValue),
        ]
        self._lib.lattice_node_get_property.restype = c_int

        # lattice_node_exists
        self._lib.lattice_node_exists.argtypes = [
            LatticeTxn,
            LatticeNodeId,
            POINTER(c_bool),
        ]
        self._lib.lattice_node_exists.restype = c_int

        # lattice_node_get_labels
        self._lib.lattice_node_get_labels.argtypes = [
            LatticeTxn,
            LatticeNodeId,
            POINTER(c_char_p),
        ]
        self._lib.lattice_node_get_labels.restype = c_int

        # lattice_free_string
        self._lib.lattice_free_string.argtypes = [c_char_p]
        self._lib.lattice_free_string.restype = None

        # lattice_node_set_vector
        self._lib.lattice_node_set_vector.argtypes = [
            LatticeTxn,
            LatticeNodeId,
            c_char_p,
            POINTER(ctypes.c_float),
            c_uint32,
        ]
        self._lib.lattice_node_set_vector.restype = c_int

        # lattice_batch_insert
        self._lib.lattice_batch_insert.argtypes = [
            LatticeTxn,
            POINTER(NodeWithVector),
            c_uint32,
            POINTER(LatticeNodeId),
            POINTER(c_uint32),
        ]
        self._lib.lattice_batch_insert.restype = c_int

        # lattice_vector_search
        self._lib.lattice_vector_search.argtypes = [
            LatticeDatabase,
            POINTER(ctypes.c_float),
            c_uint32,
            c_uint32,
            c_uint16,
            POINTER(LatticeVectorResult),
        ]
        self._lib.lattice_vector_search.restype = c_int

        # lattice_vector_result_count
        self._lib.lattice_vector_result_count.argtypes = [LatticeVectorResult]
        self._lib.lattice_vector_result_count.restype = c_uint32

        # lattice_vector_result_get
        self._lib.lattice_vector_result_get.argtypes = [
            LatticeVectorResult,
            c_uint32,
            POINTER(LatticeNodeId),
            POINTER(ctypes.c_float),
        ]
        self._lib.lattice_vector_result_get.restype = c_int

        # lattice_vector_result_free
        self._lib.lattice_vector_result_free.argtypes = [LatticeVectorResult]
        self._lib.lattice_vector_result_free.restype = None

        # lattice_fts_index
        self._lib.lattice_fts_index.argtypes = [
            LatticeTxn,
            LatticeNodeId,
            c_char_p,
            c_size_t,
        ]
        self._lib.lattice_fts_index.restype = c_int

        # lattice_fts_search
        self._lib.lattice_fts_search.argtypes = [
            LatticeDatabase,
            c_char_p,
            c_size_t,
            c_uint32,
            POINTER(LatticeFtsResult),
        ]
        self._lib.lattice_fts_search.restype = c_int

        # lattice_fts_result_count
        self._lib.lattice_fts_result_count.argtypes = [LatticeFtsResult]
        self._lib.lattice_fts_result_count.restype = c_uint32

        # lattice_fts_result_get
        self._lib.lattice_fts_result_get.argtypes = [
            LatticeFtsResult,
            c_uint32,
            POINTER(LatticeNodeId),
            POINTER(ctypes.c_float),
        ]
        self._lib.lattice_fts_result_get.restype = c_int

        # lattice_fts_result_free
        self._lib.lattice_fts_result_free.argtypes = [LatticeFtsResult]
        self._lib.lattice_fts_result_free.restype = None

        # lattice_edge_create
        self._lib.lattice_edge_create.argtypes = [
            LatticeTxn,
            LatticeNodeId,
            LatticeNodeId,
            c_char_p,
            POINTER(LatticeEdgeId),
        ]
        self._lib.lattice_edge_create.restype = c_int

        # lattice_edge_delete
        self._lib.lattice_edge_delete.argtypes = [
            LatticeTxn,
            LatticeNodeId,
            LatticeNodeId,
            c_char_p,
        ]
        self._lib.lattice_edge_delete.restype = c_int

        # lattice_edge_get_outgoing
        self._lib.lattice_edge_get_outgoing.argtypes = [
            LatticeTxn,
            LatticeNodeId,
            POINTER(LatticeEdgeResult),
        ]
        self._lib.lattice_edge_get_outgoing.restype = c_int

        # lattice_edge_get_incoming
        self._lib.lattice_edge_get_incoming.argtypes = [
            LatticeTxn,
            LatticeNodeId,
            POINTER(LatticeEdgeResult),
        ]
        self._lib.lattice_edge_get_incoming.restype = c_int

        # lattice_edge_result_count
        self._lib.lattice_edge_result_count.argtypes = [LatticeEdgeResult]
        self._lib.lattice_edge_result_count.restype = c_uint32

        # lattice_edge_result_get
        self._lib.lattice_edge_result_get.argtypes = [
            LatticeEdgeResult,
            c_uint32,
            POINTER(LatticeNodeId),
            POINTER(LatticeNodeId),
            POINTER(c_char_p),
            POINTER(c_uint32),
        ]
        self._lib.lattice_edge_result_get.restype = c_int

        # lattice_edge_result_free
        self._lib.lattice_edge_result_free.argtypes = [LatticeEdgeResult]
        self._lib.lattice_edge_result_free.restype = None

        # lattice_query_prepare
        self._lib.lattice_query_prepare.argtypes = [
            LatticeDatabase,
            c_char_p,
            POINTER(LatticeQuery),
        ]
        self._lib.lattice_query_prepare.restype = c_int

        # lattice_query_bind
        self._lib.lattice_query_bind.argtypes = [
            LatticeQuery,
            c_char_p,
            POINTER(LatticeValue),
        ]
        self._lib.lattice_query_bind.restype = c_int

        # lattice_query_bind_vector
        self._lib.lattice_query_bind_vector.argtypes = [
            LatticeQuery,
            c_char_p,
            POINTER(ctypes.c_float),
            c_uint32,
        ]
        self._lib.lattice_query_bind_vector.restype = c_int

        # lattice_query_execute
        self._lib.lattice_query_execute.argtypes = [
            LatticeQuery,
            LatticeTxn,
            POINTER(LatticeResult),
        ]
        self._lib.lattice_query_execute.restype = c_int

        # lattice_query_free
        self._lib.lattice_query_free.argtypes = [LatticeQuery]
        self._lib.lattice_query_free.restype = None

        # lattice_result_next
        self._lib.lattice_result_next.argtypes = [LatticeResult]
        self._lib.lattice_result_next.restype = c_bool

        # lattice_result_column_count
        self._lib.lattice_result_column_count.argtypes = [LatticeResult]
        self._lib.lattice_result_column_count.restype = c_uint32

        # lattice_result_column_name
        self._lib.lattice_result_column_name.argtypes = [LatticeResult, c_uint32]
        self._lib.lattice_result_column_name.restype = c_char_p

        # lattice_result_get
        self._lib.lattice_result_get.argtypes = [
            LatticeResult,
            c_uint32,
            POINTER(LatticeValue),
        ]
        self._lib.lattice_result_get.restype = c_int

        # lattice_result_free
        self._lib.lattice_result_free.argtypes = [LatticeResult]
        self._lib.lattice_result_free.restype = None

        # lattice_version
        self._lib.lattice_version.argtypes = []
        self._lib.lattice_version.restype = c_char_p

        # lattice_error_message
        self._lib.lattice_error_message.argtypes = [c_int]
        self._lib.lattice_error_message.restype = c_char_p

        # lattice_hash_embed
        self._lib.lattice_hash_embed.argtypes = [
            c_char_p,
            c_size_t,
            c_uint16,
            POINTER(POINTER(ctypes.c_float)),
            POINTER(c_uint32),
        ]
        self._lib.lattice_hash_embed.restype = c_int

        # lattice_hash_embed_free
        self._lib.lattice_hash_embed_free.argtypes = [
            POINTER(ctypes.c_float),
            c_uint32,
        ]
        self._lib.lattice_hash_embed_free.restype = None

        # lattice_embedding_client_create
        self._lib.lattice_embedding_client_create.argtypes = [
            POINTER(EmbeddingConfig),
            POINTER(LatticeEmbeddingClient),
        ]
        self._lib.lattice_embedding_client_create.restype = c_int

        # lattice_embedding_client_embed
        self._lib.lattice_embedding_client_embed.argtypes = [
            LatticeEmbeddingClient,
            c_char_p,
            c_size_t,
            POINTER(POINTER(ctypes.c_float)),
            POINTER(c_uint32),
        ]
        self._lib.lattice_embedding_client_embed.restype = c_int

        # lattice_embedding_client_free
        self._lib.lattice_embedding_client_free.argtypes = [LatticeEmbeddingClient]
        self._lib.lattice_embedding_client_free.restype = None


# Global library instance (lazy loaded)
_lib: Optional[LatticeLib] = None


def get_lib() -> LatticeLib:
    """Get the global library instance."""
    global _lib
    if _lib is None:
        _lib = LatticeLib()
    return _lib


def library_available() -> bool:
    """Check if the native library is available."""
    return _find_library() is not None


def check_error(code: int) -> None:
    """Check error code and raise appropriate exception if not OK.

    Args:
        code: The error code returned by a C API function.

    Raises:
        LatticeError: If the code indicates an error.
    """
    if code == LATTICE_OK:
        return

    # Try to get error message from library
    try:
        lib = get_lib()
        msg_ptr = lib._lib.lattice_error_message(code)
        if msg_ptr:
            message = msg_ptr.decode("utf-8")
        else:
            message = f"Unknown error (code {code})"
    except Exception:
        message = f"Error code {code}"

    # Get the appropriate exception class
    exc_class = _ERROR_MAP.get(code, LatticeError)
    raise exc_class(message, code)


def value_to_python(c_value: LatticeValue):
    """Convert a LatticeValue to a Python value.

    Args:
        c_value: The C value structure.

    Returns:
        The corresponding Python value (None, bool, int, float, str, bytes, or numpy array).
    """
    value_type = c_value.type

    if value_type == LATTICE_VALUE_NULL:
        return None
    elif value_type == LATTICE_VALUE_BOOL:
        return c_value.data.bool_val
    elif value_type == LATTICE_VALUE_INT:
        return c_value.data.int_val
    elif value_type == LATTICE_VALUE_FLOAT:
        return c_value.data.float_val
    elif value_type == LATTICE_VALUE_STRING:
        string_val = c_value.data.string_val
        if string_val.ptr and string_val.len > 0:
            return string_val.ptr[:string_val.len].decode("utf-8")
        return ""
    elif value_type == LATTICE_VALUE_BYTES:
        bytes_val = c_value.data.bytes_val
        if bytes_val.ptr and bytes_val.len > 0:
            return bytes(bytes_val.ptr[:bytes_val.len])
        return b""
    elif value_type == LATTICE_VALUE_VECTOR:
        import numpy as np
        vector_val = c_value.data.vector_val
        if vector_val.ptr and vector_val.dimensions > 0:
            # Create numpy array from C float pointer
            return np.ctypeslib.as_array(vector_val.ptr, shape=(vector_val.dimensions,)).copy()
        return np.array([], dtype=np.float32)
    else:
        # LIST and MAP not supported yet
        return None


def _is_numpy_array(value) -> bool:
    """Check if a value is a numpy array without importing numpy."""
    return type(value).__module__ == "numpy" and type(value).__name__ == "ndarray"


def python_to_value(py_val, c_value: LatticeValue):
    """Fill a LatticeValue from a Python value.

    Args:
        py_val: The Python value to convert.
        c_value: The C value structure to fill.

    Returns:
        Any references that need to be kept alive during the C call,
        or None if no references need to be kept.
    """
    if py_val is None:
        c_value.type = LATTICE_VALUE_NULL
        return None
    elif isinstance(py_val, bool):
        c_value.type = LATTICE_VALUE_BOOL
        c_value.data.bool_val = py_val
        return None
    elif isinstance(py_val, int):
        c_value.type = LATTICE_VALUE_INT
        c_value.data.int_val = py_val
        return None
    elif isinstance(py_val, float):
        c_value.type = LATTICE_VALUE_FLOAT
        c_value.data.float_val = py_val
        return None
    elif isinstance(py_val, str):
        c_value.type = LATTICE_VALUE_STRING
        encoded = py_val.encode("utf-8")
        c_value.data.string_val.ptr = encoded
        c_value.data.string_val.len = len(encoded)
        return encoded  # Keep reference alive
    elif isinstance(py_val, bytes):
        c_value.type = LATTICE_VALUE_BYTES
        c_value.data.bytes_val.ptr = ctypes.cast(py_val, POINTER(ctypes.c_uint8))
        c_value.data.bytes_val.len = len(py_val)
        return py_val  # Keep reference alive
    elif _is_numpy_array(py_val):
        import numpy as np
        # Ensure vector is float32 and contiguous
        vec = np.ascontiguousarray(py_val, dtype=np.float32)
        c_value.type = LATTICE_VALUE_VECTOR
        c_value.data.vector_val.ptr = vec.ctypes.data_as(POINTER(ctypes.c_float))
        c_value.data.vector_val.dimensions = len(vec)
        return vec  # Keep reference alive
    else:
        raise TypeError(f"Unsupported value type: {type(py_val).__name__}")
