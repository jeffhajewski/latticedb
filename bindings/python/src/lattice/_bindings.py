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
    c_uint32,
    c_uint64,
    c_void_p,
)
from pathlib import Path
from typing import Optional


def _find_library() -> Optional[Path]:
    """Find the lattice shared library."""
    # Search paths in order of preference
    search_paths = [
        # Development: relative to bindings
        Path(__file__).parent.parent.parent.parent.parent / "zig-out" / "lib",
        # Installed: system library paths
        Path("/usr/local/lib"),
        Path("/usr/lib"),
    ]

    lib_name = {
        "darwin": "liblattice.dylib",
        "linux": "liblattice.so",
        "win32": "lattice.dll",
    }.get(sys.platform, "liblattice.so")

    for path in search_paths:
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
LATTICE_VALUE_LIST = 6
LATTICE_VALUE_MAP = 7


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


class ValueData(Union):
    _fields_ = [
        ("bool_val", c_bool),
        ("int_val", c_int64),
        ("float_val", c_double),
        ("string_val", StringValue),
        ("bytes_val", BytesValue),
    ]


class LatticeValue(Structure):
    _fields_ = [
        ("type", c_int),
        ("data", ValueData),
    ]


class OpenOptions(Structure):
    _fields_ = [
        ("create", c_bool),
        ("read_only", c_bool),
        ("cache_size_mb", c_uint32),
        ("page_size", c_uint32),
    ]


# Type aliases
LatticeDatabase = c_void_p
LatticeTxn = c_void_p
LatticeQuery = c_void_p
LatticeResult = c_void_p
LatticeNodeId = c_uint64
LatticeEdgeId = c_uint64


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

        # lattice_node_set_vector
        self._lib.lattice_node_set_vector.argtypes = [
            LatticeTxn,
            LatticeNodeId,
            c_char_p,
            POINTER(ctypes.c_float),
            c_uint32,
        ]
        self._lib.lattice_node_set_vector.restype = c_int

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
        self._lib.lattice_edge_delete.argtypes = [LatticeTxn, LatticeEdgeId]
        self._lib.lattice_edge_delete.restype = c_int

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
        The corresponding Python value (None, bool, int, float, str, or bytes).
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
    else:
        # LIST and MAP not supported yet
        return None


def python_to_value(py_val, c_value: LatticeValue) -> None:
    """Fill a LatticeValue from a Python value.

    Args:
        py_val: The Python value to convert.
        c_value: The C value structure to fill.
    """
    if py_val is None:
        c_value.type = LATTICE_VALUE_NULL
    elif isinstance(py_val, bool):
        c_value.type = LATTICE_VALUE_BOOL
        c_value.data.bool_val = py_val
    elif isinstance(py_val, int):
        c_value.type = LATTICE_VALUE_INT
        c_value.data.int_val = py_val
    elif isinstance(py_val, float):
        c_value.type = LATTICE_VALUE_FLOAT
        c_value.data.float_val = py_val
    elif isinstance(py_val, str):
        c_value.type = LATTICE_VALUE_STRING
        encoded = py_val.encode("utf-8")
        c_value.data.string_val.ptr = encoded
        c_value.data.string_val.len = len(encoded)
    elif isinstance(py_val, bytes):
        c_value.type = LATTICE_VALUE_BYTES
        c_value.data.bytes_val.ptr = ctypes.cast(py_val, POINTER(ctypes.c_uint8))
        c_value.data.bytes_val.len = len(py_val)
    else:
        raise TypeError(f"Unsupported value type: {type(py_val).__name__}")
