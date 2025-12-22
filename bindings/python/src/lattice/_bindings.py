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

        # lattice_version
        self._lib.lattice_version.argtypes = []
        self._lib.lattice_version.restype = c_char_p


# Global library instance (lazy loaded)
_lib: Optional[LatticeLib] = None


def get_lib() -> LatticeLib:
    """Get the global library instance."""
    global _lib
    if _lib is None:
        _lib = LatticeLib()
    return _lib
