"""
Lattice: Embedded Property-Graph Database

A single-file local database for connected, semantic, and textual data.
Combines property graph storage, HNSW vector search, and BM25 full-text search.
"""

from latticedb.database import Database
from latticedb.transaction import Transaction
from latticedb.types import Node, Edge, QueryResult, Value, VectorSearchResult, FtsSearchResult
# Compatibility re-exports; prefer `latticedb.embedding` in new code.
from latticedb.embedding import hash_embed, EmbeddingClient, EmbeddingApiFormat
from latticedb._bindings import (
    LatticeError,
    LatticeIOError,
    LatticeCorruptionError,
    LatticeNotFoundError,
    LatticeAlreadyExistsError,
    LatticeInvalidArgError,
    LatticeTxnAbortedError,
    LatticeLockTimeoutError,
    LatticeReadOnlyError,
    LatticeFullError,
    LatticeVersionMismatchError,
    LatticeChecksumError,
    LatticeOutOfMemoryError,
    LatticeUnsupportedError,
    LatticeQueryError,
    library_available,
    get_lib,
)


def version() -> str:
    """Get the native library version string.

    Returns:
        Version string (e.g., "0.1.0").
    """
    lib = get_lib()
    result = lib._lib.lattice_version()
    if result:
        return result.decode("utf-8")
    return __version__

__version__ = "0.4.2"
__all__ = [
    # Core classes
    "Database",
    "Transaction",
    "Node",
    "Edge",
    "QueryResult",
    "Value",
    "VectorSearchResult",
    "FtsSearchResult",
    # Exceptions
    "LatticeError",
    "LatticeIOError",
    "LatticeCorruptionError",
    "LatticeNotFoundError",
    "LatticeAlreadyExistsError",
    "LatticeInvalidArgError",
    "LatticeTxnAbortedError",
    "LatticeLockTimeoutError",
    "LatticeReadOnlyError",
    "LatticeFullError",
    "LatticeVersionMismatchError",
    "LatticeChecksumError",
    "LatticeOutOfMemoryError",
    "LatticeUnsupportedError",
    "LatticeQueryError",
    # Embedding compatibility re-exports
    "hash_embed",
    "EmbeddingClient",
    "EmbeddingApiFormat",
    # Utilities
    "library_available",
    "version",
]
