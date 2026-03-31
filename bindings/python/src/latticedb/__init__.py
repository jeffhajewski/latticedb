"""
Lattice: Embedded Property-Graph Database

A single-file local database for connected, semantic, and textual data.
Combines property graph storage, HNSW vector search, and BM25 full-text search.
"""

from __future__ import annotations

import warnings

from latticedb.database import Database
from latticedb.transaction import Transaction
from latticedb.types import Node, Edge, QueryResult, Value, VectorSearchResult, FtsSearchResult
from latticedb.embedding import (
    hash_embed as _hash_embed,
    EmbeddingClient as _EmbeddingClient,
    EmbeddingApiFormat as _EmbeddingApiFormat,
)
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

# Deprecated compatibility alias for latticedb.embedding.EmbeddingApiFormat.
EmbeddingApiFormat = _EmbeddingApiFormat


def hash_embed(text: str, dimensions: int = 128):
    """Deprecated compatibility wrapper for :func:`latticedb.embedding.hash_embed`."""
    warnings.warn(
        "latticedb.hash_embed is deprecated; use latticedb.embedding.hash_embed. Earliest removal is v0.6.0.",
        DeprecationWarning,
        stacklevel=2,
    )
    return _hash_embed(text, dimensions=dimensions)


class EmbeddingClient(_EmbeddingClient):
    """Deprecated compatibility wrapper for :class:`latticedb.embedding.EmbeddingClient`."""

    def __init__(self, *args, **kwargs):
        warnings.warn(
            "latticedb.EmbeddingClient is deprecated; use latticedb.embedding.EmbeddingClient. Earliest removal is v0.6.0.",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)


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
    # Deprecated embedding compatibility re-exports
    "hash_embed",
    "EmbeddingClient",
    "EmbeddingApiFormat",
    # Utilities
    "library_available",
    "version",
]
