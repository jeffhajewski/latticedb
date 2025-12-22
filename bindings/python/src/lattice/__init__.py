"""
Lattice: Embedded Knowledge Graph Database

A single-file knowledge graph database for AI/RAG applications.
Combines property graph storage, HNSW vector search, and BM25 full-text search.
"""

from lattice.database import Database
from lattice.transaction import Transaction
from lattice.types import Node, Edge, QueryResult, Value

__version__ = "0.1.0"
__all__ = [
    "Database",
    "Transaction",
    "Node",
    "Edge",
    "QueryResult",
    "Value",
]
