"""
Type definitions for Lattice Python bindings.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional, Union

import numpy as np
from numpy.typing import NDArray


# Type alias for property values
PropertyValue = Union[None, bool, int, float, str, bytes, List[Any], Dict[str, Any]]


@dataclass
class Node:
    """Represents a node in the graph."""

    id: int
    labels: List[str] = field(default_factory=list)
    properties: Dict[str, PropertyValue] = field(default_factory=dict)
    _vectors: Dict[str, NDArray[np.float32]] = field(default_factory=dict)

    def set_property(self, key: str, value: PropertyValue) -> None:
        """Set a property on this node."""
        self.properties[key] = value

    def get_property(self, key: str, default: PropertyValue = None) -> PropertyValue:
        """Get a property from this node."""
        return self.properties.get(key, default)

    def set_vector(self, key: str, vector: NDArray[np.float32]) -> None:
        """Set a vector embedding on this node."""
        self._vectors[key] = vector.astype(np.float32)

    def get_vector(self, key: str) -> Optional[NDArray[np.float32]]:
        """Get a vector embedding from this node."""
        return self._vectors.get(key)


@dataclass
class Edge:
    """Represents an edge in the graph."""

    id: int
    source_id: int
    target_id: int
    edge_type: str
    properties: Dict[str, PropertyValue] = field(default_factory=dict)

    def set_property(self, key: str, value: PropertyValue) -> None:
        """Set a property on this edge."""
        self.properties[key] = value

    def get_property(self, key: str, default: PropertyValue = None) -> PropertyValue:
        """Get a property from this edge."""
        return self.properties.get(key, default)


@dataclass
class Value:
    """Wrapper for Lattice property values."""

    value: PropertyValue

    @classmethod
    def null(cls) -> "Value":
        """Create a null value."""
        return cls(None)

    @classmethod
    def bool_(cls, v: bool) -> "Value":
        """Create a boolean value."""
        return cls(v)

    @classmethod
    def int_(cls, v: int) -> "Value":
        """Create an integer value."""
        return cls(v)

    @classmethod
    def float_(cls, v: float) -> "Value":
        """Create a float value."""
        return cls(v)

    @classmethod
    def string(cls, v: str) -> "Value":
        """Create a string value."""
        return cls(v)

    @classmethod
    def bytes_(cls, v: bytes) -> "Value":
        """Create a bytes value."""
        return cls(v)


@dataclass
class QueryResult:
    """Result of a Cypher query."""

    columns: List[str]
    _rows: List[Dict[str, PropertyValue]] = field(default_factory=list)

    def __iter__(self) -> Iterator[Dict[str, PropertyValue]]:
        """Iterate over result rows."""
        return iter(self._rows)

    def __len__(self) -> int:
        """Return the number of rows."""
        return len(self._rows)

    def fetchone(self) -> Optional[Dict[str, PropertyValue]]:
        """Fetch the next row."""
        if self._rows:
            return self._rows.pop(0)
        return None

    def fetchall(self) -> List[Dict[str, PropertyValue]]:
        """Fetch all remaining rows."""
        rows = self._rows
        self._rows = []
        return rows


@dataclass
class VectorSearchResult:
    """Result of a vector similarity search."""

    node_id: int
    distance: float
    node: Optional[Node] = None


@dataclass
class FtsSearchResult:
    """Result of a full-text search."""

    node_id: int
    score: float
    node: Optional[Node] = None
