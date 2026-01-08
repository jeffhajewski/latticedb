"""
Basic tests for Lattice Python bindings.
"""

import pytest
import numpy as np

from latticedb import Database, Node, Edge, Value
from latticedb.types import QueryResult, VectorSearchResult


class TestNode:
    """Tests for Node class."""

    def test_create_node(self) -> None:
        """Test creating a node."""
        node = Node(id=1, labels=["Person"], properties={"name": "Alice"})
        assert node.id == 1
        assert node.labels == ["Person"]
        assert node.properties["name"] == "Alice"

    def test_set_property(self) -> None:
        """Test setting a property."""
        node = Node(id=1)
        node.set_property("age", 30)
        assert node.get_property("age") == 30

    def test_get_property_default(self) -> None:
        """Test getting a property with default."""
        node = Node(id=1)
        assert node.get_property("missing") is None
        assert node.get_property("missing", "default") == "default"

    def test_set_vector(self) -> None:
        """Test setting a vector."""
        node = Node(id=1)
        vector = np.array([1.0, 2.0, 3.0], dtype=np.float32)
        node.set_vector("embedding", vector)
        result = node.get_vector("embedding")
        assert result is not None
        np.testing.assert_array_equal(result, vector)


class TestEdge:
    """Tests for Edge class."""

    def test_create_edge(self) -> None:
        """Test creating an edge."""
        edge = Edge(
            id=1,
            source_id=10,
            target_id=20,
            edge_type="KNOWS",
            properties={"since": 2020},
        )
        assert edge.id == 1
        assert edge.source_id == 10
        assert edge.target_id == 20
        assert edge.edge_type == "KNOWS"
        assert edge.properties["since"] == 2020


class TestValue:
    """Tests for Value class."""

    def test_null_value(self) -> None:
        """Test null value."""
        v = Value.null()
        assert v.value is None

    def test_bool_value(self) -> None:
        """Test boolean value."""
        v = Value.bool_(True)
        assert v.value is True

    def test_int_value(self) -> None:
        """Test integer value."""
        v = Value.int_(42)
        assert v.value == 42

    def test_float_value(self) -> None:
        """Test float value."""
        v = Value.float_(3.14)
        assert v.value == 3.14

    def test_string_value(self) -> None:
        """Test string value."""
        v = Value.string("hello")
        assert v.value == "hello"


class TestQueryResult:
    """Tests for QueryResult class."""

    def test_empty_result(self) -> None:
        """Test empty result."""
        result = QueryResult(columns=["name", "age"])
        assert len(result) == 0
        assert result.fetchone() is None

    def test_iterate_result(self) -> None:
        """Test iterating over results."""
        result = QueryResult(
            columns=["name"],
            _rows=[{"name": "Alice"}, {"name": "Bob"}],
        )
        names = [row["name"] for row in result]
        assert names == ["Alice", "Bob"]


class TestVectorSearchResult:
    """Tests for VectorSearchResult class."""

    def test_create_result(self) -> None:
        """Test creating a search result."""
        result = VectorSearchResult(node_id=1, distance=0.5)
        assert result.node_id == 1
        assert result.distance == 0.5
        assert result.node is None


class TestValueConversion:
    """Tests for value type conversion between Python and C."""

    def test_value_type_constants(self) -> None:
        """Test value type constants match C API."""
        from latticedb._bindings import (
            LATTICE_VALUE_NULL,
            LATTICE_VALUE_BOOL,
            LATTICE_VALUE_INT,
            LATTICE_VALUE_FLOAT,
            LATTICE_VALUE_STRING,
            LATTICE_VALUE_BYTES,
            LATTICE_VALUE_VECTOR,
            LATTICE_VALUE_LIST,
            LATTICE_VALUE_MAP,
        )
        assert LATTICE_VALUE_NULL == 0
        assert LATTICE_VALUE_BOOL == 1
        assert LATTICE_VALUE_INT == 2
        assert LATTICE_VALUE_FLOAT == 3
        assert LATTICE_VALUE_STRING == 4
        assert LATTICE_VALUE_BYTES == 5
        assert LATTICE_VALUE_VECTOR == 6
        assert LATTICE_VALUE_LIST == 7
        assert LATTICE_VALUE_MAP == 8

    def test_python_to_value_numpy_array(self) -> None:
        """Test converting numpy array to LatticeValue."""
        from latticedb._bindings import (
            LATTICE_VALUE_VECTOR,
            LatticeValue,
            python_to_value,
        )

        vec = np.array([1.0, 2.0, 3.0], dtype=np.float32)
        c_value = LatticeValue()
        ref = python_to_value(vec, c_value)

        assert c_value.type == LATTICE_VALUE_VECTOR
        assert c_value.data.vector_val.dimensions == 3
        # Reference should be kept alive
        assert ref is not None

    def test_is_numpy_array(self) -> None:
        """Test numpy array detection."""
        from latticedb._bindings import _is_numpy_array

        assert _is_numpy_array(np.array([1, 2, 3])) is True
        assert _is_numpy_array([1, 2, 3]) is False
        assert _is_numpy_array("not an array") is False
        assert _is_numpy_array(42) is False
