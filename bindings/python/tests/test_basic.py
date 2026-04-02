"""
Basic tests for Lattice Python bindings.
"""

import ctypes
import os
import pytest
import numpy as np
from types import SimpleNamespace

from latticedb import Database, Node, Edge, Value
from latticedb.types import QueryResult, VectorSearchResult
from latticedb._bindings import library_available
from latticedb.embedding import EmbeddingApiFormat
import latticedb._bindings as bindings
import latticedb.embedding as embedding_mod


class TestLibraryDiscovery:
    """Tests for native library path discovery."""

    def test_find_library_uses_package_lib_before_lattice_prefix(
        self, monkeypatch, tmp_path
    ) -> None:
        """A bundled wheel library should win before prefix-based discovery."""
        package_root = tmp_path / "site-packages" / "latticedb"
        package_root.mkdir(parents=True)
        bundled_lib_dir = package_root / "lib"
        bundled_lib_dir.mkdir()
        bundled_lib = bundled_lib_dir / bindings._get_lib_name()
        bundled_lib.write_bytes(b"")

        prefix_root = tmp_path / "prefix"
        prefix_lib_dir = prefix_root / "lib"
        prefix_lib_dir.mkdir(parents=True)
        prefix_lib = prefix_lib_dir / bindings._get_lib_name()
        prefix_lib.write_bytes(b"")

        monkeypatch.setattr(bindings, "__file__", str(package_root / "_bindings.py"))
        monkeypatch.delenv("LATTICE_LIB_PATH", raising=False)
        monkeypatch.setenv("LATTICE_PREFIX", str(prefix_root))

        assert bindings._find_library() == bundled_lib

    def test_find_library_uses_lattice_prefix(self, monkeypatch, tmp_path) -> None:
        """LATTICE_PREFIX should resolve to prefix/lib/<library>."""
        lib_dir = tmp_path / "lib"
        lib_dir.mkdir()
        lib_path = lib_dir / bindings._get_lib_name()
        lib_path.write_bytes(b"")

        monkeypatch.delenv("LATTICE_LIB_PATH", raising=False)
        monkeypatch.setenv("LATTICE_PREFIX", str(tmp_path))

        assert bindings._find_library() == lib_path

    def test_find_library_uses_pkg_config_libdir(self, monkeypatch, tmp_path) -> None:
        """pkg-config libdir should be used when present."""
        lib_dir = tmp_path / "pkg-lib"
        lib_dir.mkdir()
        lib_path = lib_dir / bindings._get_lib_name()
        lib_path.write_bytes(b"")

        monkeypatch.delenv("LATTICE_LIB_PATH", raising=False)
        monkeypatch.delenv("LATTICE_PREFIX", raising=False)

        def fake_run(*args, **kwargs):
            return SimpleNamespace(stdout=f"{lib_dir}{os.linesep}")

        monkeypatch.setattr(bindings.subprocess, "run", fake_run)

        assert bindings._find_library() == lib_path


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


class TestEmbeddingApiFormat:
    """Tests for EmbeddingApiFormat enum."""

    def test_values(self) -> None:
        """Test enum values match C API."""
        assert EmbeddingApiFormat.OLLAMA == 0
        assert EmbeddingApiFormat.OPENAI == 1

    def test_is_int(self) -> None:
        """Test enum values are integers."""
        assert int(EmbeddingApiFormat.OLLAMA) == 0
        assert int(EmbeddingApiFormat.OPENAI) == 1


@pytest.mark.skipif(not library_available(), reason="Native library not found")
class TestHashEmbed:
    """Tests for hash_embed (requires native library)."""

    def test_basic(self) -> None:
        """Test basic hash embedding."""
        vec = embedding_mod.hash_embed("hello world", dimensions=128)
        assert isinstance(vec, np.ndarray)
        assert vec.dtype == np.float32
        assert vec.shape == (128,)

    def test_deterministic(self) -> None:
        """Test that same text produces same embedding."""
        vec1 = embedding_mod.hash_embed("test input", dimensions=64)
        vec2 = embedding_mod.hash_embed("test input", dimensions=64)
        np.testing.assert_array_equal(vec1, vec2)

    def test_different_text(self) -> None:
        """Test that different text produces different embeddings."""
        vec1 = embedding_mod.hash_embed("hello", dimensions=64)
        vec2 = embedding_mod.hash_embed("world", dimensions=64)
        assert not np.array_equal(vec1, vec2)

    def test_different_dimensions(self) -> None:
        """Test different dimension sizes."""
        vec64 = embedding_mod.hash_embed("test", dimensions=64)
        vec256 = embedding_mod.hash_embed("test", dimensions=256)
        assert vec64.shape == (64,)
        assert vec256.shape == (256,)

    def test_root_hash_embed_alias_warns(self) -> None:
        """Deprecated root alias should still work with a warning."""
        import latticedb

        with pytest.deprecated_call(
            match=r"latticedb\.hash_embed is deprecated; use latticedb\.embedding\.hash_embed\. Earliest removal is v0\.6\.0\."
        ):
            vec = latticedb.hash_embed("hello world", dimensions=32)

        assert isinstance(vec, np.ndarray)
        assert vec.shape == (32,)


class TestErrorMapping:
    """Tests for mapping C error codes to Python exceptions."""

    @pytest.mark.parametrize(
        ("code", "expected_exc"),
        [
            (bindings.LATTICE_ERROR_IO, bindings.LatticeIOError),
            (bindings.LATTICE_ERROR_CORRUPTION, bindings.LatticeCorruptionError),
            (bindings.LATTICE_ERROR_NOT_FOUND, bindings.LatticeNotFoundError),
            (bindings.LATTICE_ERROR_ALREADY_EXISTS, bindings.LatticeAlreadyExistsError),
            (bindings.LATTICE_ERROR_INVALID_ARG, bindings.LatticeInvalidArgError),
            (bindings.LATTICE_ERROR_TXN_ABORTED, bindings.LatticeTxnAbortedError),
            (bindings.LATTICE_ERROR_LOCK_TIMEOUT, bindings.LatticeLockTimeoutError),
            (bindings.LATTICE_ERROR_READ_ONLY, bindings.LatticeReadOnlyError),
            (bindings.LATTICE_ERROR_FULL, bindings.LatticeFullError),
            (bindings.LATTICE_ERROR_VERSION_MISMATCH, bindings.LatticeVersionMismatchError),
            (bindings.LATTICE_ERROR_CHECKSUM, bindings.LatticeChecksumError),
            (bindings.LATTICE_ERROR_OUT_OF_MEMORY, bindings.LatticeOutOfMemoryError),
            (bindings.LATTICE_ERROR_UNSUPPORTED, bindings.LatticeUnsupportedError),
        ],
    )
    def test_check_error_raises_specific_exception(self, monkeypatch, code, expected_exc):
        """Each known error code should map to its specific exception class."""
        fake_lib = SimpleNamespace(
            _lib=SimpleNamespace(lattice_error_message=lambda _code: b"native error")
        )
        monkeypatch.setattr(bindings, "get_lib", lambda: fake_lib)

        with pytest.raises(expected_exc) as exc_info:
            bindings.check_error(code)

        assert type(exc_info.value) is expected_exc
        assert exc_info.value.code == code
        assert "native error" in str(exc_info.value)

    def test_python_to_value_supports_nested_values(self) -> None:
        """Lists and dicts should convert into recursive native values."""
        c_value = bindings.LatticeValue()
        refs = bindings.python_to_value([1, {"city": "Portland"}], c_value)

        assert c_value.type == bindings.LATTICE_VALUE_LIST
        assert c_value.data.list_val
        assert c_value.data.list_val.contents.len == 2
        assert refs

    def test_value_to_python_supports_nested_value_tags(self) -> None:
        """Nested native values should decode to Python lists and dicts."""
        value_buf = ctypes.create_string_buffer(b"Portland", len(b"Portland"))
        inner_value = bindings.LatticeValue()
        inner_value.type = bindings.LATTICE_VALUE_STRING
        inner_value.data.string_val.ptr = ctypes.cast(value_buf, ctypes.POINTER(ctypes.c_char))
        inner_value.data.string_val.len = len(b"Portland")

        key_buf = ctypes.create_string_buffer(b"city", len(b"city"))
        entry = bindings.LatticeMapEntry()
        entry.key = ctypes.cast(key_buf, ctypes.POINTER(ctypes.c_char))
        entry.key_len = len(b"city")
        entry.value = inner_value

        entries = (bindings.LatticeMapEntry * 1)()
        entries[0] = entry
        map_value = bindings.LatticeValue()
        map_value.type = bindings.LATTICE_VALUE_MAP
        map_struct = bindings.LatticeMap()
        map_struct.entries = ctypes.cast(entries, ctypes.POINTER(bindings.LatticeMapEntry))
        map_struct.len = 1
        map_value.data.map_val = ctypes.pointer(map_struct)

        c_value = bindings.LatticeValue()
        c_value.type = bindings.LATTICE_VALUE_LIST
        items = (bindings.LatticeValue * 2)()
        items[0].type = bindings.LATTICE_VALUE_INT
        items[0].data.int_val = 7
        items[1] = map_value
        list_struct = bindings.LatticeList()
        list_struct.items = ctypes.cast(items, ctypes.POINTER(bindings.LatticeValue))
        list_struct.len = 2
        c_value.data.list_val = ctypes.pointer(list_struct)

        assert bindings.value_to_python(c_value) == [7, {"city": "Portland"}]

    def test_check_error_unknown_code_uses_base_exception(self, monkeypatch):
        """Unknown error codes should raise LatticeError."""
        fake_lib = SimpleNamespace(
            _lib=SimpleNamespace(lattice_error_message=lambda _code: b"unknown native error")
        )
        monkeypatch.setattr(bindings, "get_lib", lambda: fake_lib)

        with pytest.raises(bindings.LatticeError) as exc_info:
            bindings.check_error(-999)

        assert type(exc_info.value) is bindings.LatticeError
        assert exc_info.value.code == -999

    def test_check_error_falls_back_when_error_message_lookup_fails(self, monkeypatch):
        """check_error should still raise the mapped exception if message lookup fails."""
        monkeypatch.setattr(
            bindings,
            "get_lib",
            lambda: (_ for _ in ()).throw(RuntimeError("lib unavailable")),
        )

        with pytest.raises(bindings.LatticeInvalidArgError) as exc_info:
            bindings.check_error(bindings.LATTICE_ERROR_INVALID_ARG)

        assert type(exc_info.value) is bindings.LatticeInvalidArgError
        assert exc_info.value.code == bindings.LATTICE_ERROR_INVALID_ARG
        assert "Error code" in str(exc_info.value)

    def test_check_query_error_raises_lattice_query_error_with_diagnostics(self, monkeypatch):
        """Structured diagnostics should produce LatticeQueryError."""
        fake_native = SimpleNamespace(
            lattice_query_last_error_stage=lambda _ptr: bindings.LATTICE_QUERY_STAGE_PARSE,
            lattice_query_last_error_message=lambda _ptr: b"Parse error near RETURN",
            lattice_query_last_error_code=lambda _ptr: b"E_PARSE",
            lattice_query_last_error_has_location=lambda _ptr: 1,
            lattice_query_last_error_line=lambda _ptr: 2,
            lattice_query_last_error_column=lambda _ptr: 14,
            lattice_query_last_error_length=lambda _ptr: 6,
            lattice_error_message=lambda _code: b"fallback error",
        )
        fake_lib = SimpleNamespace(_lib=fake_native)
        monkeypatch.setattr(bindings, "get_lib", lambda: fake_lib)

        with pytest.raises(bindings.LatticeQueryError) as exc_info:
            bindings.check_query_error(bindings.LATTICE_ERROR_INVALID_ARG, object())

        err = exc_info.value
        assert err.code == bindings.LATTICE_ERROR_INVALID_ARG
        assert err.stage == "parse"
        assert err.diagnostic_code == "E_PARSE"
        assert err.location == {"line": 2, "column": 14, "length": 6}
        assert "Parse error near RETURN" in str(err)

    def test_check_query_error_falls_back_to_standard_error_without_diagnostics(self, monkeypatch):
        """No diagnostics should delegate to regular check_error behavior."""
        fake_native = SimpleNamespace(
            lattice_query_last_error_stage=lambda _ptr: bindings.LATTICE_QUERY_STAGE_NONE,
            lattice_query_last_error_message=lambda _ptr: None,
            lattice_query_last_error_code=lambda _ptr: None,
            lattice_query_last_error_has_location=lambda _ptr: 0,
            lattice_error_message=lambda _code: b"invalid argument",
        )
        fake_lib = SimpleNamespace(_lib=fake_native)
        monkeypatch.setattr(bindings, "get_lib", lambda: fake_lib)

        with pytest.raises(bindings.LatticeInvalidArgError) as exc_info:
            bindings.check_query_error(bindings.LATTICE_ERROR_INVALID_ARG, object())

        assert type(exc_info.value) is bindings.LatticeInvalidArgError
        assert exc_info.value.code == bindings.LATTICE_ERROR_INVALID_ARG


class TestEmbeddingClientUnit:
    """Unit tests for EmbeddingClient API behavior using a fake native layer."""

    def _install_fake_embedding_native(self, monkeypatch):
        class FakeNative:
            def __init__(self):
                self.created_configs = []
                self.embed_texts = []
                self.freed_embedding_calls = 0
                self.freed_vector_dims = []
                self._vector_storage = []

            def lattice_embedding_client_create(self, config_ptr, client_out_ptr):
                cfg = ctypes.cast(
                    config_ptr,
                    ctypes.POINTER(bindings.EmbeddingConfig),
                ).contents

                self.created_configs.append(
                    {
                        "endpoint": cfg.endpoint.decode("utf-8"),
                        "model": cfg.model.decode("utf-8"),
                        "api_format": cfg.api_format,
                        "api_key": cfg.api_key.decode("utf-8") if cfg.api_key else None,
                        "timeout_ms": cfg.timeout_ms,
                    }
                )

                out_ptr = ctypes.cast(client_out_ptr, ctypes.POINTER(ctypes.c_void_p))
                out_ptr[0] = ctypes.c_void_p(0xCAFE)
                return bindings.LATTICE_OK

            def lattice_embedding_client_embed(
                self,
                _handle,
                encoded_text,
                encoded_len,
                vector_out_ptr,
                dims_out_ptr,
            ):
                length = int(encoded_len.value if hasattr(encoded_len, "value") else encoded_len)
                self.embed_texts.append(ctypes.string_at(encoded_text, length).decode("utf-8"))

                vector = (ctypes.c_float * 4)(0.5, -1.0, 2.25, 3.0)
                self._vector_storage.append(vector)

                out_vector = ctypes.cast(
                    vector_out_ptr,
                    ctypes.POINTER(ctypes.POINTER(ctypes.c_float)),
                )
                out_vector[0] = ctypes.cast(vector, ctypes.POINTER(ctypes.c_float))

                out_dims = ctypes.cast(dims_out_ptr, ctypes.POINTER(ctypes.c_uint32))
                out_dims[0] = ctypes.c_uint32(4)
                return bindings.LATTICE_OK

            def lattice_hash_embed_free(self, _ptr, dims):
                dim_value = int(dims.value if hasattr(dims, "value") else dims)
                self.freed_vector_dims.append(dim_value)

            def lattice_embedding_client_free(self, _handle):
                self.freed_embedding_calls += 1

        fake_native = FakeNative()
        fake_lib = SimpleNamespace(_lib=fake_native)
        monkeypatch.setattr(embedding_mod, "get_lib", lambda: fake_lib)
        monkeypatch.setattr(embedding_mod, "check_error", lambda code: bindings.check_error(code))
        monkeypatch.setattr(bindings, "get_lib", lambda: fake_lib)
        return fake_native

    def test_embedding_client_init_embed_and_close(self, monkeypatch):
        """EmbeddingClient should pass config, decode vectors, and close idempotently."""
        fake_native = self._install_fake_embedding_native(monkeypatch)

        client = embedding_mod.EmbeddingClient(
            endpoint="http://localhost:11434/api/embeddings",
            model="custom-model",
            api_format=embedding_mod.EmbeddingApiFormat.OPENAI,
            api_key="secret",
            timeout_ms=1234,
        )
        vector = client.embed("hello embeddings")
        client.close()
        client.close()

        assert len(fake_native.created_configs) == 1
        cfg = fake_native.created_configs[0]
        assert cfg["endpoint"] == "http://localhost:11434/api/embeddings"
        assert cfg["model"] == "custom-model"
        assert cfg["api_format"] == int(embedding_mod.EmbeddingApiFormat.OPENAI)
        assert cfg["api_key"] == "secret"
        assert cfg["timeout_ms"] == 1234
        assert fake_native.embed_texts == ["hello embeddings"]
        assert isinstance(vector, np.ndarray)
        assert vector.dtype == np.float32
        assert vector.shape == (4,)
        np.testing.assert_allclose(vector, np.array([0.5, -1.0, 2.25, 3.0], dtype=np.float32))
        assert fake_native.freed_vector_dims == [4]
        assert fake_native.freed_embedding_calls == 1

    def test_embedding_client_context_manager_closes_client(self, monkeypatch):
        """Context manager usage should call native client free exactly once."""
        fake_native = self._install_fake_embedding_native(monkeypatch)

        with embedding_mod.EmbeddingClient(endpoint="http://localhost:11434/api/embeddings") as client:
            _ = client.embed("context manager")

        assert fake_native.freed_embedding_calls == 1

    def test_root_embedding_client_alias_warns(self, monkeypatch):
        """Deprecated root alias should still construct and close correctly."""
        import latticedb

        fake_native = self._install_fake_embedding_native(monkeypatch)

        with pytest.deprecated_call(
            match=r"latticedb\.EmbeddingClient is deprecated; use latticedb\.embedding\.EmbeddingClient\. Earliest removal is v0\.6\.0\."
        ):
            client = latticedb.EmbeddingClient(endpoint="http://localhost:11434/api/embeddings")

        vector = client.embed("deprecated alias")
        client.close()

        assert fake_native.embed_texts == ["deprecated alias"]
        assert vector.shape == (4,)


class TestPublicApiExports:
    """Tests for top-level public API exports."""

    def test_top_level_exports_include_error_and_embedding_symbols(self):
        """`latticedb` should export all documented exception and embedding APIs."""
        import latticedb

        expected = {
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
            "LatticeQueryError",
            "EmbeddingClient",
            "EmbeddingApiFormat",
            "hash_embed",
            "version",
        }

        exported = set(latticedb.__all__)
        assert expected.issubset(exported)
        for symbol in expected:
            assert hasattr(latticedb, symbol)
