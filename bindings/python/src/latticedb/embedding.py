"""
Embedding utilities for Lattice Python bindings.

Provides hash-based embeddings (built-in, no external service) and
an HTTP embedding client for services like Ollama and OpenAI.
"""

import ctypes
from ctypes import POINTER, byref, c_size_t, c_uint16, c_uint32, c_void_p
from enum import IntEnum
from typing import Optional

import numpy as np

from latticedb._bindings import (
    EmbeddingConfig,
    LatticeEmbeddingClient,
    check_error,
    get_lib,
)


class EmbeddingApiFormat(IntEnum):
    """API format for HTTP embedding services."""

    OLLAMA = 0
    OPENAI = 1


def hash_embed(text: str, dimensions: int = 128) -> np.ndarray:
    """Generate a hash embedding for text (built-in, no external service).

    Args:
        text: The text to embed.
        dimensions: Number of embedding dimensions (default 128).

    Returns:
        A float32 numpy array of the specified dimensions.
    """
    lib = get_lib()
    encoded = text.encode("utf-8")

    vector_out = POINTER(ctypes.c_float)()
    dims_out = c_uint32()

    rc = lib._lib.lattice_hash_embed(
        encoded,
        c_size_t(len(encoded)),
        c_uint16(dimensions),
        byref(vector_out),
        byref(dims_out),
    )
    check_error(rc)

    try:
        result = np.ctypeslib.as_array(vector_out, shape=(dims_out.value,)).copy()
    finally:
        lib._lib.lattice_hash_embed_free(vector_out, dims_out)

    return result


class EmbeddingClient:
    """HTTP embedding client for services like Ollama and OpenAI.

    Example:
        with EmbeddingClient("http://localhost:11434") as client:
            vec = client.embed("hello world")
    """

    def __init__(
        self,
        endpoint: str,
        model: str = "nomic-embed-text",
        *,
        api_format: EmbeddingApiFormat = EmbeddingApiFormat.OLLAMA,
        api_key: Optional[str] = None,
        timeout_ms: int = 0,
    ):
        self._lib = get_lib()
        self._handle = c_void_p()

        config = EmbeddingConfig()
        # Keep references alive for the duration of the C call
        self._endpoint_bytes = endpoint.encode("utf-8")
        self._model_bytes = model.encode("utf-8")
        self._api_key_bytes = api_key.encode("utf-8") if api_key else None

        config.endpoint = self._endpoint_bytes
        config.model = self._model_bytes
        config.api_format = int(api_format)
        config.api_key = self._api_key_bytes
        config.timeout_ms = timeout_ms

        rc = self._lib._lib.lattice_embedding_client_create(
            byref(config),
            byref(self._handle),
        )
        check_error(rc)

    def embed(self, text: str) -> np.ndarray:
        """Generate an embedding for text via the HTTP service.

        Args:
            text: The text to embed.

        Returns:
            A float32 numpy array.
        """
        encoded = text.encode("utf-8")

        vector_out = POINTER(ctypes.c_float)()
        dims_out = c_uint32()

        rc = self._lib._lib.lattice_embedding_client_embed(
            self._handle,
            encoded,
            c_size_t(len(encoded)),
            byref(vector_out),
            byref(dims_out),
        )
        check_error(rc)

        try:
            result = np.ctypeslib.as_array(vector_out, shape=(dims_out.value,)).copy()
        finally:
            self._lib._lib.lattice_hash_embed_free(vector_out, dims_out)

        return result

    def close(self) -> None:
        """Free the underlying C client."""
        if self._handle:
            self._lib._lib.lattice_embedding_client_free(self._handle)
            self._handle = c_void_p()

    def __enter__(self) -> "EmbeddingClient":
        return self

    def __exit__(self, *args) -> None:
        self.close()

    def __del__(self) -> None:
        self.close()
