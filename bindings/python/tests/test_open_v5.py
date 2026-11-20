"""
Tests for the v5 open options, node deletes that would strand edges, and
edges to missing nodes.
"""

import math

import pytest

from latticedb import (
    Database,
    LatticeConfigMismatchError,
    LatticeHasRelationshipsError,
    LatticeInvalidArgError,
    LatticeNeedsNewerWriterError,
    LatticeNotFoundError,
    LatticeTxnAbortedError,
    LatticeUnsupportedError,
    LatticeVersionMismatchError,
    deserialize,
    library_available,
)
from latticedb._bindings import get_lib

pytestmark = pytest.mark.skipif(
    not library_available(),
    reason="Native library not found - build with 'zig build' first",
)


def _require_v5() -> None:
    if not getattr(get_lib(), "_has_lattice_open_v5", False):
        pytest.skip("native library has no lattice_open_v5")


class TestOpenOptionsV5:
    def test_euclidean_index_survives_reopen_without_vector_options(self, tmp_path):
        _require_v5()
        np = pytest.importorskip("numpy")
        db_path = tmp_path / "v5_euclid.db"

        with Database(
            db_path,
            create=True,
            enable_vectors=True,
            vector_dimensions=3,
            vector_metric="euclidean",
            vector_m=8,
            vector_ef_construction=64,
        ) as db:
            with db.write() as txn:
                a = txn.create_node(labels=["P"])
                b = txn.create_node(labels=["P"])
                txn.set_vector(a.id, "embedding", np.array([3.0, 0.0, 0.0], dtype=np.float32))
                txn.set_vector(b.id, "embedding", np.array([0.0, 4.0, 0.0], dtype=np.float32))
                txn.commit()

        # No vector options at all: the file's index comes with it.
        with Database(db_path) as db:
            results = db.vector_search(np.array([0.0, 0.0, 0.0], dtype=np.float32), k=2)
            assert [r.node_id for r in results] == [a.id, b.id]
            # L2 distance of vectors stored as given, not normalised.
            assert math.isclose(results[0].distance, 3.0, rel_tol=1e-5)
            assert math.isclose(results[1].distance, 4.0, rel_tol=1e-5)

        # vector_dimensions=0 with vectors enabled means "the file's".
        with Database(db_path, enable_vectors=True, vector_dimensions=0) as db:
            assert len(db.vector_search(np.array([3.0, 0.0, 0.0], dtype=np.float32), k=1)) == 1

    def test_contradicting_options_are_config_mismatch(self, tmp_path):
        _require_v5()
        db_path = tmp_path / "v5_mismatch.db"
        with Database(db_path, create=True, enable_vectors=True, vector_dimensions=3):
            pass

        with pytest.raises(LatticeConfigMismatchError) as info:
            Database(db_path, enable_vectors=True, vector_dimensions=4).open()
        assert info.value.code == -18
        with pytest.raises(LatticeConfigMismatchError):
            Database(db_path, vector_metric="dot_product").open()
        with pytest.raises(LatticeConfigMismatchError):
            Database(db_path, vector_m=12).open()

    def test_out_of_range_values_are_invalid_arg(self, tmp_path):
        _require_v5()
        with pytest.raises(LatticeInvalidArgError):
            Database(
                tmp_path / "v5_bad_m.db", create=True, enable_vectors=True, vector_m=64
            ).open()

    def test_unknown_names_are_value_errors(self, tmp_path):
        with pytest.raises(ValueError):
            Database(tmp_path / "x.db", sync_mode="sometimes")
        with pytest.raises(ValueError):
            Database(tmp_path / "x.db", vector_metric="manhattan")

    def test_sync_options_are_accepted(self, tmp_path):
        _require_v5()
        db_path = tmp_path / "v5_sync.db"
        for sync_mode, full_fsync in [("normal", False), ("full", True), (None, None)]:
            with Database(
                db_path, create=True, sync_mode=sync_mode, full_fsync=full_fsync
            ) as db:
                with db.write() as txn:
                    txn.create_node(labels=["S"])
                    txn.commit()
        with Database(db_path) as db:
            assert db.query("MATCH (n:S) RETURN count(n) AS c").fetchall()[0]["c"] == 3

    def test_v5_only_option_without_v5_is_unsupported(self, tmp_path, monkeypatch):
        lib = get_lib()
        monkeypatch.setattr(lib, "_has_lattice_open_v5", False)
        with pytest.raises(LatticeUnsupportedError):
            Database(tmp_path / "v4.db", create=True, sync_mode="normal").open()
        # Without any v5-only option, the v4 path still opens.
        with Database(tmp_path / "v4.db", create=True):
            pass


def _require_deserialize_v5() -> None:
    lib = get_lib()
    if not (
        getattr(lib, "_has_deserialize_v5", False)
        and getattr(lib, "_has_deserialize_borrowed_v5", False)
    ):
        pytest.skip("native library has no lattice_deserialize_v5")


class TestDeserializeV5:
    @staticmethod
    def _euclidean_bytes(tmp_path):
        np = pytest.importorskip("numpy")
        with Database(
            tmp_path / "deser_euclid.db",
            create=True,
            enable_vectors=True,
            vector_dimensions=2,
            vector_metric="euclidean",
        ) as db:
            with db.write() as txn:
                a = txn.create_node(labels=["P"])
                b = txn.create_node(labels=["P"])
                txn.set_vector(a.id, "embedding", np.array([0.0, 0.0], dtype=np.float32))
                txn.set_vector(b.id, "embedding", np.array([3.0, 4.0], dtype=np.float32))
                txn.commit()
            return db.serialize()

    @pytest.mark.parametrize("copy", [True, False])
    def test_index_in_the_bytes_brings_its_metric(self, tmp_path, copy):
        _require_deserialize_v5()
        np = pytest.importorskip("numpy")
        data = self._euclidean_bytes(tmp_path)

        db = deserialize(data, copy=copy)
        try:
            results = db.vector_search(np.array([0.0, 0.0], dtype=np.float32), k=2)
            assert len(results) == 2
            # L2 distance from the origin to (3, 4); cosine could not give 5.
            assert math.isclose(results[1].distance, 5.0, rel_tol=1e-5)
        finally:
            db.close()

    @pytest.mark.parametrize("copy", [True, False])
    def test_contradicting_metric_is_config_mismatch(self, tmp_path, copy):
        _require_deserialize_v5()
        data = self._euclidean_bytes(tmp_path)

        with pytest.raises(LatticeConfigMismatchError) as info:
            deserialize(data, vector_metric="cosine", copy=copy)
        assert info.value.code == -18

    @pytest.mark.parametrize("copy", [True, False])
    def test_enable_vectors_configures_a_new_index(self, tmp_path, copy):
        _require_deserialize_v5()
        np = pytest.importorskip("numpy")
        with Database(tmp_path / "deser_plain.db", create=True) as db:
            data = db.serialize()

        db = deserialize(
            data,
            enable_vectors=True,
            vector_dimensions=2,
            vector_metric="euclidean",
            copy=copy,
        )
        try:
            with db.write() as txn:
                a = txn.create_node(labels=["P"])
                b = txn.create_node(labels=["P"])
                txn.set_vector(a.id, "embedding", np.array([0.0, 0.0], dtype=np.float32))
                txn.set_vector(b.id, "embedding", np.array([3.0, 4.0], dtype=np.float32))
                txn.commit()
            results = db.vector_search(np.array([0.0, 0.0], dtype=np.float32), k=2)
            assert len(results) == 2
            assert math.isclose(results[1].distance, 5.0, rel_tol=1e-5)
        finally:
            db.close()

    @pytest.mark.parametrize("copy", [True, False])
    def test_changefeed_retention_sets_the_policy(self, tmp_path, copy):
        _require_deserialize_v5()
        with Database(tmp_path / "deser_feed.db", create=True) as db:
            data = db.serialize()

        db = deserialize(data, changefeed_retention="last:5", copy=copy)
        try:
            assert db.changefeed_status().retention == "last:5"
        finally:
            db.close()

    def test_v5_only_option_without_v5_is_unsupported(self, tmp_path, monkeypatch):
        with Database(tmp_path / "deser_v4.db", create=True) as db:
            data = db.serialize()
        lib = get_lib()
        monkeypatch.setattr(lib, "_has_deserialize_v5", False)
        monkeypatch.setattr(lib, "_has_deserialize_borrowed_v5", False)
        for copy in (True, False):
            with pytest.raises(LatticeUnsupportedError):
                deserialize(data, sync_mode="normal", copy=copy)
            with pytest.raises(LatticeUnsupportedError):
                deserialize(data, changefeed_retention="off", copy=copy)
            # Without any v5-only option, the v4 path still opens.
            deserialize(data, copy=copy).close()


class TestNeedsNewerWriter:
    def test_file_marked_for_newer_writers_opens_only_read_only(self, tmp_path):
        db_path = tmp_path / "v5_newer_writer.db"
        with Database(db_path, create=True) as db:
            with db.write() as txn:
                txn.create_node(labels=["Old"])
                txn.commit()

        # min_writer_version is a little-endian u16 at byte 222 of the header.
        # This release writes format 5; 6 is a format it does not know.
        with open(db_path, "r+b") as f:
            f.seek(222)
            f.write((6).to_bytes(2, "little"))

        with pytest.raises(LatticeNeedsNewerWriterError) as info:
            Database(db_path).open()
        assert info.value.code == -19

        with Database(db_path, read_only=True) as db:
            rows = db.query("MATCH (n:Old) RETURN count(n) AS c").fetchall()
            assert rows[0]["c"] == 1


class TestVersionTooNew:
    def test_file_in_a_newer_format_is_a_version_mismatch(self, tmp_path):
        db_path = tmp_path / "v5_newer_reader.db"
        with Database(db_path, create=True) as db:
            with db.write() as txn:
                txn.create_node(labels=["Old"])
                txn.commit()

        # min_reader_version is a little-endian u16 at byte 6 of the header.
        # This release reads format 5; 6 is a format it does not know.
        with open(db_path, "r+b") as f:
            f.seek(6)
            f.write((6).to_bytes(2, "little"))

        for read_only in (False, True):
            with pytest.raises(LatticeVersionMismatchError) as info:
                Database(db_path, read_only=read_only).open()
            assert info.value.code == -11


class TestNodeDelete:
    def test_plain_delete_refused_while_node_has_relationships(self, tmp_path):
        with Database(tmp_path / "v5_del.db", create=True) as db:
            with db.write() as txn:
                a = txn.create_node(labels=["N"])
                b = txn.create_node(labels=["N"])
                txn.create_edge(a.id, b.id, "R")
                with pytest.raises(LatticeHasRelationshipsError) as info:
                    txn.delete_node(a.id)
                assert info.value.code == -17
                assert txn.node_exists(a.id)
                txn.commit()
            with db.read() as txn:
                assert txn.node_exists(a.id)
                assert len(txn.get_outgoing_edges(a.id)) == 1

    def test_detach_delete_removes_node_and_relationships(self, tmp_path):
        with Database(tmp_path / "v5_detach.db", create=True) as db:
            with db.write() as txn:
                a = txn.create_node(labels=["N"])
                b = txn.create_node(labels=["N"])
                c = txn.create_node(labels=["N"])
                txn.create_edge(a.id, b.id, "R")
                txn.create_edge(c.id, a.id, "R")
                txn.commit()
            with db.write() as txn:
                txn.delete_node(a.id, detach=True)
                txn.commit()
            with db.read() as txn:
                assert not txn.node_exists(a.id)
                assert txn.get_incoming_edges(b.id) == []
                assert txn.get_outgoing_edges(c.id) == []
            # With its relationships gone, b now deletes plainly.
            with db.write() as txn:
                txn.delete_node(b.id)
                txn.commit()

    def test_deleting_missing_node_is_ok(self, tmp_path):
        with Database(tmp_path / "v5_missing.db", create=True) as db:
            with db.write() as txn:
                txn.delete_node(987654)
                txn.delete_node(987654, detach=True)
                txn.commit()

    def test_edge_to_missing_node_is_not_found(self, tmp_path):
        with Database(tmp_path / "v5_edge.db", create=True) as db:
            with db.write() as txn:
                a = txn.create_node(labels=["N"])
                with pytest.raises(LatticeNotFoundError):
                    txn.create_edge(a.id, 987654, "R")
                with pytest.raises(LatticeNotFoundError):
                    txn.create_edge(987654, a.id, "R")
                txn.commit()


class TestCommitAborted:
    def test_commit_reporting_txn_aborted_ends_the_transaction(self, tmp_path):
        with Database(tmp_path / "v5_aborted_commit.db", create=True) as db:
            with db.write() as txn:
                txn.create_node(labels=["N"])
                # Retire the native handle behind the wrapper's back, so the
                # wrapper's own commit gets TXN_ABORTED from the C side.
                assert get_lib()._lib.lattice_commit(txn._handle) == 0
                with pytest.raises(LatticeTxnAbortedError):
                    txn.commit()
                # Nothing is left to roll back; this must not reach the
                # retired handle and raise again.
                txn.rollback()
            with pytest.raises(RuntimeError, match="rolled back"):
                txn.commit()
