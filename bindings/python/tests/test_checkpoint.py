"""
Tests for Database.checkpoint and Database.set_auto_checkpoint.
"""

import os
from pathlib import Path

import pytest

from latticedb import (
    CheckpointStats,
    Database,
    LatticeReadOnlyError,
    LatticeUnsupportedError,
    library_available,
)
from latticedb._bindings import get_lib

pytestmark = pytest.mark.skipif(
    not library_available(),
    reason="Native library not found - build with 'zig build' first",
)

PAGE_SIZE = 4096


def _require_checkpoint() -> None:
    lib = get_lib()
    if not getattr(lib, "_has_checkpoint", False) or not getattr(
        lib, "_has_set_auto_checkpoint", False
    ):
        pytest.skip("native library has no lattice_checkpoint")


def _wal_size(db_path: Path) -> int:
    wal = Path(str(db_path) + "-wal")
    return os.path.getsize(wal) if wal.exists() else 0


def _commit_one(db: Database, i: int) -> None:
    with db.write() as txn:
        txn.create_node(labels=["Item"], properties={"i": i})
        txn.commit()


class TestCheckpoint:
    def test_log_grows_with_automatic_checkpoints_off_and_truncate_empties_it(
        self, tmp_path: Path
    ) -> None:
        _require_checkpoint()
        db_path = tmp_path / "ckpt_off.db"
        with Database(db_path, create=True) as db:
            db.set_auto_checkpoint(0)
            _commit_one(db, 0)
            first = _wal_size(db_path)
            for i in range(1, 100):
                _commit_one(db, i)
            grown = _wal_size(db_path)
            # One small commit is one 4096-byte frame, and nothing folds them.
            assert grown >= first + 99 * PAGE_SIZE

            full = db.checkpoint("full")
            assert isinstance(full, CheckpointStats)
            assert full.wal_truncated is False
            assert full.pages_flushed > 0
            assert full.checkpoint_lsn > 0
            assert _wal_size(db_path) >= grown

            truncated = db.checkpoint("truncate")
            assert truncated.wal_truncated is True
            assert truncated.checkpoint_lsn >= full.checkpoint_lsn
            assert truncated.duration_ns >= 0
            assert _wal_size(db_path) < grown
            assert _wal_size(db_path) <= 2 * PAGE_SIZE

            # The default mode is truncate.
            _commit_one(db, 100)
            assert db.checkpoint().wal_truncated is True

        with Database(db_path) as db:
            assert len(db.get_nodes_by_label("Item")) == 101

    def test_passive_leaves_the_log_in_place(self, tmp_path: Path) -> None:
        _require_checkpoint()
        db_path = tmp_path / "ckpt_passive.db"
        with Database(db_path, create=True) as db:
            db.set_auto_checkpoint(0)
            for i in range(10):
                _commit_one(db, i)
            before = _wal_size(db_path)
            stats = db.checkpoint("passive")
            assert stats.wal_truncated is False
            assert _wal_size(db_path) >= before

    def test_small_interval_keeps_the_log_short(self, tmp_path: Path) -> None:
        _require_checkpoint()
        db_path = tmp_path / "ckpt_four.db"
        with Database(db_path, create=True) as db:
            db.set_auto_checkpoint(4)
            largest = 0
            for i in range(50):
                _commit_one(db, i)
                largest = max(largest, _wal_size(db_path))
            assert 0 < largest < 8 * PAGE_SIZE

        with Database(db_path) as db:
            assert len(db.get_nodes_by_label("Item")) == 50

    def test_min_interval_is_accepted(self, tmp_path: Path) -> None:
        _require_checkpoint()
        db_path = tmp_path / "ckpt_interval.db"
        with Database(db_path, create=True) as db:
            db.set_auto_checkpoint(4, min_interval_ms=60_000)
            for i in range(20):
                _commit_one(db, i)
            # The first automatic checkpoint may run; a minute must pass before
            # another, so the log holds more than four frames by the end.
            assert _wal_size(db_path) > 8 * PAGE_SIZE

    def test_environment_sets_the_default_and_the_setter_wins(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _require_checkpoint()
        db_path = tmp_path / "ckpt_env.db"
        monkeypatch.setenv("LATTICE_AUTO_CHECKPOINT_FRAMES", "4")
        with Database(db_path, create=True) as db:
            largest = 0
            for i in range(30):
                _commit_one(db, i)
                largest = max(largest, _wal_size(db_path))
            assert largest < 8 * PAGE_SIZE

            db.set_auto_checkpoint(0)
            before = _wal_size(db_path)
            for i in range(30, 60):
                _commit_one(db, i)
            assert _wal_size(db_path) >= before + 30 * PAGE_SIZE

        monkeypatch.setenv("LATTICE_AUTO_CHECKPOINT_FRAMES", "0")
        with Database(db_path) as db:
            before = _wal_size(db_path)
            for i in range(60, 80):
                _commit_one(db, i)
            assert _wal_size(db_path) >= before + 20 * PAGE_SIZE
            assert len(db.get_nodes_by_label("Item")) == 80

    def test_read_only_handle_refuses_checkpoint(self, tmp_path: Path) -> None:
        _require_checkpoint()
        db_path = tmp_path / "ckpt_ro.db"
        with Database(db_path, create=True) as db:
            _commit_one(db, 0)

        with Database(db_path, read_only=True) as db:
            with pytest.raises(LatticeReadOnlyError):
                db.checkpoint()
            with pytest.raises(LatticeReadOnlyError):
                db.checkpoint("passive")
            # The setter has no effect on a read-only handle, and says nothing.
            db.set_auto_checkpoint(10)
            assert len(db.get_nodes_by_label("Item")) == 1

    def test_bad_arguments_are_refused_before_the_library(self, tmp_path: Path) -> None:
        _require_checkpoint()
        db_path = tmp_path / "ckpt_args.db"
        db = Database(db_path, create=True)
        # Not open: a bad argument is reported, not the closed handle.
        with pytest.raises(ValueError, match="mode"):
            db.checkpoint("sometimes")
        with pytest.raises(ValueError, match="max_wal_frames"):
            db.set_auto_checkpoint(-1)
        with db:
            with pytest.raises(ValueError, match="mode"):
                db.checkpoint("TRUNCATE")
            with pytest.raises(ValueError, match="mode"):
                db.checkpoint(2)  # type: ignore[arg-type]
            with pytest.raises(ValueError, match="max_wal_frames"):
                db.set_auto_checkpoint(-1)
            with pytest.raises(ValueError, match="min_interval_ms"):
                db.set_auto_checkpoint(10, -5)
            with pytest.raises(ValueError, match="max_wal_frames"):
                db.set_auto_checkpoint(2**64)
            with pytest.raises(TypeError):
                db.set_auto_checkpoint(1.5)  # type: ignore[arg-type]
            with pytest.raises(TypeError):
                db.set_auto_checkpoint(True)

    def test_older_library_raises_unsupported(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        lib = get_lib()
        monkeypatch.setattr(lib, "_has_checkpoint", False)
        monkeypatch.setattr(lib, "_has_set_auto_checkpoint", False)
        with Database(tmp_path / "ckpt_old.db", create=True) as db:
            _commit_one(db, 0)
            with pytest.raises(LatticeUnsupportedError):
                db.checkpoint()
            with pytest.raises(LatticeUnsupportedError):
                db.set_auto_checkpoint(0)

    def test_closed_database_raises(self, tmp_path: Path) -> None:
        _require_checkpoint()
        db = Database(tmp_path / "ckpt_closed.db", create=True)
        with pytest.raises(RuntimeError):
            db.checkpoint()
        with pytest.raises(RuntimeError):
            db.set_auto_checkpoint(0)
