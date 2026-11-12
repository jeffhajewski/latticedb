"""
Tests for the changefeed's retention policy: setting it at open, reading its
status, trimming, and what a reader sees when events have been dropped.
"""

import pytest

from latticedb import (
    ChangefeedStatus,
    Database,
    LatticeChangesTrimmedError,
    LatticeLockTimeoutError,
    LatticeNotFoundError,
    LatticeUnsupportedError,
    library_available,
)
from latticedb._bindings import get_lib

pytestmark = pytest.mark.skipif(
    not library_available(),
    reason="Native library not found - build with 'zig build' first",
)


def _require_changefeed() -> None:
    if not getattr(get_lib(), "_has_changefeed_status", False):
        pytest.skip("native library has no lattice_changefeed_status")


def _commit_nodes(db: Database, count: int) -> None:
    """Commit ``count`` transactions of one labelled node: two events each."""
    for _ in range(count):
        with db.write() as txn:
            txn.create_node(labels=["N"])
            txn.commit()


class TestChangefeedRetention:
    def test_default_keeps_everything(self, tmp_path):
        _require_changefeed()
        with Database(tmp_path / "all.db", create=True) as db:
            _commit_nodes(db, 3)
            assert db.changefeed_status() == ChangefeedStatus("all", 1, 6)
            assert db.trim_changefeed() == 0
            assert [r.sequence for r in db.changes(limit=100)] == [1, 2, 3, 4, 5, 6]

    def test_last_n_keeps_the_newest_and_tells_a_reader_it_fell_behind(self, tmp_path):
        _require_changefeed()
        with Database(tmp_path / "last.db", create=True, changefeed_retention="last:4") as db:
            _commit_nodes(db, 5)  # events 1..10
            assert db.trim_changefeed() == 6
            status = db.changefeed_status()
            assert status == ChangefeedStatus("last:4", 7, 10)

            # From the start: the oldest event held on.
            assert [r.sequence for r in db.changes()] == [7, 8, 9, 10]
            # On from an event still held, or the one just before the oldest.
            assert [r.sequence for r in db.changes(after_sequence=6)] == [7, 8, 9, 10]
            # On from a dropped event: the reader missed changes, and is told.
            with pytest.raises(LatticeChangesTrimmedError) as info:
                db.changes(after_sequence=3)
            assert isinstance(info.value, LatticeNotFoundError)

            # Records read the same as ever.
            first = db.changes(after_sequence=6, limit=1)[0]
            assert first.kind == "node.insert"
            assert first.payload["entity"] == "node"

    def test_policy_is_recorded_in_the_file(self, tmp_path):
        _require_changefeed()
        path = tmp_path / "stored.db"
        with Database(path, create=True, changefeed_retention="age:7d") as db:
            assert db.changefeed_status().retention == "age:7d"
        # Unset keeps the file's policy.
        with Database(path) as db:
            assert db.changefeed_status().retention == "age:7d"
        # A read-only open reports it and changes nothing.
        with Database(path, read_only=True, changefeed_retention="off") as db:
            assert db.changefeed_status().retention == "age:7d"
        # A writable open that names another replaces it.
        with Database(path, changefeed_retention="all") as db:
            assert db.changefeed_status().retention == "all"
        with Database(path) as db:
            assert db.changefeed_status().retention == "all"

    def test_off_writes_nothing_and_reading_is_unsupported(self, tmp_path):
        _require_changefeed()
        path = tmp_path / "off.db"
        with Database(path, create=True) as db:
            _commit_nodes(db, 2)  # events 1..4
        with Database(path, changefeed_retention="off") as db:
            _commit_nodes(db, 2)
            with pytest.raises(LatticeUnsupportedError):
                db.changes()
            db.trim_changefeed()
            assert db.changefeed_status() == ChangefeedStatus("off", None, 4)
        # Back on: the gap is visible to a reader from before.
        with Database(path, changefeed_retention="all") as db:
            _commit_nodes(db, 1)
            with pytest.raises(LatticeChangesTrimmedError):
                db.changes(after_sequence=4)
            assert [r.sequence for r in db.changes()] == [6, 7]

    def test_trim_refused_while_a_write_transaction_is_open(self, tmp_path):
        _require_changefeed()
        with Database(tmp_path / "busy.db", create=True, changefeed_retention="last:1") as db:
            _commit_nodes(db, 2)
            with db.write() as txn:
                with pytest.raises(LatticeLockTimeoutError):
                    db.trim_changefeed()
                txn.rollback()

    @pytest.mark.parametrize(
        "policy",
        ["sometimes", "last:0", "last:", "last:-3", "age:7", "age:0s", "age:7w", "age:d", 7],
    )
    def test_invalid_policies_are_refused(self, tmp_path, policy):
        with pytest.raises((ValueError, TypeError)):
            Database(tmp_path / "bad.db", create=True, changefeed_retention=policy)

    @pytest.mark.parametrize(
        "policy",
        ["all", "ALL", "off", "last:1", "age:500ms", "age:90s", "age:30m", "age:12h", "age:7d"],
    )
    def test_valid_policies_round_trip(self, tmp_path, policy):
        _require_changefeed()
        with Database(tmp_path / "ok.db", create=True, changefeed_retention=policy) as db:
            expected = {"ALL": "all", "age:90s": "age:90s"}.get(policy, policy)
            assert db.changefeed_status().retention == expected
