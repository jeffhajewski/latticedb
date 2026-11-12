"""A database opened with enable_wal=False has transactions like any other.

Turning the log off means nothing is logged, so nothing survives a crash
until the next flush. It used to mean as well that no write transaction
could begin, so this binding could not write to such a database at all.
"""

import pytest

from latticedb import Database, LatticeError, LatticeLockTimeoutError


@pytest.fixture(params=[True, False], ids=["log", "no_log"])
def db(request, tmp_path):
    with Database(tmp_path / "db.lattice", create=True, enable_wal=request.param) as database:
        yield database


def count(db, cypher):
    rows = list(db.query(cypher))
    return next(iter(rows[0].values()))


def test_writes_without_a_log_reach_the_file_at_close(tmp_path):
    path = tmp_path / "nolog.lattice"
    with Database(path, create=True, enable_wal=False) as db:
        with db.write() as txn:
            txn.create_node(labels=["P"], properties={"name": "a"})
            txn.commit()
        db.query("CREATE (:P {name: 'b'})")
    assert not (tmp_path / "nolog.lattice-wal").exists()

    with Database(path, enable_wal=False) as db:
        assert count(db, "MATCH (n:P) RETURN count(n)") == 2


def test_a_query_that_fails_part_way_changes_nothing(db):
    db.query("CREATE (:D {i: 1})-[:R]->(:D {i: 2}), (:D {i: 3})")
    with pytest.raises(LatticeError):
        db.query("MATCH (a:D {i: 3}), (b:D {i: 2}) DELETE a CREATE (b)-[:R]->(a)")
    assert count(db, "MATCH (n:D) RETURN count(n)") == 3

    # Over several rows, failing on the last.
    db.query("CREATE (:M {i: 1}), (:M {i: 2}), (:M {i: 3})-[:R]->(:X)")
    with pytest.raises(LatticeError):
        db.query("MATCH (m:M) WITH m ORDER BY m.i DELETE m")
    assert count(db, "MATCH (m:M) RETURN count(m)") == 3


def test_rollback_and_one_writer(db):
    with db.write() as txn:
        txn.create_node(labels=["P"])
        txn.commit()

    with db.write() as txn:
        txn.create_node(labels=["P"])
        with pytest.raises(LatticeLockTimeoutError):
            with db.write():
                pass
        txn.rollback()
    assert count(db, "MATCH (n:P) RETURN count(n)") == 1

    # Leaving the block without committing rolls back too.
    with pytest.raises(RuntimeError):
        with db.write() as txn:
            txn.create_node(labels=["P"])
            raise RuntimeError("abandon")
    assert count(db, "MATCH (n:P) RETURN count(n)") == 1


def test_a_reader_keeps_its_snapshot_across_a_commit(db):
    db.query("CREATE (:P {v: 1})")
    with db.read() as reader:
        db.query("MATCH (n:P) SET n.v = 2 CREATE (:P {v: 3})")
        assert len(reader.query("MATCH (n:P) RETURN n.v")) == 1
        assert list(reader.query("MATCH (n:P) RETURN n.v AS v"))[0]["v"] == 1
    assert count(db, "MATCH (n:P) RETURN count(n)") == 2
