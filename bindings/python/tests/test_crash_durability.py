"""Fresh-process recovery must preserve acknowledged vector transactions."""

import os
import subprocess
import sys

import pytest

from latticedb import Database, library_available

pytestmark = pytest.mark.skipif(
    not library_available(), reason="Native library not found - build with 'zig build shared'"
)


def _run_child(script: str, *args: object) -> None:
    result = subprocess.run(
        [sys.executable, "-c", script, *map(str, args)],
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
        env=os.environ.copy(),
    )
    assert result.returncode == 91, result.stdout + result.stderr


@pytest.mark.parametrize("existing", [False, True])
@pytest.mark.parametrize("dimensions", [128, 1536])
def test_committed_vectors_recover_without_close(tmp_path, existing, dimensions):
    path = tmp_path / "crash.lattice"
    if existing:
        with Database(path, create=True, enable_vectors=True, vector_dimensions=dimensions) as db:
            db.create_node_fts_index("EvidenceChunk", "text")

    _run_child(
        """
import os, sys
from latticedb import Database

path, dimensions, existing = sys.argv[1:]
dimensions = int(dimensions)
old = [1.0, 0.0, 0.0] + [0.0] * (dimensions - 3)
new = [0.0, 1.0, 0.0] + [0.0] * (dimensions - 3)
other = [0.0, 0.0, 1.0] + [0.0] * (dimensions - 3)
with Database(path, create=True, enable_vectors=True, vector_dimensions=dimensions) as db:
    if existing == "False":
        db.create_node_fts_index("EvidenceChunk", "text")
    with db.write() as txn:
        keep = txn.create_node(labels=["EvidenceChunk"], properties={"text": "original"})
        remove = txn.create_node(labels=["EvidenceChunk"], properties={"text": "removed"})
        txn.set_vector(keep.id, "embedding", old)
        txn.set_vector(remove.id, "embedding", other)
        txn.commit()
    with db.write() as txn:
        txn.set_property(keep.id, "text", "retained")
        txn.set_vector(keep.id, "embedding", new)
        txn.delete_node(remove.id)
        txn.commit()
    hits = db.vector_search(new, k=10)
    assert len(hits) == 1 and hits[0].node_id == keep.id
    assert abs(hits[0].distance) < 1e-5
    # A staged vector write must remain absent, even though there is no rollback.
    with db.write() as txn:
        poison = txn.create_node(labels=["EvidenceChunk"], properties={"text": "poison"})
        txn.set_vector(poison.id, "embedding", old)
        os._exit(91)
""",
        path,
        dimensions,
        existing,
    )

    reader = """
import os, sys
from latticedb import Database

path, dimensions = sys.argv[1:]
dimensions = int(dimensions)
query = [0.0, 1.0, 0.0] + [0.0] * (dimensions - 3)
with Database(path, enable_vectors=True, vector_dimensions=dimensions) as db:
    rows = list(db.query("MATCH (c:EvidenceChunk) RETURN c.text AS text"))
    assert rows == [{"text": "retained"}], rows
    hits = db.vector_search(query, k=10)
    assert len(hits) == 1, hits
    assert abs(hits[0].distance) < 1e-5, hits[0].distance
    assert [h.node_id for h in db.fts_search("EvidenceChunk", "text", "retained")] == [hits[0].node_id]
    assert not db.fts_search("EvidenceChunk", "text", "removed")
    assert not db.fts_search("EvidenceChunk", "text", "poison")
    # Recovery itself must survive termination without normal finalization.
    os._exit(91)
"""
    _run_child(reader, path, dimensions)
    _run_child(reader, path, dimensions)
