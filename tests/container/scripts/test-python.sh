#!/bin/bash
# Python bindings smoke test.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SUITE_NAME="Python Binding Tests"
source "$SCRIPT_DIR/helpers.sh"

ARTIFACTS="${ARTIFACTS:-/artifacts}"
SRC="${SRC:-/src}"
PY_WORK="/tmp/py-build"
PY_WHEELHOUSE="/tmp/py-wheelhouse"

# Check if python3 is available
if ! command -v python3 &> /dev/null; then
    echo "# Python3 not available, skipping Python tests"
    TOTAL=1
    PASS=1
    test_summary
    exit 0
fi

# On Alpine (musl), numpy wheels don't work — use system packages in venv
VENV_OPTS=""
if [ -f /etc/alpine-release ]; then
    VENV_OPTS="--system-site-packages"
fi

test_begin "create virtual environment"
run python3 -m venv $VENV_OPTS /tmp/pytest-venv
assert_exit_code "$EXIT_CODE" 0

# Activate venv
source /tmp/pytest-venv/bin/activate

# Copy Python source to a writable location (source is mounted read-only)
rm -rf "$PY_WORK"
cp -r "$SRC/bindings/python" "$PY_WORK"

test_begin "build latticedb wheel with bundled native library"
run env LATTICE_BUNDLE_LIB_DIR="$ARTIFACTS/lib" pip wheel --quiet --no-deps "$PY_WORK" -w "$PY_WHEELHOUSE"
assert_exit_code "$EXIT_CODE" 0

test_begin "install latticedb wheel"
run pip install --quiet "$PY_WHEELHOUSE"/latticedb-*.whl
assert_exit_code "$EXIT_CODE" 0

test_begin "python smoke test: import, resolve bundled lib, create, query"
run env -u LATTICE_LIB_PATH -u LATTICE_PREFIX -u LD_LIBRARY_PATH python3 -c "
from latticedb import Database
from latticedb._bindings import _find_library

lib_path = _find_library()
assert lib_path is not None, 'Expected bundled liblattice path'
resolved = str(lib_path).replace('\\\\', '/')
assert '/latticedb/lib/' in resolved, resolved
assert '/artifacts/lib/' not in resolved, resolved
print('Bundled library path:', resolved)

# Open and create a database
db = Database('/tmp/pytest-smoke.lattice', create=True)
db.open()

# Use a write transaction
with db.write() as txn:
    node = txn.create_node(labels=['Person'], properties={'name': 'Alice', 'age': 30})
    txn.commit()

# Query the data
result = db.query('MATCH (n:Person) RETURN n.name')
assert len(result) > 0, f'Expected rows, got {len(result)}'
print(f'Query returned {len(result)} row(s)')

db.close()
print('Python smoke test passed')
"
assert_exit_code "$EXIT_CODE" 0

test_begin "smoke test output confirms bundled library path"
assert_contains "$STDOUT" "/latticedb/lib/"

test_begin "smoke test output confirms success"
assert_contains "$STDOUT" "Python smoke test passed"

# Cleanup
rm -f /tmp/pytest-smoke.lattice /tmp/pytest-smoke.lattice-wal
rm -rf /tmp/pytest-venv
rm -rf "$PY_WORK"
rm -rf "$PY_WHEELHOUSE"

test_summary
