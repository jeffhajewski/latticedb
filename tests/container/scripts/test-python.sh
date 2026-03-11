#!/bin/bash
# Python bindings smoke test.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SUITE_NAME="Python Binding Tests"
source "$SCRIPT_DIR/helpers.sh"

ARTIFACTS="${ARTIFACTS:-/artifacts}"
SRC="${SRC:-/src}"

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

test_begin "install latticedb from source"
run pip install --quiet "$SRC/bindings/python"
assert_exit_code "$EXIT_CODE" 0

test_begin "python smoke test: import, create, query"
run env LATTICE_LIB_PATH="$ARTIFACTS/lib" python3 -c "
from latticedb import Database

# Open and create a database
db = Database('/tmp/pytest-smoke.lattice', create=True)
db.open()

# Use a write transaction
with db.write() as txn:
    node = txn.create_node(labels=['Person'], properties={'name': 'Alice', 'age': 30})
    txn.commit()

# Query the data
result = db.query('MATCH (n:Person) RETURN n.name')
assert len(result.rows) > 0, f'Expected rows, got {len(result.rows)}'
print(f'Query returned {len(result.rows)} row(s)')

db.close()
print('Python smoke test passed')
"
assert_exit_code "$EXIT_CODE" 0

test_begin "smoke test output confirms success"
assert_contains "$STDOUT" "Python smoke test passed"

# Cleanup
rm -f /tmp/pytest-smoke.lattice /tmp/pytest-smoke.lattice-wal
rm -rf /tmp/pytest-venv

test_summary
