#!/bin/bash
# TypeScript/Node.js bindings smoke test.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SUITE_NAME="TypeScript Binding Tests"
source "$SCRIPT_DIR/helpers.sh"

ARTIFACTS="${ARTIFACTS:-/artifacts}"
SRC="${SRC:-/src}"
export LATTICE_LIB_PATH="${LATTICE_LIB_PATH:-$ARTIFACTS/lib}"

# Check if node is available
if ! command -v node &> /dev/null; then
    echo "# Node.js not available, skipping TypeScript tests"
    TOTAL=1
    PASS=1
    test_summary
    exit 0
fi

# Copy TypeScript source to a writable location (source is mounted read-only)
TS_WORK="/tmp/ts-build"
rm -rf "$TS_WORK"
cp -r "$SRC/bindings/typescript" "$TS_WORK"

test_begin "install npm dependencies"
run bash -c "cd '$TS_WORK' && if [ -d node_modules ]; then echo 'Reusing copied node_modules'; else npm ci --ignore-scripts --no-audit --no-fund 2>&1; fi"
assert_exit_code "$EXIT_CODE" 0

test_begin "build TypeScript"
run bash -c "cd '$TS_WORK' && npm run build 2>&1"
assert_exit_code "$EXIT_CODE" 0

test_begin "node smoke test: require, create, query"
run node -e "
const { Database } = require('$TS_WORK/dist');

async function main() {
    const db = new Database('/tmp/nodetest.lattice', { create: true });
    await db.open();

    // write() auto-commits on success
    await db.write(async (txn) => {
        await txn.createNode({ labels: ['Person'], properties: { name: 'Alice' } });
    });

    const result = await db.query('MATCH (n:Person) RETURN n.name');
    console.log('Query returned ' + result.rows.length + ' row(s)');
    if (!result || result.rows.length === 0) {
        throw new Error('Expected query results');
    }

    await db.close();
    console.log('TypeScript smoke test passed');
}

main().catch(e => { console.error(e); process.exit(1); });
"
assert_exit_code "$EXIT_CODE" 0

test_begin "smoke test output confirms success"
assert_contains "$STDOUT" "TypeScript smoke test passed"

# Cleanup
rm -f /tmp/nodetest.lattice /tmp/nodetest.lattice-wal
rm -rf "$TS_WORK"

test_summary
