#!/bin/bash
# TypeScript/Node.js bindings smoke test.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SUITE_NAME="TypeScript Binding Tests"
source "$SCRIPT_DIR/helpers.sh"

ARTIFACTS="${ARTIFACTS:-/artifacts}"
SRC="${SRC:-/src}"

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
TS_CONSUMER="/tmp/ts-consumer"
rm -rf "$TS_WORK"
rm -rf "$TS_CONSUMER"
cp -r "$SRC/bindings/typescript" "$TS_WORK"

test_begin "install npm dependencies"
run bash -c "cd '$TS_WORK' && if [ -d node_modules ]; then echo 'Reusing copied node_modules'; else npm ci --ignore-scripts --no-audit --no-fund 2>&1; fi"
assert_exit_code "$EXIT_CODE" 0

test_begin "bundle native library into package layout"
run bash -c "cd '$TS_WORK' && env LATTICE_BUNDLE_LIB_DIR='$ARTIFACTS/lib' npm run bundle:native 2>&1"
assert_exit_code "$EXIT_CODE" 0

test_begin "build TypeScript"
run bash -c "cd '$TS_WORK' && npm run build 2>&1"
assert_exit_code "$EXIT_CODE" 0

test_begin "pack TypeScript package"
run bash -c "cd '$TS_WORK' && npm pack --ignore-scripts --quiet 2>&1"
assert_exit_code "$EXIT_CODE" 0

test_begin "install packed TypeScript package"
run bash -c "mkdir -p '$TS_CONSUMER' && cd '$TS_CONSUMER' && npm init -y >/dev/null 2>&1 && npm install --ignore-scripts --no-audit --no-fund '$TS_WORK'/*.tgz 2>&1"
assert_exit_code "$EXIT_CODE" 0

test_begin "node smoke test: require packaged bundle, create, query"
run env -u LATTICE_LIB_PATH -u LATTICE_PREFIX -u LD_LIBRARY_PATH node -e "
const { Database } = require('$TS_CONSUMER/node_modules/@hajewski/latticedb');
const { getLibraryPath } = require('$TS_CONSUMER/node_modules/@hajewski/latticedb/dist/ffi');

async function main() {
    const db = new Database('/tmp/nodetest.lattice', { create: true });
    await db.open();

    const resolved = (getLibraryPath() || '').replace(/\\\\/g, '/');
    console.log('Bundled library path: ' + resolved);
    if (!resolved.includes('/node_modules/@hajewski/latticedb/lib/')) {
        throw new Error('Expected packaged bundled library path, got: ' + resolved);
    }
    if (resolved.includes('/artifacts/lib/')) {
        throw new Error('Expected packaged bundled library, not artifact fallback: ' + resolved);
    }

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

test_begin "smoke test output confirms bundled library path"
assert_contains "$STDOUT" "/node_modules/@hajewski/latticedb/lib/"

test_begin "smoke test output confirms success"
assert_contains "$STDOUT" "TypeScript smoke test passed"

# Cleanup
rm -f /tmp/nodetest.lattice /tmp/nodetest.lattice-wal
rm -rf "$TS_WORK"
rm -rf "$TS_CONSUMER"

test_summary
