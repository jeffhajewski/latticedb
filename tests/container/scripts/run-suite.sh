#!/bin/bash
# Entrypoint for container tests. Runs all test scripts and collects results.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DISTRO="${DISTRO:-unknown}"
OVERALL_EXIT=0

echo "========================================"
echo "Container Integration Tests: $DISTRO"
echo "========================================"
echo ""

run_test() {
    local script="$1"
    local name="$2"

    if [ ! -f "$script" ]; then
        echo "# Skipping $name (script not found)"
        return 0
    fi

    echo "--- $name ---"
    if bash "$script"; then
        echo ""
    else
        echo ""
        OVERALL_EXIT=1
    fi
}

run_test "$SCRIPT_DIR/test-cli.sh" "CLI Tests"
run_test "$SCRIPT_DIR/test-shared-lib.sh" "Shared Library Tests"
run_test "$SCRIPT_DIR/test-python.sh" "Python Binding Tests"
run_test "$SCRIPT_DIR/test-typescript.sh" "TypeScript Binding Tests"

echo "========================================"
if [ "$OVERALL_EXIT" -eq 0 ]; then
    echo "ALL TEST SUITES PASSED on $DISTRO"
else
    echo "SOME TEST SUITES FAILED on $DISTRO"
fi
echo "========================================"

exit "$OVERALL_EXIT"
