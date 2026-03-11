#!/bin/bash
# Test that the shared library can be loaded and basic C API functions work.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SUITE_NAME="Shared Library Tests"
source "$SCRIPT_DIR/helpers.sh"

ARTIFACTS="${ARTIFACTS:-/artifacts}"
C_TEST_DIR="${C_TEST_DIR:-/c-test}"

test_begin "compile test program against lattice.h"
run gcc -o /tmp/test_load_lib \
    "$C_TEST_DIR/test_load_lib.c" \
    -I"$ARTIFACTS/include" \
    -L"$ARTIFACTS/lib" \
    -llattice \
    -Wl,-rpath,"$ARTIFACTS/lib"
assert_exit_code "$EXIT_CODE" 0

test_begin "run compiled test program"
run env LD_LIBRARY_PATH="$ARTIFACTS/lib" /tmp/test_load_lib
assert_exit_code "$EXIT_CODE" 0

test_begin "lattice_version() returned a version string"
assert_contains "$STDOUT" "lattice_version()"

test_begin "lattice_open() succeeded"
assert_contains "$STDOUT" "lattice_open() succeeded"

test_begin "lattice_node_create() succeeded"
assert_contains "$STDOUT" "lattice_node_create()"

test_begin "0 failures reported"
assert_contains "$STDOUT" "0 failures"

# Cleanup
rm -f /tmp/test_load_lib /tmp/c_api_test.lattice

test_summary
