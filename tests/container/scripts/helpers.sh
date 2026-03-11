#!/bin/bash
# Test assertion helpers for container integration tests.
# Source this file in test scripts: source /scripts/helpers.sh

set -euo pipefail

PASS=0
FAIL=0
TOTAL=0
CURRENT_TEST=""
SUITE_NAME="${SUITE_NAME:-tests}"

# Colors (if terminal supports it)
if [ -t 1 ]; then
    GREEN='\033[0;32m'
    RED='\033[0;31m'
    YELLOW='\033[0;33m'
    BOLD='\033[1m'
    RESET='\033[0m'
else
    GREEN=''
    RED=''
    YELLOW=''
    BOLD=''
    RESET=''
fi

test_begin() {
    CURRENT_TEST="$1"
    TOTAL=$((TOTAL + 1))
}

pass() {
    PASS=$((PASS + 1))
    echo -e "${GREEN}ok ${TOTAL}${RESET} - ${CURRENT_TEST}"
}

fail() {
    local msg="${1:-}"
    FAIL=$((FAIL + 1))
    echo -e "${RED}not ok ${TOTAL}${RESET} - ${CURRENT_TEST}"
    if [ -n "$msg" ]; then
        echo "  # $msg"
    fi
}

# Assert that the exit code matches expected value.
# Usage: assert_exit_code <actual> <expected>
assert_exit_code() {
    local actual="$1"
    local expected="$2"
    if [ "$actual" -eq "$expected" ]; then
        pass
    else
        fail "expected exit code $expected, got $actual"
    fi
}

# Assert that output contains a substring.
# Usage: assert_contains "$output" "expected substring"
assert_contains() {
    local output="$1"
    local expected="$2"
    if echo "$output" | grep -q "$expected"; then
        pass
    else
        fail "output does not contain '$expected'"
        echo "  # actual output: $(echo "$output" | head -5)"
    fi
}

# Assert that output does NOT contain a substring.
# Usage: assert_not_contains "$output" "unexpected substring"
assert_not_contains() {
    local output="$1"
    local unexpected="$2"
    if echo "$output" | grep -q "$unexpected"; then
        fail "output unexpectedly contains '$unexpected'"
    else
        pass
    fi
}

# Assert that a file exists.
# Usage: assert_file_exists "/path/to/file"
assert_file_exists() {
    local path="$1"
    if [ -f "$path" ]; then
        pass
    else
        fail "file does not exist: $path"
    fi
}

# Assert that a file does NOT exist.
# Usage: assert_file_not_exists "/path/to/file"
assert_file_not_exists() {
    local path="$1"
    if [ -f "$path" ]; then
        fail "file unexpectedly exists: $path"
    else
        pass
    fi
}

# Assert that output is valid JSON (uses python3).
# Usage: assert_valid_json "$output"
assert_valid_json() {
    local output="$1"
    if echo "$output" | python3 -m json.tool > /dev/null 2>&1; then
        pass
    else
        fail "output is not valid JSON"
        echo "  # actual output: $(echo "$output" | head -3)"
    fi
}

# Assert that a JSON field has a specific value (uses python3).
# Usage: assert_json_field "$json" "field_name" "expected_value"
assert_json_field() {
    local json="$1"
    local field="$2"
    local expected="$3"
    local actual
    actual=$(echo "$json" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['$field'])" 2>/dev/null) || true
    if [ "$actual" = "$expected" ]; then
        pass
    else
        fail "JSON field '$field': expected '$expected', got '$actual'"
    fi
}

# Run a command and capture its output and exit code.
# Usage: run lattice version
#   Then check: $EXIT_CODE, $STDOUT, $STDERR
run() {
    local tmpout tmperr
    tmpout=$(mktemp)
    tmperr=$(mktemp)
    set +e
    "$@" > "$tmpout" 2> "$tmperr"
    EXIT_CODE=$?
    set -e
    STDOUT=$(cat "$tmpout")
    STDERR=$(cat "$tmperr")
    rm -f "$tmpout" "$tmperr"
}

# Print test summary and exit with appropriate code.
test_summary() {
    echo ""
    echo -e "${BOLD}# ${SUITE_NAME}: ${PASS} passed, ${FAIL} failed, ${TOTAL} total${RESET}"
    if [ "$FAIL" -gt 0 ]; then
        echo -e "${RED}FAIL${RESET}"
        return 1
    else
        echo -e "${GREEN}PASS${RESET}"
        return 0
    fi
}
