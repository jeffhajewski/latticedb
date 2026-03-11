#!/bin/bash
# CLI user-perspective integration tests.
# Tests the `lattice` binary from a user's point of view.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SUITE_NAME="CLI Tests"
source "$SCRIPT_DIR/helpers.sh"

LATTICE="${LATTICE:-lattice}"
DB="/tmp/test-cli.lattice"
DB2="/tmp/test-cli-import.lattice"
DB3="/tmp/test-cli-roundtrip.lattice"
FIXTURES="${FIXTURES:-/fixtures}"

# Clean up any previous test databases
rm -f "$DB" "$DB-wal" "$DB2" "$DB2-wal" "$DB3" "$DB3-wal"

# ---------- Basic commands ----------

test_begin "version command returns version string"
run "$LATTICE" version
assert_contains "$STDOUT" "0."

test_begin "help command shows usage"
run "$LATTICE" help
assert_contains "$STDOUT" "Usage"

test_begin "--help flag works"
run "$LATTICE" --help
assert_contains "$STDOUT" "Usage"

# ---------- Database creation ----------

test_begin "create database"
run "$LATTICE" create "$DB"
assert_exit_code "$EXIT_CODE" 0

test_begin "created database file exists"
assert_file_exists "$DB"

test_begin "create database with vector options"
DB_VEC="/tmp/test-cli-vec.lattice"
rm -f "$DB_VEC" "$DB_VEC-wal"
run "$LATTICE" create "$DB_VEC" --enable-vector --vector-dims=384
assert_exit_code "$EXIT_CODE" 0
rm -f "$DB_VEC" "$DB_VEC-wal"

# ---------- Database info ----------

test_begin "info command shows database metadata"
run "$LATTICE" info "$DB"
assert_contains "$STDOUT" "Nodes"

test_begin "info command with JSON format"
run "$LATTICE" info "$DB" --format=json
assert_valid_json "$STDOUT"

# ---------- Count (empty DB) ----------

test_begin "count on empty database"
run "$LATTICE" count "$DB" --format=json
assert_exit_code "$EXIT_CODE" 0

# ---------- Exec CREATE ----------

test_begin "exec CREATE node"
run "$LATTICE" exec "$DB" --query='CREATE (n:Person {name: "Alice", age: 30})'
assert_exit_code "$EXIT_CODE" 0

test_begin "exec CREATE second node"
run "$LATTICE" exec "$DB" --query='CREATE (n:Person {name: "Bob", age: 25})'
assert_exit_code "$EXIT_CODE" 0

# ---------- Exec MATCH ----------

test_begin "exec MATCH returns created node"
run "$LATTICE" exec "$DB" --query='MATCH (n:Person) WHERE n.name = "Alice" RETURN n.name' --format=json
assert_contains "$STDOUT" "Alice"

# ---------- Labels ----------

test_begin "labels command shows Person"
run "$LATTICE" labels "$DB"
assert_contains "$STDOUT" "Person"

# ---------- Schema ----------

test_begin "schema command returns valid JSON"
run "$LATTICE" schema "$DB" --format=json
assert_valid_json "$STDOUT"

# ---------- Edge creation and query ----------

test_begin "create edge between nodes"
run "$LATTICE" exec "$DB" --query='MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"}) CREATE (a)-[:KNOWS {since: 2020}]->(b)'
assert_exit_code "$EXIT_CODE" 0

test_begin "types command shows KNOWS"
run "$LATTICE" types "$DB"
assert_contains "$STDOUT" "KNOWS"

# ---------- Count after inserts ----------

test_begin "count after inserts shows correct totals"
run "$LATTICE" count "$DB" --format=json
assert_exit_code "$EXIT_CODE" 0

# ---------- Import ----------

test_begin "import JSON data"
run "$LATTICE" create "$DB2"
run "$LATTICE" import "$DB2" --file="$FIXTURES/sample-graph.json"
assert_exit_code "$EXIT_CODE" 0

test_begin "count after import"
run "$LATTICE" count "$DB2" --format=json
assert_contains "$STDOUT" "4"

# ---------- Export ----------

test_begin "export to JSON"
EXPORT_FILE="/tmp/test-export.json"
rm -f "$EXPORT_FILE"
run "$LATTICE" export "$DB2" --file="$EXPORT_FILE"
assert_exit_code "$EXIT_CODE" 0

test_begin "exported JSON file exists and is valid"
assert_file_exists "$EXPORT_FILE"

# ---------- Dump ----------

test_begin "dump outputs valid JSON to stdout"
run "$LATTICE" dump "$DB2"
assert_valid_json "$STDOUT"

# ---------- Round-trip ----------

test_begin "round-trip: export then import into new DB"
run "$LATTICE" create "$DB3"
run "$LATTICE" import "$DB3" --file="$EXPORT_FILE"
assert_exit_code "$EXIT_CODE" 0

# ---------- Query from file ----------

test_begin "exec with --file flag"
run "$LATTICE" exec "$DB" --file="$FIXTURES/test-queries.cypher"
assert_exit_code "$EXIT_CODE" 0

# ---------- Error handling ----------

test_begin "info on nonexistent database fails"
run "$LATTICE" info "/tmp/does-not-exist-12345.db"
assert_exit_code "$EXIT_CODE" 1

test_begin "invalid query syntax fails"
run "$LATTICE" exec "$DB" --query='THIS IS NOT VALID CYPHER'
if [ "$EXIT_CODE" -ne 0 ]; then
    pass
else
    fail "expected non-zero exit code for invalid query"
fi

# ---------- Output formats ----------

test_begin "table format works"
run "$LATTICE" exec "$DB" --query='MATCH (n:Person) RETURN n.name' --format=table
assert_exit_code "$EXIT_CODE" 0

test_begin "csv format works"
run "$LATTICE" exec "$DB" --query='MATCH (n:Person) RETURN n.name' --format=csv
assert_exit_code "$EXIT_CODE" 0

# ---------- Cleanup ----------
rm -f "$DB" "$DB-wal" "$DB2" "$DB2-wal" "$DB3" "$DB3-wal" "$EXPORT_FILE"
rm -f "$DB_VEC" "$DB_VEC-wal"

test_summary
