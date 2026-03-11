/*
 * Minimal test program to verify the LatticeDB shared library can be loaded
 * and basic C API functions work.
 *
 * Compile: gcc -o test_load_lib test_load_lib.c -I/artifacts/include -L/artifacts/lib -llattice
 * Run:     LD_LIBRARY_PATH=/artifacts/lib ./test_load_lib
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "lattice.h"

int main(void) {
    int failures = 0;

    /* Test 1: lattice_version() returns a non-NULL string */
    const char *version = lattice_version();
    if (version == NULL) {
        fprintf(stderr, "FAIL: lattice_version() returned NULL\n");
        failures++;
    } else {
        printf("ok - lattice_version() = \"%s\"\n", version);
    }

    /* Test 2: lattice_error_message() returns descriptions */
    const char *ok_msg = lattice_error_message(LATTICE_OK);
    if (ok_msg == NULL || strlen(ok_msg) == 0) {
        fprintf(stderr, "FAIL: lattice_error_message(LATTICE_OK) returned empty\n");
        failures++;
    } else {
        printf("ok - lattice_error_message(LATTICE_OK) = \"%s\"\n", ok_msg);
    }

    /* Test 3: Open, create node, close a database */
    const char *db_path = "/tmp/c_api_test.lattice";
    lattice_open_options opts = {0};
    opts.create = 1;

    lattice_database *db = NULL;
    lattice_error err = lattice_open(db_path, &opts, &db);
    if (err != LATTICE_OK) {
        fprintf(stderr, "FAIL: lattice_open() = %d (%s)\n", err, lattice_error_message(err));
        failures++;
    } else {
        printf("ok - lattice_open() succeeded\n");

        /* Begin a write transaction */
        lattice_txn *txn = NULL;
        err = lattice_begin(db, LATTICE_TXN_READ_WRITE, &txn);
        if (err != LATTICE_OK) {
            fprintf(stderr, "FAIL: lattice_begin() = %d\n", err);
            failures++;
        } else {
            printf("ok - lattice_begin(READ_WRITE) succeeded\n");

            /* Create a node */
            lattice_node_id node_id = 0;
            err = lattice_node_create(txn, "TestNode", &node_id);
            if (err != LATTICE_OK) {
                fprintf(stderr, "FAIL: lattice_node_create() = %d\n", err);
                failures++;
            } else {
                printf("ok - lattice_node_create() id=%llu\n", (unsigned long long)node_id);
            }

            /* Commit */
            err = lattice_commit(txn);
            if (err != LATTICE_OK) {
                fprintf(stderr, "FAIL: lattice_commit() = %d\n", err);
                failures++;
            } else {
                printf("ok - lattice_commit() succeeded\n");
            }
        }

        lattice_close(db);
        printf("ok - lattice_close() succeeded\n");
    }

    /* Clean up */
    remove(db_path);

    printf("\n# C API test: %d failures\n", failures);
    return failures > 0 ? 1 : 0;
}
