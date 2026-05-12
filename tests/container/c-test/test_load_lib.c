/*
 * Minimal test program to verify the LatticeDB shared library can be loaded
 * and basic C API functions work.
 *
 * Compile: gcc -o test_load_lib test_load_lib.c -I/artifacts/include -L/artifacts/lib -llattice
 * Run:     LD_LIBRARY_PATH=/artifacts/lib ./test_load_lib
 */

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "lattice.h"

_Static_assert(sizeof(lattice_open_options) == 16, "lattice_open_options ABI size changed");
_Static_assert(offsetof(lattice_open_options, create) == 0, "create offset changed");
_Static_assert(offsetof(lattice_open_options, read_only) == 1, "read_only offset changed");
_Static_assert(offsetof(lattice_open_options, cache_size_mb) == 4, "cache_size_mb offset changed");
_Static_assert(offsetof(lattice_open_options, page_size) == 8, "page_size offset changed");
_Static_assert(sizeof(((lattice_open_options *)0)->page_size) == sizeof(uint32_t), "page_size must be uint32_t");
_Static_assert(offsetof(lattice_open_options, enable_vector) == 12, "enable_vector offset changed");
_Static_assert(offsetof(lattice_open_options, vector_dimensions) == 14, "vector_dimensions offset changed");

_Static_assert(sizeof(lattice_open_options_v2) == 32, "lattice_open_options_v2 ABI size changed");
_Static_assert(offsetof(lattice_open_options_v2, struct_size) == 0, "struct_size offset changed");
_Static_assert(offsetof(lattice_open_options_v2, create) == 8, "v2 create offset changed");
_Static_assert(offsetof(lattice_open_options_v2, read_only) == 9, "v2 read_only offset changed");
_Static_assert(offsetof(lattice_open_options_v2, cache_size_mb) == 12, "v2 cache_size_mb offset changed");
_Static_assert(offsetof(lattice_open_options_v2, page_size) == 16, "v2 page_size offset changed");
_Static_assert(sizeof(((lattice_open_options_v2 *)0)->page_size) == sizeof(uint32_t), "v2 page_size must be uint32_t");
_Static_assert(offsetof(lattice_open_options_v2, enable_vector) == 20, "v2 enable_vector offset changed");
_Static_assert(offsetof(lattice_open_options_v2, vector_dimensions) == 22, "v2 vector_dimensions offset changed");
_Static_assert(offsetof(lattice_open_options_v2, enable_wal) == 24, "v2 enable_wal offset changed");

_Static_assert(sizeof(lattice_open_options_v3) == 32, "lattice_open_options_v3 ABI size changed");
_Static_assert(offsetof(lattice_open_options_v3, struct_size) == 0, "v3 struct_size offset changed");
_Static_assert(offsetof(lattice_open_options_v3, create) == 8, "v3 create offset changed");
_Static_assert(offsetof(lattice_open_options_v3, read_only) == 9, "v3 read_only offset changed");
_Static_assert(offsetof(lattice_open_options_v3, cache_size_mb) == 12, "v3 cache_size_mb offset changed");
_Static_assert(offsetof(lattice_open_options_v3, page_size) == 16, "v3 page_size offset changed");
_Static_assert(sizeof(((lattice_open_options_v3 *)0)->page_size) == sizeof(uint32_t), "v3 page_size must be uint32_t");
_Static_assert(offsetof(lattice_open_options_v3, enable_vector) == 20, "v3 enable_vector offset changed");
_Static_assert(offsetof(lattice_open_options_v3, vector_dimensions) == 22, "v3 vector_dimensions offset changed");
_Static_assert(offsetof(lattice_open_options_v3, enable_wal) == 24, "v3 enable_wal offset changed");
_Static_assert(offsetof(lattice_open_options_v3, enable_adjacency_cache) == 25, "v3 enable_adjacency_cache offset changed");

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
