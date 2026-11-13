/*
 * LatticeDB public-API benchmark.
 *
 * Every LatticeDB number this program prints is taken through include/lattice.h
 * against liblattice, the way an application or a binding meets the database:
 * lattice_open with the default options unless a flag says otherwise (log on,
 * default sync mode, 100 MB buffer pool, changefeed at its default), real
 * transactions, and lattice_commit returning before the clock stops.
 *
 * One invocation runs one workload once, on a fresh database, and prints one
 * line starting with "RESULT" (key=value pairs, times in microseconds). The
 * driver, tests/benchmark/capi_bench.py, runs each workload several times in
 * separate processes and reports the median and spread. Run through
 * `zig build capi-benchmark`; see book/src/performance/benchmarks.md.
 *
 * The program compiles against any LatticeDB release that has this C API
 * (0.16.0 onwards). Options that need lattice_open_v5 (--sync normal) are
 * refused when the header lacks it. The sqlite_* workloads run the same data
 * and the same questions through SQLite's public C API, for the comparison page.
 */

#include "lattice.h"

#include <errno.h>
#include <math.h>
#include <sqlite3.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#ifdef __APPLE__
#include <mach/mach.h>
#endif

#ifdef LATTICE_OPEN_OPTIONS_V5_DEFAULT
#define HAVE_OPEN_V5 1
#else
#define HAVE_OPEN_V5 0
#endif

/* ------------------------------------------------------------------------ */
/* Options                                                                   */
/* ------------------------------------------------------------------------ */

typedef struct {
    const char *workload;
    const char *db;          /* database path; removed first */
    long n;                  /* data size (nodes, documents, vectors) */
    long ops;                /* measured operations */
    long warmup;             /* unmeasured operations before measuring */
    long batch;              /* operations per transaction for batch workloads */
    long degree;             /* out-edges per node for graph workloads */
    const char *sync;        /* "default", "full", "normal" */
    int no_wal;
    long cache_mb;           /* 0: leave the default */
    int keep;                /* leave the database in place at the end */
    const char *ef_list;     /* vector: comma-separated ef_search values */
    long recall_queries;
    unsigned long long seed;
    const char *query;       /* lookup, traverse: which queries */
    long id_ops;             /* lookup: measured calls of the id() query */
    const char *changefeed;  /* "default" or "off" */
} options;

static options opt = {
    .workload = NULL,
    .db = NULL,
    .n = 100000,
    .ops = 2000,
    .warmup = -1,
    .batch = 1000,
    .degree = 5,
    .sync = "default",
    .no_wal = 0,
    .cache_mb = 0,
    .keep = 0,
    .ef_list = "0",
    .recall_queries = 200,
    .seed = 42,
    .query = "all",
    .id_ops = 0,
    .changefeed = "default",
};

static void die(const char *what, int code) {
    fprintf(stderr, "capi_bench: %s failed: %d (%s)\n", what, code,
            code < 0 ? lattice_error_message((lattice_error)code) : "");
    exit(2);
}

#define CHECK(call)                                   \
    do {                                              \
        lattice_error _e = (call);                    \
        if (_e != LATTICE_OK) die(#call, (int)_e);    \
    } while (0)

/* ------------------------------------------------------------------------ */
/* Clock, statistics, memory                                                 */
/* ------------------------------------------------------------------------ */

static inline uint64_t now_ns(void) {
    struct timespec ts;
#ifdef CLOCK_MONOTONIC_RAW
    clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
#else
    clock_gettime(CLOCK_MONOTONIC, &ts);
#endif
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static int cmp_u64(const void *a, const void *b) {
    uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
    return x < y ? -1 : x > y;
}

typedef struct {
    double p50, p90, p99, p999, max, mean;
} stats;

/* Sorts the samples. Nanoseconds in, microseconds out. */
static stats summarize(uint64_t *s, long n, double divisor) {
    stats st = {0};
    if (n <= 0) return st;
    qsort(s, (size_t)n, sizeof(uint64_t), cmp_u64);
    double sum = 0;
    for (long i = 0; i < n; i++) sum += (double)s[i];
    long i50 = (long)(0.50 * (double)(n - 1) + 0.5);
    long i90 = (long)(0.90 * (double)(n - 1) + 0.5);
    long i99 = (long)(0.99 * (double)(n - 1) + 0.5);
    long i999 = (long)(0.999 * (double)(n - 1) + 0.5);
    st.p50 = (double)s[i50] / divisor / 1000.0;
    st.p90 = (double)s[i90] / divisor / 1000.0;
    st.p99 = (double)s[i99] / divisor / 1000.0;
    st.p999 = (double)s[i999] / divisor / 1000.0;
    st.max = (double)s[n - 1] / divisor / 1000.0;
    st.mean = sum / (double)n / divisor / 1000.0;
    return st;
}

static double peak_rss_mb(void) {
    struct rusage ru;
    getrusage(RUSAGE_SELF, &ru);
#ifdef __APPLE__
    return (double)ru.ru_maxrss / (1024.0 * 1024.0);
#else
    return (double)ru.ru_maxrss / 1024.0;
#endif
}

/* What macOS counts against the process (resident plus compressed). */
static double footprint_mb(void) {
#ifdef __APPLE__
    task_vm_info_data_t info;
    mach_msg_type_number_t count = TASK_VM_INFO_COUNT;
    if (task_info(mach_task_self(), TASK_VM_INFO, (task_info_t)&info, &count) == KERN_SUCCESS)
        return (double)info.phys_footprint / (1024.0 * 1024.0);
#endif
    return -1;
}

static double file_mb(const char *path) {
    struct stat st;
    if (stat(path, &st) != 0) return 0;
    return (double)st.st_size / (1024.0 * 1024.0);
}

static double db_files_mb(const char *path) {
    char buf[4096];
    double total = file_mb(path);
    snprintf(buf, sizeof buf, "%s-wal", path);
    total += file_mb(buf);
    return total;
}

static void remove_db(const char *path) {
    static const char *suffixes[] = {"", "-wal", "-journal", "-shm", "-lock", ".lock", "-flush"};
    char buf[4096];
    for (size_t i = 0; i < sizeof suffixes / sizeof suffixes[0]; i++) {
        snprintf(buf, sizeof buf, "%s%s", path, suffixes[i]);
        unlink(buf);
    }
}

/* ------------------------------------------------------------------------ */
/* Deterministic random numbers                                              */
/* ------------------------------------------------------------------------ */

static inline uint64_t splitmix64(uint64_t *state) {
    uint64_t z = (*state += 0x9E3779B97F4A7C15ull);
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
    return z ^ (z >> 31);
}

static inline double rand01(uint64_t *state) {
    return (double)(splitmix64(state) >> 11) * (1.0 / 9007199254740992.0);
}

static inline uint64_t rand_below(uint64_t *state, uint64_t n) {
    return splitmix64(state) % n;
}

static double gaussian(uint64_t *state) {
    for (;;) {
        double u = rand01(state) * 2.0 - 1.0;
        double v = rand01(state) * 2.0 - 1.0;
        double s = u * u + v * v;
        if (s > 0.0 && s < 1.0) return u * sqrt(-2.0 * log(s) / s);
    }
}

/* ------------------------------------------------------------------------ */
/* Opening                                                                   */
/* ------------------------------------------------------------------------ */

static lattice_database *open_db(int create, int enable_vector, long cache_mb) {
    lattice_database *db = NULL;
    uint32_t cache = cache_mb > 0 ? (uint32_t)cache_mb : 100;
    int normal = strcmp(opt.sync, "normal") == 0;
    int full = strcmp(opt.sync, "full") == 0;
    int feed_off = strcmp(opt.changefeed, "off") == 0;
    if (normal || full || feed_off) {
#if HAVE_OPEN_V5
        lattice_open_options_v5 o = LATTICE_OPEN_OPTIONS_V5_DEFAULT;
        o.create = create;
        o.cache_size_mb = cache;
        o.enable_vector = enable_vector;
        o.vector_dimensions = 128;
        o.enable_wal = !opt.no_wal;
        o.sync_mode = normal ? LATTICE_SYNC_MODE_NORMAL
                      : full ? LATTICE_SYNC_MODE_FULL
                             : LATTICE_SYNC_MODE_DEFAULT;
        if (feed_off) o.changefeed_retention = LATTICE_CHANGEFEED_RETENTION_OFF;
        CHECK(lattice_open_v5(opt.db, &o, &db));
        return db;
#else
        fprintf(stderr,
                "capi_bench: --sync and --changefeed need lattice_open_v5, which this release "
                "lacks\n");
        exit(3);
#endif
    }
    if (opt.no_wal) {
        lattice_open_options_v2 o = LATTICE_OPEN_OPTIONS_V2_DEFAULT;
        o.create = create;
        o.cache_size_mb = cache;
        o.enable_vector = enable_vector;
        o.vector_dimensions = 128;
        o.enable_wal = false;
        CHECK(lattice_open_v2(opt.db, &o, &db));
        return db;
    }
    lattice_open_options o = LATTICE_OPEN_OPTIONS_DEFAULT;
    o.create = create;
    o.cache_size_mb = cache;
    o.enable_vector = enable_vector;
    o.vector_dimensions = 128;
    CHECK(lattice_open(opt.db, &o, &db));
    return db;
}

static lattice_value int_value(int64_t v) {
    lattice_value val;
    memset(&val, 0, sizeof val);
    val.type = LATTICE_VALUE_INT;
    val.data.int_val = v;
    return val;
}

static lattice_value str_value(const char *s, size_t len) {
    lattice_value val;
    memset(&val, 0, sizeof val);
    val.type = LATTICE_VALUE_STRING;
    val.data.string_val.ptr = s;
    val.data.string_val.len = len;
    return val;
}

/* A Person node with an integer uid and a short name. */
static lattice_node_id create_person(lattice_txn *txn, long uid) {
    lattice_node_id id;
    char name[32];
    int len = snprintf(name, sizeof name, "person-%ld", uid);
    CHECK(lattice_node_create(txn, "Person", &id));
    lattice_value v = int_value(uid);
    CHECK(lattice_node_set_property(txn, id, "uid", &v));
    lattice_value s = str_value(name, (size_t)len);
    CHECK(lattice_node_set_property(txn, id, "name", &s));
    return id;
}

/* Loads n Person nodes in transactions of `per_txn`; returns their ids. */
static lattice_node_id *load_people(lattice_database *db, long n, long per_txn) {
    lattice_node_id *ids = malloc(sizeof(lattice_node_id) * (size_t)n);
    long i = 0;
    while (i < n) {
        lattice_txn *txn;
        CHECK(lattice_begin(db, LATTICE_TXN_READ_WRITE, &txn));
        for (long k = 0; k < per_txn && i < n; k++, i++) ids[i] = create_person(txn, i);
        CHECK(lattice_commit(txn));
    }
    return ids;
}

/* Loads `degree` KNOWS edges out of every node, to uniformly random other nodes. */
static void load_edges(lattice_database *db, const lattice_node_id *ids, long n, long degree,
                       long per_txn, uint64_t *rng) {
    long total = n * degree, done = 0;
    while (done < total) {
        lattice_txn *txn;
        CHECK(lattice_begin(db, LATTICE_TXN_READ_WRITE, &txn));
        for (long k = 0; k < per_txn && done < total; k++, done++) {
            long src = done / degree;
            long dst = (long)rand_below(rng, (uint64_t)(n - 1));
            if (dst >= src) dst++;
            lattice_edge_id e;
            CHECK(lattice_edge_create(txn, ids[src], ids[dst], "KNOWS", &e));
        }
        CHECK(lattice_commit(txn));
    }
}

static void print_common(void) {
    printf(" version=%s sync=%s wal=%s changefeed=%s cache_mb=%ld", lattice_version(), opt.sync,
           opt.no_wal ? "off" : "on", opt.changefeed, opt.cache_mb > 0 ? opt.cache_mb : 100);
}

static void print_stats(const char *prefix, stats st) {
    printf(" %s_p50=%.3f %s_p90=%.3f %s_p99=%.3f %s_p999=%.3f %s_max=%.3f %s_mean=%.3f", prefix,
           st.p50, prefix, st.p90, prefix, st.p99, prefix, st.p999, prefix, st.max, prefix,
           st.mean);
}

/* ------------------------------------------------------------------------ */
/* Writes                                                                    */
/* ------------------------------------------------------------------------ */

/* One node (label + two properties) per transaction; the clock covers
 * lattice_begin to the return of lattice_commit. */
static void wl_commit_node(void) {
    remove_db(opt.db);
    lattice_database *db = open_db(1, 0, opt.cache_mb);
    long warm = opt.warmup < 0 ? 200 : opt.warmup;
    uint64_t *s = malloc(sizeof(uint64_t) * (size_t)opt.ops);
    uint64_t t_all = 0;
    for (long i = 0; i < warm + opt.ops; i++) {
        uint64_t t0 = now_ns();
        lattice_txn *txn;
        CHECK(lattice_begin(db, LATTICE_TXN_READ_WRITE, &txn));
        create_person(txn, i);
        CHECK(lattice_commit(txn));
        uint64_t dt = now_ns() - t0;
        if (i >= warm) {
            s[i - warm] = dt;
            t_all += dt;
        }
    }
    CHECK(lattice_close(db));
    stats st = summarize(s, opt.ops, 1.0);
    printf("RESULT workload=commit_node ops=%ld", opt.ops);
    print_common();
    print_stats("commit_us", st);
    printf(" per_sec=%.0f\n", (double)opt.ops / ((double)t_all / 1e9));
}

/* One edge between two existing nodes per transaction. */
static void wl_commit_edge(void) {
    remove_db(opt.db);
    lattice_database *db = open_db(1, 0, opt.cache_mb);
    lattice_node_id *ids = load_people(db, opt.n, 10000);
    uint64_t rng = opt.seed;
    long warm = opt.warmup < 0 ? 200 : opt.warmup;
    uint64_t *s = malloc(sizeof(uint64_t) * (size_t)opt.ops);
    uint64_t t_all = 0;
    for (long i = 0; i < warm + opt.ops; i++) {
        long a = (long)rand_below(&rng, (uint64_t)opt.n);
        long b = (long)rand_below(&rng, (uint64_t)opt.n);
        uint64_t t0 = now_ns();
        lattice_txn *txn;
        lattice_edge_id e;
        CHECK(lattice_begin(db, LATTICE_TXN_READ_WRITE, &txn));
        CHECK(lattice_edge_create(txn, ids[a], ids[b], "KNOWS", &e));
        CHECK(lattice_commit(txn));
        uint64_t dt = now_ns() - t0;
        if (i >= warm) {
            s[i - warm] = dt;
            t_all += dt;
        }
    }
    CHECK(lattice_close(db));
    stats st = summarize(s, opt.ops, 1.0);
    printf("RESULT workload=commit_edge n=%ld ops=%ld", opt.n, opt.ops);
    print_common();
    print_stats("commit_us", st);
    printf(" per_sec=%.0f\n", (double)opt.ops / ((double)t_all / 1e9));
}

/* opt.ops nodes in transactions of opt.batch. The rate is nodes per second of
 * wall time, commits included; each transaction's time is a sample. */
static void wl_batch_nodes(void) {
    remove_db(opt.db);
    lattice_database *db = open_db(1, 0, opt.cache_mb);
    long txns = (opt.ops + opt.batch - 1) / opt.batch;
    uint64_t *s = malloc(sizeof(uint64_t) * (size_t)txns);
    long i = 0, t = 0;
    uint64_t start = now_ns();
    while (i < opt.ops) {
        uint64_t t0 = now_ns();
        lattice_txn *txn;
        CHECK(lattice_begin(db, LATTICE_TXN_READ_WRITE, &txn));
        for (long k = 0; k < opt.batch && i < opt.ops; k++, i++) create_person(txn, i);
        CHECK(lattice_commit(txn));
        s[t++] = now_ns() - t0;
    }
    uint64_t total = now_ns() - start;
    uint64_t c0 = now_ns();
    CHECK(lattice_close(db));
    uint64_t close_ns = now_ns() - c0;
    stats st = summarize(s, t, 1.0);
    printf("RESULT workload=batch_nodes ops=%ld batch=%ld", opt.ops, opt.batch);
    print_common();
    printf(" per_sec=%.0f total_s=%.3f close_s=%.3f", (double)opt.ops / ((double)total / 1e9),
           (double)total / 1e9, (double)close_ns / 1e9);
    print_stats("txn_us", st);
    printf(" per_op_us=%.3f file_mb=%.1f peak_rss_mb=%.1f\n",
           (double)total / 1e3 / (double)opt.ops, db_files_mb(opt.db), peak_rss_mb());
}

/* opt.ops edges between opt.n existing nodes, in transactions of opt.batch. */
static void wl_batch_edges(void) {
    remove_db(opt.db);
    lattice_database *db = open_db(1, 0, opt.cache_mb);
    lattice_node_id *ids = load_people(db, opt.n, 10000);
    uint64_t rng = opt.seed;
    long txns = (opt.ops + opt.batch - 1) / opt.batch;
    uint64_t *s = malloc(sizeof(uint64_t) * (size_t)txns);
    long i = 0, t = 0;
    uint64_t start = now_ns();
    while (i < opt.ops) {
        uint64_t t0 = now_ns();
        lattice_txn *txn;
        CHECK(lattice_begin(db, LATTICE_TXN_READ_WRITE, &txn));
        for (long k = 0; k < opt.batch && i < opt.ops; k++, i++) {
            long a = (long)rand_below(&rng, (uint64_t)opt.n);
            long b = (long)rand_below(&rng, (uint64_t)opt.n);
            lattice_edge_id e;
            CHECK(lattice_edge_create(txn, ids[a], ids[b], "KNOWS", &e));
        }
        CHECK(lattice_commit(txn));
        s[t++] = now_ns() - t0;
    }
    uint64_t total = now_ns() - start;
    CHECK(lattice_close(db));
    stats st = summarize(s, t, 1.0);
    printf("RESULT workload=batch_edges n=%ld ops=%ld batch=%ld", opt.n, opt.ops, opt.batch);
    print_common();
    printf(" per_sec=%.0f total_s=%.3f", (double)opt.ops / ((double)total / 1e9),
           (double)total / 1e9);
    print_stats("txn_us", st);
    printf(" per_op_us=%.3f\n", (double)total / 1e3 / (double)opt.ops);
}

/* A bulk load: opt.n Person nodes, then opt.degree * opt.n KNOWS edges
 * between random pairs, in transactions of opt.batch, then close. The time
 * includes the close, which writes what the load left in the cache. */
static void wl_bulk(void) {
    remove_db(opt.db);
    lattice_database *db = open_db(1, 0, opt.cache_mb);
    uint64_t t0 = now_ns();
    lattice_node_id *ids = load_people(db, opt.n, opt.batch);
    uint64_t t1 = now_ns();
    uint64_t rng = opt.seed;
    long total = opt.n * opt.degree, done = 0;
    while (done < total) {
        lattice_txn *txn;
        CHECK(lattice_begin(db, LATTICE_TXN_READ_WRITE, &txn));
        for (long k = 0; k < opt.batch && done < total; k++, done++) {
            long a = (long)rand_below(&rng, (uint64_t)opt.n);
            long b = (long)rand_below(&rng, (uint64_t)opt.n);
            lattice_edge_id e;
            CHECK(lattice_edge_create(txn, ids[a], ids[b], "KNOWS", &e));
        }
        CHECK(lattice_commit(txn));
    }
    uint64_t t2 = now_ns();
    CHECK(lattice_close(db));
    uint64_t t3 = now_ns();
    printf("RESULT workload=bulk n=%ld edges=%ld batch=%ld", opt.n, total, opt.batch);
    print_common();
    printf(" total_s=%.2f nodes_s=%.2f edges_s=%.2f close_s=%.2f file_mb=%.1f peak_rss_mb=%.1f\n",
           (double)(t3 - t0) / 1e9, (double)(t1 - t0) / 1e9, (double)(t2 - t1) / 1e9,
           (double)(t3 - t2) / 1e9, db_files_mb(opt.db), peak_rss_mb());
    free(ids);
}

/* ------------------------------------------------------------------------ */
/* Reads                                                                     */
/* ------------------------------------------------------------------------ */

/* Runs a prepared, single-parameter Cypher query the way a binding does
 * (bind, read transaction, execute, read every row, free, commit), and
 * returns the sum of the first column's integers, or the row count when the
 * column is not an integer. */
static int64_t run_prepared(lattice_database *db, lattice_query *q, const char *param,
                            int64_t arg) {
    lattice_value v = int_value(arg);
    CHECK(lattice_query_bind(q, param, &v));
    lattice_txn *txn;
    CHECK(lattice_begin(db, LATTICE_TXN_READ_ONLY, &txn));
    lattice_result *r;
    lattice_error e = lattice_query_execute(q, txn, &r);
    if (e != LATTICE_OK) {
        const char *m = lattice_query_last_error_message(q);
        fprintf(stderr, "capi_bench: execute: %s\n", m ? m : "(no message)");
        die("lattice_query_execute", e);
    }
    int64_t acc = 0;
    while (lattice_result_next(r)) {
        lattice_value out;
        if (lattice_result_get(r, 0, &out) == LATTICE_OK && out.type == LATTICE_VALUE_INT)
            acc += out.data.int_val;
        else
            acc += 1;
    }
    lattice_result_free(r);
    CHECK(lattice_commit(txn));
    return acc;
}

/* The same, preparing and freeing the query on every call: what a binding's
 * db.query(cypher, params) does. */
static int64_t run_unprepared(lattice_database *db, const char *cypher, const char *param,
                              int64_t arg) {
    lattice_query *q;
    CHECK(lattice_query_prepare(db, cypher, &q));
    int64_t acc = run_prepared(db, q, param, arg);
    lattice_query_free(q);
    return acc;
}

static lattice_query *prepare(lattice_database *db, const char *cypher) {
    lattice_query *q;
    lattice_error e = lattice_query_prepare(db, cypher, &q);
    if (e != LATTICE_OK) {
        fprintf(stderr, "capi_bench: prepare failed for: %s\n", cypher);
        die("lattice_query_prepare", e);
    }
    return q;
}

/* Builds the Person graph used by lookup and traverse: opt.n nodes with a
 * property index on Person.uid, opt.degree random KNOWS edges out of each,
 * loaded in transactions of 10,000, then closed and reopened. */
static lattice_database *build_graph(lattice_node_id **ids_out, int with_edges) {
    remove_db(opt.db);
    lattice_database *db = open_db(1, 0, opt.cache_mb);
    CHECK(lattice_node_property_index_create(db, "Person", "uid"));
    uint64_t t0 = now_ns();
    lattice_node_id *ids = load_people(db, opt.n, 10000);
    uint64_t rng = opt.seed;
    if (with_edges) load_edges(db, ids, opt.n, opt.degree, 10000, &rng);
    double load_s = (double)(now_ns() - t0) / 1e9;
    CHECK(lattice_close(db));
    fprintf(stderr, "capi_bench: built graph n=%ld degree=%ld in %.2fs, %.1f MB\n", opt.n,
            with_edges ? opt.degree : 0, load_s, db_files_mb(opt.db));
    *ids_out = ids;
    return open_db(0, 0, opt.cache_mb);
}

typedef int64_t (*op_fn)(void *ctx, long i);

/* Times opt.ops calls of fn after `warm` unmeasured ones, `group` calls per
 * sample (for operations too short to time singly); returns per-call stats. */
static stats time_ops(op_fn fn, void *ctx, long warm, long group, int64_t *acc_out) {
    long samples = opt.ops / group;
    uint64_t *s = malloc(sizeof(uint64_t) * (size_t)samples);
    int64_t acc = 0;
    for (long i = 0; i < warm; i++) acc += fn(ctx, i);
    acc = 0;
    long k = 0;
    for (long j = 0; j < samples; j++) {
        uint64_t t0 = now_ns();
        for (long g = 0; g < group; g++, k++) acc += fn(ctx, warm + k);
        s[j] = now_ns() - t0;
    }
    if (acc_out) *acc_out = acc;
    stats st = summarize(s, samples, (double)group);
    free(s);
    return st;
}

typedef struct {
    lattice_database *db;
    lattice_txn *txn;
    lattice_node_id *ids;
    lattice_query *q;
    const char *cypher;
    const char *param;
    long *order;   /* the node index each call uses */
    long n_order;
} rctx;

static int64_t op_get_property(void *p, long i) {
    rctx *c = p;
    lattice_value v;
    CHECK(lattice_node_get_property(c->txn, c->ids[c->order[i % c->n_order]], "uid", &v));
    int64_t r = v.data.int_val;
    lattice_value_free(&v);
    return r;
}

static int64_t op_get_property_txn(void *p, long i) {
    rctx *c = p;
    lattice_txn *txn;
    CHECK(lattice_begin(c->db, LATTICE_TXN_READ_ONLY, &txn));
    lattice_value v;
    CHECK(lattice_node_get_property(txn, c->ids[c->order[i % c->n_order]], "uid", &v));
    int64_t r = v.data.int_val;
    lattice_value_free(&v);
    CHECK(lattice_commit(txn));
    return r;
}

static int64_t op_prepared_uid(void *p, long i) {
    rctx *c = p;
    return run_prepared(c->db, c->q, c->param, (int64_t)c->order[i % c->n_order]);
}

static int64_t op_prepared_id(void *p, long i) {
    rctx *c = p;
    return run_prepared(c->db, c->q, c->param, (int64_t)c->ids[c->order[i % c->n_order]]);
}

static int64_t op_unprepared_uid(void *p, long i) {
    rctx *c = p;
    return run_unprepared(c->db, c->cypher, c->param, (int64_t)c->order[i % c->n_order]);
}

static int64_t op_outgoing(void *p, long i) {
    rctx *c = p;
    lattice_txn *txn;
    CHECK(lattice_begin(c->db, LATTICE_TXN_READ_ONLY, &txn));
    lattice_edge_result *r;
    CHECK(lattice_edge_get_outgoing(txn, c->ids[c->order[i % c->n_order]], &r));
    uint32_t count = lattice_edge_result_count(r);
    int64_t acc = 0;
    for (uint32_t k = 0; k < count; k++) {
        lattice_node_id src, dst;
        const char *type;
        uint32_t type_len;
        CHECK(lattice_edge_result_get(r, k, &src, &dst, &type, &type_len));
        acc += 1;
    }
    lattice_edge_result_free(r);
    CHECK(lattice_commit(txn));
    return acc;
}

static long *random_order(long n, long count, uint64_t seed) {
    long *o = malloc(sizeof(long) * (size_t)count);
    uint64_t rng = seed;
    for (long i = 0; i < count; i++) o[i] = (long)rand_below(&rng, (uint64_t)n);
    return o;
}

static int want(const char *name) {
    if (strcmp(opt.query, "all") == 0) return 1;
    /* comma-separated list */
    size_t len = strlen(name);
    const char *p = opt.query;
    while (*p) {
        const char *end = strchr(p, ',');
        size_t l = end ? (size_t)(end - p) : strlen(p);
        if (l == len && strncmp(p, name, len) == 0) return 1;
        if (!end) break;
        p = end + 1;
    }
    return 0;
}

/* Point lookups over opt.n Person nodes, all of them in the buffer pool. */
static void wl_lookup(void) {
    lattice_node_id *ids;
    lattice_database *db = build_graph(&ids, 0);
    long count = opt.ops + opt.n;
    rctx c = {.db = db, .ids = ids, .order = random_order(opt.n, count, opt.seed + 1),
              .n_order = count};
    long warm = opt.warmup < 0 ? opt.n : opt.warmup;

    printf("RESULT workload=lookup n=%ld ops=%ld", opt.n, opt.ops);
    print_common();
    if (want("get_property")) {
        CHECK(lattice_begin(db, LATTICE_TXN_READ_ONLY, &c.txn));
        stats st = time_ops(op_get_property, &c, warm, 100, NULL);
        CHECK(lattice_commit(c.txn));
        print_stats("get_property_us", st);
    }
    if (want("get_property_txn")) {
        stats st = time_ops(op_get_property_txn, &c, warm, 10, NULL);
        print_stats("get_property_txn_us", st);
    }
    if (want("cypher_uid")) {
        c.q = prepare(db, "MATCH (n:Person {uid: $uid}) RETURN n.name");
        c.param = "uid";
        int64_t rows;
        stats st = time_ops(op_prepared_uid, &c, warm / 10, 1, &rows);
        lattice_query_free(c.q);
        print_stats("cypher_uid_us", st);
        printf(" cypher_uid_rows=%lld", (long long)rows);
    }
    if (want("cypher_uid_unprepared")) {
        c.cypher = "MATCH (n:Person {uid: $uid}) RETURN n.name";
        c.param = "uid";
        stats st = time_ops(op_unprepared_uid, &c, warm / 10, 1, NULL);
        print_stats("cypher_uid_unprepared_us", st);
    }
    if (want("cypher_id")) {
        c.q = prepare(db, "MATCH (n) WHERE id(n) = $id RETURN n.name");
        c.param = "id";
        /* --id-ops: releases before 0.17 scan every node for this query. */
        long saved = opt.ops;
        if (opt.id_ops > 0) opt.ops = opt.id_ops;
        int64_t rows;
        stats st = time_ops(op_prepared_id, &c, 10, 1, &rows);
        lattice_query_free(c.q);
        print_stats("cypher_id_us", st);
        printf(" cypher_id_rows=%lld", (long long)rows);
        opt.ops = saved;
    }
    printf(" peak_rss_mb=%.1f\n", peak_rss_mb());
    CHECK(lattice_close(db));
}

/* Traversals over the Person graph, each anchored at a random node found
 * through the uid property index, through Cypher. */
static void wl_traverse(void) {
    lattice_node_id *ids;
    lattice_database *db = build_graph(&ids, 1);
    long count = opt.ops + 1000;
    rctx c = {.db = db, .ids = ids, .order = random_order(opt.n, count, opt.seed + 2),
              .n_order = count};
    long warm = opt.warmup < 0 ? 200 : opt.warmup;
    static const struct {
        const char *name;
        const char *cypher;
    } queries[] = {
        {"hop1", "MATCH (a:Person {uid: $uid})-[:KNOWS]->(b) RETURN count(b)"},
        {"hop2", "MATCH (a:Person {uid: $uid})-[:KNOWS]->()-[:KNOWS]->(c) RETURN count(c)"},
        {"hop3",
         "MATCH (a:Person {uid: $uid})-[:KNOWS]->()-[:KNOWS]->()-[:KNOWS]->(d) RETURN count(d)"},
        {"var1_5", "MATCH (a:Person {uid: $uid})-[:KNOWS*1..5]->(b) RETURN count(b)"},
        {"hop2_distinct",
         "MATCH (a:Person {uid: $uid})-[:KNOWS]->()-[:KNOWS]->(c) RETURN count(DISTINCT c)"},
    };

    printf("RESULT workload=traverse n=%ld degree=%ld ops=%ld", opt.n, opt.degree, opt.ops);
    print_common();
    for (size_t k = 0; k < sizeof queries / sizeof queries[0]; k++) {
        if (!want(queries[k].name)) continue;
        c.q = prepare(db, queries[k].cypher);
        c.param = "uid";
        int64_t rows;
        stats st = time_ops(op_prepared_uid, &c, warm, 1, &rows);
        lattice_query_free(c.q);
        char key[64];
        snprintf(key, sizeof key, "%s_us", queries[k].name);
        print_stats(key, st);
        printf(" %s_mean_count=%.1f", queries[k].name, (double)rows / (double)opt.ops);
    }
    if (want("outgoing_capi")) {
        int64_t rows;
        stats st = time_ops(op_outgoing, &c, warm, 1, &rows);
        print_stats("outgoing_capi_us", st);
        printf(" outgoing_capi_mean_count=%.1f", (double)rows / (double)opt.ops);
    }
    printf(" peak_rss_mb=%.1f\n", peak_rss_mb());
    CHECK(lattice_close(db));
}

/* ------------------------------------------------------------------------ */
/* Full-text search                                                          */
/* ------------------------------------------------------------------------ */

#define FTS_VOCAB 5000
#define FTS_WORDS_PER_DOC 50

static const char *CONS = "bcfghjklmnprtvz"; /* 15 */
static const char *VOWS = "aeiou";           /* 5 */

/* A pronounceable five-letter word for each vocabulary rank. */
static int vocab_word(long r, char *out) {
    out[0] = CONS[r % 15];
    r /= 15;
    out[1] = VOWS[r % 5];
    r /= 5;
    out[2] = CONS[r % 15];
    r /= 15;
    out[3] = VOWS[r % 5];
    r /= 5;
    out[4] = CONS[(r + 3) % 15];
    out[5] = 0;
    return 5;
}

static double zipf_cdf[FTS_VOCAB];

static void zipf_init(void) {
    double sum = 0;
    for (int i = 0; i < FTS_VOCAB; i++) sum += 1.0 / (double)(i + 1);
    double acc = 0;
    for (int i = 0; i < FTS_VOCAB; i++) {
        acc += 1.0 / (double)(i + 1) / sum;
        zipf_cdf[i] = acc;
    }
}

static long zipf_sample(uint64_t *rng) {
    double u = rand01(rng);
    long lo = 0, hi = FTS_VOCAB - 1;
    while (lo < hi) {
        long mid = (lo + hi) / 2;
        if (zipf_cdf[mid] < u)
            lo = mid + 1;
        else
            hi = mid;
    }
    return lo;
}

typedef struct {
    lattice_database *db;
    char (*queries)[64];
    long nq;
} fctx;

static int64_t op_fts(void *p, long i) {
    fctx *c = p;
    const char *q = c->queries[i % c->nq];
    lattice_fts_result *r;
    CHECK(lattice_fts_search(c->db, "Doc", "text", q, strlen(q), 10, &r));
    int64_t n = lattice_fts_result_count(r);
    lattice_fts_result_free(r);
    return n;
}

/* opt.n documents of 50 Zipf-distributed words from a 5,000-word vocabulary,
 * indexed through a declared full-text index on Doc.text; top-10 searches. */
static void wl_fts(void) {
    zipf_init();
    remove_db(opt.db);
    lattice_database *db = open_db(1, 0, opt.cache_mb);
    CHECK(lattice_node_fts_index_create(db, "Doc", "text"));
    uint64_t rng = opt.seed;
    char doc[FTS_WORDS_PER_DOC * 6 + 1];
    long i = 0;
    uint64_t t0 = now_ns();
    while (i < opt.n) {
        lattice_txn *txn;
        CHECK(lattice_begin(db, LATTICE_TXN_READ_WRITE, &txn));
        for (long k = 0; k < opt.batch && i < opt.n; k++, i++) {
            size_t len = 0;
            for (int w = 0; w < FTS_WORDS_PER_DOC; w++) {
                if (w) doc[len++] = ' ';
                len += (size_t)vocab_word(zipf_sample(&rng), doc + len);
            }
            lattice_node_id id;
            CHECK(lattice_node_create(txn, "Doc", &id));
            lattice_value v = str_value(doc, len);
            CHECK(lattice_node_set_property(txn, id, "text", &v));
        }
        CHECK(lattice_commit(txn));
    }
    double load_s = (double)(now_ns() - t0) / 1e9;
    CHECK(lattice_close(db));
    db = open_db(0, 0, opt.cache_mb);

    /* Query terms: ranks 20..2000, i.e. neither stop-word-common nor unseen. */
    long nq = 1000;
    fctx c = {.db = db, .queries = malloc(sizeof(char[64]) * (size_t)nq), .nq = nq};
    uint64_t qr = opt.seed + 7;
    long warm = opt.warmup < 0 ? 200 : opt.warmup;
    printf("RESULT workload=fts n=%ld ops=%ld", opt.n, opt.ops);
    print_common();
    printf(" index_docs_per_sec=%.0f file_mb=%.1f", (double)opt.n / load_s, db_files_mb(opt.db));
    for (int terms = 1; terms <= 2; terms++) {
        for (long k = 0; k < nq; k++) {
            char w1[8], w2[8];
            vocab_word(20 + (long)rand_below(&qr, 1980), w1);
            if (terms == 1) {
                snprintf(c.queries[k], 64, "%s", w1);
            } else {
                vocab_word(20 + (long)rand_below(&qr, 380), w2);
                snprintf(c.queries[k], 64, "%s %s", w1, w2);
            }
        }
        int64_t hits;
        stats st = time_ops(op_fts, &c, warm, 1, &hits);
        char key[32];
        snprintf(key, sizeof key, "term%d_us", terms);
        print_stats(key, st);
        printf(" term%d_mean_hits=%.2f", terms, (double)hits / (double)opt.ops);
    }
    printf(" peak_rss_mb=%.1f\n", peak_rss_mb());
    CHECK(lattice_close(db));
}

/* ------------------------------------------------------------------------ */
/* Vector search                                                             */
/* ------------------------------------------------------------------------ */

#define DIM 128

/* Clustered unit vectors, as tests/benchmark/vector_benchmark.zig makes them:
 * max(n/1000, 10) random unit centres, each vector a centre plus N(0, 0.1)
 * per dimension, normalised. Vector i is a pure function of (seed, i), so the
 * brute-force pass regenerates vectors instead of holding them. */
typedef struct {
    long n, clusters;
    float *centres;
} vgen;

static void vgen_init(vgen *g, long n) {
    g->n = n;
    g->clusters = n / 1000 > 10 ? n / 1000 : 10;
    g->centres = malloc(sizeof(float) * DIM * (size_t)g->clusters);
    uint64_t rng = opt.seed ^ 0xC3A5C85C97CB3127ull;
    for (long c = 0; c < g->clusters; c++) {
        float *v = g->centres + c * DIM;
        double norm = 0;
        for (int d = 0; d < DIM; d++) {
            v[d] = (float)(rand01(&rng) * 2.0 - 1.0);
            norm += (double)v[d] * v[d];
        }
        norm = sqrt(norm);
        for (int d = 0; d < DIM; d++) v[d] = (float)(v[d] / norm);
    }
}

static void normalize(float *v) {
    double norm = 0;
    for (int d = 0; d < DIM; d++) norm += (double)v[d] * v[d];
    norm = sqrt(norm);
    if (norm > 0)
        for (int d = 0; d < DIM; d++) v[d] = (float)(v[d] / norm);
}

static void vgen_vector(const vgen *g, long i, float *out) {
    uint64_t rng = opt.seed * 0x100000001B3ull + (uint64_t)i * 0x9E3779B97F4A7C15ull + 1;
    const float *c = g->centres + (i % g->clusters) * DIM;
    for (int d = 0; d < DIM; d++) out[d] = c[d] + (float)(gaussian(&rng) * 0.1);
    normalize(out);
}

/* A query: a random stored vector plus N(0, 0.05) per dimension, normalised. */
static void vgen_query(const vgen *g, long q, float *out) {
    uint64_t rng = (opt.seed + 99) * 0x2545F4914F6CDD1Dull + (uint64_t)q * 0x9E3779B97F4A7C15ull;
    long base = (long)rand_below(&rng, (uint64_t)g->n);
    vgen_vector(g, base, out);
    for (int d = 0; d < DIM; d++) out[d] += (float)(gaussian(&rng) * 0.05);
    normalize(out);
}

typedef struct {
    lattice_database *db;
    float *queries; /* nq x DIM */
    long nq;
    uint16_t ef;
    lattice_node_id (*found)[10];
} vctx;

static int64_t op_vector(void *p, long i) {
    vctx *c = p;
    long q = i % c->nq;
    lattice_vector_result *r;
    CHECK(lattice_vector_search(c->db, c->queries + q * DIM, DIM, 10, c->ef, &r));
    uint32_t n = lattice_vector_result_count(r);
    if (c->found) {
        for (uint32_t k = 0; k < 10; k++) {
            lattice_node_id id = 0;
            float dist;
            if (k < n) lattice_vector_result_get(r, k, &id, &dist);
            c->found[q][k] = id;
        }
    }
    lattice_vector_result_free(r);
    return n;
}

/* Exact top-10 by cosine (the vectors are unit length, so by dot product)
 * for the first `nq` queries, in one pass over regenerated vectors. */
static void brute_force(const vgen *g, const float *queries, long nq, long (*truth)[10]) {
    float (*best)[10] = malloc(sizeof(float[10]) * (size_t)nq);
    for (long q = 0; q < nq; q++)
        for (int k = 0; k < 10; k++) {
            best[q][k] = -2.0f;
            truth[q][k] = -1;
        }
    float v[DIM];
    for (long i = 0; i < g->n; i++) {
        vgen_vector(g, i, v);
        for (long q = 0; q < nq; q++) {
            const float *qq = queries + q * DIM;
            float dot = 0;
            for (int d = 0; d < DIM; d++) dot += qq[d] * v[d];
            if (dot <= best[q][9]) continue;
            int k = 9;
            while (k > 0 && best[q][k - 1] < dot) {
                best[q][k] = best[q][k - 1];
                truth[q][k] = truth[q][k - 1];
                k--;
            }
            best[q][k] = dot;
            truth[q][k] = i;
        }
    }
    free(best);
}

static void vector_phase(const char *label, lattice_database *db, float *queries, long nq,
                         long (*truth)[10], lattice_node_id *ids, uint16_t ef, long warm) {
    vctx c = {.db = db, .queries = queries, .nq = nq, .ef = ef, .found = NULL};
    stats st = time_ops(op_vector, &c, warm, 1, NULL);
    /* Recall over the first recall_queries queries, outside the timing. */
    long rq = opt.recall_queries < nq ? opt.recall_queries : nq;
    c.found = malloc(sizeof(lattice_node_id[10]) * (size_t)nq);
    for (long q = 0; q < rq; q++) op_vector(&c, q);
    double hits = 0;
    for (long q = 0; q < rq; q++)
        for (int a = 0; a < 10; a++) {
            if (truth[q][a] < 0) continue;
            lattice_node_id want_id = ids[truth[q][a]];
            for (int b = 0; b < 10; b++)
                if (c.found[q][b] == want_id) {
                    hits += 1;
                    break;
                }
        }
    free(c.found);
    char key[64];
    snprintf(key, sizeof key, "%s_ef%u_us", label, (unsigned)ef);
    print_stats(key, st);
    printf(" %s_ef%u_recall=%.4f", label, (unsigned)ef, hits / (10.0 * (double)rq));
}

static void ids_path(char *buf, size_t len) { snprintf(buf, len, "%s.ids", opt.db); }

/* opt.n 128-dimensional vectors inserted with lattice_batch_insert in
 * transactions of opt.batch, then closed. Keeps the database and writes the
 * node ids beside it (<db>.ids) for vector_search, which runs in a fresh
 * process so its memory figure is not mixed with the build's. */
static void wl_vector_build(void) {
    vgen g;
    vgen_init(&g, opt.n);
    remove_db(opt.db);
    lattice_database *db = open_db(1, 1, opt.cache_mb);
    lattice_node_id *ids = malloc(sizeof(lattice_node_id) * (size_t)opt.n);
    float *buf = malloc(sizeof(float) * DIM * (size_t)opt.batch);
    lattice_node_with_vector *specs = malloc(sizeof(lattice_node_with_vector) * (size_t)opt.batch);
    long i = 0;
    uint64_t t0 = now_ns(), last = t0;
    while (i < opt.n) {
        long m = opt.n - i < opt.batch ? opt.n - i : opt.batch;
        for (long k = 0; k < m; k++) {
            vgen_vector(&g, i + k, buf + k * DIM);
            specs[k].label = "Doc";
            specs[k].vector = buf + k * DIM;
            specs[k].dimensions = DIM;
        }
        lattice_txn *txn;
        uint32_t made = 0;
        CHECK(lattice_begin(db, LATTICE_TXN_READ_WRITE, &txn));
        CHECK(lattice_batch_insert(txn, specs, (uint32_t)m, ids + i, &made));
        if ((long)made != m) die("lattice_batch_insert count", (int)made);
        CHECK(lattice_commit(txn));
        i += m;
        uint64_t now = now_ns();
        if (now - last > 60ull * 1000000000ull) {
            fprintf(stderr, "capi_bench: %ld / %ld vectors, %.0f/s\n", i, opt.n,
                    (double)i / ((double)(now - t0) / 1e9));
            last = now;
        }
    }
    double insert_s = (double)(now_ns() - t0) / 1e9;
    uint64_t c0 = now_ns();
    CHECK(lattice_close(db));
    double close_s = (double)(now_ns() - c0) / 1e9;

    char path[4096];
    ids_path(path, sizeof path);
    FILE *f = fopen(path, "wb");
    if (!f || fwrite(ids, sizeof(lattice_node_id), (size_t)opt.n, f) != (size_t)opt.n) {
        fprintf(stderr, "capi_bench: cannot write %s\n", path);
        exit(2);
    }
    fclose(f);
    printf("RESULT workload=vector_build n=%ld batch=%ld", opt.n, opt.batch);
    print_common();
    printf(" insert_per_sec=%.0f insert_s=%.1f close_s=%.1f file_mb=%.1f peak_rss_mb=%.1f\n",
           (double)opt.n / insert_s, insert_s, close_s, db_files_mb(opt.db), peak_rss_mb());
}

/* Top-10 searches over the database vector_build left: opt.ops queries timed
 * after the warm-up, for each ef_search in --ef (0 = the default, 64), and
 * recall@10 against exact search over the first --recall-queries queries.
 * The footprint is what opening and searching added to the process. */
static void wl_vector_search(void) {
    vgen g;
    vgen_init(&g, opt.n);
    lattice_node_id *ids = malloc(sizeof(lattice_node_id) * (size_t)opt.n);
    char path[4096];
    ids_path(path, sizeof path);
    FILE *f = fopen(path, "rb");
    if (!f || fread(ids, sizeof(lattice_node_id), (size_t)opt.n, f) != (size_t)opt.n) {
        fprintf(stderr, "capi_bench: cannot read %s; run vector_build first\n", path);
        exit(2);
    }
    fclose(f);

    long nq = opt.ops;
    float *queries = malloc(sizeof(float) * DIM * (size_t)nq);
    for (long q = 0; q < nq; q++) vgen_query(&g, q, queries + q * DIM);
    long rq = opt.recall_queries < nq ? opt.recall_queries : nq;
    long (*truth)[10] = malloc(sizeof(long[10]) * (size_t)rq);
    uint64_t b0 = now_ns();
    brute_force(&g, queries, rq, truth);
    fprintf(stderr, "capi_bench: exact search for %ld queries took %.1fs\n", rq,
            (double)(now_ns() - b0) / 1e9);

    long warm = opt.warmup < 0 ? 100 : opt.warmup;
    double fp0 = footprint_mb();
    uint64_t o0 = now_ns();
    lattice_database *db = open_db(0, 1, opt.cache_mb);
    double open_ms = (double)(now_ns() - o0) / 1e6;
    printf("RESULT workload=vector_search n=%ld ops=%ld", opt.n, opt.ops);
    print_common();
    printf(" open_ms=%.1f", open_ms);
    char efs[256];
    snprintf(efs, sizeof efs, "%s", opt.ef_list);
    for (char *tok = strtok(efs, ","); tok; tok = strtok(NULL, ","))
        vector_phase("search", db, queries, nq, truth, ids, (uint16_t)atoi(tok), warm);
    printf(" footprint_mb=%.1f", footprint_mb() - fp0);
    CHECK(lattice_close(db));
    printf(" file_mb=%.1f peak_rss_mb=%.1f\n", db_files_mb(opt.db), peak_rss_mb());
}

/* ------------------------------------------------------------------------ */
/* SQLite: the same data and questions through SQLite's C API                */
/* ------------------------------------------------------------------------ */

static void sq_check(sqlite3 *db, int rc, const char *what) {
    if (rc != SQLITE_OK && rc != SQLITE_DONE && rc != SQLITE_ROW) {
        fprintf(stderr, "capi_bench: sqlite %s: %s\n", what, sqlite3_errmsg(db));
        exit(2);
    }
}

static void sq_exec(sqlite3 *db, const char *sql) {
    char *err = NULL;
    if (sqlite3_exec(db, sql, NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "capi_bench: sqlite: %s: %s\n", sql, err);
        exit(2);
    }
}

/* WAL journal, synchronous FULL (a commit fsyncs the log, as LatticeDB's
 * default does) or NORMAL with --sync normal; fullfsync off on both sides; a
 * 100 MB page cache, as LatticeDB's default buffer pool. */
static sqlite3 *sq_open(int fresh) {
    if (fresh) remove_db(opt.db);
    sqlite3 *db;
    if (sqlite3_open(opt.db, &db) != SQLITE_OK) {
        fprintf(stderr, "capi_bench: sqlite open failed\n");
        exit(2);
    }
    sq_exec(db, "PRAGMA journal_mode=WAL");
    sq_exec(db, strcmp(opt.sync, "normal") == 0 ? "PRAGMA synchronous=NORMAL"
                                                : "PRAGMA synchronous=FULL");
    sq_exec(db, "PRAGMA fullfsync=OFF");
    sq_exec(db, "PRAGMA checkpoint_fullfsync=OFF");
    sq_exec(db, "PRAGMA cache_size=-102400");
    if (fresh) {
        sq_exec(db, "CREATE TABLE person(id INTEGER PRIMARY KEY, uid INTEGER NOT NULL, name TEXT)");
        sq_exec(db, "CREATE UNIQUE INDEX person_uid ON person(uid)");
        sq_exec(db, "CREATE TABLE knows(src INTEGER NOT NULL, dst INTEGER NOT NULL)");
        sq_exec(db, "CREATE INDEX knows_src ON knows(src)");
    }
    return db;
}

static void print_sqlite_common(void) {
    printf(" sqlite=%s sync=%s journal=wal cache_mb=100", sqlite3_libversion(),
           strcmp(opt.sync, "normal") == 0 ? "normal" : "full");
}

static void sq_insert_person(sqlite3_stmt *st, long i) {
    char name[32];
    int len = snprintf(name, sizeof name, "person-%ld", i);
    sqlite3_bind_int64(st, 1, i);
    sqlite3_bind_text(st, 2, name, len, SQLITE_TRANSIENT);
    if (sqlite3_step(st) != SQLITE_DONE) {
        fprintf(stderr, "capi_bench: sqlite insert failed\n");
        exit(2);
    }
    sqlite3_reset(st);
}

static void sqlite_commit_node(void) {
    sqlite3 *db = sq_open(1);
    sqlite3_stmt *ins, *begin, *commit;
    sqlite3_prepare_v2(db, "INSERT INTO person(uid, name) VALUES(?, ?)", -1, &ins, NULL);
    sqlite3_prepare_v2(db, "BEGIN", -1, &begin, NULL);
    sqlite3_prepare_v2(db, "COMMIT", -1, &commit, NULL);
    long warm = opt.warmup < 0 ? 200 : opt.warmup;
    uint64_t *s = malloc(sizeof(uint64_t) * (size_t)opt.ops);
    uint64_t t_all = 0;
    for (long i = 0; i < warm + opt.ops; i++) {
        uint64_t t0 = now_ns();
        sqlite3_step(begin);
        sqlite3_reset(begin);
        sq_insert_person(ins, i);
        sq_check(db, sqlite3_step(commit), "commit");
        sqlite3_reset(commit);
        uint64_t dt = now_ns() - t0;
        if (i >= warm) {
            s[i - warm] = dt;
            t_all += dt;
        }
    }
    sqlite3_finalize(ins);
    sqlite3_finalize(begin);
    sqlite3_finalize(commit);
    sqlite3_close(db);
    stats st = summarize(s, opt.ops, 1.0);
    printf("RESULT workload=sqlite_commit_node ops=%ld", opt.ops);
    print_sqlite_common();
    print_stats("commit_us", st);
    printf(" per_sec=%.0f\n", (double)opt.ops / ((double)t_all / 1e9));
}

static void sqlite_batch_nodes(void) {
    sqlite3 *db = sq_open(1);
    sqlite3_stmt *ins;
    sqlite3_prepare_v2(db, "INSERT INTO person(uid, name) VALUES(?, ?)", -1, &ins, NULL);
    long i = 0;
    uint64_t start = now_ns();
    while (i < opt.ops) {
        sq_exec(db, "BEGIN");
        for (long k = 0; k < opt.batch && i < opt.ops; k++, i++) sq_insert_person(ins, i);
        sq_exec(db, "COMMIT");
    }
    uint64_t total = now_ns() - start;
    sqlite3_finalize(ins);
    sqlite3_close(db);
    printf("RESULT workload=sqlite_batch_nodes ops=%ld batch=%ld", opt.ops, opt.batch);
    print_sqlite_common();
    printf(" per_sec=%.0f total_s=%.3f per_op_us=%.3f\n",
           (double)opt.ops / ((double)total / 1e9), (double)total / 1e9,
           (double)total / 1e3 / (double)opt.ops);
}

/* Builds the same graph as build_graph: the same uids, and edges from the
 * same random sequence (row id = uid + 1). */
static sqlite3 *sq_build_graph(int with_edges) {
    sqlite3 *db = sq_open(1);
    sqlite3_stmt *ins, *edge;
    sqlite3_prepare_v2(db, "INSERT INTO person(id, uid, name) VALUES(?3, ?1, ?2)", -1, &ins,
                       NULL);
    sqlite3_prepare_v2(db, "INSERT INTO knows(src, dst) VALUES(?, ?)", -1, &edge, NULL);
    long i = 0;
    while (i < opt.n) {
        sq_exec(db, "BEGIN");
        for (long k = 0; k < 10000 && i < opt.n; k++, i++) {
            sqlite3_bind_int64(ins, 3, i + 1);
            sq_insert_person(ins, i);
        }
        sq_exec(db, "COMMIT");
    }
    if (with_edges) {
        uint64_t rng = opt.seed;
        long total = opt.n * opt.degree, done = 0;
        while (done < total) {
            sq_exec(db, "BEGIN");
            for (long k = 0; k < 10000 && done < total; k++, done++) {
                long src = done / opt.degree;
                long dst = (long)rand_below(&rng, (uint64_t)(opt.n - 1));
                if (dst >= src) dst++;
                sqlite3_bind_int64(edge, 1, src + 1);
                sqlite3_bind_int64(edge, 2, dst + 1);
                sqlite3_step(edge);
                sqlite3_reset(edge);
            }
            sq_exec(db, "COMMIT");
        }
    }
    sqlite3_finalize(ins);
    sqlite3_finalize(edge);
    sq_exec(db, "ANALYZE");
    sqlite3_close(db);
    return sq_open(0);
}

typedef struct {
    sqlite3 *db;
    sqlite3_stmt *st;
    long *order;
    long n_order;
} sqctx;

/* Bind, step every row, reset, inside BEGIN/COMMIT as a read transaction. */
static int64_t op_sqlite(void *p, long i) {
    sqctx *c = p;
    sqlite3_exec(c->db, "BEGIN", NULL, NULL, NULL);
    sqlite3_bind_int64(c->st, 1, c->order[i % c->n_order]);
    int64_t acc = 0;
    int rc;
    while ((rc = sqlite3_step(c->st)) == SQLITE_ROW) {
        if (sqlite3_column_type(c->st, 0) == SQLITE_INTEGER)
            acc += sqlite3_column_int64(c->st, 0);
        else
            acc += 1;
    }
    sq_check(c->db, rc, "step");
    sqlite3_reset(c->st);
    sqlite3_exec(c->db, "COMMIT", NULL, NULL, NULL);
    return acc;
}

static void sqlite_traverse(int with_edges) {
    sqlite3 *db = sq_build_graph(with_edges);
    long count = opt.ops + (with_edges ? 1000 : opt.n);
    sqctx c = {.db = db, .order = random_order(opt.n, count, opt.seed + (with_edges ? 2 : 1)),
               .n_order = count};
    static const struct {
        const char *name;
        const char *sql;
        int edges;
    } queries[] = {
        {"cypher_uid", "SELECT name FROM person WHERE uid = ?", 0},
        {"hop1",
         "SELECT count(*) FROM person a JOIN knows k1 ON k1.src = a.id WHERE a.uid = ?", 1},
        {"hop2",
         "SELECT count(*) FROM person a JOIN knows k1 ON k1.src = a.id "
         "JOIN knows k2 ON k2.src = k1.dst WHERE a.uid = ?",
         1},
        {"hop3",
         "SELECT count(*) FROM person a JOIN knows k1 ON k1.src = a.id "
         "JOIN knows k2 ON k2.src = k1.dst JOIN knows k3 ON k3.src = k2.dst WHERE a.uid = ?",
         1},
        /* Paths of length 1..5, counted as Cypher counts them except that a
         * path may reuse a relationship (Cypher's may not); the count is
         * printed so the two can be compared. */
        {"var1_5",
         "WITH RECURSIVE p(node, depth) AS ("
         " SELECT k.dst, 1 FROM person a JOIN knows k ON k.src = a.id WHERE a.uid = ?"
         " UNION ALL"
         " SELECT k.dst, p.depth + 1 FROM p JOIN knows k ON k.src = p.node WHERE p.depth < 5)"
         " SELECT count(*) FROM p",
         1},
        {"hop2_distinct",
         "SELECT count(DISTINCT k2.dst) FROM person a JOIN knows k1 ON k1.src = a.id "
         "JOIN knows k2 ON k2.src = k1.dst WHERE a.uid = ?",
         1},
    };
    long warm = opt.warmup < 0 ? 200 : opt.warmup;
    printf("RESULT workload=sqlite_%s n=%ld degree=%ld ops=%ld", with_edges ? "traverse" : "lookup",
           opt.n, with_edges ? opt.degree : 0, opt.ops);
    print_sqlite_common();
    for (size_t k = 0; k < sizeof queries / sizeof queries[0]; k++) {
        if (queries[k].edges != with_edges || !want(queries[k].name)) continue;
        sq_check(db, sqlite3_prepare_v2(db, queries[k].sql, -1, &c.st, NULL), "prepare");
        int64_t rows;
        stats st = time_ops(op_sqlite, &c, with_edges ? warm : opt.n / 10, 1, &rows);
        sqlite3_finalize(c.st);
        char key[64];
        snprintf(key, sizeof key, "%s_us", queries[k].name);
        print_stats(key, st);
        if (with_edges) printf(" %s_mean_count=%.1f", queries[k].name, (double)rows / (double)opt.ops);
    }
    printf("\n");
    sqlite3_close(db);
}

/* ------------------------------------------------------------------------ */

static void usage(void) {
    fprintf(stderr,
            "usage: capi_bench <workload> --db PATH [options]\n"
            "workloads: commit_node commit_edge batch_nodes batch_edges bulk lookup traverse fts\n"
            "           vector_build vector_search (on vector_build's database)\n"
            "           sqlite_commit_node sqlite_batch_nodes sqlite_lookup\n"
            "           sqlite_traverse\n"
            "options: --n N --ops N --warmup N --batch N --degree N --seed N\n"
            "         --sync default|full|normal --no-wal --cache MB\n"
            "         --ef LIST --recall-queries N --query NAMES --keep\n"
            "         --id-ops N --changefeed default|off\n"
            "Prints one RESULT line; times in microseconds. Use capi_bench.py to\n"
            "repeat runs and summarise them.\n");
    exit(1);
}

int main(int argc, char **argv) {
    if (argc < 2) usage();
    opt.workload = argv[1];
    for (int i = 2; i < argc; i++) {
        const char *a = argv[i];
        const char *v = i + 1 < argc ? argv[i + 1] : NULL;
#define ARG(name) (strcmp(a, name) == 0 && v && (i++, 1))
        if (ARG("--db")) opt.db = v;
        else if (ARG("--n")) opt.n = atol(v);
        else if (ARG("--ops")) opt.ops = atol(v);
        else if (ARG("--warmup")) opt.warmup = atol(v);
        else if (ARG("--batch")) opt.batch = atol(v);
        else if (ARG("--degree")) opt.degree = atol(v);
        else if (ARG("--seed")) opt.seed = strtoull(v, NULL, 10);
        else if (ARG("--sync")) opt.sync = v;
        else if (ARG("--cache")) opt.cache_mb = atol(v);
        else if (ARG("--ef")) opt.ef_list = v;
        else if (ARG("--recall-queries")) opt.recall_queries = atol(v);
        else if (ARG("--query")) opt.query = v;
        else if (ARG("--id-ops")) opt.id_ops = atol(v);
        else if (ARG("--changefeed")) opt.changefeed = v;
        else if (strcmp(a, "--no-wal") == 0) opt.no_wal = 1;
        else if (strcmp(a, "--keep") == 0) opt.keep = 1;
        else usage();
#undef ARG
    }
    if (!opt.db) usage();
    if (opt.ops <= 0 || opt.batch <= 0 || opt.n <= 1) usage();

    const char *w = opt.workload;
    if (strcmp(w, "commit_node") == 0) wl_commit_node();
    else if (strcmp(w, "commit_edge") == 0) wl_commit_edge();
    else if (strcmp(w, "batch_nodes") == 0) wl_batch_nodes();
    else if (strcmp(w, "batch_edges") == 0) wl_batch_edges();
    else if (strcmp(w, "bulk") == 0) wl_bulk();
    else if (strcmp(w, "lookup") == 0) wl_lookup();
    else if (strcmp(w, "traverse") == 0) wl_traverse();
    else if (strcmp(w, "fts") == 0) wl_fts();
    else if (strcmp(w, "vector_build") == 0) {
        wl_vector_build();
        opt.keep = 1;
    } else if (strcmp(w, "vector_search") == 0) wl_vector_search();
    else if (strcmp(w, "sqlite_commit_node") == 0) sqlite_commit_node();
    else if (strcmp(w, "sqlite_batch_nodes") == 0) sqlite_batch_nodes();
    else if (strcmp(w, "sqlite_lookup") == 0) sqlite_traverse(0);
    else if (strcmp(w, "sqlite_traverse") == 0) sqlite_traverse(1);
    else usage();
    fflush(stdout);
    if (!opt.keep) {
        char path[4096];
        remove_db(opt.db);
        ids_path(path, sizeof path);
        unlink(path);
    }
    return 0;
}
