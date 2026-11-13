#!/usr/bin/env python3
"""Run the public-API benchmark (capi_bench.c) and summarise it.

Each row runs one workload of capi_bench several times, every run in a fresh
process on a fresh database, and reports the median of the runs with the
range (lowest to highest run) beside it. Rows run one after another, never
side by side.

    zig build capi-benchmark                          # the published tables
    zig build capi-benchmark -- --suite vector-1m     # the 1M-vector row (long)
    zig build capi-benchmark -- --list                # what each suite runs

To measure another checkout (for example a release tag) with the same
program, build it against that checkout's library and point --bin at it:

    python3 tests/benchmark/capi_bench.py build-against ../v0.16.0 --out /tmp/cb-016
    python3 tests/benchmark/capi_bench.py run --bin /tmp/cb-016 --json old.json
    python3 tests/benchmark/capi_bench.py compare old.json new.json

Only the standard library is used.
"""

import argparse
import json
import os
import platform
import shutil
import statistics
import subprocess
import sys
import tempfile
import time

# A row: (id, workload, arguments, runs, metrics to show, description).
# Metric names are the keys capi_bench prints; times are in microseconds.

LAT = ["{p}_p50", "{p}_p99"]


def lat(prefix):
    return [m.format(p=prefix) for m in LAT]


# Single commits: 20,000 per run, so the automatic checkpoints (one per
# 1,000 log frames) sit inside p99.9 rather than on the edge of p99.
def commit_lat():
    return ["commit_us_p50", "commit_us_p99", "commit_us_p999", "commit_us_max"]


CORE = [
    ("commit_node", "commit_node", ["--ops", "20000"], 5, commit_lat() + ["per_sec"],
     "1 node (label + 2 properties) per transaction"),
    ("commit_edge", "commit_edge", ["--n", "100000", "--ops", "20000"], 5,
     commit_lat() + ["per_sec"], "1 edge per transaction, 100K nodes present"),
    ("batch_nodes_10", "batch_nodes", ["--ops", "50000", "--batch", "10"], 5,
     ["per_sec", "txn_us_p50"], "nodes in transactions of 10"),
    ("batch_nodes_100", "batch_nodes", ["--ops", "200000", "--batch", "100"], 5,
     ["per_sec", "txn_us_p50"], "nodes in transactions of 100"),
    ("batch_nodes_1000", "batch_nodes", ["--ops", "200000", "--batch", "1000"], 5,
     ["per_sec", "txn_us_p50"], "nodes in transactions of 1,000"),
    ("batch_nodes_10000", "batch_nodes", ["--ops", "200000", "--batch", "10000"], 5,
     ["per_sec", "txn_us_p50"], "nodes in transactions of 10,000"),
    ("batch_edges_1000", "batch_edges", ["--n", "100000", "--ops", "500000", "--batch", "1000"],
     5, ["per_sec", "txn_us_p50"], "edges in transactions of 1,000, 100K nodes"),
    ("batch_edges_10000", "batch_edges",
     ["--n", "100000", "--ops", "500000", "--batch", "10000"], 5, ["per_sec", "txn_us_p50"],
     "edges in transactions of 10,000, 100K nodes"),
    ("lookup_100k", "lookup", ["--n", "100000", "--ops", "20000", "--id-ops", "1000"], 5,
     lat("get_property_us") + lat("get_property_txn_us") + lat("cypher_uid_us")
     + lat("cypher_uid_unprepared_us") + lat("cypher_id_us"),
     "point lookups, 100K nodes, warm"),
    ("traverse_10k", "traverse", ["--n", "10000", "--ops", "2000"], 5,
     lat("hop1_us") + lat("hop2_us") + lat("hop3_us") + lat("var1_5_us")
     + lat("hop2_distinct_us") + lat("outgoing_capi_us")
     + ["hop1_mean_count", "hop2_mean_count", "hop3_mean_count", "var1_5_mean_count"],
     "Cypher traversals, 10K nodes / 50K edges"),
    ("traverse_100k", "traverse", ["--n", "100000", "--ops", "2000"], 5,
     lat("hop1_us") + lat("hop2_us") + lat("hop3_us") + lat("var1_5_us")
     + lat("hop2_distinct_us") + lat("outgoing_capi_us")
     + ["hop1_mean_count", "hop2_mean_count", "hop3_mean_count", "var1_5_mean_count"],
     "Cypher traversals, 100K nodes / 500K edges"),
    ("fts_10k", "fts", ["--n", "10000", "--ops", "2000"], 5,
     lat("term1_us") + lat("term2_us") + ["term1_mean_hits", "term2_mean_hits",
                                          "index_docs_per_sec", "file_mb"],
     "BM25 top-10, 10K documents of 50 words"),
    ("fts_100k", "fts", ["--n", "100000", "--ops", "2000"], 5,
     lat("term1_us") + lat("term2_us") + ["term1_mean_hits", "term2_mean_hits",
                                          "index_docs_per_sec", "file_mb"],
     "BM25 top-10, 100K documents of 50 words"),
]

# Settings other than the defaults; never in place of a default row.
OPTIONS = [
    ("commit_node_normal", "commit_node", ["--ops", "20000", "--sync", "normal"], 5,
     commit_lat() + ["per_sec"], "1 node per transaction, sync_mode normal"),
    ("batch_nodes_1000_normal", "batch_nodes",
     ["--ops", "200000", "--batch", "1000", "--sync", "normal"], 5,
     ["per_sec", "txn_us_p50"], "nodes in transactions of 1,000, sync_mode normal"),
    ("bulk_default", "bulk", ["--n", "1000000", "--degree", "2", "--batch", "10000"], 3,
     ["total_s", "nodes_s", "edges_s", "close_s", "file_mb", "peak_rss_mb"],
     "1M nodes + 2M edges, transactions of 10,000, defaults"),
    ("bulk_recipe", "bulk",
     ["--n", "1000000", "--degree", "2", "--batch", "10000", "--no-wal", "--changefeed", "off",
      "--cache", "1024"], 3,
     ["total_s", "nodes_s", "edges_s", "close_s", "file_mb", "peak_rss_mb"],
     "the same with no log, changefeed off, 1 GB cache"),
]

EF = "0,16,32,128,256"
VSEARCH = (lat("search_ef0_us") + ["search_ef0_us_mean", "search_ef0_recall"]
           + [f"search_ef{e}_{k}" for e in (16, 32, 128, 256) for k in ("us_mean", "recall")]
           + ["footprint_mb", "file_mb", "open_ms"])


def vector_rows(n, runs, cache_extra):
    tag = {1000: "1k", 10000: "10k", 100000: "100k", 1000000: "1m"}[n]
    rows = [(f"vector_{tag}", "vector", ["--n", str(n), "--ops", "1000", "--ef", EF], runs,
             ["insert_per_sec", "insert_s"] + VSEARCH,
             f"{n:,} vectors, 128-d cosine, top-10")]
    if cache_extra:
        rows[0][2].extend(["--extra-cache", str(cache_extra)])
        rows[0][4].extend(["cache_" + m for m in lat("search_ef0_us")
                           + ["search_ef0_recall", "footprint_mb"]])
    return rows


VECTOR = vector_rows(1000, 3, 0) + vector_rows(10000, 3, 0) + vector_rows(100000, 3, 1024)
VECTOR_1M = vector_rows(1000000, 1, 2048)

SQLITE = [
    ("sqlite_commit_node", "sqlite_commit_node", ["--ops", "20000"], 5,
     commit_lat() + ["per_sec"], "1 row per transaction"),
    ("sqlite_commit_node_normal", "sqlite_commit_node", ["--ops", "20000", "--sync", "normal"], 5,
     commit_lat() + ["per_sec"], "1 row per transaction, synchronous=NORMAL"),
    ("sqlite_batch_nodes_1000", "sqlite_batch_nodes", ["--ops", "200000", "--batch", "1000"], 5,
     ["per_sec"], "rows in transactions of 1,000"),
    ("sqlite_lookup_100k", "sqlite_lookup", ["--n", "100000", "--ops", "20000"], 5,
     lat("cypher_uid_us"), "lookup by indexed uid, 100K rows"),
    ("sqlite_traverse_10k", "sqlite_traverse", ["--n", "10000", "--ops", "2000"], 5,
     lat("hop1_us") + lat("hop2_us") + lat("hop3_us") + lat("var1_5_us")
     + lat("hop2_distinct_us")
     + ["hop1_mean_count", "hop2_mean_count", "hop3_mean_count", "var1_5_mean_count"],
     "joins / recursive CTE, 10K nodes / 50K edges"),
    ("sqlite_traverse_100k", "sqlite_traverse", ["--n", "100000", "--ops", "2000"], 5,
     lat("hop1_us") + lat("hop2_us") + lat("hop3_us") + lat("var1_5_us")
     + lat("hop2_distinct_us")
     + ["hop1_mean_count", "hop2_mean_count", "hop3_mean_count", "var1_5_mean_count"],
     "joins / recursive CTE, 100K nodes / 500K edges"),
]

SUITES = {
    "core": CORE,
    "options": OPTIONS,
    "vector": VECTOR,
    "vector-1m": VECTOR_1M,
    "sqlite": SQLITE,
}
DEFAULT_SUITES = ["core", "options", "vector", "sqlite"]


def parse_result(text):
    for line in text.splitlines():
        if line.startswith("RESULT "):
            out = {}
            for tok in line.split()[1:]:
                k, _, v = tok.partition("=")
                try:
                    out[k] = float(v)
                except ValueError:
                    out[k] = v
            return out
    return None


def run_once(binary, workload, args, db):
    cmd = [binary, workload, "--db", db] + args
    proc = subprocess.run(cmd, capture_output=True, text=True)
    # Progress and notes (insert rates every minute, exact-search time).
    for line in proc.stderr.splitlines():
        if line.startswith("capi_bench:") and proc.returncode == 0:
            print("    " + line, file=sys.stderr, flush=True)
    if proc.returncode != 0:
        return None, (proc.stderr or proc.stdout).strip().splitlines()[-1:] or ["failed"]
    return parse_result(proc.stdout), None


def run_row(binary, row, scratch, runs_override):
    rid, workload, args, runs, _metrics, _desc = row
    runs = runs_override or runs
    db = os.path.join(scratch, f"{rid}.db")
    results, error = [], None
    for r in range(runs):
        t0 = time.time()
        if workload == "vector":
            res, error = run_vector(binary, args, db)
        else:
            res, error = run_once(binary, workload, args, db)
        if error:
            break
        results.append(res)
        print(f"  {rid} run {r + 1}/{runs}: {time.time() - t0:.1f}s", file=sys.stderr, flush=True)
    return {"id": rid, "workload": workload, "args": args, "runs": results, "error": error}


def run_vector(binary, args, db):
    """vector_build, then vector_search in a fresh process (and once more with
    --extra-cache MB as the cache, if given), merged into one result."""
    args = list(args)
    extra = None
    if "--extra-cache" in args:
        i = args.index("--extra-cache")
        extra = args[i + 1]
        del args[i:i + 2]
    n = args[args.index("--n") + 1]
    build, err = run_once(binary, "vector_build", ["--n", n], db)
    if err:
        return None, err
    search, err = run_once(binary, "vector_search", args + ["--keep"], db)
    if err:
        return None, err
    merged = dict(build)
    merged.update(search)
    if extra:
        cached, err = run_once(binary, "vector_search",
                               ["--n", n, "--ops", "1000", "--ef", "0", "--cache", extra, "--keep"],
                               db)
        if err:
            return None, err
        for k, v in cached.items():
            merged["cache_" + k] = v
    for suffix in ("", "-wal", ".ids"):
        try:
            os.unlink(db + suffix)
        except FileNotFoundError:
            pass
    return merged, None


def fmt(v):
    if isinstance(v, str):
        return v
    a = abs(v)
    if a >= 1000:
        return f"{v:,.0f}"
    if a >= 100:
        return f"{v:.0f}"
    if a >= 10:
        return f"{v:.1f}"
    if a >= 1:
        return f"{v:.2f}"
    return f"{v:.3f}"


def summarize(row_result, metrics):
    runs = row_result["runs"]
    out = {}
    for m in metrics:
        vals = [r[m] for r in runs if m in r and not isinstance(r[m], str)]
        if vals:
            out[m] = (statistics.median(vals), min(vals), max(vals), len(vals))
    return out


def print_row(row, res, metrics):
    rid, _w, _a, _r, _m, desc = row
    print(f"\n### {rid}: {desc}")
    if res["error"]:
        print(f"  could not run: {res['error'][0]}")
        return
    if res["runs"]:
        r0 = res["runs"][0]
        conds = " ".join(f"{k}={r0[k]}" for k in ("version", "sync", "wal", "changefeed",
                                                   "cache_mb", "sqlite", "journal") if k in r0)
        print(f"  ({conds}; {len(res['runs'])} runs)")
    s = summarize(res, metrics)
    for m in metrics:
        if m in s:
            med, lo, hi, n = s[m]
            spread = "" if n == 1 else f"   [{fmt(lo)} .. {fmt(hi)}]"
            print(f"  {m:34s} {fmt(med):>12s}{spread}")


def machine():
    info = {"date": time.strftime("%Y-%m-%d"), "os": platform.platform()}
    if sys.platform == "darwin":
        for key, name in (("machdep.cpu.brand_string", "cpu"), ("hw.ncpu", "cores"),
                          ("hw.memsize", "memory")):
            try:
                info[name] = subprocess.run(["sysctl", "-n", key], capture_output=True,
                                            text=True).stdout.strip()
            except OSError:
                pass
        try:
            info["macos"] = subprocess.run(["sw_vers", "-productVersion"], capture_output=True,
                                           text=True).stdout.strip()
        except OSError:
            pass
    return info


def cmd_run(ns):
    if ns.list:
        for name, rows in SUITES.items():
            print(f"{name}:")
            for row in rows:
                print(f"  {row[0]:28s} {row[1]} {' '.join(row[2])}  x{row[3]}  ({row[5]})")
        return 0
    if not ns.bin:
        print("capi_bench.py: --bin is required", file=sys.stderr)
        return 2
    suites = ns.suite or DEFAULT_SUITES
    rows = [row for s in suites for row in SUITES[s]]
    if ns.only:
        rows = [row for row in rows if row[0] in ns.only.split(",")]
    scratch = ns.scratch or tempfile.mkdtemp(prefix="capi-bench-")
    os.makedirs(scratch, exist_ok=True)
    out = {"machine": machine(), "bin": ns.bin, "label": ns.label, "rows": []}
    m = out["machine"]
    print(f"# capi_bench {ns.label or ''}: {m.get('cpu', '')}, {m.get('cores', '')} cores, "
          f"{m.get('macos', m['os'])}, {m['date']}")
    for row in rows:
        print(f"running {row[0]} ...", file=sys.stderr, flush=True)
        res = run_row(ns.bin, row, scratch, ns.runs)
        res["metrics"] = row[4]
        res["desc"] = row[5]
        out["rows"].append(res)
        print_row(row, res, row[4])
        sys.stdout.flush()
        if ns.json:
            with open(ns.json, "w") as f:
                json.dump(out, f, indent=1)
    if not ns.scratch:
        shutil.rmtree(scratch, ignore_errors=True)
    return 0


HIGHER_IS_BETTER = ("per_sec", "recall", "mean_hits", "mean_count")


def cmd_compare(ns):
    with open(ns.old) as f:
        old = json.load(f)
    with open(ns.new) as f:
        new = json.load(f)
    old_rows = {r["id"]: r for r in old["rows"]}
    print(f"| row | metric | {old.get('label') or 'old'} | {new.get('label') or 'new'} "
          f"| new/old | verdict |")
    print("|---|---|---:|---:|---:|---|")
    for r in new["rows"]:
        o = old_rows.get(r["id"])
        if o is None:
            continue
        if o["error"] or r["error"]:
            err = (o["error"] or ["-"])[0] if o["error"] else "ran"
            print(f"| {r['id']} | - | {err} | {'ran' if not r['error'] else r['error'][0]} | | |")
            continue
        so, sn = summarize(o, r["metrics"]), summarize(r, r["metrics"])
        for m in r["metrics"]:
            if m not in so or m not in sn:
                continue
            (mo, lo_o, hi_o, _), (mn, lo_n, hi_n, _) = so[m], sn[m]
            ratio = mn / mo if mo else float("inf")
            higher = any(h in m for h in HIGHER_IS_BETTER)
            if any(h in m for h in ("mean_count", "mean_hits", "file_mb", "footprint",
                                    "peak_rss", "open_ms", "_max")):
                verdict = ""
            else:
                worse = (ratio < 0.9 and hi_n < lo_o) if higher else (ratio > 1.1 and lo_n > hi_o)
                better = (ratio > 1.1 and lo_n > hi_o) if higher else (ratio < 0.9 and hi_n < lo_o)
                verdict = "SLOWER" if worse else ("faster" if better else "same")
            print(f"| {r['id']} | {m} | {fmt(mo)} | {fmt(mn)} | {ratio:.2f} | {verdict} |")
    return 0


def cmd_build_against(ns):
    here = os.path.dirname(os.path.abspath(__file__))
    checkout = os.path.abspath(ns.checkout)
    subprocess.run(["zig", "build", "-Doptimize=ReleaseFast"], cwd=checkout, check=True)
    lib = os.path.join(checkout, "zig-out", "lib", "liblattice.a")
    subprocess.run(["zig", "cc", "-O2", "-I", os.path.join(checkout, "include"),
                    os.path.join(here, "capi_bench.c"), lib, "-lsqlite3", "-o", ns.out],
                   check=True)
    print(ns.out)
    return 0


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = p.add_subparsers(dest="cmd")
    r = sub.add_parser("run", help="run suites (the default command)")
    r.add_argument("--bin", help="the capi_bench executable")
    r.add_argument("--suite", action="append", choices=sorted(SUITES),
                   help="suite to run (repeatable); default: " + " ".join(DEFAULT_SUITES))
    r.add_argument("--only", help="comma-separated row ids")
    r.add_argument("--runs", type=int, help="runs per row instead of each row's own count")
    r.add_argument("--scratch", help="directory for the databases (default: a temp dir)")
    r.add_argument("--json", help="write every run's results here")
    r.add_argument("--label", default="", help="a name for this binary in the output")
    r.add_argument("--list", action="store_true", help="list the suites and exit")
    c = sub.add_parser("compare", help="compare two --json files")
    c.add_argument("old")
    c.add_argument("new")
    b = sub.add_parser("build-against", help="build capi_bench against another checkout")
    b.add_argument("checkout")
    b.add_argument("--out", required=True)
    argv = sys.argv[1:]
    if not argv or argv[0] not in ("run", "compare", "build-against", "-h", "--help"):
        argv = ["run"] + argv
    ns = p.parse_args(argv)
    if ns.cmd == "compare":
        return cmd_compare(ns)
    if ns.cmd == "build-against":
        return cmd_build_against(ns)
    return cmd_run(ns)


if __name__ == "__main__":
    sys.exit(main())
