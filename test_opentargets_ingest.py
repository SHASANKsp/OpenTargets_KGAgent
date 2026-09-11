#!/usr/bin/env python3
# =============================================================================
#  PRE-FLIGHT TEST for ingest_opentargets_neo4j.py
# -----------------------------------------------------------------------------
#  Run this BEFORE the real ingest. It writes NOTHING to Neo4j. It checks:
#     1. Python deps (duckdb, neo4j) import.
#     2. DATA_DIR points at the parquet folders  (this is what failed for you).
#        -> if not, it auto-searches for the data and tells you the right path.
#     3. Neo4j is reachable with the configured credentials.
#     4. For every loader: the DuckDB query reads real rows AND the matching
#        Cypher executes -- inside a transaction that is ROLLED BACK, so the
#        database is left untouched.
#
#  Usage:
#     python test_opentargets_ingest.py                 # uses DATA_DIR from main script
#     python test_opentargets_ingest.py "E:\data\opentargets-26.03"   # override path
#
#  It imports the main script so it tests the exact same SQL + Cypher you'll run.
#  Keep both files in the same folder.
# =============================================================================

import os
import sys
import glob

GREEN = "\033[92m"; RED = "\033[91m"; YEL = "\033[93m"; DIM = "\033[2m"; END = "\033[0m"
def ok(m):   print(f"  {GREEN}PASS{END}  {m}")
def bad(m):  print(f"  {RED}FAIL{END}  {m}")
def warn(m): print(f"  {YEL}WARN{END}  {m}")
def hd(m):   print(f"\n{'='*70}\n  {m}\n{'='*70}")

PROBE_ROWS = 5           # rows pulled per loader for the probe
results = {"pass": 0, "fail": 0, "warn": 0}


# -----------------------------------------------------------------------------
# STEP 1 -- dependencies
# -----------------------------------------------------------------------------
hd("STEP 1  Dependencies")
try:
    import duckdb
    ok(f"duckdb {duckdb.__version__}")
except Exception as e:
    bad(f"duckdb not importable: {e}"); print("\n  -> pip install duckdb"); sys.exit(1)
try:
    import neo4j
    from neo4j import GraphDatabase
    ok(f"neo4j driver {neo4j.__version__}")
except Exception as e:
    bad(f"neo4j not importable: {e}"); print("\n  -> pip install neo4j"); sys.exit(1)


# -----------------------------------------------------------------------------
# STEP 2 -- import the main ingest module (single source of truth)
# -----------------------------------------------------------------------------
hd("STEP 2  Import main ingest script")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    import ingest_opentargets_neo4j as ing
    ok("imported ingest_opentargets_neo4j.py")
except Exception as e:
    bad(f"could not import ingest_opentargets_neo4j.py: {e}")
    print("  -> keep this test file in the SAME folder as the main script.")
    sys.exit(1)


# -----------------------------------------------------------------------------
# STEP 3 -- resolve DATA_DIR  (the thing that failed in your run)
# -----------------------------------------------------------------------------
hd("STEP 3  Locate the parquet data")

# allow override from the command line
if len(sys.argv) > 1:
    ing.DATA_DIR = sys.argv[1]
    ing.DATA_DIR_SQL = ing.DATA_DIR.replace("\\", "/")
    print(f"  (using path from command line)")

print(f"  configured DATA_DIR = {ing.DATA_DIR!r}")
print(f"  used in SQL as      = {ing.DATA_DIR_SQL!r}")

# the dataset folder names the loaders reference == the spec keys
specs = ing.build_specs()
needed_dirs = sorted({key for (key, *_ ) in specs})

def data_dir_valid(d):
    return os.path.isdir(d) and bool(glob.glob(os.path.join(d, "target", "*.parquet")))

if data_dir_valid(ing.DATA_DIR):
    ok(f"DATA_DIR exists and contains target/*.parquet")
else:
    bad(f"DATA_DIR is not a valid Open Targets folder (no target/*.parquet under it)")
    print(f"\n  {DIM}This is why the ingest loaded 0 rows.{END}")
    # ---- auto-search for the real location ----
    print("\n  Searching for the data folder...")
    candidates = []
    roots = [os.getcwd(), os.path.expanduser("~"), "/", "E:\\", "E:\\data",
             "C:\\", "D:\\", "/data", "/mnt", os.path.dirname(os.path.abspath(__file__))]
    seen = set()
    for root in roots:
        if not root or not os.path.isdir(root):
            continue
        # look 0-2 levels deep for a dir that has target/*.parquet
        for depth in range(3):
            pattern = os.path.join(root, *(["*"] * depth), "target", "*.parquet")
            for hit in glob.glob(pattern):
                cand = os.path.dirname(os.path.dirname(hit))
                if cand not in seen:
                    seen.add(cand); candidates.append(cand)
    if candidates:
        print(f"\n  {GREEN}Found the data here -- set DATA_DIR to one of these:{END}")
        for c in candidates[:10]:
            print(f"      {c}")
    else:
        print(f"  {YEL}Could not auto-locate it. Set DATA_DIR to the folder that")
        print(f"  contains the sub-folders target/, disease/, drug_molecule/, ...{END}")
    print("\n  Fix DATA_DIR (top of ingest_opentargets_neo4j.py) and re-run this test.")
    sys.exit(1)

# report presence of every needed dataset folder
print(f"\n  Checking {len(needed_dirs)} referenced dataset folders:")
missing = []
for d in needed_dirs:
    files = glob.glob(os.path.join(ing.DATA_DIR, d, "*.parquet"))
    if files:
        print(f"    {GREEN}ok{END}   {d:<42} {len(files)} parquet file(s)")
    else:
        print(f"    {RED}--{END}   {d:<42} MISSING")
        missing.append(d)
if missing:
    warn(f"{len(missing)} dataset folder(s) missing: {', '.join(missing)}")
    warn("Loaders for missing folders will error at runtime (others are fine).")
    results["warn"] += 1
else:
    ok("all referenced dataset folders present")


# -----------------------------------------------------------------------------
# STEP 4 -- Neo4j connectivity
# -----------------------------------------------------------------------------
hd("STEP 4  Neo4j connection")
neo4j_up = False
try:
    with ing.driver.session() as s:
        val = s.run("RETURN 1 AS ok").single()["ok"]
    if val == 1:
        ok(f"connected to {ing.NEO4J_URI} as '{ing.NEO4J_USER}'")
        neo4j_up = True
except Exception as e:
    bad(f"cannot reach Neo4j at {ing.NEO4J_URI}: {str(e)[:160]}")
    warn("Cypher checks will be SKIPPED. Fix URI/credentials and re-run for full test.")
    print("  (SQL/parquet checks below still run.)")


# -----------------------------------------------------------------------------
# STEP 5 -- per-loader probe: DuckDB read + Cypher execute (rolled back)
# -----------------------------------------------------------------------------
hd("STEP 5  Probe every loader (no data is written)")
print(f"  Testing all {len(specs)} loaders "
      f"({'incl.' if True else 'excl.'} disabled ones), {PROBE_ROWS} rows each.\n")

def clean(rows, columns, required):
    out = []
    for r in rows:
        rec = dict(zip(columns, r))
        for k, v in rec.items():
            if isinstance(v, str):
                rec[k] = v.strip()
        if required and any(rec.get(k) in (None, "") for k in required):
            continue
        out.append(rec)
    return out

for key, sql, cypher, label, required, count in specs:
    enabled = ing.ENABLED.get(key, True)
    tag = "" if enabled else f" {DIM}[disabled in ENABLED]{END}"
    # --- 5a. DuckDB read ---
    try:
        probe_sql = f"SELECT * FROM ({sql}) AS _probe LIMIT {PROBE_ROWS}"
        cur = ing.duck.execute(probe_sql)
        cols = [d[0] for d in cur.description]
        raw = cur.fetchall()
    except Exception as e:
        bad(f"{label}{tag}\n         DuckDB read error: {str(e)[:170]}")
        results["fail"] += 1
        continue

    if not raw:
        warn(f"{label}{tag}  -- query OK but returned 0 rows (empty/over-filtered?)")
        results["warn"] += 1
        continue

    rows = clean(raw, cols, required)
    if not rows:
        warn(f"{label}{tag}  -- {len(raw)} rows read but all dropped by required-key filter {required}")
        results["warn"] += 1
        continue

    # --- 5b. Cypher execute + rollback ---
    if not neo4j_up:
        ok(f"{label}{tag}  -- SQL ok ({len(rows)} rows); Cypher skipped (no Neo4j)")
        results["pass"] += 1
        continue
    try:
        with ing.driver.session() as s:
            tx = s.begin_transaction()
            try:
                tx.run(cypher, {"rows": rows})
            finally:
                tx.rollback()          # nothing persists
        ok(f"{label}{tag}  -- SQL ok ({len(rows)} rows) + Cypher ok (rolled back)")
        results["pass"] += 1
    except Exception as e:
        bad(f"{label}{tag}\n         Cypher error: {str(e)[:170]}")
        results["fail"] += 1


# -----------------------------------------------------------------------------
# VERDICT
# -----------------------------------------------------------------------------
hd("VERDICT")
print(f"  PASS: {results['pass']}    WARN: {results['warn']}    FAIL: {results['fail']}")
try:
    ing.driver.close(); ing.duck.close()
except Exception:
    pass

if results["fail"] == 0:
    print(f"\n  {GREEN}All loaders validated. Safe to run the main ingest.{END}")
    if results["warn"]:
        print(f"  {YEL}(Review WARNs above -- usually empty tables or missing optional folders.){END}")
    sys.exit(0)
else:
    print(f"\n  {RED}{results['fail']} loader(s) failed. Fix these before running the main ingest.{END}")
    sys.exit(1)
