#!/usr/bin/env python3
"""Seed synthetic ERP material master data with injected near-duplicate clusters.

Generates deterministic material rows (same env -> same data): industrial
vocab with structured identity (brand, part_no, spec, dims) plus DUPE_PCT%
variant rows that share a canonical row's full identity but carry messy,
varied descriptions (casing, word order, unit spacing, SS304/SUS304 synonym
swaps, pack-quantity noise). Those variants are the ground-truth duplicates
the embedding + similarity pipeline should surface.

Env:
  DATABASE_URL   postgres DSN (default docker-compose stack)
  ROWS           total rows incl. duplicate variants (default 100000)
  DUPE_PCT       percent of ROWS that are near-dup variants (default 10)
  TRUNCATE       1 = wipe materials first (default 1)
"""

import hashlib
import os
import subprocess
import sys

TYPES = {
    "HEX BOLT": ["M6X20", "M8X30", "M8X50", "M10X40", "M12X60"],
    "SOCKET SCREW": ["M5X16", "M6X25", "M8X40"],
    "O-RING": ["10X2", "15X3", "20X3", "25X4"],
    "BALL BEARING": ["6204-2RS", "6205-2Z", "6000ZZ", "6308"],
    "V-BELT": ["A-850", "A-900", "B-1250", "B-1400"],
    "HYDRAULIC HOSE": ["1/4IN-2SN", "3/8IN-2SN", "1/2IN-4SP"],
    "GASKET SHEET": ["1X1000X1500", "2X1000X1500", "3X1270X4270"],
    "AIR FILTER": ["AF25553", "AF25554", "C131053"],
}
BRANDS = ["SKF", "BOSCH", "PARKER", "GATES", "NITTO", "FESTO", "SMC", "WURTH", "TIMKEN", "3M"]
SPECS = ["SS304", "SS316", "A2-70", "GR8.8 ZN", "NBR70", "EPDM",
         "FKM", "CHROME STEEL", "RUBBER", "PTFE", "CNAF", "GRAPHITE"]


def h(*parts):
    return hashlib.md5("|".join(parts).encode()).hexdigest()


def pick(seed, n):
    return int(h(seed)[:8], 16) % n


def identity(ci):
    type_name = list(TYPES)[pick(f"type:{ci}", len(TYPES))]
    dims = TYPES[type_name][pick(f"dims:{ci}", len(TYPES[type_name]))]
    spec = SPECS[pick(f"spec:{ci}", len(SPECS))]
    brand = BRANDS[pick(f"brand:{ci}", len(BRANDS))]
    series = h(f"series:{ci}")[:8].upper()
    dimsfull = f"{dims}-{series}"
    part_no = h(type_name, brand, dimsfull, spec)[:8].upper()
    return dict(mtype=type_name, dimsfull=dimsfull, mspec=spec, mbrand=brand, mpartno=part_no)


def canonical_name(a):
    return f"{a['mtype']} {a['dimsfull']} {a['mspec']}"


def messy_name(v, a):
    style = v % 6
    if style == 0:
        return f"{a['mtype'].title()} {a['dimsfull'].replace('X', ' X ')}MM {a['mspec']}"
    if style == 1:
        return f"{a['mtype']} {a['dimsfull']} {a['mspec']}".lower()
    if style == 2:
        box = ((v * 31) % 9 + 1) * 50
        return f"{a['mtype']}, {a['dimsfull']},{a['mspec']} (BOX OF {box})"
    if style == 3:
        spec = {"SS304": "SUS304", "SS316": "SUS316"}.get(a["mspec"], a["mspec"])
        return f"{a['dimsfull']}-{a['mtype']} {spec}"
    if style == 4:
        return f"{a['mtype'].upper()}  {a['dimsfull']} / {a['mspec']} GRADE"
    return f"{a['mbrand'].title()} {a['mtype']} {a['dimsfull']} {a['mspec']}"


def mat_no(n):
    return "M" + str(n).zfill(7)


def psql(url, sql=None, stdin=None):
    cmd = ["psql", "-v", "ON_ERROR_STOP=1", url]
    if isinstance(stdin, str):
        stdin = stdin.encode()
    elif isinstance(sql, str):
        stdin = sql.encode()
    return subprocess.run(cmd, input=stdin or b"", check=True, capture_output=True)


def main():
    pgurl = os.environ.get("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/postgres")
    rows = int(os.environ.get("ROWS", "100000"))
    dupe_pct = int(os.environ.get("DUPE_PCT", "10"))
    truncate = os.environ.get("TRUNCATE", "1") == "1"

    n_canon = max(rows * (100 - dupe_pct) // 100, 1)
    n_var = rows - n_canon

    print("==> seed-materials: extending materials schema (brand, part_no, spec, dims)")
    psql(pgurl, """
CREATE EXTENSION IF NOT EXISTS vector;
ALTER TABLE materials ADD COLUMN IF NOT EXISTS brand   VARCHAR(60);
ALTER TABLE materials ADD COLUMN IF NOT EXISTS part_no VARCHAR(40);
ALTER TABLE materials ADD COLUMN IF NOT EXISTS spec    VARCHAR(60);
ALTER TABLE materials ADD COLUMN IF NOT EXISTS dims    VARCHAR(60);
""")
    if truncate:
        print("==> seed-materials: truncating materials (+ referencing tables)")
        psql(pgurl, "TRUNCATE materials, sizes, colorways, material_features CASCADE;")

    tsv = []
    for i in range(1, n_canon + 1):
        a = identity(i)
        status = "discontinued" if (i * 17) % 20 == 0 else "active"
        tsv.append("\t".join([mat_no(i), canonical_name(a), status,
                              a["mbrand"], a["mpartno"], a["mspec"], a["dimsfull"]]))
    for v in range(1, n_var + 1):
        ci = (v * 7919) % n_canon + 1
        a = identity(ci)
        tsv.append("\t".join([mat_no(n_canon + v), messy_name(v, a), "active",
                              a["mbrand"], a["mpartno"], a["mspec"], a["dimsfull"]]))

    print(f"==> seed-materials: bulk-loading {len(tsv)} rows via COPY")
    psql(pgurl, stdin="\\copy materials (mat_no, name, status, brand, part_no, spec, dims) FROM STDIN\n"
         + "\n".join(tsv) + "\n")

    print("==> seed-materials: summary")
    out = subprocess.run(
        ["psql", "-At", pgurl], text=True, capture_output=True, check=True, input="""\
SELECT 'total_rows=' || count(*) FROM materials;
SELECT 'distinct_partnos=' || count(DISTINCT part_no) FROM materials WHERE part_no IS NOT NULL;
SELECT 'sample_cluster:';
SELECT mat_no || ' | ' || name || ' | ' || brand || ' | ' || part_no
FROM materials WHERE part_no = (
  SELECT part_no FROM materials GROUP BY part_no HAVING count(*) > 1
  ORDER BY min(mat_no) LIMIT 1)
ORDER BY mat_no;
""").stdout.strip().splitlines()
    for line in out:
        print(line)
    print("==> seed-materials: done")


if __name__ == "__main__":
    sys.exit(main())
