"""Read-only checks on IM_calculation output, per realisation and across a fault.

Per realisation:
  - <rel>.csv and <rel>_imcalc.info both exist
  - the station set equals the BB.bin station set exactly
  - every station has every expected component, exactly once
  - no NaN or infinite values, and PGA > 0 everywhere
Across realisations:
  - median log(PGA) and log(pSA 1.0 s) of the geom component per realisation,
    flagging any realisation outside the range spanned by all the others.

Usage: python verify_ims.py REL_DIR [REL_DIR ...]
"""

import struct
import sys
from pathlib import Path

import numpy as np
import pandas as pd

EXPECTED_COMPONENTS = {"000", "090", "ver", "geom", "rotd50", "rotd100_50"}
HEAD_SIZE = 0x500
STATION_DTYPE = np.dtype([
    ("lon", "<f4"), ("lat", "<f4"), ("name", "|S8"), ("x", "<i4"), ("y", "<i4"),
    ("z", "<i4"), ("e_dist", "<f4"), ("hf_vs_ref", "<f4"), ("lf_vs_ref", "<f4"), ("vsite", "<f4"),
])


def bb_station_names(rel_dir: Path) -> set[str]:
    with open(rel_dir / "BB" / "Acc" / "BB.bin", "rb") as f:
        nstat = struct.unpack("<i", f.read(4))[0]
        f.seek(HEAD_SIZE)
        stations = np.fromfile(f, dtype=STATION_DTYPE, count=nstat)
    return {raw.split(b"\0")[0].decode() for raw in stations["name"]}


def check(rel_dir: Path):
    rel = rel_dir.name
    problems = []
    csv, info = rel_dir / "IM_calc" / f"{rel}.csv", rel_dir / "IM_calc" / f"{rel}_imcalc.info"
    for p in (csv, info):
        if not p.is_file():
            problems.append(f"missing {p.name}")
    if problems:
        return rel, problems, None

    df = pd.read_csv(csv, dtype={"station": str, "component": str})
    expected = bb_station_names(rel_dir)
    got = set(df["station"])
    if got != expected:
        problems.append(f"station set differs from BB: {len(expected - got)} missing, {len(got - expected)} extra")
    comps = set(df["component"])
    if comps != EXPECTED_COMPONENTS:
        problems.append(f"components {sorted(comps)} != expected {sorted(EXPECTED_COMPONENTS)}")
    per_station = df.groupby("station")["component"].agg(["count", "nunique"])
    bad = per_station[(per_station["count"] != len(EXPECTED_COMPONENTS)) | (per_station["nunique"] != len(EXPECTED_COMPONENTS))]
    if len(bad):
        problems.append(f"{len(bad)} station(s) without exactly one row per component")
    numeric = df.select_dtypes("number")
    n_bad = int((~np.isfinite(numeric.to_numpy())).sum())
    if n_bad:
        problems.append(f"{n_bad} NaN/inf value(s)")
    if (df["PGA"] <= 0).any():
        problems.append(f"{int((df['PGA'] <= 0).sum())} row(s) with PGA <= 0")

    geom = df[df["component"] == "geom"]
    stats = {
        "stations": len(got),
        "rows": len(df),
        "med_lnPGA": float(np.median(np.log(geom["PGA"]))),
        "med_lnSA1": float(np.median(np.log(geom["pSA_1.0"]))),
    }
    return rel, problems, stats


def main():
    results = [check(Path(d)) for d in sys.argv[1:]]
    print(f"{'realisation':<22} {'stations':>8} {'rows':>7} {'med lnPGA':>10} {'med lnSA1':>10}  status")
    for rel, problems, stats in results:
        if stats:
            print(f"{rel:<22} {stats['stations']:>8} {stats['rows']:>7} {stats['med_lnPGA']:>10.3f} {stats['med_lnSA1']:>10.3f}  "
                  f"{'OK' if not problems else '; '.join(problems)}")
        else:
            print(f"{rel:<22} {'-':>8} {'-':>7} {'-':>10} {'-':>10}  {'; '.join(problems)}")

    ok = [r for r in results if r[2] and not r[1]]
    if len(ok) >= 3:
        print("\nOutlier check (each realisation vs the range of all the others):")
        for rel, _, stats in ok:
            others = [s for r, _, s in ok if r != rel]
            flags = [key for key in ("med_lnPGA", "med_lnSA1")
                     if not min(o[key] for o in others) <= stats[key] <= max(o[key] for o in others)]
            if flags:
                print(f"  {rel}: outside the others' range on {flags}")
        print("  (realisations not listed are inside the others' range)")

    n_fail = sum(1 for r in results if r[1])
    print(f"\n{len(results) - n_fail} of {len(results)} realisation(s) passed")
    sys.exit(1 if n_fail else 0)


if __name__ == "__main__":
    main()
