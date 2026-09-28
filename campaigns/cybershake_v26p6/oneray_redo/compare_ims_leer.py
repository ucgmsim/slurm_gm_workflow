"""Compare the OneRay redo's IMs with the leer IMs they replace, realisation by realisation.

Read-only. For each realisation, IM_calc/<rel>.csv (OneRay) and
IM_calc.leer/<rel>.csv (leer) must list the same stations and components. Their
ratio OneRay / leer is reported for PGA, PGV and pSA at 0.1, 1.0 and 3.0 s, on
the geom component: the median, and the 0.1st and 99.9th percentiles over
stations. The one-station HF pilots found OneRay 0.04-0.10% higher, with the
waveform shape unchanged. So these ratios should sit within about 0.1% of 1,
with long periods (LF-dominated) closest to 1.

Usage: python compare_ims_leer.py TARGETS  (sarah2024 or any venv with pandas)
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

MEASURES = ["PGA", "PGV", "pSA_0.1", "pSA_1.0", "pSA_3.0"]


def main():
    targets = [Path(t) for t in Path(sys.argv[1]).read_text().split()]
    rows, problems = [], 0
    for rel in targets:
        new_csv, old_csv = rel / "IM_calc" / f"{rel.name}.csv", rel / "IM_calc.leer" / f"{rel.name}.csv"
        if not (new_csv.exists() and old_csv.exists()):
            if (rel / "HF.leer").exists() and (rel / "LF").exists():
                print(f"{rel.name:20s} missing {'OneRay' if not new_csv.exists() else 'leer'} IM")
                problems += 1
            continue
        new = pd.read_csv(new_csv, dtype={"station": str, "component": str}).set_index(["station", "component"])
        old = pd.read_csv(old_csv, dtype={"station": str, "component": str}).set_index(["station", "component"])
        if not new.index.sort_values().equals(old.index.sort_values()):
            print(f"{rel.name:20s} FAIL: station/component sets differ ({len(new)} vs {len(old)} rows)")
            problems += 1
            continue
        ratio = (new.loc[old.index, MEASURES] / old[MEASURES]).xs("geom", level="component")
        row = {"realisation": rel.name}
        for m in MEASURES:
            r = ratio[m].to_numpy()
            row[m] = (np.median(r), np.percentile(r, 0.1), np.percentile(r, 99.9))
        rows.append(row)
    print(f"{'realisation':20s} " + " ".join(f"{m:>26s}" for m in MEASURES))
    for row in rows:
        print(f"{row['realisation']:20s} " + " ".join(
            f"{row[m][0]:8.5f} [{row[m][1]:7.5f},{row[m][2]:7.5f}]" for m in MEASURES))
    allr = {m: np.array([row[m][0] for row in rows]) for m in MEASURES}
    print("\nmedian OneRay/leer ratio over realisations: " +
          ", ".join(f"{m} {np.median(allr[m]):.5f}" for m in MEASURES) + f"  ({len(rows)} realisations)")
    print("RESULT:", "OK" if not problems else f"{problems} realisation(s) missing or mismatched")
    sys.exit(1 if problems else 0)


if __name__ == "__main__":
    main()
