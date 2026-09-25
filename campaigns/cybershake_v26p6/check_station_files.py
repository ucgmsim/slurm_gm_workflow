"""Find IM per-station files left malformed by an interrupted run.

calculate_ims_mpi.py writes each station's CSV non-atomically and, on resume,
skips any station whose file is non-empty. A file truncated by a cancel or a
timeout would therefore be silently carried into the final CSV. This checks
every file and, with --apply, moves malformed ones to a quarantine directory
(never deletes), so a resumed run recomputes them.

A valid file: parses, has exactly the expected 40-column header, exactly one
row for each of the 6 components, station column equal to the filename, and
only finite numbers.

Usage: python check_station_files.py STATIONS_DIR [--apply QUARANTINE_DIR]
"""

import shutil
import sys
from pathlib import Path

import numpy as np
import pandas as pd

COMPONENTS = {"000", "090", "ver", "geom", "rotd50", "rotd100_50"}
PERIODS = ["0.01", "0.02", "0.03", "0.04", "0.05", "0.075", "0.1", "0.12", "0.15", "0.17", "0.2",
           "0.25", "0.3", "0.4", "0.5", "0.6", "0.7", "0.75", "0.8", "0.9", "1.0", "1.25", "1.5",
           "2.0", "2.5", "3.0", "4.0", "5.0", "6.0", "7.5", "10.0"]
HEADER = ["station", "component", "PGA", "PGV", "CAV", "AI", "Ds575", "Ds595", "MMI"] + [f"pSA_{p}" for p in PERIODS]


def problem(path: Path) -> str | None:
    # A checker must report bad input, never crash on it.
    try:
        return _problem(path)
    except Exception as e:
        return f"unreadable ({type(e).__name__}: {e})"


def _problem(path: Path) -> str | None:
    try:
        df = pd.read_csv(path, dtype={"station": str, "component": str})
    except Exception as e:
        return f"does not parse ({type(e).__name__})"
    if list(df.columns) != HEADER:
        return f"header mismatch ({len(df.columns)} columns)"
    if len(df) != len(COMPONENTS) or set(df["component"]) != COMPONENTS:
        return f"{len(df)} rows, components {sorted(map(str, set(df['component'])))}"
    if set(df["station"]) != {path.stem}:
        return f"station column {sorted(map(str, set(df['station'])))} != filename"
    values = df[HEADER[2:]].to_numpy(dtype=float)
    if not np.isfinite(values).all():
        return f"{int((~np.isfinite(values)).sum())} non-finite value(s)"
    return None


def main():
    stations_dir = Path(sys.argv[1])
    quarantine = Path(sys.argv[3]) if len(sys.argv) > 3 and sys.argv[2] == "--apply" else None
    files = sorted(stations_dir.glob("*.csv"))
    bad = [(f, p) for f in files if (p := problem(f)) is not None]
    others = [f for f in stations_dir.iterdir() if f.suffix != ".csv"]
    print(f"{stations_dir}: {len(files)} station files, {len(bad)} malformed, {len(others)} non-csv entries")
    for f, p in bad:
        print(f"  MALFORMED {f.name}: {p}")
    for f in others:
        print(f"  UNEXPECTED {f.name}")
    if quarantine and bad:
        quarantine.mkdir(parents=True, exist_ok=True)
        for f, _ in bad:
            shutil.move(str(f), str(quarantine / f.name))
        print(f"moved {len(bad)} file(s) to {quarantine}")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
