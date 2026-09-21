#!/usr/bin/env python
"""Compare the station layout of two BB.bin files.

Used to confirm a newly produced realisation matches the ones already
finished for the same fault: same station count, same timesteps, same
names in the same order. Reads only the header, so it is cheap on the
multi-gigabyte outputs this is meant for.
"""

import argparse
import struct
import sys
from pathlib import Path
from typing import NamedTuple

import numpy as np

# Mirrors qcore.timeseries.BBSeis so the layout is stated in one shape
# rather than as hand-counted byte offsets. The name field sits at offset
# 8, not at the start of the record.
HEAD_SIZE = 0x500
HEAD_STAT = 0x2C
STATION_DTYPE = np.dtype(
    [
        ("lon", "<f4"),
        ("lat", "<f4"),
        ("name", "|S8"),
        ("x", "<i4"),
        ("y", "<i4"),
        ("z", "<i4"),
        ("e_dist", "<f4"),
        ("hf_vs_ref", "<f4"),
        ("lf_vs_ref", "<f4"),
        ("vsite", "<f4"),
    ]
)
assert STATION_DTYPE.itemsize == HEAD_STAT, "station record must match BBSeis.HEAD_STAT"


class BBHeader(NamedTuple):
    nstat: int
    nt: int
    names: list[str]


def read_bb_header(path) -> BBHeader:
    """Read station count, timestep count and station names from a BB.bin."""
    with open(path, "rb") as f:
        nstat, nt = struct.unpack("<ii", f.read(8))
        f.seek(HEAD_SIZE)
        stations = np.fromfile(f, dtype=STATION_DTYPE, count=nstat)
    if stations.size != nstat:
        raise ValueError(
            f"{path}: station header is truncated "
            f"({stations.size} records, expected {nstat})"
        )
    names = [raw.split(b"\0")[0].decode() for raw in stations["name"]]
    return BBHeader(nstat=nstat, nt=nt, names=names)


def compare(path_a, path_b) -> tuple[bool, list[str]]:
    """Return (match, differences) for two BB.bin files."""
    a, b = read_bb_header(path_a), read_bb_header(path_b)
    differences: list[str] = []

    if a.nstat != b.nstat:
        differences.append(f"nstat differs: {a.nstat} vs {b.nstat}")
    if a.nt != b.nt:
        differences.append(f"nt differs: {a.nt} vs {b.nt}")

    set_a, set_b = set(a.names), set(b.names)
    only_a, only_b = sorted(set_a - set_b), sorted(set_b - set_a)
    if only_a or only_b:
        differences.append(
            f"station sets differ: {len(only_a)} only in A {only_a[:10]}, "
            f"{len(only_b)} only in B {only_b[:10]}"
        )
    elif a.names != b.names:
        first = next(i for i, (x, y) in enumerate(zip(a.names, b.names)) if x != y)
        differences.append(
            f"same stations, different order: first at row {first} "
            f"({a.names[first]} vs {b.names[first]})"
        )

    return not differences, differences


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bb_a", type=Path)
    parser.add_argument("bb_b", type=Path)
    args = parser.parse_args()

    ok, differences = compare(args.bb_a, args.bb_b)
    header = read_bb_header(args.bb_a)
    print(f"A: {args.bb_a}")
    print(f"B: {args.bb_b}")
    print(f"nstat={header.nstat} nt={header.nt}")
    if ok:
        print("MATCH: same stations, same order, same nt.")
        return 0
    for difference in differences:
        print(f"DIFFER: {difference}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
