"""Decide which stations a broadband simulation will produce.

LF and HF are not guaranteed to have been run over the same station list.
EMOD3D writes a station twice when it sits on the boundary between two MPI
domains, and can leave a boundary station written by neither domain as a
blank-named slot. Separately, an LF run may simply be missing a station
that HF has.

This module holds every decision that follows from that, deliberately free
of MPI and qcore imports: it takes plain arrays of station names and
returns index arrays, so it can be tested without a cluster or real
seismogram files.
"""

from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import NamedTuple

import numpy as np


class StationSetError(Exception):
    """A situation the caller has to decide about, rather than the library."""


def first_occurrence_indices(names) -> tuple[np.ndarray, int, int]:
    """Index of the first occurrence of each distinct, non-blank name.

    Keeping the first occurrence of a duplicate (rather than averaging, or
    keeping the last) is the convention established by
    cs_nshm_2022/bin/dedupe_lf_stations.py: the two copies are the same
    waveform recorded twice from adjacent MPI domains, so neither is more
    correct and the choice is arbitrary but must be consistent.

    Returns (keep_idx, n_blank, n_duplicate). keep_idx is ascending, so the
    original relative order of the surviving stations is preserved.
    """
    seen: set[str] = set()
    keep: list[int] = []
    n_blank = 0
    for i, raw in enumerate(names):
        name = str(raw)
        if not name:
            n_blank += 1
            continue
        if name in seen:
            continue
        seen.add(name)
        keep.append(i)
    n_duplicate = len(names) - n_blank - len(keep)
    return np.asarray(keep, dtype=np.int64), n_blank, n_duplicate


def read_station_list(path) -> list[str]:
    """Read a canonical station list: one name per line.

    Blank lines and anything after a '#' are ignored. Order is preserved
    exactly as written, because it becomes the output row order.
    """
    names: list[str] = []
    for line in Path(path).read_text().splitlines():
        name = line.split("#", 1)[0].strip()
        if name:
            names.append(name)

    if not names:
        raise StationSetError(f"{path} contains no station names.")

    repeated = sorted(n for n, count in Counter(names).items() if count > 1)
    if repeated:
        raise StationSetError(
            f"{path} lists {len(repeated)} station name(s) more than once: "
            f"{repeated[:10]}"
        )
    return names


class StationSet(NamedTuple):
    """The stations a BB run will produce, and where to find each one.

    names[k] is the station written to output row k. lf_idx[k] and
    hf_idx[k] are that station's positions in the LF and HF station
    arrays. Every quantity in bb_sim.py is keyed by k, which is what keeps
    LF order, HF order and output row order from being conflated.
    """

    names: np.ndarray
    lf_idx: np.ndarray
    hf_idx: np.ndarray
    report: list[str]


def _positions(names, label: str, report: list[str]) -> dict[str, int]:
    """Map each distinct name to the index of its first occurrence."""
    keep, n_blank, n_duplicate = first_occurrence_indices(names)
    if n_blank or n_duplicate:
        report.append(
            f"{label} de-duplicated: {len(names)} records -> {len(keep)} "
            f"stations ({n_duplicate} duplicate, {n_blank} blank-named)."
        )
    return {str(names[i]): int(i) for i in keep}


def resolve_station_set(
    lf_names,
    hf_names,
    station_list: list[str] | None = None,
    allow_subset: bool = False,
) -> StationSet:
    """Decide the station set, and how to index into LF and HF for it.

    With station_list, the set is exactly those names in that order and
    every one must be present in both inputs. Without it, the set is the
    LF/HF intersection in HF order, and any difference between the two
    sides is an error unless allow_subset is set: dropping a station HF
    has is a real reduction of the output and must be an explicit choice.
    """
    if station_list is not None and allow_subset:
        raise StationSetError(
            "--station-list and --allow-station-subset are mutually "
            "exclusive: with an explicit list, every listed station must be "
            "present in both LF and HF."
        )

    lf_names = np.asarray(lf_names)
    hf_names = np.asarray(hf_names)
    report: list[str] = []
    lf_pos = _positions(lf_names, "LF", report)
    hf_pos = _positions(hf_names, "HF", report)

    if station_list is not None:
        names = list(station_list)
        missing_lf = [n for n in names if n not in lf_pos]
        missing_hf = [n for n in names if n not in hf_pos]
        if missing_lf or missing_hf:
            raise StationSetError(
                f"Station list names {len(missing_lf)} station(s) absent from "
                f"LF {missing_lf[:10]} and {len(missing_hf)} absent from HF "
                f"{missing_hf[:10]}."
            )
        report.append(f"Using explicit station list: {len(names)} stations.")
    else:
        # hf_pos preserves HF first-occurrence order, so this is HF order.
        names = [n for n in hf_pos if n in lf_pos]
        only_hf = sorted(set(hf_pos) - set(lf_pos))
        only_lf = sorted(set(lf_pos) - set(hf_pos))
        if only_hf or only_lf:
            detail = (
                f"LF and HF cover different stations: {len(only_hf)} only in HF "
                f"{only_hf[:10]}, {len(only_lf)} only in LF {only_lf[:10]}."
            )
            if not allow_subset:
                raise StationSetError(
                    detail + " Pass --allow-station-subset to drop them and "
                    "continue on the common set, or --station-list to state "
                    "the intended set explicitly."
                )
            report.append("WARNING: " + detail + " Continuing on the common set.")
        else:
            report.append(f"LF and HF station sets match: {len(names)} stations.")

    if not names:
        raise StationSetError("LF and HF have no stations in common.")

    return StationSet(
        names=np.asarray(names),
        lf_idx=np.asarray([lf_pos[n] for n in names], dtype=np.int64),
        hf_idx=np.asarray([hf_pos[n] for n in names], dtype=np.int64),
        report=report,
    )
