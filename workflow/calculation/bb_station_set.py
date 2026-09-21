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
