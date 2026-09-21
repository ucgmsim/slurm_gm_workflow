# BB Station Reconciliation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `bb_sim.py` combine LF and HF seismograms when their station sets differ, so the stalled Cybershake v26p6 BB stages for PalliserKai and WellTeast can finish.

**Architecture:** All station-set decisions move into one new pure module, `workflow/calculation/bb_station_set.py`, which takes two arrays of station names and returns a canonical ordered name array plus `lf_idx`/`hf_idx` index arrays. `bb_sim.py` then keys every downstream quantity — file size, station header, `vs30s`, `lfvs30refs`, work mask, byte offsets — off that one sequence. The module imports neither MPI nor qcore, so the logic that matters is unit-testable without a cluster or real seismogram files.

**Tech Stack:** Python 3.11 (NeSI runtime), numpy, mpi4py, qcore `timeseries`. Tests run with pytest under `uv run --no-project`, which needs only numpy.

**Spec:** `docs/superpowers/specs/2026-09-21-bb-station-reconciliation-design.md`

## Global Constraints

- Branch is `nesi-cybershake-v26p6`, based on `cc47cbd7`, **not** on `master`. The 36 completed PalliserKai BB outputs came from `cc47cbd7` code; the remaining realisations must be comparable with them.
- Target runtime is Python 3.11.6 with numpy 2.3.0 (NeSI `mrd87_4/py311`). No syntax newer than 3.11.
- Canonical station order is **HF order** on the inferred route, **file order** on the explicit-list route. Never sorted — PalliserKai `REL08` must reproduce the byte layout of its 36 siblings.
- De-duplication keeps the **first** occurrence of a repeated name and drops blank-named slots, matching `cs_nshm_2022/bin/dedupe_lf_stations.py`. Never average duplicates.
- Dropping a station that HF has is never a silent default. It requires `--allow-station-subset` or an explicit `--station-list`.
- `--station-list` and `--allow-station-subset` are mutually exclusive; passing both is a usage error.
- Checkpointing behaviour is preserved, not removed.
- Nothing on NeSI or Dropbox is modified until Task 8, which needs explicit user go-ahead.

**Test command (all tasks):**

```bash
cd /home/arr65/src/slurm_gm_workflow && uv run --no-project --with numpy --with pytest pytest <path> -v
```

**Import note.** The repository root carries a zero-byte `__init__.py`, so
pytest treats the repo itself as a package and sets its import root to the
parent directory, `/home/arr65/src` — which holds an unrelated project also
called `workflow` that then shadows this one. `workflow/calculation/tests/
conftest.py` (added in Task 2) puts the repository root first on `sys.path`,
which is also how `bb_sim.py` is imported at runtime. Without it every test
in Tasks 2-7 fails to collect.

---

### Task 1: Revert the unfinished in-place patch

Restores `bb_sim.py` to exactly the `cc47cbd7` content — the code that produced the 36 completed PalliserKai BB outputs. The captured patch is reverted wholesale rather than repaired: it can never reach past its own `array_equiv` abort, and its indexing is wrong in three separate places.

**Files:**
- Modify: `workflow/calculation/bb_sim.py` (restore to `cc47cbd7`)

**Interfaces:**
- Consumes: nothing
- Produces: a `bb_sim.py` identical to `cc47cbd7:workflow/calculation/bb_sim.py`, 541 lines, which every later task edits

- [ ] **Step 1: Restore the file**

```bash
cd /home/arr65/src/slurm_gm_workflow
git checkout cc47cbd7 -- workflow/calculation/bb_sim.py
```

- [ ] **Step 2: Verify it matches the base commit exactly**

```bash
cd /home/arr65/src/slurm_gm_workflow
diff <(git show cc47cbd7:workflow/calculation/bb_sim.py) workflow/calculation/bb_sim.py && echo "IDENTICAL"
wc -l workflow/calculation/bb_sim.py
```

Expected: `IDENTICAL`, and `541 workflow/calculation/bb_sim.py`.

- [ ] **Step 3: Confirm the patch's markers are gone**

```bash
cd /home/arr65/src/slurm_gm_workflow
grep -c "BEGIN PATCH\|common_names\|common_hf_indices" workflow/calculation/bb_sim.py
```

Expected: `0`.

- [ ] **Step 4: Commit**

```bash
cd /home/arr65/src/slurm_gm_workflow
git add workflow/calculation/bb_sim.py
git commit -m "Revert the unfinished in-place BB station-mismatch patch

Restores bb_sim.py to cc47cbd7, the code that produced the 36 completed
PalliserKai BB outputs on 2026-07-07.

The reverted patch could never succeed: it computed a station
intersection and then fell through to the original array_equiv abort.
Past that point it would also have been wrong - it disabled
checkpointing, sized the output file from LF while writing rows at HF
indices, and indexed vs30s/lfvs30refs (built in LF order) with HF-order
indices. Replacing it wholesale is clearer than repairing it.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 2: Station-set module — de-duplication helper

The first half of the new module: collapsing EMOD3D's boundary-duplicate records and blank-named slots down to one index per real station.

**Files:**
- Create: `workflow/calculation/bb_station_set.py`
- Test: `workflow/calculation/tests/test_bb_station_set.py`
- Create: `workflow/calculation/tests/__init__.py` (empty)

**Interfaces:**
- Consumes: nothing
- Produces:
  - `class StationSetError(Exception)`
  - `first_occurrence_indices(names) -> tuple[np.ndarray, int, int]` returning `(keep_idx, n_blank, n_duplicate)`; `keep_idx` is `int64`, ascending, one entry per distinct non-blank name, pointing at its first occurrence

- [ ] **Step 1: Write the failing test**

Create `workflow/calculation/tests/__init__.py` as an empty file, and
`workflow/calculation/tests/conftest.py` with the `sys.path` fix described
under Global Constraints:

```python
import sys
from pathlib import Path

REPO_ROOT = str(Path(__file__).resolve().parents[3])

if sys.path and sys.path[0] != REPO_ROOT:
    sys.path.insert(0, REPO_ROOT)
```

Then `workflow/calculation/tests/test_bb_station_set.py`:

```python
import numpy as np
import pytest

from workflow.calculation.bb_station_set import (
    StationSetError,
    first_occurrence_indices,
)


def test_clean_names_are_all_kept_in_order():
    names = np.array(["aaa", "bbb", "ccc"])
    keep, n_blank, n_dup = first_occurrence_indices(names)
    assert keep.tolist() == [0, 1, 2]
    assert (n_blank, n_dup) == (0, 0)


def test_duplicate_keeps_first_occurrence():
    names = np.array(["aaa", "bbb", "aaa", "ccc"])
    keep, n_blank, n_dup = first_occurrence_indices(names)
    assert keep.tolist() == [0, 1, 3]
    assert (n_blank, n_dup) == (0, 1)


def test_blank_names_are_dropped():
    names = np.array(["aaa", "", "bbb", ""])
    keep, n_blank, n_dup = first_occurrence_indices(names)
    assert keep.tolist() == [0, 2]
    assert (n_blank, n_dup) == (2, 0)


def test_blanks_and_duplicates_together():
    # The PalliserKai REL08 shape in miniature.
    names = np.array(["aaa", "bbb", "aaa", "", "bbb", "ccc"])
    keep, n_blank, n_dup = first_occurrence_indices(names)
    assert keep.tolist() == [0, 1, 5]
    assert (n_blank, n_dup) == (1, 2)


def test_returns_int64_so_it_can_index_large_arrays():
    keep, _, _ = first_occurrence_indices(np.array(["aaa"]))
    assert keep.dtype == np.int64


def test_empty_input_is_empty_output():
    keep, n_blank, n_dup = first_occurrence_indices(np.array([], dtype="<U8"))
    assert keep.tolist() == []
    assert (n_blank, n_dup) == (0, 0)
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /home/arr65/src/slurm_gm_workflow
uv run --no-project --with numpy --with pytest pytest workflow/calculation/tests/test_bb_station_set.py -v
```

Expected: collection error — `ModuleNotFoundError: No module named 'workflow.calculation.bb_station_set'`.

- [ ] **Step 3: Write minimal implementation**

Create `workflow/calculation/bb_station_set.py`:

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd /home/arr65/src/slurm_gm_workflow
uv run --no-project --with numpy --with pytest pytest workflow/calculation/tests/test_bb_station_set.py -v
```

Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
cd /home/arr65/src/slurm_gm_workflow
git add workflow/calculation/bb_station_set.py workflow/calculation/tests/
git commit -m "Add bb_station_set with LF duplicate/blank collapsing

first_occurrence_indices() collapses the EMOD3D boundary-duplicate
records and blank-named slots that inflate LF nstat, keeping the first
occurrence of each name to match the convention in
cs_nshm_2022/bin/dedupe_lf_stations.py.

The module imports neither MPI nor qcore so the station-set logic can be
tested directly.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 3: Station list file reader

Reads the canonical station list that makes WellTeast uniform across realisations.

**Files:**
- Modify: `workflow/calculation/bb_station_set.py`
- Test: `workflow/calculation/tests/test_bb_station_set.py`

**Interfaces:**
- Consumes: `StationSetError` from Task 2
- Produces: `read_station_list(path) -> list[str]` — one name per line, `#` comments and blank lines ignored, raising `StationSetError` on an empty file or a repeated name

- [ ] **Step 1: Write the failing test**

Append to `workflow/calculation/tests/test_bb_station_set.py`:

```python
from workflow.calculation.bb_station_set import read_station_list


def test_reads_one_name_per_line(tmp_path):
    p = tmp_path / "stations.txt"
    p.write_text("aaa\nbbb\nccc\n")
    assert read_station_list(p) == ["aaa", "bbb", "ccc"]


def test_ignores_comments_and_blank_lines(tmp_path):
    p = tmp_path / "stations.txt"
    p.write_text("# canonical WellTeast set\n\naaa\n  bbb  \n\nccc  # trailing\n")
    assert read_station_list(p) == ["aaa", "bbb", "ccc"]


def test_preserves_file_order_not_sorted(tmp_path):
    p = tmp_path / "stations.txt"
    p.write_text("ccc\naaa\nbbb\n")
    assert read_station_list(p) == ["ccc", "aaa", "bbb"]


def test_rejects_empty_file(tmp_path):
    p = tmp_path / "stations.txt"
    p.write_text("# nothing but a comment\n")
    with pytest.raises(StationSetError, match="no station names"):
        read_station_list(p)


def test_rejects_repeated_name(tmp_path):
    p = tmp_path / "stations.txt"
    p.write_text("aaa\nbbb\naaa\n")
    with pytest.raises(StationSetError, match="aaa"):
        read_station_list(p)
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /home/arr65/src/slurm_gm_workflow
uv run --no-project --with numpy --with pytest pytest workflow/calculation/tests/test_bb_station_set.py -v -k station_list
```

Expected: `ImportError: cannot import name 'read_station_list'`.

- [ ] **Step 3: Write minimal implementation**

Add to `workflow/calculation/bb_station_set.py` — the `Counter` import goes at the top of the file with the other imports:

```python
from collections import Counter
from pathlib import Path
```

and the function after `first_occurrence_indices`:

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd /home/arr65/src/slurm_gm_workflow
uv run --no-project --with numpy --with pytest pytest workflow/calculation/tests/test_bb_station_set.py -v
```

Expected: 11 passed.

- [ ] **Step 5: Commit**

```bash
cd /home/arr65/src/slurm_gm_workflow
git add workflow/calculation/bb_station_set.py workflow/calculation/tests/test_bb_station_set.py
git commit -m "Add read_station_list for explicit canonical station sets

One name per line, comments and blanks ignored, file order preserved
because it becomes the BB output row order. Rejects an empty file or a
repeated name rather than silently producing a degenerate run.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 4: Resolve the station set — inferred route

The default path: intersect LF and HF, in HF order, and refuse to drop anything without being told to.

**Files:**
- Modify: `workflow/calculation/bb_station_set.py`
- Test: `workflow/calculation/tests/test_bb_station_set.py`

**Interfaces:**
- Consumes: `first_occurrence_indices`, `StationSetError`
- Produces:
  - `class StationSet(NamedTuple)` with fields `names: np.ndarray`, `lf_idx: np.ndarray`, `hf_idx: np.ndarray`, `report: list[str]`
  - `resolve_station_set(lf_names, hf_names, station_list=None, allow_subset=False) -> StationSet`

- [ ] **Step 1: Write the failing test**

Append to `workflow/calculation/tests/test_bb_station_set.py`:

```python
from workflow.calculation.bb_station_set import StationSet, resolve_station_set


def test_identical_sets_resolve_unchanged_in_hf_order():
    lf = np.array(["ccc", "aaa", "bbb"])
    hf = np.array(["aaa", "bbb", "ccc"])
    result = resolve_station_set(lf, hf)
    assert isinstance(result, StationSet)
    # HF order wins, not LF order and not sorted order.
    assert result.names.tolist() == ["aaa", "bbb", "ccc"]
    assert result.lf_idx.tolist() == [1, 2, 0]
    assert result.hf_idx.tolist() == [0, 1, 2]


def test_index_arrays_round_trip_to_the_right_names():
    lf = np.array(["ccc", "aaa", "bbb"])
    hf = np.array(["aaa", "bbb", "ccc"])
    result = resolve_station_set(lf, hf)
    assert lf[result.lf_idx].tolist() == result.names.tolist()
    assert hf[result.hf_idx].tolist() == result.names.tolist()


def test_duplicate_only_mismatch_resolves_to_full_hf_set():
    # The PalliserKai REL08 shape: LF has duplicates and a blank, but once
    # collapsed it covers exactly the HF set. Nothing is lost, so no flag.
    lf = np.array(["aaa", "bbb", "aaa", "", "ccc", "bbb"])
    hf = np.array(["aaa", "bbb", "ccc"])
    result = resolve_station_set(lf, hf)
    assert result.names.tolist() == ["aaa", "bbb", "ccc"]
    assert lf[result.lf_idx].tolist() == ["aaa", "bbb", "ccc"]
    assert any("de-duplicated" in line for line in result.report)


def test_genuine_subset_aborts_by_default():
    # The WellTeast shape: HF has a station LF genuinely lacks.
    lf = np.array(["aaa", "bbb", ""])
    hf = np.array(["aaa", "bbb", "320077e"])
    with pytest.raises(StationSetError, match="320077e"):
        resolve_station_set(lf, hf)


def test_genuine_subset_proceeds_with_allow_subset_and_names_the_loss():
    lf = np.array(["aaa", "bbb", ""])
    hf = np.array(["aaa", "bbb", "320077e"])
    result = resolve_station_set(lf, hf, allow_subset=True)
    assert result.names.tolist() == ["aaa", "bbb"]
    assert any("320077e" in line for line in result.report)


def test_station_only_in_lf_also_counts_as_a_mismatch():
    lf = np.array(["aaa", "bbb", "extra"])
    hf = np.array(["aaa", "bbb"])
    with pytest.raises(StationSetError, match="extra"):
        resolve_station_set(lf, hf)


def test_no_overlap_at_all_is_always_an_error():
    lf = np.array(["aaa"])
    hf = np.array(["bbb"])
    with pytest.raises(StationSetError):
        resolve_station_set(lf, hf, allow_subset=True)
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /home/arr65/src/slurm_gm_workflow
uv run --no-project --with numpy --with pytest pytest workflow/calculation/tests/test_bb_station_set.py -v -k resolve
```

Expected: `ImportError: cannot import name 'StationSet'`.

- [ ] **Step 3: Write minimal implementation**

Add to `workflow/calculation/bb_station_set.py`:

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd /home/arr65/src/slurm_gm_workflow
uv run --no-project --with numpy --with pytest pytest workflow/calculation/tests/test_bb_station_set.py -v
```

Expected: 18 passed.

- [ ] **Step 5: Commit**

```bash
cd /home/arr65/src/slurm_gm_workflow
git add workflow/calculation/bb_station_set.py workflow/calculation/tests/test_bb_station_set.py
git commit -m "Resolve BB station set by intersecting LF and HF by name

resolve_station_set() returns one canonical ordered name array plus
lf_idx/hf_idx, so callers never have to assume LF order, HF order and
output row order coincide - conflating those is what made the previous
in-place patch wrong.

Order is HF order, never sorted, so PalliserKai REL08 reproduces the
byte layout its 36 completed siblings already have.

A duplicate-only mismatch resolves silently because nothing is lost. A
genuine difference aborts unless --allow-station-subset is given, and
the dropped stations are always named.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 5: Resolve the station set — explicit list route

Covers the route WellTeast uses. The implementation already landed in Task 4; this task proves it and pins the behaviour that guarantees uniformity across realisations.

**Files:**
- Test: `workflow/calculation/tests/test_bb_station_set.py`

**Interfaces:**
- Consumes: `resolve_station_set`, `StationSetError`
- Produces: no new code — regression cover for the explicit-list contract

- [ ] **Step 1: Write the failing test**

Append to `workflow/calculation/tests/test_bb_station_set.py`:

```python
def test_station_list_selects_exactly_those_names_in_file_order():
    lf = np.array(["aaa", "bbb", "ccc"])
    hf = np.array(["ccc", "bbb", "aaa"])
    result = resolve_station_set(lf, hf, station_list=["ccc", "aaa"])
    assert result.names.tolist() == ["ccc", "aaa"]
    assert lf[result.lf_idx].tolist() == ["ccc", "aaa"]
    assert hf[result.hf_idx].tolist() == ["ccc", "aaa"]


def test_station_list_drops_a_station_both_sides_have():
    # The WellTeast case for the six already-complete realisations: both
    # LF and HF hold 320077e, and only the explicit list removes it.
    lf = np.array(["aaa", "320077e", "bbb"])
    hf = np.array(["aaa", "320077e", "bbb"])
    result = resolve_station_set(lf, hf, station_list=["aaa", "bbb"])
    assert result.names.tolist() == ["aaa", "bbb"]
    assert "320077e" not in result.names.tolist()


def test_station_list_aborts_when_a_name_is_absent_from_lf():
    lf = np.array(["aaa", "bbb"])
    hf = np.array(["aaa", "bbb", "320077e"])
    with pytest.raises(StationSetError, match="320077e"):
        resolve_station_set(lf, hf, station_list=["aaa", "bbb", "320077e"])


def test_station_list_aborts_when_a_name_is_absent_from_hf():
    lf = np.array(["aaa", "bbb", "zzz"])
    hf = np.array(["aaa", "bbb"])
    with pytest.raises(StationSetError, match="zzz"):
        resolve_station_set(lf, hf, station_list=["aaa", "bbb", "zzz"])


def test_station_list_still_sees_through_lf_duplicates():
    lf = np.array(["aaa", "", "bbb", "aaa"])
    hf = np.array(["aaa", "bbb"])
    result = resolve_station_set(lf, hf, station_list=["bbb", "aaa"])
    assert result.names.tolist() == ["bbb", "aaa"]
    assert lf[result.lf_idx].tolist() == ["bbb", "aaa"]


def test_station_list_with_allow_subset_is_a_usage_error():
    lf = np.array(["aaa"])
    hf = np.array(["aaa"])
    with pytest.raises(StationSetError, match="mutually exclusive"):
        resolve_station_set(lf, hf, station_list=["aaa"], allow_subset=True)
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /home/arr65/src/slurm_gm_workflow
uv run --no-project --with numpy --with pytest pytest workflow/calculation/tests/test_bb_station_set.py -v -k station_list
```

Expected: these six pass already if Task 4 is correct. If any fails, fix `resolve_station_set` before continuing — do not edit the test to match the code.

- [ ] **Step 3: Confirm the whole module passes**

```bash
cd /home/arr65/src/slurm_gm_workflow
uv run --no-project --with numpy --with pytest pytest workflow/calculation/tests/test_bb_station_set.py -v
```

Expected: 24 passed.

- [ ] **Step 4: Commit**

```bash
cd /home/arr65/src/slurm_gm_workflow
git add workflow/calculation/tests/test_bb_station_set.py
git commit -m "Cover the explicit station-list contract

Pins the behaviour WellTeast depends on: an explicit list can remove a
station both LF and HF hold, which no intersection would ever drop, and
that is the only way to guarantee every realisation of a fault shares
one station list.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 6: Wire the resolver into bb_sim.py

Replaces the two aborting checks with the resolver, and re-keys every downstream quantity to the canonical sequence.

**Files:**
- Modify: `workflow/calculation/bb_sim.py` (lines are for the `cc47cbd7` file restored in Task 1)
- Test: `workflow/calculation/tests/test_bb_sim_indexing.py`

**Interfaces:**
- Consumes: `resolve_station_set`, `read_station_list`, `StationSetError`, `StationSet`
- Produces: `bb_sim.py` accepting `--station-list FILE` and `--allow-station-subset`

- [ ] **Step 1: Write the failing test**

Create `workflow/calculation/tests/test_bb_sim_indexing.py`. This is a source-level guard: the bug class that broke the previous patch was sizing the output from one station array while indexing it with another, so the test asserts those attributes are gone from the file entirely.

```python
"""Guards against the indexing bug class that broke the previous patch.

bb_sim.py cannot be imported here - it pulls in mpi4py and qcore - so
these read the source. Crude, but they catch exactly the mistake that
made the captured in-place patch wrong: sizing the output from
lf.stations while writing rows at hf.stations indices.
"""

from pathlib import Path

import pytest

BB_SIM = Path(__file__).resolve().parents[1] / "bb_sim.py"


@pytest.fixture(scope="module")
def source() -> str:
    return BB_SIM.read_text()


@pytest.mark.parametrize(
    "forbidden",
    ["lf.stations.size", "hf.stations.size", "lf.nstat", "hf.nstat"],
)
def test_output_layout_never_keys_off_a_raw_station_array(source, forbidden):
    assert forbidden not in source, (
        f"{forbidden} still drives part of bb_sim.py. Every quantity must be "
        "keyed by the canonical station set (n_bb / lf_idx / hf_idx) instead."
    )


def test_resolver_is_used(source):
    assert "resolve_station_set" in source
    assert "from workflow.calculation.bb_station_set import" in source


def test_new_flags_are_exposed(source):
    assert '"--station-list"' in source
    assert '"--allow-station-subset"' in source


def test_checkpointing_is_still_present(source):
    # The previous patch removed this. Runs of this size need resume.
    assert "def unfinished(" in source
    assert "Checkpoints found." in source


def test_old_aborting_checks_are_gone(source):
    assert "LF nstat != HF nstat" not in source
    assert "LF and HF were run with different station files" not in source
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /home/arr65/src/slurm_gm_workflow
uv run --no-project --with numpy --with pytest pytest workflow/calculation/tests/test_bb_sim_indexing.py -v
```

Expected: 7 failed, 1 passed — the forbidden attributes are all still present and the resolver is not wired in; the checkpointing guard already holds.

- [ ] **Step 3: Add the import**

In `workflow/calculation/bb_sim.py`, after line 22 (`from workflow.automation import platform_config`), add:

```python
from workflow.calculation.bb_station_set import (
    StationSetError,
    read_station_list,
    resolve_station_set,
)
```

- [ ] **Step 4: Add the two new arguments**

In `args_parser`, after the `--site-amp-uncertainty` block (ends line 84, before `args = parser.parse_args(cmd)`), add:

```python
    arg(
        "--station-list",
        help="File of station names, one per line, giving the exact set and "
        "order to produce. Every name must be present in both LF and HF. "
        "Use this to keep a station list identical across realisations.",
    )
    arg(
        "--allow-station-subset",
        help="Proceed when LF and HF cover different stations, using only "
        "those common to both. Without this, a difference is an error.",
        action="store_true",
    )
```

- [ ] **Step 5: Resolve the station set and replace the old checks**

Replace lines 141-164 — from `# load data stores` through the end of the `array_equiv` and duration checks — with:

```python
    # load data stores
    lf = timeseries.LFSeis(args.lf_dir)
    hf = timeseries.HFSeis(args.hf_file)

    # Decide which stations this run produces. LF may carry EMOD3D
    # boundary-duplicate records and blank-named slots, and LF and HF are
    # not guaranteed to cover the same stations.
    try:
        stations = resolve_station_set(
            lf.stations.name,
            hf.stations.name,
            station_list=(
                read_station_list(args.station_list) if args.station_list else None
            ),
            allow_subset=args.allow_station_subset,
        )
    except StationSetError as error:
        if is_master:
            logger.error(str(error))
        comm.Abort()

    bb_names = stations.names
    lf_idx = stations.lf_idx
    hf_idx = stations.hf_idx
    n_bb = len(bb_names)

    # compatibility validation
    # abort if behaviour is undefined
    if is_master:
        logger.debug("=" * 50)
        for line in stations.report:
            logger.info(line)
        if not np.isclose(
            lf.dt * lf.nt + lf.start_sec, hf.dt * hf.nt, atol=min(lf.dt, hf.dt)
        ):
            logger.error(
                "LF duration != HF duration. {} vs {}".format(
                    lf.dt * lf.nt + lf.start_sec, hf.dt * hf.nt
                )
            )
            comm.Abort()
```

- [ ] **Step 6: Re-key the output layout**

Replace the two `head_total`/`file_size` lines (originally 213-214):

```python
    head_total = HEAD_SIZE + n_bb * HEAD_STAT
    file_size = head_total + n_bb * bb_nt * N_COMP * FLOAT_SIZE
```

In the `lfvs30refs` block (originally 229-242), change the memmap subscript and the fixed-value branch:

```python
            )[lf.stations.y[lf_idx], 0, lf.stations.x[lf_idx]]
```

```python
        lfvs30refs = np.ones(n_bb, dtype=np.float32) * args.lfvsref
```

In the `vs30s` block (originally 249-258), change the array the lookup is applied to:

```python
        )(bb_names)
```

- [ ] **Step 7: Re-key the station header written by initialise()**

Inside `initialise` (originally 268-332), change the count, the station array length, and the three columns taken from LF and HF:

```python
            i = np.array([n_bb, bb_nt], dtype="i4")
```

```python
            bb_stations = np.rec.array(
                np.zeros(
                    n_bb,
```

```python
            # copy most from LF, addressed through the canonical set
            for col in bb_stations.dtype.names[:-3]:
                bb_stations[col] = lf.stations[col][lf_idx]
            # add e_dist and hf_vs_ref from HF, same stations, HF's own order
            bb_stations.e_dist = hf.stations.e_dist[hf_idx]
            bb_stations.hf_vs_ref = hf.stations.vs[hf_idx]
```

- [ ] **Step 8: Re-key checkpointing and work distribution**

In `unfinished` (originally 334-368), change the checkpoint read count:

```python
                        count=n_bb,
```

In the `station_mask` block (originally 370-394), replace every `lf.stations.size` with `n_bb`, and replace the two `stations_todo` lines with a single index array:

```python
    station_mask = None
    if is_master:
        station_mask = unfinished()
        if station_mask is None or sum(station_mask) == n_bb:
            logger.debug("No valid checkpoints found. Starting fresh simulation.")
            initialise()
            station_mask = np.ones(n_bb, dtype=bool)
        else:
            try:
                initialise(check_only=True)
                logger.info(
                    "{} of {} stations completed. Resuming simulation.".format(
                        n_bb - sum(station_mask), n_bb
                    )
                )

            except AssertionError:
                logger.warning(
                    "Simulation parameters mismatch. Starting fresh simulation."
                )
                initialise()
                station_mask = np.ones(n_bb, dtype=bool)
    station_mask = comm.bcast(station_mask, root=master)
    # Output row indices this rank owns. Everything below is keyed by these.
    stations_todo_idx = np.arange(n_bb)[station_mask][rank::size]
```

- [ ] **Step 9: Re-key the work loop**

Replace the loop header (originally 406-412) so it walks output rows and takes the station record from HF via `hf_idx`:

```python
    for i, k in enumerate(stations_todo_idx):
        stat_name = str(bb_names[k])
        stat = hf.stations[hf_idx[k]]
        logger.debug(
            f"Working on {stat_name}, {100*i/len(stations_todo_idx):.2f}% complete"
        )
        lf_acc = np.copy(lf.acc(stat_name, dt=bb_dt))
        hf_acc = np.copy(hf.acc(stat_name, dt=bb_dt))
        station_yaml = os.path.join(str(args.site_response_dir), f"{stat_name}.yaml")
```

Inside the loop, replace every `stations_todo_idx[i]` subscript with `k`, and the `lf_seed` offset:

```python
                    vs30s[k],
```
```python
                    lfvs30refs[k],
                    vs30s[k],
```
```python
                        hf_seed = args.site_amp_uncertainty + k
                        lf_seed = hf_seed + n_bb
```
```python
        vs30s[k].tofile(bin_data)
```

Also replace the two remaining `stations_todo` references — `f"Station {stat.name} has a site specific file..."` and the two later `stat.name` uses inside the site-response branch become `stat_name`, and the final log line (originally 530-534) becomes:

```python
    logger.debug(
        "Process {} of {} completed {} stations ({:.2f}).".format(
            rank, size, len(stations_todo_idx), MPI.Wtime() - t0
        )
    )
```

- [ ] **Step 10: Run the guard tests**

```bash
cd /home/arr65/src/slurm_gm_workflow
uv run --no-project --with numpy --with pytest pytest workflow/calculation/tests/ -v
```

Expected: 32 passed (24 from the station-set module, 8 guards).

- [ ] **Step 11: Check the file is syntactically valid and has no stale references**

```bash
cd /home/arr65/src/slurm_gm_workflow
python3 -m py_compile workflow/calculation/bb_sim.py && echo "COMPILES"
grep -n "stations_todo\b\|lf.stations.size\|hf.stations.size\|lf.nstat\|hf.nstat" workflow/calculation/bb_sim.py
```

Expected: `COMPILES`, and the grep returns nothing.

- [ ] **Step 12: Commit**

```bash
cd /home/arr65/src/slurm_gm_workflow
git add workflow/calculation/bb_sim.py workflow/calculation/tests/test_bb_sim_indexing.py
git commit -m "bb_sim: reconcile LF and HF station sets by name

Replaces the nstat and array_equiv aborts with resolve_station_set, and
keys every downstream quantity off the canonical sequence it returns:
file size, station header, vs30s, lfvs30refs, the work mask, the
site-amp uncertainty seeds and the output byte offsets. Waveform access
stays name-based, which LFSeis resolves through a name dictionary and
is therefore unaffected by duplicate records.

Adds --station-list to state the intended set explicitly, and
--allow-station-subset to continue on the LF/HF intersection. Without
one of them a station-set difference is still an error.

Checkpointing is retained: the output file is now sized and ordered by
the canonical set, so the existing vsite scheme stays self-consistent.

test_bb_sim_indexing.py guards the bug class this replaces, asserting
no raw lf/hf station array drives the output layout.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 7: BB output comparison tool

Before anything is trusted, `REL08` has to be shown to match its completed siblings. This is the tool that shows it.

**Files:**
- Create: `workflow/calculation/verification/compare_bb_stations.py`
- Test: `workflow/calculation/tests/test_compare_bb_stations.py`

**Interfaces:**
- Consumes: nothing from earlier tasks
- Produces: `read_bb_header(path) -> BBHeader` with fields `nstat: int`, `nt: int`, `names: list[str]`; CLI `compare_bb_stations.py A.bin B.bin` exiting 0 when the two agree, 1 otherwise

- [ ] **Step 1: Write the failing test**

Create `workflow/calculation/tests/test_compare_bb_stations.py`:

```python
import struct

import numpy as np
import pytest

from workflow.calculation.verification.compare_bb_stations import (
    HEAD_SIZE,
    STATION_DTYPE,
    compare,
    read_bb_header,
)


def write_bb(path, names, nt=100):
    """Write just enough of a real BB.bin for the header reader."""
    stations = np.zeros(len(names), dtype=STATION_DTYPE)
    stations["name"] = [n.encode() for n in names]
    stations["vsite"] = 1.0
    with open(path, "wb") as f:
        f.write(struct.pack("<ii", len(names), nt))
        f.write(b"\0" * (HEAD_SIZE - 8))
        stations.tofile(f)
    return path


def test_reads_nstat_nt_and_names(tmp_path):
    p = write_bb(tmp_path / "BB.bin", ["aaa", "bbb", "ccc"], nt=42)
    header = read_bb_header(p)
    assert header.nstat == 3
    assert header.nt == 42
    assert header.names == ["aaa", "bbb", "ccc"]


def test_identical_files_compare_equal(tmp_path):
    a = write_bb(tmp_path / "a.bin", ["aaa", "bbb"])
    b = write_bb(tmp_path / "b.bin", ["aaa", "bbb"])
    ok, differences = compare(a, b)
    assert ok
    assert differences == []


def test_different_station_count_is_reported(tmp_path):
    a = write_bb(tmp_path / "a.bin", ["aaa", "bbb"])
    b = write_bb(tmp_path / "b.bin", ["aaa"])
    ok, differences = compare(a, b)
    assert not ok
    assert any("nstat" in d for d in differences)


def test_different_station_order_is_reported(tmp_path):
    a = write_bb(tmp_path / "a.bin", ["aaa", "bbb"])
    b = write_bb(tmp_path / "b.bin", ["bbb", "aaa"])
    ok, differences = compare(a, b)
    assert not ok
    assert any("order" in d for d in differences)


def test_different_nt_is_reported(tmp_path):
    a = write_bb(tmp_path / "a.bin", ["aaa"], nt=100)
    b = write_bb(tmp_path / "b.bin", ["aaa"], nt=200)
    ok, differences = compare(a, b)
    assert not ok
    assert any("nt" in d for d in differences)
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /home/arr65/src/slurm_gm_workflow
uv run --no-project --with numpy --with pytest pytest workflow/calculation/tests/test_compare_bb_stations.py -v
```

Expected: `ModuleNotFoundError: No module named 'workflow.calculation.verification.compare_bb_stations'`.

- [ ] **Step 3: Write minimal implementation**

Create `workflow/calculation/verification/compare_bb_stations.py`:

```python
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
        first = next(
            i for i, (x, y) in enumerate(zip(a.names, b.names)) if x != y
        )
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
```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd /home/arr65/src/slurm_gm_workflow
uv run --no-project --with numpy --with pytest pytest workflow/calculation/tests/test_compare_bb_stations.py -v
```

Expected: 5 passed.

- [ ] **Step 5: Run the full suite**

```bash
cd /home/arr65/src/slurm_gm_workflow
uv run --no-project --with numpy --with pytest pytest workflow/calculation/tests/ -v
```

Expected: 37 passed.

- [ ] **Step 6: Commit**

```bash
cd /home/arr65/src/slurm_gm_workflow
git add workflow/calculation/verification/compare_bb_stations.py workflow/calculation/tests/test_compare_bb_stations.py
git commit -m "Add compare_bb_stations for checking a run against its siblings

Reads only the BB.bin header, so it is cheap on multi-gigabyte outputs,
and reports station count, timestep count, set differences and order
differences separately. This is what establishes that PalliserKai REL08
came out consistent with the 36 realisations already finished.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 8: Push the branch and deploy to NeSI

First task that touches anything outside this repository. **Needs explicit user go-ahead before starting.**

**Files:**
- None in this repository

**Interfaces:**
- Consumes: the finished branch from Tasks 1-7
- Produces: `/nesi/project/nesi00213/Environments/arr65_v26p6/workflow` — a clean checkout at a known commit

- [ ] **Step 1: Confirm the branch is clean and tests pass**

```bash
cd /home/arr65/src/slurm_gm_workflow
git status --porcelain && echo "(clean)"
uv run --no-project --with numpy --with pytest pytest workflow/calculation/tests/ -q
git log --oneline cc47cbd7..HEAD
```

Expected: clean tree, all tests passing, six commits listed.

- [ ] **Step 2: Push the branch**

```bash
cd /home/arr65/src/slurm_gm_workflow
git push -u origin nesi-cybershake-v26p6
```

- [ ] **Step 3: Record the deployed commit**

```bash
cd /home/arr65/src/slurm_gm_workflow
git rev-parse HEAD
```

Note the hash; it goes in the run notes.

- [ ] **Step 4: Clone it on NeSI into a new environment directory**

`mrd87_4` is left untouched so the old state stays available as evidence.

```bash
ssh nesi 'git clone --branch nesi-cybershake-v26p6 \
  git@github.com:ucgmsim/slurm_gm_workflow.git \
  /nesi/project/nesi00213/Environments/arr65_v26p6/workflow'
```

- [ ] **Step 5: Verify the deployment is clean and at the right commit**

```bash
ssh nesi 'E=/nesi/project/nesi00213/Environments/arr65_v26p6/workflow;
  git -C $E rev-parse HEAD;
  git -C $E rev-parse --abbrev-ref HEAD;
  git -C $E status --porcelain && echo "(clean tree)"'
```

Expected: the hash from Step 3, branch `nesi-cybershake-v26p6`, and a clean tree. This is the point of the whole exercise — production now has a hash.

- [ ] **Step 6: Confirm bb_sim.py imports under the NeSI runtime**

```bash
ssh nesi 'E=/nesi/project/nesi00213/Environments/arr65_v26p6/workflow;
  PYTHONPATH=$E /nesi/project/nesi00213/Environments/mrd87_4/py311/bin/python -c "
from workflow.calculation.bb_station_set import resolve_station_set
import numpy as np
r = resolve_station_set(np.array([\"a\",\"b\",\"a\",\"\"]), np.array([\"a\",\"b\"]))
print(\"resolver OK:\", r.names.tolist(), r.lf_idx.tolist(), r.hf_idx.tolist())
"'
```

Expected: `resolver OK: ['a', 'b'] [0, 1] [0, 1]`.

---

### Task 9: Run and verify PalliserKai REL08

**Needs explicit user go-ahead.** Writes one new `BB.bin`; overwrites nothing.

**Files:**
- None in this repository

**Interfaces:**
- Consumes: the deployment from Task 8, `compare_bb_stations.py` from Task 7
- Produces: `.../PalliserKai/PalliserKai_REL08/BB/Acc/BB.bin`

- [ ] **Step 1: Confirm nothing is there to overwrite**

```bash
ssh nesi 'ls -la /nesi/nobackup/nesi00213/RunFolder/Cybershake/v26p6/Runs/PalliserKai/PalliserKai_REL08/BB/Acc/'
```

Expected: `BB.log` only, no `BB.bin`. Stop if a `BB.bin` exists.

- [ ] **Step 2: Submit the run**

Same settings Sung used for the 36 completed realisations — `--flo 1.0 --fmin 0.5 --fmidbot 1.0 --dt 0.005 --no-lf-amp` — and no new flags, because REL08's LF/HF sets match once duplicates collapse.

Write the submission script on NeSI at `/home/arr65/run_bb_rel08.sl`:

```bash
#!/bin/bash
#SBATCH --job-name=bb_PalliserKai_REL08
#SBATCH --account=nesi00213
#SBATCH --ntasks=80
#SBATCH --mem-per-cpu=3G
#SBATCH --time=06:00:00
#SBATCH --output=/home/arr65/bb_rel08_%j.out
#SBATCH --error=/home/arr65/bb_rel08_%j.err

E=/nesi/project/nesi00213/Environments/arr65_v26p6/workflow
R=/nesi/nobackup/nesi00213/RunFolder/Cybershake/v26p6/Runs/PalliserKai/PalliserKai_REL08
V=/nesi/nobackup/nesi00213/RunFolder/Cybershake/v26p6/Data/VMs/PalliserKai

mkdir -p $R/BB/Acc
export PYTHONPATH=$E
srun /nesi/project/nesi00213/Environments/mrd87_4/py311/bin/python \
  $E/workflow/calculation/bb_sim.py \
  $R/LF/OutBin \
  $V \
  $R/HF/Acc/HF.bin \
  /nesi/project/nesi00213/StationInfo/non_uniform_whole_nz_with_real_stations-hh400_v20p3_land.vs30 \
  $R/BB/Acc/BB.bin \
  --flo 1.0 --fmin 0.5 --fmidbot 1.0 --dt 0.005 --no-lf-amp
```

Then: `ssh nesi 'sbatch /home/arr65/run_bb_rel08.sl'`

- [ ] **Step 3: Check the log shows the de-duplication and no abort**

```bash
ssh nesi 'tail -30 /nesi/nobackup/nesi00213/RunFolder/Cybershake/v26p6/Runs/PalliserKai/PalliserKai_REL08/BB/Acc/BB.log'
```

Expected: a line reading `LF de-duplicated: 27804 records -> 17760 stations (10043 duplicate, 1 blank-named).`, then `LF and HF station sets match: 17760 stations.`, and no `ERROR`.

- [ ] **Step 4: Verify against a completed sibling**

```bash
ssh nesi 'E=/nesi/project/nesi00213/Environments/arr65_v26p6/workflow;
  R=/nesi/nobackup/nesi00213/RunFolder/Cybershake/v26p6/Runs/PalliserKai;
  PYTHONPATH=$E /nesi/project/nesi00213/Environments/mrd87_4/py311/bin/python \
    $E/workflow/calculation/verification/compare_bb_stations.py \
    $R/PalliserKai_REL01/BB/Acc/BB.bin \
    $R/PalliserKai_REL08/BB/Acc/BB.bin'
```

Expected: `MATCH: same stations, same order, same nt.` with `nstat=17760`.

- [ ] **Step 5: Confirm the file size matches its siblings exactly**

```bash
ssh nesi 'R=/nesi/nobackup/nesi00213/RunFolder/Cybershake/v26p6/Runs/PalliserKai;
  stat -c "%s %n" $R/PalliserKai_REL01/BB/Acc/BB.bin $R/PalliserKai_REL08/BB/Acc/BB.bin'
```

Expected: both `12597666560`.

---

## Remaining work, not planned here

WellTeast needs its own plan once PalliserKai is verified. It depends on decisions that are made but not yet actioned, and on two facts not yet established:

- Generating and archiving the canonical 18431-name list.
- Moving the six existing outputs to `BB.bin.with_320077e` — needs ~79 GB of `nobackup` headroom confirmed first.
- Running all 31 available realisations against the list.
- `WellTeast_REL04` has no LF output at all and needs a separate decision.
- Any IM results already derived from those six were computed over 18432 stations and go stale.
