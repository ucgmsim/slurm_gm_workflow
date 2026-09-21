# BB station reconciliation for Cybershake v26p6

Design document. Written 2026-09-21, branch `nesi-cybershake-v26p6`.

## Problem

`bb_sim.py` combines low-frequency (LF) and high-frequency (HF) seismograms.
It assumes the two were run over an identical station list, in identical
order, and aborts otherwise:

```python
if not lf.nstat == hf.nstat:            comm.Abort()
if not np.array_equiv(lf.stations.name, hf.stations.name):  comm.Abort()
```

Two Cybershake v26p6 faults on NeSI violate that assumption and cannot
finish their BB stage:

- **PalliserKai** — 36 of 37 realisations complete. `REL08` reports
  `LF nstat != HF nstat. 27804 vs 17760`.
- **WellTeast** — 6 of 32 complete. The other 26 report
  `LF nstat != HF nstat. 18433 vs 18432`.

## What the investigation found

### Code provenance

Production ran from `/nesi/project/nesi00213/Environments/mrd87_4/workflow`:
a checkout of branch `mrd87` at `cc47cbd7` (2023-11-06) with twelve tracked
files modified and never committed. The running code matched no commit in
this repository. Commit `ef0584f2` on this branch captures that tree
verbatim so the rest of this work is a reviewable diff.

The captured `bb_sim.py` contains an unfinished attempt at this same
problem. It computes an intersection, then leaves the `array_equiv` abort
in place, so it can never succeed — which is exactly what the WellTeast
logs show:

```
WARNING: Station count mismatch: LF=18433, HF=18432. Using 18431 common stations.
DEBUG:   Using 18431 common stations.
ERROR:   LF and HF were run with different station files      <- aborts here
```

Had it got past that line it would still have been wrong: it disables
checkpointing entirely, sizes the output file from LF but writes rows at
HF indices, and indexes `vs30s`/`lfvs30refs` (built in LF order) with
HF-order indices. It is reverted in full rather than repaired.

**Timeline.** The 36 completed PalliserKai BB outputs were produced
2026-07-07; the patch above has mtime 2026-07-09, and none of its log
messages appear in those 36 runs. The completed realisations therefore came
from the clean `cc47cbd7` `bb_sim.py`. This is why the work branches from
`cc47cbd7` rather than from `master`, which is roughly two years ahead: the
remaining realisations must be comparable with the ones already finished.

### The data

EMOD3D writes a station twice when it sits on the boundary between two MPI
domains — both domains record it from their own local view of the grid. The
two copies are the same signal at slightly different amplitude. The
established convention (`cs_nshm_2022/bin/dedupe_lf_stations.py`) is to keep
the first occurrence and never average. The same mechanism can also leave a
boundary station written by *neither* domain, appearing as a blank-named
slot.

Measured across every realisation of both faults:

| | PalliserKai `REL08` | WellTeast (25 of 26) | WellTeast `REL04` |
|---|---|---|---|
| LF records | 27804 | 18433 | none |
| duplicates | 10043 | 1 | — |
| blank-named slots | 1 | 1 | — |
| real LF stations | 17760 | 18431 | — |
| HF stations | 17760 | 18432 | 18432 |
| LF ∩ HF | **17760** | **18431** | — |
| in HF but not LF | none | `320077e` | — |

- **PalliserKai `REL08`** is purely the duplicate artefact. Once duplicates
  and the blank are collapsed, its station set is *exactly* the 17760 that
  the other 36 realisations already used. Nothing is lost and no policy
  decision is required.
- **WellTeast** is different. All 25 affected realisations are missing the
  same station, `320077e` (174.29567, −39.44569), which is present in the
  fault station list and in HF. It is genuinely absent from LF — a real
  reduction, not an artefact.
- **WellTeast `REL04`** has no LF output at all and is out of scope here.

### Decision taken

For WellTeast, the canonical station list is made **uniform at 18431 across
all 32 realisations**: `320077e` is dropped everywhere, including from the
six realisations already finished, which are re-run. Every site in the
canonical set is then sampled by every realisation.

The six existing 18432-station outputs are **kept, not overwritten**. They
are the only BB results that contain `320077e`, so they are preserved
alongside the canonical ones for anyone who later wants to look at that
site. Following the convention in `dedupe_lf_stations.py`, the original is
moved aside rather than deleted:

    BB/Acc/BB.bin  ->  BB/Acc/BB.bin.with_320077e

and the new canonical 18431-station output is written at the standard
`BB/Acc/BB.bin` path, so the IM and upload stages need no changes.

This is a user decision recorded here, not a technical conclusion.

**Consequence for the design.** In those six realisations `320077e` is
present in *both* LF and HF, so an intersection would keep it. Uniformity
cannot be inferred from the data — it requires an explicit canonical
station list supplied to the run.

## Design

### Station set resolution

Two changes to `bb_sim.py`, both before the existing validation block.

**1. De-duplicate LF, always.** Drop blank-named slots, and for repeated
names keep the first occurrence. Matches the convention in
`dedupe_lf_stations.py`. Logged with counts. This alone resolves
PalliserKai.

**2. Resolve the BB station set,** by one of two routes:

| Route | Rule |
|---|---|
| default | `bb_names` = LF ∩ HF, in HF order. If that is smaller than either side, **abort** unless `--allow-station-subset` is given. |
| `--station-list FILE` | `bb_names` = exactly the names in `FILE`, in file order. Abort if any is absent from LF or HF. |

Abort-by-default matters: silently dropping a station HF has is a data
reduction and must be a recorded choice, not a library default. The dropped
names are always logged individually.

`--station-list` takes one station name per line; blank lines and `#`
comments ignored. It is what guarantees WellTeast uniformity, and it is an
archivable artefact of the campaign.

The two routes are mutually exclusive: `--allow-station-subset` applies
only to the inferred route and is ignored with `--station-list`, where an
absent name is always an abort. Passing both is a usage error.

Ordering is HF order (or file order), never sorted. PalliserKai `REL08`
therefore reproduces the byte layout its 36 siblings already have.

### Indexing

The existing code is correct only because LF order, HF order and output row
order coincide. They no longer do, and conflating them is what made the
captured patch wrong. The fix introduces one canonical sequence and derives
everything from it:

```
bb_names[k]    canonical station name for output row k,  k in [0, n_bb)
lf_idx[k]      its index in lf.stations
hf_idx[k]      its index in hf.stations
```

Every downstream quantity is then keyed by `k`:

| Quantity | Before | After |
|---|---|---|
| `head_total`, `file_size` | `lf.stations.size` | `n_bb` |
| `bb_stations` geometry | `lf.stations[col]` | `lf.stations[col][lf_idx]` |
| `bb_stations.e_dist`, `.hf_vs_ref` | `hf.stations.*` | `hf.stations.*[hf_idx]` |
| `lfvs30refs` | LF order | indexed via `lf_idx` |
| `vs30s` | `lf.stations.name` | `bb_names` |
| `station_mask` | `lf.stations.size` | `n_bb` |
| `stations_todo_idx` | `arange(hf.stations.size)[mask]` | `arange(n_bb)[mask]` |
| per-station `stat.vs` | `hf.stations[...]` | `hf.stations[hf_idx[k]]` |
| output byte offset | from HF index | from `k` |

Waveform access stays name-based — `lf.acc(name)` / `hf.acc(name)` — which
`LFSeis` resolves through a name dictionary, so it is unaffected by
duplicate records.

### Checkpointing

Retained unchanged in behaviour. Because the output file is now sized and
ordered by `bb_names`, the existing vsite-based checkpoint scheme stays
self-consistent and resume still works. The captured patch discarded this;
for runs of this size that is a real loss and it is not repeated.

### Out of scope

- `WellTeast_REL04` (no LF output).
- Porting the fix to `master`; that is a separate PR once this campaign is
  finished.
- Any change to LF, HF, or IM stages.

## Testing

Unit tests over the station-resolution logic, which is extracted into pure
functions taking name arrays so it can be tested without MPI or real
seismogram files:

- de-duplication keeps first occurrence, drops blanks, preserves order
- identical LF/HF sets resolve unchanged, and to HF order
- duplicate-only mismatch (PalliserKai shape) resolves to the full HF set
- genuine subset (WellTeast shape) aborts by default, proceeds with
  `--allow-station-subset`, and names the dropped station
- `--station-list` selects exactly the listed names in file order
- `--station-list` aborts when a listed name is absent from LF or HF
- index arrays `lf_idx` / `hf_idx` round-trip to the right names

Then, before any production submission, an end-to-end check on PalliserKai
`REL08` against a completed sibling: same `nstat`, same `nt`, same station
names in the same order, same file size.

## Rollout

1. `ef0584f2` — capture deployed state (done).
2. Revert the unfinished in-place patch, restoring `cc47cbd7` `bb_sim.py`.
3. Implement the fix with tests.
4. Push `nesi-cybershake-v26p6` to `origin`.
5. Deploy as a **fresh clone at that commit** into a new environment
   directory, leaving `mrd87_4` untouched as evidence.
6. PalliserKai `REL08`: run with Sung's exact settings
   (`--flo 1.0 --fmin 0.5 --fmidbot 1.0 --dt 0.005 --no-lf-amp`), no new
   flags. Verify against a sibling.
7. WellTeast: generate and archive the 18431-name list, then run all 31
   available realisations against it. For the six already complete, move
   the existing output to `BB.bin.with_320077e` first — never delete it.

Step 7 needs its own go-ahead. Two things to settle before it runs:

- **Disk.** Keeping the six originals costs roughly 79 GB on `nobackup`
  (~13.2 GB each) on top of the new outputs. Confirm headroom first.
- **Downstream.** Any IM results already derived from those six were
  computed over 18432 stations and will no longer match the canonical BB.
  They need the same treatment — preserved and recomputed — but that is
  the IM stage, not this change.
