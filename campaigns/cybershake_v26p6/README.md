# Cybershake v26p6: finishing PalliserKai and WellTeast on NeSI

These are the inputs and scripts used to finish the BB and IM stages that Sung
started. Each file is kept verbatim as it ran. The design and its decisions
are in `docs/superpowers/specs/2026-09-21-bb-station-reconciliation-design.md`.

Runs directory: `/nesi/nobackup/nesi00213/RunFolder/Cybershake/v26p6/Runs`.

## WellTeast BB (canonical 18431-station set)

| File | Role |
|---|---|
| `wellteast_bb_stations_18431.txt` | The canonical station list passed to `bb_sim.py --station-list`, md5 `b4a51ad3b0380950c5db6d569a3ed23a`. It is the HF station order with `320077e` removed. That order is identical in all 33 WellTeast HF headers and matches `fd_rt01-h0.100.ll`. On NeSI it lives at `/nesi/project/nesi00213/Environments/arr65_v26p6/`. |
| `make_wellteast_station_list.py` | Generated the list. |
| `preflight_wellteast_bb.py` | Read-only check run before submission. It resolves every target's station set with the same function and inputs as `bb_sim.py`, checks the result is the list, checks HF finished every listed station, and checks the vs30 file covers the list. All 32 passed on 2026-09-25. |
| `wellteast_bb_targets.txt` | The 32 targets: the median `WellTeast` plus `REL01`–`REL32`, minus `REL04`, which has no LF output. |
| `run_bb_wellteast.sl` | The array job script: job 9310308, `nesi00213`, code `2c6a79a8`. |
| `verify_wellteast_bb.py` | Read-only check of the outputs. It checks size and names, and every station record against its LF, HF and vs30 source by name. It checks waveform rows by correlation with LF, including the rows either side of `320077e`, and compares the six re-runs with their originals. Self-tested before any output existed: REL02's original passes against its own inputs (records identical, 18/18 waveforms bit-identical, LF r ≥ 0.9956 vs control ≤ 0.09). REL08's original presented as REL02 fails (LF r down to −0.46, 0/18 identical). |

`320077e` is dropped from every realisation, so every station in the set is
present in every realisation. The six realisations that had finished with it
(`REL02`, `REL08`, `REL14`, `REL16`, `REL19`, `REL32`) are re-run. Their
original 18432-station outputs were renamed, not deleted, to
`BB/Acc/BB.bin.with_320077e` and `BB/Acc/BB.log.with_320077e`. Sizes and
mtimes before and after the rename are recorded in
`/home/arr65/bb_v26p6/moved_aside_manifest.txt` on NeSI.

## IM (both faults)

IMs are computed with Sung's wrapper
`/nesi/nobackup/nesi00213/RunFolder/submit/run_im_job_array.sl`, unmodified,
using `gmsim=/nesi/project/nesi00213/Environments/sarah2024`. Every submit and
cancel is logged in `/home/arr65/im_v26p6/submissions.txt`.

| File | Role |
|---|---|
| `verify_ims.py` | Per-realisation checks on the final CSV, plus an outlier check across realisations. |
| `check_station_files.py` | Run it before resuming any interrupted IM job. `calculate_ims_mpi.py` writes per-station files non-atomically and, on resume, trusts any non-empty file. This finds truncated files and quarantines them (never deletes). |
