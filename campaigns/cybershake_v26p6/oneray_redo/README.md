# OneRay redo of PalliserKai and WellTeast HF, BB and IM (v26p6)

All 71 original HF runs used `Cant1D_v2-midQ_leer.1d`, `hf_sim.py`'s default,
instead of the configured `Cant1D_v3-midQ_OneRay.1d`
(`../hf_1d_model_fallback.md`). This redo repeats HF with OneRay, then BB and
IM from it:
- PalliserKai: the median and REL01–REL37, i.e. 38 of each.
- WellTeast: HF for the median and REL01–REL32 (33). BB and IM for 32 of them,
  because REL04 has no LF.

The user chose the redo on 2026-09-27. The leer results are to be deleted once
the redo is verified, so no caveat has to travel with the data.

On NeSI these files live in `/home/arr65/v26p6_oneray_redo/`.

| File | Role |
|---|---|
| `make_hf_commands.py` | Writes `targets.txt` and `hf_commands.txt`, one line per realisation (array index = line − 1; index 42 is WellTeast_REL04). |
| `targets.txt` | The 71 realisation directories, md5 `53cec4ad8dda9a30d4ba530db60bdfe7`. |
| `hf_commands.txt` | The 71 HF commands, md5 `e87500b62b19d86b846eae0ca468a6b0`. |
| `preflight_hf.py` | Read-only check of every command against the original run (details below). |
| `pilot_hf.py` | One-station pilots compared with the originals (results below). |
| `move_aside.py` | Renames each realisation's leer outputs to `*.leer`. Dry run unless `--apply`. |
| `run_hf_redo.sl` | HF array 0–70. |
| `run_bb_redo.sl` | BB array 0–41,43–70; each task waits for its HF task. |
| `submit_redo.sh` | Re-runs the pre-flight and the HF guards, then submits HF, BB and IM (Sung's `run_im_job_array.sl`), each task waiting for its predecessor (`aftercorr`). |

## How the commands were built

Sung's generator can't be rerun on the v26p6 tree, which has the stoch files but
not the SRFs. Each command is built instead from the original run, so it
reproduces that run except for the 1D model:
- **Seed and duration:** from the original `HF.bin` header.
- **Settings:** dt, version, sdrop, kappa, rvfac, rayset (1) and path_dur, from
  v26p6's `root_params.yaml`. That file also names OneRay.
- **Inputs:** the fault's `FD_STATLIST` and the stoch file that
  `sim_params.yaml` names (v26p6's copy).
- **1D model and binary:** named explicitly, as for HikWgtnmax.

`preflight_hf.py` checks each command against the original run:
- every header field the command sets matches the original header;
- the fields left to `hf_sim.py`'s defaults match a HikWgtnmax OneRay run;
- every station's name, lon and lat match, in order;
- the stoch file is byte-identical to the one the original read, whose path is
  in its `HF.log`.

On 2026-09-27 it passed 71/71. A broken copy failed on all 9 injected errors:
seed, duration, model, station list, stoch, extra option, rayset, output and
kappa.

## Pilot: how much the 1D model matters

`pilot_hf.py` ran the redo commands for single stations of REL01 of each fault,
with the original seeds. It covered 20 stations from 1.1 to 774 km: the nearest,
the farthest, the 0.1st to 99th distance percentiles, and HNPS. Compared with
the leer originals:
- **Shape:** every component correlates at 1.0000000.
- **Amplitude:** OneRay is higher by +0.04% to +0.10%. The difference is largest
  within 30 km of the fault and about +0.045% beyond 500 km.
- **Largest difference:** 0.088% of the peak, e.g. 0.6 cm/s² on a 721 cm/s²
  trace.
- **Determinism:** the leer original re-run on the same login node is
  bit-identical to Sung's July output, so this is a real model effect, not
  numerical noise.

The redo therefore changes the results by about 0.1% at most. It was done so
that every v26p6 header names the configured model.

## Moving the leer results aside

`move_aside.py --apply` ran on 2026-09-27 at 15:50. Each rename is logged, with
the size and mtime of every file moved, in
`/home/arr65/v26p6_oneray_redo/moved_aside_manifest.tsv` (282 renames). In each
realisation it renamed:
- `HF` → `HF.leer`, `BB` → `BB.leer` and `IM_calc` → `IM_calc.leer`;
- `<rel>_im_calc.log` → `<rel>_im_calc.log.leer`.

`BB.leer` took Sung's `bb_sim_command.sh` with it. For the six WellTeast
realisations re-run on 2026-09-25, it also took the `BB.*.with_320077e`
originals, which are now in `BB.leer/Acc/`. Nothing was copied or deleted.

## Checks in the jobs

- **HF.**
  - It refuses to start unless `HF.leer/Acc/HF.bin` exists and any existing
    `HF/Acc/HF.bin` names OneRay.
  - Afterwards the new `HF.bin` must be the original's size. Its header and
    station table must be byte-identical to the original's except for the
    model name: every setting, the seed, the stoch name, the stations, and each
    station's e_dist and Vs.
- **BB.**
  - It refuses unless the HF is a completed OneRay redo of the original's size.
    It never overwrites a `BB.bin`.
  - Code, arguments and station sets are those of the leer BB runs: the pinned
    `2c6a79a8`, Sung's flags, and WellTeast's canonical 18431-station list.
  - Afterwards the new `BB.bin` must be the original's size, with a
    byte-identical header and station table.
- **IM.** Sung's wrapper, unmodified: 32 tasks, 84G, 4 h, `gmsim=sarah2024`,
  as on 2026-09-25. It marks IM done in Sung's status DB, as before.

## Status

- **Submitted** 2026-09-27 15:50 (`submissions.txt` on NeSI):
  - HF array **9334892** (0–70);
  - BB array **9334893** (0–41,43–70, `aftercorr` on HF);
  - IM array **9334894** (0–41,43–70, `aftercorr` on BB).
- **Still to do:**
  - verify the outputs;
  - compare the IMs with the leer ones;
  - delete the `*.leer` results (user decision, after verification).
