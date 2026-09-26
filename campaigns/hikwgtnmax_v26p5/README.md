# HikWgtnmax HF (Cybershake v26p5) on NeSI

HF for the 51 HikWgtnmax realisations (median and REL01–REL50) in
`/nesi/nobackup/nesi00213/RunFolder/Cybershake/v26p5/Runs/HikWgtnmax`, with
the old workflow's `hf_sim.py` from NeSI's `mrd87_4` environment. This is the
same code and binary as Sung's v26p6 HF runs. The LF was computed on Cascade;
see the project notes for that part.

On NeSI these files live in `/home/arr65/hikwgtnmax_v26p5/hf/`.

| File | Role |
|---|---|
| `make_hf_commands.sh` | Runs Sung's generator (`hpc3_submit/run_hf_command.py`) print-only for each realisation. It checks each command, then appends `--hf_vel_mod_1d .../Cant1D_v3-midQ_OneRay.1d --sim_bin .../hb_high_binmod_v5.4.5.3`. |
| `hf_commands.txt` | The 51 generated commands, md5 `6a9811d302ca58627caa2f7ab4d0525b`. |
| `preflight_hf.py` | Read-only check of every command against its sources (details below). |
| `run_hf_hikwgtnmax.sl` | The array job, 0–50, one realisation per task. |

`preflight_hf.py` checks:
- station list, output path, and that no `HF.bin` exists yet;
- the seed, which the generator derives from the md5 of the SRF file name;
- the stoch file;
- the settings, against v26p5's `root_params.yaml`;
- the pinned 1D model and binary;
- that seeds and outputs are unique, and that there is free space.

On 2026-09-26 it passed 51/51. A deliberately broken copy failed on every
injected error.

## Status

- **Submitted** 2026-09-26 23:00 as array job **9325809** (tasks 0–50),
  account `nesi00213`, after a final pre-flight (51/51 OK).
- **First task:** task 0 (the median) started at 23:00:54 on `g04`. Its
  parameter blocks all name OneRay.
- **Record:** every submit is logged in
  `/home/arr65/hikwgtnmax_v26p5/hf/submissions.txt`, and job logs go to
  `/home/arr65/hikwgtnmax_v26p5/hf/logs/hf_<job>_<task>.out`.
- **Still to do:**
  - Verify all 51 `HF.bin` files.
  - Then BB, which needs the old `bb_sim` to read the Cascade LF NetCDF.
  - Then IM.

## Decisions

- **1D model is `Cant1D_v3-midQ_OneRay.1d`, passed explicitly.**
  - Every Cybershake config names it, as did all the v25p11 and Cylc runs
    checked.
  - v26p5's `root_params.yaml` gives its KISTI path. Sung's generator drops
    that path silently, and `hf_sim.py` would fall back to leer. See
    `../cybershake_v26p6/hf_1d_model_fallback.md`.
  - The deeper `Cant1D_v3-midQ_OneRay_Sub.1d` is identical down to 27 km.
    The HikWgtnmax sources lie at 5–30 km and only direct rays are traced,
    so it should make little or no difference.
- **HF binary is pinned** to `/nesi/project/nesi00213/tools/hb_high_binmod_v5.4.5.3`.
  - qcore picks the binary by hostname.
  - NeSI's compute nodes resolve to this path, which is the binary Sung's
    v26p6 runs used.
  - Hosts whose names start with `login` are taken for KISTI's Nurion.
- **Sung's wrapper is not used.**
  - Its `fix_old_nesi_path.sh` rewrites `Cybershake/v26p5` paths to
    `Cybershake/v26p6`. That would repoint these configs at v26p6, which has
    no HikWgtnmax sources.
  - Its status-DB updates target the v26p6 DB, which has no HikWgtnmax rows.
- **Direct rays only (`rayset` 1)**, as v26p5 configures. This was the user's
  decision on 2026-09-26, after the comparison below.
- **Other settings come from v26p5's `root_params.yaml`:** dt 0.005,
  sdrop 50, kappa 0.045, rvfac 0.8, rayset 1, path_dur 11, version 5.4.5.3.
  - A full header comparison shows every one matches the v26p6 runs.
  - Every `root_params.yaml` configures `rayset: 1`, i.e. direct rays only,
    and all the Cylc Alpine runs used it.
  - 61 of the 63 v25p11 runs checked (February 2026) differ in this one
    setting: they used `rayset` [1, 2], which adds Moho-reflected rays and is
    `hf_sim.py`'s default. The two re-run in March used `rayset` 1.

## Sizing

The timing test on 2026-09-26 ran one station (REL01, HNPS) in one process
on a login node:
- 184 s per station, with 154 MB max RSS;
- the output was the exact expected size, and its header names OneRay.

At that rate one realisation is 17186 × 184 s ≈ 880 core-hours, and all 51
are about 45,000. The job uses 96 tasks on one node, about 9 h per
realisation, with 16 h requested. Each task counts as 2 CPUs, so 28 jobs fit
under the 5376-CPU per-user limit, and all 51 run in two waves.

## Running and resuming

```bash
cd /home/arr65/hikwgtnmax_v26p5/hf && sbatch --array=0-50 run_hf_hikwgtnmax.sl
```

- **Skipping:** a task whose `HF.log` says `Simulation completed` does
  nothing.
- **Resuming:** `hf_sim.py` checkpoints per station and resumes when rerun
  with the same command, so a timed-out or failed task is just resubmitted.
- **Self-checks:** each task checks its `HF.log`, the `HF.bin` size
  (14643091208 bytes), and the 1D model named in the `HF.bin` header.
