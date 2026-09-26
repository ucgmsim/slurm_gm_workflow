# Why the v26p6 HF runs used the leer 1D model

Written 2026-09-26 from read-only checks on NeSI and Dropbox. Step 2 below is
partly inferred, and says which part.

## Summary

All 71 Cybershake v26p6 HF runs on NeSI used the 1D velocity model
`Cant1D_v2-midQ_leer.1d`: PalliserKai (median and REL01–37) and WellTeast
(median and REL01–32). Every configuration names `Cant1D_v3-midQ_OneRay.1d`,
and all 63 sampled v25p11 runs used it.

The most likely chain of events:

1. At HF time the v26p6 realisations' configs still pointed into
   `Cybershake/v26p5`. There, `root_params.yaml` gives the model's path on
   KISTI (`/scratch/x2568a02/...`), which does not exist on NeSI.
2. Sung's HF command generator, `run_hf_command.py`, adds `--hf_vel_mod_1d`
   only if that path exists. It left the flag out without a warning. Its job
   wrapper would also have hidden any warning.
3. Without the flag, `hf_sim.py` uses its built-in default,
   `Cant1D_v2-midQ_leer.1d`. That file exists on NeSI, so every run
   completed normally.

No step failed, so nothing drew attention to the substitution.

## What was used and what was intended

**Used.** Across the 71 runs' `HF.log` files there are 1,285,472 parameter
blocks, and every one names
`/nesi/project/nesi00213/VelocityModel/Mod-1D/Cant1D_v2-midQ_leer.1d`. The
model name stored in each `HF.bin` header says the same. This includes the 40
runs that were interrupted and resumed. The survey table is
`/home/arr65/hf_model_survey/hf_models.tsv` on NeSI.

**Intended.** `Cant1D_v3-midQ_OneRay.1d`. The NeSI `root_params.yaml` of
v25p10, v25p11, v26p4, v26p5 and v26p6 all name it as `v_1d_mod`. All five were
built from gmsim template version `20.4.1.1_bulldozed`; this repo's
`workflow/calculation/gmsim_templates/20.4.1.1/root_defaults.yaml` names the
same model. It is also the model in `hf.hf_vel_mod_1d` in all five, and in
Cascade's v26p5. Only the directory part of that path differs; see step 1.

**What other runs used.**
- **v25p11 (Dropbox):** 63 of the 669 HF tarballs were checked: the median,
  first and last realisation of all 21 faults. All used OneRay. Every other HF
  setting matches v26p6: dt 0.005, sdrop 50, kappa 0.045, qfexp 0.6, fmax 10,
  rvfac 0.8.
- **Newer Cylc HF runs** (`RunFolder/hf_sims/cylc/cylc-run`), counted by
  realisation:
  - `adhoc_hf_alpinef2k`: all 48 (the AlpineF2K median and REL01–47) use
    OneRay exactly.
  - `alpine_vs30_update`: the 9 `clarence_R*`, `hope_R*` and `wairau_R*`
    realisations use OneRay exactly. The 3 `base_R*` realisations use
    OneRay with its half-space row repeated as a 35th layer. That adds no
    new interface, so physically it is the same model.
  - None use leer.

**How the two models differ.** Layer thicknesses, Vp and density are the same
in both. The differences are in attenuation, plus one placeholder value:

| | leer | OneRay |
|---|---|---|
| Qp, surface layer | 38 | 116 |
| Qp, top 2.8 km (layers 1–18) | lower | higher, by up to 3.05× |
| Qp, below 2.8 km | higher | lower, by up to 14% |
| Qs | 53.9 and 57.3 in the top two layers | 58 in both; below them the two files match |
| Half-space Vs | 999.999 (a placeholder) | 4.6 |

## How it happened

### 1. The configured path depends on the machine

`install_cybershake.py` builds the path from the installing machine's
velocity-model directory
(`workflow/automation/install_scripts/install_cybershake.py:180-184, 200`):

```python
v1d_full_path = (
    Path(platform_config[constants.PLATFORM_CONFIG.VELOCITY_MODEL_DIR.name])
    / "Mod-1D"
    / root_params_dict["v_1d_mod"]
)
...
root_params_dict["hf"][wf_constants.HF_VEL_MOD_1D] = str(v1d_full_path)
```

KISTI's directory is `/scratch/x2568a02/gmsim_home/VelocityModel`
(`workflow/automation/org/kisti/config.json:18`). The NeSI copies of the
version trees name:

| Version | `hf.hf_vel_mod_1d` in `Runs/root_params.yaml` on NeSI |
|---|---|
| v25p10 | `/scratch/x2568a02/gmsim_home/VelocityModel/Mod-1D/Cant1D_v3-midQ_OneRay.1d` (KISTI) |
| v25p11 | `/nesi/project/nesi00213/VelocityModel/Mod-1D/Cant1D_v3-midQ_OneRay.1d` (NeSI) |
| v26p4 | the KISTI path |
| v26p5 | the KISTI path |
| v26p6 | the NeSI path |

Cascade's copy of v26p5 names
`/uoc/project/uoc40001/scratch/baes/VelocityModel/Mod-1D/Cant1D_v3-midQ_OneRay.1d`.

v26p6's `root_params.yaml` is identical to v26p5's except for two lines:
- `hf_vel_mod_1d`: the KISTI path in v26p5, the NeSI path in v26p6;
- `mgmt_db_location`: v26p5 in one, v26p6 in the other.

Sung's `fix_old_nesi_path.sh` rewrites Cascade paths to NeSI paths, and
`Cybershake/v26p5` to `Cybershake/v26p6`, in whatever directory it is run on.
It has no rule for the KISTI path.

### 2. At HF time the v26p6 realisations read the v26p5 tree

The generator takes the stoch file from the realisation's `sim_params.yaml`
(`hf.slip`). It finds `root_params.yaml` by following `sim_params.yaml` →
`fault_yaml_path` → `fault_params.yaml` → `root_yaml_path`
(`hpc3_submit/config_helper.py:30-34`).

- **Measured.** All 71 v26p6 HF logs read their stoch files from
  `Cybershake/v26p5/Data/Sources/...`, while writing output to
  `Cybershake/v26p6/Runs/...`. So at HF time `sim_params.yaml` pointed into
  v26p5.
  - The generator also refuses to run unless the SRF exists
    (`run_hf_command.py:146-147`). v26p6 has no SRF files for these faults,
    while v26p5 has all 71, so the SRF path must have pointed into v26p5 as
    well.
  - This holds for all 1,285,472 parameter blocks, resumes included. The
    last resume started on 7 July at 11:50.
- **Measured.** Today all 73 of these config files point into v26p6, and none
  into v26p5. That is each fault's `fault_params.yaml` plus every
  `sim_params.yaml`. Resolving PalliserKai REL01's config on 2026-09-25 gave
  v26p6's `root_params.yaml`, whose NeSI OneRay path exists. So these files
  were rewritten after HF ran.
- **Likely cause of the rewrite.** `fix_old_nesi_path.sh`. Sung's HF and BB
  wrappers run it over the fault directory before every job
  (`run_hf_job_array.sl:62`, `run_bb_job_array.sl:80`), and it rewrites
  `Cybershake/v26p5` to `Cybershake/v26p6` in every `*.yaml` there.
  - The HF wrapper now runs it just before generating the command (line 62,
    then line 68).
  - So when the HF commands were generated, either the script lacked that
    rule or it wasn't run. Its errors are discarded.
  - It was last modified on 7 July at 14:09, after every HF parameter block
    had been written.
  - It appears to have been run on the v26p5 tree too. v26p5's own
    PalliserKai `fault_params.yaml` now names v26p6's `root_params.yaml`,
    whereas v26p5's WellTeast one still names v26p5's.
- **Inferred: which `root_params.yaml` the generator loaded.** This can't be
  recovered.
  - Sung's HF job logs are not in `hpc3_submit/logs`.
  - File times don't help. Every config and HF output in the two v26p6 fault
    directories carries a modification time between 2026-08-24 17:28:36 and
    17:30:31. v26p5's `root_params.yaml`, and its `fault_params.yaml` for both
    faults, carry 17:28 the same day. Yet WellTeast REL01's `HF.bin`, for
    example, finished on 7 July.

  There are two possibilities, with the same outcome:
  - `fault_yaml_path` still led to v26p5's `root_params.yaml`, which names the
    KISTI path to this day; or
  - it led to v26p6's `root_params.yaml`, which still carried the KISTI line
    copied from v26p5 and was corrected later.

  Either way, the path checked on 3 July did not exist on NeSI. A NeSI path
  would have passed the check. The OneRay file sits in the same directory as
  the leer file these runs read, and v25p11 read it from there in February and
  March 2026.

### 3. The generator drops the flag silently

`hpc3_submit/run_hf_command.py:223-227`:

```python
    # --hf_vel_mod_1d
    if 'hf_vel_mod_1d' in hf_params:
        vel_mod_1d = hf_params['hf_vel_mod_1d']
        if Path(vel_mod_1d).exists():
            command += f" --hf_vel_mod_1d {vel_mod_1d}"
```

There is no `else` branch and no warning. By contrast, a missing binary does
print a warning, at line 120.

Even a warning would have been lost. `run_hf_job_array.sl:68-72` captures
the generator's stdout and stderr, then logs and runs only the last line:

```bash
HF_CMD_OUTPUT=$(python "$SCRIPTS_DIR/run_hf_command.py" "$REL_DIR" 2>&1)
HF_CMD=$(echo "$HF_CMD_OUTPUT" | tail -n 1)

echo "→ Executing: $HF_CMD"
eval "$HF_CMD"
```

### 4. `hf_sim.py` falls back to leer

This is `workflow/calculation/hf_sim.py:142-150`, the same default as in NeSI's
`mrd87_4` environment, which ran these jobs:

```python
    arg(
        "-m",
        "--hf_vel_mod_1d",
        help="path to velocity model (1D). ignored if --site_specific is set",
        default=os.path.join(
            platform_config[constants.PLATFORM_CONFIG.VELOCITY_MODEL_DIR.name],
            "Mod-1D/Cant1D_v2-midQ_leer.1d",
        ),
    )
```

On NeSI this resolves to
`/nesi/project/nesi00213/VelocityModel/Mod-1D/Cant1D_v2-midQ_leer.1d`. That
file exists, so the runs completed normally, and each recorded the model it
used in its log and header.

The workflow's own submission path cannot fail this way.
`workflow/automation/submit/submit_hf.py:37` always passes the configured
path, and `hf_sim.py` opens it at line 491. A path that doesn't exist
therefore makes the job fail rather than fall back.

## Timeline

| When | What |
|---|---|
| Feb–Mar 2026 | v25p11 HF runs; all 63 sampled used OneRay. v25p11's `root_params.yaml` names the NeSI path. The tarballs checked list arr65 as owner, and the one log examined (AlpineF2K REL01) uses a relative output path. So these were probably not launched with Sung's generator. |
| 2026-07-02 13:42 | Most files in `hpc3_submit` carry this time, so the folder was probably copied into place then. `run_hf_command.py` was modified at 13:44. |
| 2026-07-03 | All 71 v26p6 HF runs start. Every parameter block reads v26p5 stoch files and uses leer. |
| 2026-07-03 to 07-07 | 40 runs resume (the first at 12:45 on 3 July, the last at 11:50 on 7 July), still reading v26p5 and using leer. |
| 2026-07-07 | `run_hf_job_array.sl` modified at 11:20. `fix_old_nesi_path.sh` modified at 14:09. |
| 2026-08-24 17:28–17:31 | The configs and HF outputs in the v26p6 fault directories, and v26p5's configs, carry these modification times. So file times cannot date the config edits. |

## What this affects

- **Affected:** the HF of all 71 v26p6 realisations, and therefore their BB
  and IM. That is PalliserKai (median and REL01–37) and WellTeast (median and
  REL01–32). WellTeast REL04 has HF but no LF, so it has no BB or IM.
- **Not affected:** v25p11 (63 of 63 sampled tarballs used OneRay), and the
  Cylc runs listed above.
- **At risk:** HF generated with `run_hf_command.py` from any tree whose
  `root_params.yaml` names a path that does not exist on NeSI. On NeSI that
  means v25p10, v26p4 and v26p5. This includes HikWgtnmax, which is to run in
  v26p5, unless the model is passed explicitly. Running Sung's wrappers there
  would also let `fix_old_nesi_path.sh` repoint that fault's configs at
  v26p6.
- **Not checked:** HF from other versions archived on Dropbox.
  `survey_hf_tar_models.py`, on NeSI in `/home/arr65/hf_model_survey/`, reads
  the model from any HF tarball with a byte-range read of about 1 KB.

## Preventing a repeat

- **Generator:** make `run_hf_command.py` raise an error when the configured
  1D model is missing, instead of dropping the flag.
- **Wrapper:** make `run_hf_job_array.sl` log the generator's full output, not
  only the command.
- **Configs:** correct `hf_vel_mod_1d` in the NeSI copies of
  `root_params.yaml` that name KISTI paths, or teach
  `fix_old_nesi_path.sh` the KISTI prefix.
- **After HF runs:** compare the model recorded in each `HF.bin` header with
  the configuration.
