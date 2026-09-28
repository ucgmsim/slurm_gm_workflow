# HikWgtnmax BB and IM (Cybershake v26p5) on NeSI

BB and IM for the 51 HikWgtnmax realisations (the median and REL01–REL50) in
`/nesi/nobackup/nesi00213/RunFolder/Cybershake/v26p5/Runs/HikWgtnmax`.

- **LF:** computed on Cascade and staged on NeSI as NetCDF files (`../README.md`
  and the project notes).
- **HF:** the OneRay runs in `../` (array 9325809, all 51 checked).

On NeSI these files live in `/home/arr65/hikwgtnmax_v26p5/bb_im/`.

| File | Role |
|---|---|
| `make_bb_inputs.py` | Writes `targets.txt` and stages each realisation's `e3d.par` from its Cascade `*_other.tar.gz` into `HikWgtnmax_LF_from_Cascade/e3d_par/`. It checks each LF run's rlog ends with `FINISHED`, and that its `e3d.par` matches the VM grid and the expected nt, dt, flo and h. It passed 51/51 on 2026-09-28. |
| `targets.txt` | The 51 realisation directories, md5 `7f953932d21f00684358075c5cbec862`. |
| `preflight_bb_hikwgtnmax.py` | Read-only check of every BB input (below). |
| `run_bb_hikwgtnmax.sl` | BB array 0–50. |
| `check_bb_output.py` | Run by each BB task afterwards: checks the output against every source (below). |
| `submit_bb_im_hikwgtnmax.sh` | Re-runs the pre-flight and the BB guards, then submits BB and the 51 IM jobs. |

## How the LF NetCDF becomes BB

The old workflow's `bb_sim` reads EMOD3D's OutBin through qcore's `LFSeis`. The
Cascade LF only exists as the new workflow's NetCDF. These were written by
`lf-to-xarray` at workflow `pegasus` 7e465c5, which calls qcore-utils
2025.12.2 `read_lfseis_directory`: it rotates the velocity and differentiates it
with `np.gradient`.

`workflow/calculation/lf_netcdf.py` reads the NetCDF as `LFSeis` would read the
OutBin:
- it rebuilds the velocity exactly from the central differences;
- it applies `LFSeis`'s own backward difference;
- it takes the start time from the run's `e3d.par`;
- it takes `z` from the station coordinates.

`bb_sim` uses it when `lf_dir` is a `.nc` file, which then needs `--lf-e3d-par`
and `--lf-statcords`. The same change fixes `bb_sim`'s checkpoint bug, so an
interrupted run can resume safely (`../../cybershake_v26p6/oneray_redo/README.md`).

Tests in `workflow/calculation/tests/test_lf_netcdf.py` run a synthetic OutBin
through the real converter:
- the reader matches `LFSeis` to within 2e-6 of the peak, and 4e-8 after
  bb_sim's 1 Hz lowpass;
- metadata, station tables and duplicate handling match;
- an `e3d.par` or station file from a different run is refused.

The real-data check is on PalliserKai REL01 (`../netcdf_reader_validation/`):
1. convert its OutBin with the same converter;
2. compare the two readers;
3. compare BB built from the NetCDF with the production BB built from the
   OutBin.

## Settings

The v26p5 configs (`root_params.yaml`, `fault_params.yaml`) name everything:
- `bb`: dt 0.005, fmin 0.5, fmidbot 1.0, `no-lf-amp`, with `flo` 1.0. These are
  the same as v26p6, so the same as the PalliserKai and WellTeast BB.
- the vs30 file: `StationInfo/non_uniform_whole_nz_with_real_stations-hh400_v20p3_land.vs30`;
- the VM: `v26p5/Data/VMs/HikWgtnmax`. Its `vs3dfile.s` was copied from Dropbox
  on 2026-09-28, and bb_sim reads only its surface layer, for `lf_vs_ref`;
- the station coordinates: `v26p5/Runs/HikWgtnmax/fd_rt01-h0.100.statcords`;
- `ims`: components 000, 090, ver, geom, rotd50 and rotd100_50, and the same 31
  pSA periods as v26p6. Sung's IM command generator gives these realisations
  exactly the PalliserKai command apart from paths.

## Checks

`preflight_bb_hikwgtnmax.py` checks each realisation:
- HF is the finished OneRay run;
- the reader accepts its NetCDF with its `e3d.par` and the station coordinates;
- LF and HF hold the same 17186 stations, with no duplicates;
- their durations agree;
- no BB.bin exists yet.

Once overall, it checks:
- the VM surface Vs is positive at every station (500–3232 m/s);
- the vs30 file covers every station;
- there is enough free space.

It passed 51/51 on 2026-09-28. Each BB.bin will be 14,767,174,896 bytes, about
0.75 TB in all.

`run_bb_hikwgtnmax.sl` refuses to run unless:
- the code is a clean checkout of the deployed commit;
- HF is the finished OneRay run;
- no BB.bin exists, except with `RESUME=1` at the expected size.

`check_bb_output.py` then checks each BB.bin against its sources:
- header numbers and source paths;
- in every station record:
  - name order, `e_dist` and `hf_vs_ref` against HF;
  - lon, lat, x and y against the NetCDF;
  - `z` against the station coordinates;
  - `lf_vs_ref` against the VM;
  - `vsite` against the vs30 file, which also shows every station finished;
- the exact size;
- every waveform row is finite and not all zero.

## Submitting

After the reader's commit is deployed at
`/nesi/project/nesi00213/Environments/arr65_v26p6_netcdf/workflow` and set as
`COMMIT` in `run_bb_hikwgtnmax.sl`:

```bash
bash /home/arr65/hikwgtnmax_v26p5/bb_im/submit_bb_im_hikwgtnmax.sh
```

IM runs Sung's `run_im_job_array.sl` (sarah2024), as for PalliserKai and
WellTeast. It runs as 51 separate jobs, each waiting for its own BB task. On
NeSI, `kill_invalid_depend` cancels every pending task of an array whose
dependency fails. So a single IM array chained to the BB array would lose every
IM not yet started if one BB task failed, as happened on 2026-09-28.
