# Validating bb_sim's LF NetCDF reader on PalliserKai REL01

HikWgtnmax's LF only exists as the new workflow's NetCDF, so `bb_sim` reads it
through `workflow/calculation/lf_netcdf.py` (`../bb_im/README.md`). This checks
the reader on real data from a run that also has its EMOD3D OutBin: PalliserKai
REL01 (v26p6), converted exactly as HikWgtnmax was.

On NeSI these files live in `/home/arr65/hikwgtnmax_v26p5/netcdf_reader_validation/`.
Outputs go to
`/nesi/nobackup/nesi00213/RunFolder/Cybershake/v26p5/Runs/HikWgtnmax_LF_from_Cascade/reader_validation/`.

| File | Role |
|---|---|
| `convert_lf_to_netcdf.sl` | Converts an OutBin as `lf-to-xarray` did at workflow `pegasus` 7e465c5. It uses qcore-utils 2025.12.2 and that commit's `uv.lock` versions, in the venv `/home/arr65/venvs/qcore2025122`. |
| `compare_readers.py` | Compares `LFNetCDF` on the NetCDF with `LFSeis` on the OutBin: metadata, station table, and `acc` for about 1100 stations, raw and after bb_sim's 1 Hz lowpass. |
| `validate_bb_netcdf.sl` | Runs `compare_readers.py`, then bb_sim from the NetCDF with the production OneRay HF, VM, vs30 file and flags, then `compare_bb_files.py`. It uses a test copy of the code at `/nesi/project/nesi00213/Environments/arr65_v26p6_netcdf_test/workflow`: the pinned `2c6a79a8` plus the reader and the bb_sim changes. |
| `compare_bb_files.py` | Compares two BB.bin files: headers equal apart from the LF path, identical station records, and each station's largest waveform difference as a fraction of its peak. |

## Runs

- **Conversion,** job 9352381, 2026-09-28: 2 min 46 s, giving a 12,598,361,390
  byte NetCDF.
- **Validation, first attempt,** job 9352382: failed in 6 s. PalliserKai's
  old-workflow `e3d.par` writes `nt="59107"`, and the reader did not yet strip
  the quotes. It now does, and a test covers it.
- **Validation,** job 9353481, 2026-09-28 (`lf_netcdf.py` md5 df537660636d):
  **all OK.**
  1. Readers: nt, dt, hh, rot, duration and start_sec agree in value and type,
     and all 17760 stations agree in name, order, x, y, z, lat and lon. At 1170
     stations the `acc` difference is at most 7.2e-5 of the peak raw (median
     1.3e-5), and 1.06e-7 after the 1 Hz lowpass (median 2.1e-8).
  2. bb_sim from the NetCDF took 10.5 min in all. Station processing took
     about 45 s per rank, against 1–2.8 h per task reading the OutBin.
  3. Against the production BB.bin from the OutBin:
     - headers identical apart from the LF path;
     - all 17760 station records byte-identical;
     - the waveforms' worst difference is 1.32e-7 of the peak, with 0 stations
       above 1e-5 and no all-zero rows.
  `../bb_im/check_bb_output.py` also passes this BB.bin against its sources.
