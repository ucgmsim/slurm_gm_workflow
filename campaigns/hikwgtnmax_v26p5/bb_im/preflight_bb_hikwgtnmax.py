"""Read-only pre-flight check of the HikWgtnmax BB inputs, one realisation per target.

For each target it checks:
  - HF: HF/Acc/HF.bin is the finished OneRay run (header model, HF.log, size).
  - LF: the reader bb_sim will use (LFNetCDF) accepts the NetCDF with the staged
    e3d.par and the fault's station coordinates file. It thereby checks nt, dt,
    h, rotation, start_sec and every station's x and y.
  - LF and HF hold the same 17186 station names, which bb_sim will produce in
    HF order, and their durations agree as bb_sim requires.
  - No BB.bin exists yet.
Once for all targets, it checks:
  - the VM's vs3dfile.s has its full size, with a positive surface Vs at every
    station (bb_sim's lf_vs_ref);
  - the vs30 file covers every station;
  - nobackup has room.
It prints the BB.bin size bb_sim will write.

Run with the mrd87_4 venv and PYTHONPATH set to the checkout holding lf_netcdf.py.
Usage: python preflight_bb_hikwgtnmax.py TARGETS
"""

import os
import sys
from pathlib import Path

import numpy as np
import yaml
from qcore.timeseries import HFSeis

from workflow.calculation.lf_netcdf import LFNetCDF

RUNS = Path("/nesi/nobackup/nesi00213/RunFolder/Cybershake/v26p5/Runs")
STAGE = RUNS / "HikWgtnmax_LF_from_Cascade"
STATCORDS = RUNS / "HikWgtnmax" / "fd_rt01-h0.100.statcords"
VM = Path("/nesi/nobackup/nesi00213/RunFolder/Cybershake/v26p5/Data/VMs/HikWgtnmax")
VS30 = Path("/nesi/project/nesi00213/StationInfo/non_uniform_whole_nz_with_real_stations-hh400_v20p3_land.vs30")
HF_MODEL = "Cant1D_v3-midQ_OneRay.1d"
HF_SIZE = 14643091208
BB_DT = 0.005
HEAD_SIZE, HEAD_STAT = 0x500, 44


def main():
    targets = [Path(t) for t in Path(sys.argv[1]).read_text().split()]
    vm = yaml.safe_load((VM / "vm_params.yaml").read_text())
    vs = np.memmap(VM / "vs3dfile.s", dtype="<f4", mode="r", shape=(vm["ny"], vm["nz"], vm["nx"]))
    vs30 = {str(k): v for k, v in np.loadtxt(VS30, dtype=[("name", "U8"), ("vs30", "f4")], comments=("#", "%"))}
    problems_total, bb_size, surface_checked = 0, None, False
    for rel in targets:
        name, problems = rel.name, []
        hf_path = rel / "HF" / "Acc" / "HF.bin"
        with open(hf_path, "rb") as f:
            f.seek(224)
            model = f.read(64).split(b"\0")[0].decode()
        log = (rel / "HF" / "Acc" / "HF.log").read_bytes()[-65536:]
        if model != HF_MODEL or b"Simulation completed" not in log or hf_path.stat().st_size != HF_SIZE:
            problems.append(f"HF not a finished OneRay run (model {model}, size {hf_path.stat().st_size})")
        hf = HFSeis(str(hf_path))
        try:
            lf = LFNetCDF(STAGE / f"{name}_seis.nc", STAGE / "e3d_par" / f"{name}_e3d.par", STATCORDS)
        except Exception as e:  # noqa: BLE001 - reported as a failed check
            problems.append(f"LF reader refused it: {e}")
            lf = None
        if lf is not None:
            if set(lf.stations.name) != set(hf.stations.name) or lf.nstat != hf.stations.size:
                problems.append(f"LF has {lf.nstat} stations, HF {hf.stations.size}, sets differ")
            if lf.n_duplicate:
                problems.append(f"{lf.n_duplicate} duplicate LF stations")
            if not np.isclose(lf.dt * lf.nt + lf.start_sec, hf.dt * hf.nt, atol=min(lf.dt, hf.dt)):
                problems.append(f"LF ends at {lf.dt * lf.nt + lf.start_sec}, HF at {hf.dt * hf.nt}")
            if not surface_checked:
                idx = [lf.stat_idx[s] for s in hf.stations.name]
                lf_vs_ref = vs[lf.stations.y[idx], 0, lf.stations.x[idx]] * 1000.0
                missing_vs30 = [s for s in hf.stations.name if s not in vs30]
                print(f"surface Vs at the {len(idx)} stations: {lf_vs_ref.min():.0f}-{lf_vs_ref.max():.0f} m/s; "
                      f"vs30 file lacks {len(missing_vs30)} stations")
                if not (lf_vs_ref > 0).all() or missing_vs30:
                    problems.append("VM surface Vs or vs30 incomplete")
                surface_checked = True
            # bb_sim's own padding arithmetic
            lf_off, hf_off = max(lf.start_sec - hf.start_sec, 0), max(hf.start_sec - lf.start_sec, 0)
            lf_end_pad = int(round(max(hf.duration + hf_off - (lf.duration + lf_off), 0) / BB_DT))
            bb_nt = int(int(round(lf_off / BB_DT)) + round(lf.duration / BB_DT) + lf_end_pad)
            size = HEAD_SIZE + lf.nstat * HEAD_STAT + lf.nstat * bb_nt * 3 * 4
            if bb_size not in (None, size):
                problems.append(f"BB size {size} differs from other realisations' {bb_size}")
            bb_size = size
        if (rel / "BB" / "Acc" / "BB.bin").exists():
            problems.append("BB.bin already exists")
        problems_total += bool(problems)
        print(f"{name:18s} {'OK' if not problems else 'FAIL: ' + '; '.join(problems)}")

    st = os.statvfs(RUNS)
    free = st.f_bavail * st.f_frsize
    print(f"\nBB.bin will be {bb_size} bytes each; {len(targets)} need {len(targets) * (bb_size or 0) / 1e12:.2f} TB, "
          f"{free / 1e12:.0f} TB free")
    if bb_size and len(targets) * bb_size > free:
        print("FAIL: not enough free space")
        problems_total += 1
    print(f"all {len(targets)} OK" if not problems_total else f"{problems_total} problem(s)")
    sys.exit(1 if problems_total else 0)


if __name__ == "__main__":
    main()
