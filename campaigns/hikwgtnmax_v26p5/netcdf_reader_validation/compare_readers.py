"""Compare LFNetCDF on a converted NetCDF with LFSeis on the OutBin it came from.

Read-only. Checks what bb_sim takes from its LF reader:
  - the metadata (nt, dt, hh, rot, duration, start_sec), values and types;
  - the station table (order, name, x, y, z, lat, lon);
  - `acc` for a sample of stations, raw and after bb_sim's lowpass at flo.
LFSeis re-reads a whole seis file per station, so the sample is every 16th
station plus the first 64, grouped by file so the page cache helps. run in the
mrd87_4 venv with PYTHONPATH set to the checkout holding lf_netcdf.py.

Usage: python compare_readers.py OUTBIN NETCDF E3D_PAR STATCORDS [FLO]
"""

import sys
import time

import numpy as np
from qcore.timeseries import LFSeis, bwfilter

from workflow.calculation.lf_netcdf import LFNetCDF

outbin, netcdf, e3d_par, statcords = sys.argv[1:5]
flo = float(sys.argv[5]) if len(sys.argv) > 5 else 1.0
t0 = time.time()
ref = LFSeis(outbin)
lf = LFNetCDF(netcdf, e3d_par, statcords)
problems = []

for key in ("nt", "dt", "hh", "rot", "duration", "start_sec"):
    a, b = getattr(lf, key), getattr(ref, key)
    same = a == b and type(a) is type(b)
    print(f"{key:10s} NetCDF {a!r:30s} OutBin {b!r:30s} {'same' if same else 'DIFFERENT'}")
    if not same:
        problems.append(key)
if list(lf.stations.name) != list(ref.stations.name):
    problems.append("station names or order")
for col in ("x", "y", "z", "lat", "lon"):
    if not np.array_equal(lf.stations[col], ref.stations[col]):
        problems.append(f"station {col}")
print(f"stations: {lf.nstat} (NetCDF) vs {ref.nstat} (OutBin); duplicates in the NetCDF: {lf.n_duplicate}")

sample = sorted(set(range(0, ref.nstat, 16)) | set(range(min(64, ref.nstat))),
                key=lambda i: tuple(ref.stations.seis_idx[i]))
dt = float(lf.dt)
raw, low = [], []
for n, i in enumerate(sample):
    name = ref.stations.name[i]
    a, b = lf.acc(name, dt=dt), ref.acc(name, dt=dt)
    peak = np.abs(b).max()
    if peak == 0:
        continue
    raw.append(np.abs(a - b).max() / peak)
    la = np.stack([bwfilter(a[:, c], dt, flo, "lowpass") for c in range(3)], axis=1)
    lb = np.stack([bwfilter(b[:, c], dt, flo, "lowpass") for c in range(3)], axis=1)
    low.append(np.abs(la - lb).max() / np.abs(lb).max())
    if n % 200 == 0:
        print(f"  {n}/{len(sample)} stations compared ({time.time() - t0:.0f} s)", flush=True)
raw, low = np.array(raw), np.array(low)
print(f"acc compared at {raw.size} stations: worst |NetCDF - OutBin| / peak raw {raw.max():.2e} "
      f"(median {np.median(raw):.1e}), after the {flo} Hz lowpass {low.max():.2e} (median {np.median(low):.1e})")
if low.max() > 1e-5:
    problems.append(f"lowpassed acc differs by up to {low.max():.2e} of peak")
print("RESULT:", "OK" if not problems else "PROBLEMS: " + "; ".join(problems), f"({time.time() - t0:.0f} s)")
sys.exit(1 if problems else 0)
