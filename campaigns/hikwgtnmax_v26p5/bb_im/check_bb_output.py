"""Read-only check of one HikWgtnmax BB.bin against every source it was built from.

There is no earlier BB to compare with, so each part is checked against its
source:
  - header: station count and nt (as bb_sim computes them), dt, start time, and
    the LF, VM and HF paths it names;
  - every station record:
      - name, e_dist and hf_vs_ref from the HF header, in HF order;
      - lon, lat, x and y from the LF NetCDF, and z from the station coordinates;
      - lf_vs_ref from the VM surface layer;
      - vsite from the vs30 file.
    A finished station is marked by its vsite, so this also shows every station
    was finished;
  - size: exactly as the header implies;
  - waveforms: every row finite and not all zero.

Usage: python check_bb_output.py BB_BIN HF_BIN NETCDF STATCORDS VM_DIR VS30_FILE
"""

import sys

import h5py
import numpy as np
import yaml

bb_path, hf_path, nc_path, statcords, vm_dir, vs30_path = sys.argv[1:7]
HEAD_SIZE, HEAD_STAT = 0x500, 44
REC = np.dtype({"names": ["lon", "lat", "name", "x", "y", "z", "e_dist", "hf_vs_ref", "lf_vs_ref", "vsite"],
                "formats": ["f4", "f4", "S8", "i4", "i4", "i4", "f4", "f4", "f4", "f4"],
                "offsets": [0, 4, 8, 16, 20, 24, 28, 32, 36, 40], "itemsize": HEAD_STAT})
HF_REC = np.dtype({"names": ["name", "e_dist", "vs"], "formats": ["S8", "f4", "f4"],
                   "offsets": [8, 16, 20], "itemsize": 0x18})
problems = []

with open(bb_path, "rb") as f:
    n, nt = (int(v) for v in np.fromfile(f, "i4", 2))
    duration, dt, start = (float(v) for v in np.fromfile(f, "f4", 3))
    lf_dir, lf_vm, hf_file = (s.split(b"\0")[0].decode() for s in np.fromfile(f, "S256", 3))
    f.seek(HEAD_SIZE)
    rec = np.fromfile(f, REC, n)
hf_n, hf_nt = (int(v) for v in np.fromfile(hf_path, "i4", 2))
hf = np.fromfile(hf_path, HF_REC, hf_n, offset=0x200)

with h5py.File(nc_path, "r") as nc:
    expect_nt = int(nc["waveform"].shape[2])  # LF starts 3 s before HF and ends with it, so BB spans LF
checks = [("stations", n, hf_n), ("nt", nt, expect_nt), ("dt", dt, float(np.float32(0.005))),
          ("start_sec", start, -3.0), ("duration", duration, float(np.float32(nt * 0.005))),  # bb_sim: float64 product stored as f4
          ("lf_dir", lf_dir, nc_path), ("lf_vm", lf_vm, vm_dir), ("hf_file", hf_file, hf_path)]
problems += [f"{k} {a!r}, expected {b!r}" for k, a, b in checks if a != b]
size = HEAD_SIZE + n * HEAD_STAT + n * nt * 12
with open(bb_path, "rb") as f:
    f.seek(0, 2)
    if f.tell() != size:
        problems.append(f"size {f.tell()}, expected {size}")

if n == hf_n:
    if not np.array_equal(rec["name"], hf["name"]):
        problems.append("station names or order differ from HF")
    for bb_field, hf_field in (("e_dist", "e_dist"), ("hf_vs_ref", "vs")):
        if not np.array_equal(rec[bb_field], hf[hf_field]):
            problems.append(f"{bb_field} differs from HF")
    with h5py.File(nc_path, "r") as nc:
        lf_names = [s.decode() if isinstance(s, bytes) else str(s) for s in nc["station"][:]]
        pos = {name: i for i, name in enumerate(lf_names)}
        idx = np.array([pos[s.decode()] for s in rec["name"]])
        for col in ("lon", "lat", "x", "y"):
            if not np.array_equal(rec[col], nc[col][:][idx]):
                problems.append(f"{col} differs from the LF NetCDF")
    z = {ln.split()[3]: int(ln.split()[2]) for ln in open(statcords).read().splitlines()[1:] if ln.strip()}
    if not np.array_equal(rec["z"], [z[s.decode()] for s in rec["name"]]):
        problems.append("z differs from the station coordinates")
    vm = yaml.safe_load(open(f"{vm_dir}/vm_params.yaml"))
    vs = np.memmap(f"{vm_dir}/vs3dfile.s", dtype="<f4", mode="r", shape=(vm["ny"], vm["nz"], vm["nx"]))
    if not np.array_equal(rec["lf_vs_ref"], (vs[rec["y"], 0, rec["x"]] * 1000.0).astype("f4")):
        problems.append("lf_vs_ref differs from the VM surface layer")
    vs30 = {str(k): v for k, v in np.loadtxt(vs30_path, dtype=[("name", "U8"), ("vs30", "f4")], comments=("#", "%"))}
    unfinished = int((rec["vsite"] != np.array([vs30[s.decode()] for s in rec["name"]], dtype="f4")).sum())
    if unfinished:
        problems.append(f"{unfinished} stations whose vsite is not their vs30 (unfinished)")

zero = bad = 0
head = HEAD_SIZE + n * HEAD_STAT
for start_row in range(0, n, 400):
    m = min(400, n - start_row)
    rows = np.fromfile(bb_path, "f4", m * nt * 3, offset=head + start_row * nt * 12).reshape(m, -1)
    zero += int((np.abs(rows).max(axis=1) == 0).sum())
    bad += int((~np.isfinite(rows)).any(axis=1).sum())
if zero or bad:
    problems.append(f"{zero} all-zero and {bad} non-finite waveform rows")

for p in problems:
    print(f"CHECK FAILED: {p}")
print(f"BB.bin       : {n} stations, nt {nt}, {size} bytes; "
      + ("every record and waveform as expected" if not problems else "NOT as expected"))
sys.exit(1 if problems else 0)
