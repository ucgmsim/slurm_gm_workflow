"""Compare two BB.bin files of the same realisation, e.g. bb_sim run from an LF
NetCDF against bb_sim run from the OutBin it came from.

Read-only. The headers must be identical apart from the LF source path, and so
must every station record. For the waveforms it reports, over all stations, the
largest |a - b| as a fraction of b's peak, and how many stations exceed TOL.

Usage: python compare_bb_files.py A_BB_BIN B_BB_BIN [TOL]
"""

import sys

import numpy as np

a_path, b_path = sys.argv[1:3]
tol = float(sys.argv[3]) if len(sys.argv) > 3 else 1e-5
HEAD_SIZE, HEAD_STAT = 0x500, 44
problems = []


def header(path):
    with open(path, "rb") as f:
        i4 = np.fromfile(f, "i4", 2)
        f4 = np.fromfile(f, "f4", 3)
        s = [x.split(b"\0")[0].decode() for x in np.fromfile(f, "S256", 3)]
        f.seek(HEAD_SIZE)
        stations = f.read(int(i4[0]) * HEAD_STAT)
    return i4, f4, s, stations


ai4, af4, as_, astat = header(a_path)
bi4, bf4, bs, bstat = header(b_path)
if not (np.array_equal(ai4, bi4) and np.array_equal(af4, bf4)):
    problems.append(f"header numbers differ: {ai4}/{af4} vs {bi4}/{bf4}")
for label, x, y in zip(("lf_dir", "lf_vm", "hf_file"), as_, bs):
    print(f"{label:8s} {'same' if x == y else 'differs'}: {x}" + ("" if x == y else f" vs {y}"))
    if x != y and label != "lf_dir":
        problems.append(f"{label} differs")
if astat != bstat:
    problems.append("station records differ")
n, nt = int(ai4[0]), int(ai4[1])
print(f"{n} stations, nt {nt}; station records {'identical' if astat == bstat else 'DIFFERENT'}")

head = HEAD_SIZE + n * HEAD_STAT
row = nt * 3
worst, worst_at, over, zero_rows, chunk = 0.0, -1, 0, 0, 400
for start in range(0, n, chunk):
    m = min(chunk, n - start)
    a = np.fromfile(a_path, "f4", m * row, offset=head + start * row * 4).reshape(m, row)
    b = np.fromfile(b_path, "f4", m * row, offset=head + start * row * 4).reshape(m, row)
    peak = np.abs(b).max(axis=1)
    zero_rows += int((np.abs(a).max(axis=1) == 0).sum())
    rel = np.abs(a - b).max(axis=1) / np.where(peak > 0, peak, 1)
    over += int((rel > tol).sum())
    j = int(rel.argmax())
    if rel[j] > worst:
        worst, worst_at = float(rel[j]), start + j
print(f"waveforms: worst |a - b| / peak {worst:.2e} (station row {worst_at}); "
      f"{over} stations above {tol:g}; {zero_rows} all-zero rows in {a_path}")
if over or zero_rows:
    problems.append(f"{over} stations differ by more than {tol:g}, {zero_rows} zero rows")
print("RESULT:", "OK" if not problems else "PROBLEMS: " + "; ".join(problems))
sys.exit(1 if problems else 0)
