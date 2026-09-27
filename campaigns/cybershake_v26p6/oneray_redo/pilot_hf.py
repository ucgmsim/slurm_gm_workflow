"""One-station pilots of the OneRay HF redo commands, compared with the original
(leer) runs. Writes only under pilot/ in the current directory.

For each (array task, station index) it runs that task's command without srun,
on a one-line station list holding that station's line of the FD list. Station 0
uses the command unchanged, so the pilot header must equal the original's in
every field but the station count and the 1D model. Any other station uses the
seed hf_sim.py gave it in the full run (base + index), so its waveform is
comparable too; only the header's seed then differs.

For every pilot it compares station records (e_dist, Vs) and waveforms with the
original's, reporting each component's peak ratio (OneRay / leer), the
correlation, and the largest difference (or bit-identity). PILOT_MODEL=<1D model>
swaps the model, e.g. to rerun the leer original on the same machine.

Usage: python pilot_hf.py TARGETS COMMANDS TASK:STATION [TASK:STATION ...]
"""

import os
import shlex
import subprocess
import sys
from pathlib import Path

import numpy as np

HEAD_SIZE, HEAD_STAT = 0x200, 0x18
STAT = np.dtype({"names": ["lon", "lat", "name", "e_dist", "vs"], "formats": ["f4", "f4", "S8", "f4", "f4"],
                 "offsets": [0, 4, 8, 16, 20], "itemsize": HEAD_STAT})


def header(path):
    with open(path, "rb") as f:
        i4 = np.fromfile(f, "i4", 16)
        f4 = np.fromfile(f, "f4", 24)
        names = [s.decode() for s in np.fromfile(f, "S64", 2)]
        f.seek(HEAD_SIZE)
        stations = np.fromfile(f, STAT, int(i4[0]))
    return i4, f4, names, stations


def waveform(path, i4, idx):
    nstat, nt = int(i4[0]), int(i4[1])
    with open(path, "rb") as f:
        f.seek(HEAD_SIZE + nstat * HEAD_STAT + idx * nt * 3 * 4)
        return np.fromfile(f, "f4", nt * 3).reshape(nt, 3)


def main():
    targets = Path(sys.argv[1]).read_text().split()
    commands = Path(sys.argv[2]).read_text().splitlines()
    failures = 0
    for spec in sys.argv[3:]:
        task, idx = (int(x) for x in spec.split(":"))
        rel = Path(targets[task])
        tokens = shlex.split(commands[task])
        hf_sim, statlist, opts = tokens[4], tokens[5], tokens[7:]
        station_line = Path(statlist).read_text().splitlines()[idx]
        orig_path = rel / "HF" / "Acc" / "HF.bin"
        if not orig_path.exists():
            orig_path = rel / "HF.leer" / "Acc" / "HF.bin"
        oi4, of4, onames, ostations = header(orig_path)
        # PILOT_MODEL swaps the 1D model, e.g. to rerun the leer original on this machine
        model = os.environ.get("PILOT_MODEL")
        if model:
            opts[opts.index("--hf_vel_mod_1d") + 1] = model
        if idx:
            k = opts.index("--seed")
            opts[k + 1] = str(int(opts[k + 1]) + idx)
        out_dir = Path("pilot") / f"{rel.name}_{idx}{'_' + Path(model).stem if model else ''}"
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "station.ll").write_text(station_line + "\n")
        out = out_dir / "HF.bin"
        out.unlink(missing_ok=True)
        run = subprocess.run([sys.executable, hf_sim, str(out_dir / "station.ll"), str(out)] + opts,
                             capture_output=True, text=True)
        if run.returncode:
            print(f"{rel.name} station {idx}: hf_sim.py failed\n{run.stdout[-2000:]}{run.stderr[-2000:]}")
            failures += 1
            continue
        pi4, pf4, pnames, pstations = header(out)
        problems = []
        i4_diff = [j for j in range(16) if pi4[j] != oi4[j] and j != 0 and not (idx and j == 2)]
        if i4_diff or not np.array_equal(pf4, of4) or pnames[0] != onames[0]:
            problems.append(f"header differs: int fields {i4_diff}, float fields "
                            f"{[j for j in range(24) if pf4[j] != of4[j]]}, stoch {pnames[0]} vs {onames[0]}")
        if pnames[1] != Path(model or "Cant1D_v3-midQ_OneRay.1d").name or onames[1] != "Cant1D_v2-midQ_leer.1d":
            problems.append(f"models {pnames[1]} (pilot) vs {onames[1]} (original)")
        p, o = pstations[0], ostations[idx]
        for key in ("lon", "lat", "name", "e_dist", "vs"):
            if p[key] != o[key]:
                problems.append(f"station {key}: pilot {p[key]!r}, original {o[key]!r}")
        new, old = waveform(out, pi4, 0), waveform(orig_path, oi4, idx)
        ratio = np.abs(new).max(axis=0) / np.abs(old).max(axis=0)
        corr = [np.corrcoef(new[:, c], old[:, c])[0, 1] for c in range(3)]
        same = "bit-identical to leer" if np.array_equal(new, old) else f"max |OneRay - leer| {np.abs(new - old).max():.3g}"
        failures += bool(problems)
        print(f"{rel.name} station {idx} ({o['name'].decode()}, e_dist {o['e_dist']:.1f} km): "
              + ("header and station record as expected" if not problems else "FAIL: " + "; ".join(problems))
              + f"; peak OneRay/leer {', '.join(f'{r:.2f}' for r in ratio)}; "
              f"correlation {', '.join(f'{c:.6f}' for c in corr)}; {same}; "
              f"leer peak {np.abs(old).max():.1f} cm/s^2")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
