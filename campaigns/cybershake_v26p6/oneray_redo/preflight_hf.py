"""Read-only pre-flight check of the OneRay HF redo commands (PalliserKai, WellTeast).

For every line of the commands file and the same line of the targets file, it
checks that the command:
  - runs mrd87_4's hf_sim.py under srun, reads the fault's FD_STATLIST, writes
    <target>/HF/Acc/HF.bin, and has exactly the expected options, with v26p6's
    root_params settings, Cant1D_v3-midQ_OneRay.1d and the pinned binary;
  - reproduces the original (leer) run:
      - every HF.bin header field the command sets (station count, nt, seed,
        duration, dt, sdrop, kappa, rvfac, rayset, path_dur, stoch name) equals
        the original header's;
      - the fields it leaves to hf_sim.py's defaults are equal in the original
        header and in a HikWgtnmax OneRay header, which used the defaults;
      - every station's name, lon and lat equal the original header's, in order;
      - the stoch file is byte-identical to the one the original run read (the
        path is in its HF.log).
It also reports each target's state:
  - "in place": the leer HF is still at HF/Acc (before move_aside.py);
  - "moved": the leer HF is at HF.leer/Acc and HF/ is absent, i.e. ready to run.
With --expect STATE, any target in another state is a failure. It also checks
that seeds and outputs are distinct and that nobackup has room.

Usage: python preflight_hf.py TARGETS COMMANDS [--expect in-place|moved]
"""

import argparse
import hashlib
import os
import shlex
import sys
from pathlib import Path

import numpy as np
import yaml

RUNS = Path("/nesi/nobackup/nesi00213/RunFolder/Cybershake/v26p6/Runs")
HF_SIM = "/nesi/project/nesi00213/Environments/mrd87_4/workflow/workflow/calculation/hf_sim.py"
MODEL = "/nesi/project/nesi00213/VelocityModel/Mod-1D/Cant1D_v3-midQ_OneRay.1d"
SIM_BIN = "/nesi/project/nesi00213/tools/hb_high_binmod_v5.4.5.3"
LEER = "Cant1D_v2-midQ_leer.1d"
# a completed run of the same hf_sim.py with its defaults (HikWgtnmax, 2026-09-27)
DEFAULTS_REF = "/nesi/nobackup/nesi00213/RunFolder/Cybershake/v26p5/Runs/HikWgtnmax/HikWgtnmax/HF/Acc/HF.bin"
PREFIX = ["srun", "--quit-on-interrupt", "--kill-on-bad-exit=1", "python", HF_SIM]
FIXED = {"--version": "5.4.5.3", "--dt": "0.005", "--sdrop": "50", "--kappa": "0.045", "--rvfac": "0.8",
         "--rayset": "1", "--path_dur": "11", "--hf_vel_mod_1d": MODEL, "--sim_bin": SIM_BIN}
OPTIONS = set(FIXED) | {"--duration", "--seed", "--slip"}
HEAD_SIZE, HEAD_STAT = 0x200, 0x18  # qcore HFSeis layout
I4 = ["nstat", "nt", "seed", "siteamp", "path_dur", "nrayset", "rayset1", "rayset2", "rayset3",
      "rayset4", "nbu", "ift", "nl_skip", "ic_flag", "seed_given", "site_specific"]
F4 = ["duration", "dt", "t_sec", "sdrop", "kappa", "qfexp", "fmax", "flo", "fhi", "rvfac",
      "rvfac_shal", "rvfac_deep", "czero", "calpha", "mom", "rupv", "vs_moho", "vp_sig", "vsh_sig",
      "rho_sig", "qs_sig", "fa_sig1", "fa_sig2", "rv_sig1"]
STAT = np.dtype({"names": ["lon", "lat", "name"], "formats": ["f4", "f4", "S8"], "offsets": [0, 4, 8],
                 "itemsize": HEAD_STAT})


def read_header(path, with_stations=False):
    with open(path, "rb") as f:
        head = dict(zip(I4, np.fromfile(f, "i4", len(I4)).tolist()))
        head.update(zip(F4, np.fromfile(f, "f4", len(F4)).tolist()))
        head["stoch"], head["model"] = (s.decode() for s in np.fromfile(f, "S64", 2))
        stations = None
        if with_stations:
            f.seek(HEAD_SIZE)
            stations = np.fromfile(f, STAT, head["nstat"])
    return head, stations


def md5(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def original_stoch(hf_log):
    """The stoch path in the first stdin block hf_sim.py logged."""
    with open(hf_log) as f:
        for line in f:
            if line.strip().endswith(".stoch"):
                return line.strip()
    return None


def state(rel):
    here, moved = rel / "HF" / "Acc" / "HF.bin", rel / "HF.leer" / "Acc" / "HF.bin"
    if here.exists() and not (rel / "HF.leer").exists():
        return "in place", here
    if moved.exists() and not (rel / "HF").exists():
        return "moved", moved
    return "unexpected", None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("targets")
    ap.add_argument("commands")
    ap.add_argument("--expect", choices=["in-place", "moved"])
    args = ap.parse_args()
    targets = [Path(t) for t in Path(args.targets).read_text().split()]
    lines = [ln for ln in Path(args.commands).read_text().splitlines() if ln.strip()]
    root_hf = yaml.safe_load((RUNS / "root_params.yaml").read_text())["hf"]
    ref, _ = read_header(DEFAULTS_REF)
    print(f"hf_sim.py md5 {md5(HF_SIM)}; model md5 {md5(MODEL)}; sim_bin md5 {md5(SIM_BIN)}, "
          f"executable={os.access(SIM_BIN, os.X_OK)}")
    print(f"defaults compared with {DEFAULTS_REF} (model {ref['model']})")

    problems_total, seeds, outs, states, need = 0, {}, {}, {}, 0
    stations_cache = {}
    if len(lines) != len(targets):
        print(f"FAIL: {len(lines)} commands for {len(targets)} targets")
        problems_total += 1
    for rel, line in zip(targets, lines):
        problems = []
        tokens = shlex.split(line)
        statlist, out, rest = tokens[5], tokens[6], tokens[7:]
        opts = dict(zip(rest[::2], rest[1::2]))
        st, orig_path = state(rel)
        states[st] = states.get(st, 0) + 1
        if args.expect and st != args.expect.replace("-", " "):
            problems.append(f"state {st}, expected {args.expect}")
        if tokens[:5] != PREFIX:
            problems.append("unexpected command prefix")
        fault_params = yaml.safe_load((rel.parent / "fault_params.yaml").read_text())
        if statlist != fault_params["FD_STATLIST"]:
            problems.append(f"station list {statlist} is not FD_STATLIST")
        if out != str(rel / "HF" / "Acc" / "HF.bin"):
            problems.append(f"output {out}")
        if len(rest) % 2 or set(opts) != OPTIONS or len(opts) != len(rest) // 2:
            problems.append(f"options {sorted(opts)}")
        for flag, value in FIXED.items():
            if opts.get(flag) != value:
                problems.append(f"{flag} {opts.get(flag)} != {value}")
        for key in ("dt", "version", "sdrop", "kappa", "rvfac", "rayset", "path_dur", "hf_vel_mod_1d"):
            if str(root_hf[key]) != opts.get(f"--{key}"):
                problems.append(f"--{key} {opts.get(f'--{key}')} differs from root_params {root_hf[key]}")
        if orig_path is None:
            problems.append("no original HF.bin found")
        else:
            orig, orig_stations = read_header(orig_path, with_stations=True)
            if orig["model"] != LEER:
                problems.append(f"original names {orig['model']}")
            if statlist not in stations_cache:
                stations_cache[statlist] = np.loadtxt(
                    statlist, ndmin=1, dtype=[("lon", "f4"), ("lat", "f4"), ("name", "|S8")])
            stations = stations_cache[statlist]
            dur, dt = np.float32(opts["--duration"]), np.float32(opts["--dt"])
            predicted = {"nstat": stations.size, "nt": int(round(float(opts["--duration"]) / float(opts["--dt"]))),
                         "seed": int(opts["--seed"]), "siteamp": 1, "path_dur": int(opts["--path_dur"]),
                         "nrayset": 1, "rayset1": int(opts["--rayset"]), "rayset2": 0, "rayset3": 0,
                         "rayset4": 0, "seed_given": 1, "site_specific": 0, "duration": float(dur),
                         "dt": float(dt), "sdrop": float(np.float32(opts["--sdrop"])),
                         "kappa": float(np.float32(opts["--kappa"])),
                         "rvfac": float(np.float32(opts["--rvfac"])), "stoch": Path(opts["--slip"]).name}
            problems += [f"{k}: command gives {v!r}, original has {orig[k]!r}"
                         for k, v in predicted.items() if orig[k] != v]
            problems += [f"default {k}: original {orig[k]!r}, hf_sim default {ref[k]!r}"
                         for k in I4 + F4 if k not in predicted and orig[k] != ref[k]]
            for key in ("lon", "lat", "name"):
                if orig_stations.size != stations.size or not np.array_equal(orig_stations[key], stations[key]):
                    problems.append(f"station {key}s differ from the original header")
            log = orig_path.parent / "HF.log"
            read = original_stoch(log)
            if read is None:
                problems.append(f"no stoch path in {log}")
            elif not Path(opts["--slip"]).is_file() or md5(opts["--slip"]) != md5(read):
                problems.append(f"--slip {opts['--slip']} is not byte-identical to {read}")
            need += orig_path.stat().st_size
        seeds.setdefault(opts.get("--seed"), []).append(rel.name)
        outs.setdefault(out, []).append(rel.name)
        problems_total += bool(problems)
        print(f"{rel.parent.name + '/' + rel.name:<30} {st:<10} "
              + ("OK" if not problems else "FAIL: " + "; ".join(problems)))

    for kind, seen in (("seed", seeds), ("output", outs)):
        for value, names in seen.items():
            if len(names) > 1:
                print(f"FAIL: {kind} {value} shared by {names}")
                problems_total += 1
    st = os.statvfs(RUNS)
    free = st.f_bavail * st.f_frsize
    # the new HF and BB outputs sit beside the moved-aside originals
    print(f"\nstates: {states}; new HF needs {need / 1e12:.2f} TB (BB about the same); "
          f"free {free / 1e12:.0f} TB")
    if 2 * need > free:
        print("FAIL: not enough free space")
        problems_total += 1
    print(f"all {len(targets)} OK" if not problems_total else f"{problems_total} problem(s)")
    sys.exit(1 if problems_total else 0)


if __name__ == "__main__":
    main()
