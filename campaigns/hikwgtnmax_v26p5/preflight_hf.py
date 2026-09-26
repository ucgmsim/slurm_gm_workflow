"""Read-only pre-flight check of the HikWgtnmax HF commands.

For every line of the commands file (median first, then REL01-REL50) it checks
that the command:
  - runs mrd87_4's hf_sim.py (the code Sung's v26p6 HF runs used) under srun;
  - uses the fault's FD station list and writes to that realisation's
    HF/Acc/HF.bin, which must not exist yet;
  - carries the realisation's own seed (Sung's generator: md5 of the SRF file
    name) and stoch file, and v26p5's HF settings;
  - names Cant1D_v3-midQ_OneRay.1d and the pinned hb_high binary explicitly.
It also checks the seeds and outputs are all distinct, and that nobackup has
room for every output.

Usage: python preflight_hf.py COMMANDS_FILE
"""

import hashlib
import os
import shlex
import sys
from pathlib import Path

import yaml

RUNS = Path("/nesi/nobackup/nesi00213/RunFolder/Cybershake/v26p5/Runs")
FAULT = RUNS / "HikWgtnmax"
HF_SIM = "/nesi/project/nesi00213/Environments/mrd87_4/workflow/workflow/calculation/hf_sim.py"
MODEL = "/nesi/project/nesi00213/VelocityModel/Mod-1D/Cant1D_v3-midQ_OneRay.1d"
SIM_BIN = "/nesi/project/nesi00213/tools/hb_high_binmod_v5.4.5.3"
PREFIX = ["srun", "--quit-on-interrupt", "--kill-on-bad-exit=1", "python", HF_SIM]
EXPECTED = {"--duration": "355.005", "--dt": "0.005", "--version": "5.4.5.3", "--sdrop": "50",
            "--kappa": "0.045", "--rvfac": "0.8", "--rayset": "1", "--path_dur": "11",
            "--hf_vel_mod_1d": MODEL, "--sim_bin": SIM_BIN}
HEAD_SIZE, HEAD_STAT = 0x200, 0x18  # qcore HFSeis layout


def md5(path):
    with open(path, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()


def generator_seed(srf_file):
    return str(int(hashlib.md5(os.path.basename(srf_file).encode()).hexdigest()[:8], 16) & 0x7FFFFFFF)


def parse(line):
    tokens = shlex.split(line)
    if tokens[:5] != PREFIX:
        return None, None, None, f"does not start with {' '.join(PREFIX)}"
    statlist, out, rest = tokens[5], tokens[6], tokens[7:]
    if len(rest) % 2:
        return None, None, None, "options are not flag/value pairs"
    opts = dict(zip(rest[::2], rest[1::2]))
    if len(opts) != len(rest) // 2:
        return None, None, None, "an option is repeated"
    return statlist, out, opts, None


def main():
    lines = [ln for ln in Path(sys.argv[1]).read_text().splitlines() if ln.strip()]
    targets = [FAULT / "HikWgtnmax"] + [FAULT / f"HikWgtnmax_REL{i:02d}" for i in range(1, 51)]
    root_hf = yaml.safe_load((RUNS / "root_params.yaml").read_text())["hf"]
    statlist_path = FAULT / "fd_rt01-h0.100.ll"
    stations = [ln.split()[2] for ln in statlist_path.read_text().splitlines() if ln.strip()]
    nstat = len(stations)
    nt = int(round(float(EXPECTED["--duration"]) / float(EXPECTED["--dt"])))
    size = HEAD_SIZE + nstat * HEAD_STAT + nstat * nt * 3 * 4

    print(f"hf_sim.py md5 {md5(HF_SIM)}")
    print(f"1D model     md5 {md5(MODEL)}  first layer: {Path(MODEL).read_text().splitlines()[1].split()}")
    print(f"sim_bin      md5 {md5(SIM_BIN)}  executable={os.access(SIM_BIN, os.X_OK)}")
    print(f"station list {statlist_path}: {nstat} stations, {len(set(stations))} distinct names")
    print(f"expected HF.bin size {size} bytes (nt {nt}); {len(targets)} outputs need {size * len(targets) / 1e9:.0f} GB")
    st = os.statvfs(RUNS)
    free = st.f_bavail * st.f_frsize
    print(f"free on {RUNS}: {free / 1e12:.1f} TB\n")

    problems_total, seeds, outs = 0, {}, {}
    if len(lines) != len(targets):
        print(f"FAIL: {len(lines)} commands for {len(targets)} targets")
        problems_total += 1
    for rel, line in zip(targets, lines):
        problems = []
        statlist, out, opts, err = parse(line)
        if err:
            problems.append(err)
        else:
            sim = yaml.safe_load((rel / "sim_params.yaml").read_text())
            if statlist != str(statlist_path):
                problems.append(f"station list {statlist}")
            if out != str(rel / "HF" / "Acc" / "HF.bin"):
                problems.append(f"output {out}")
            elif Path(out).exists():
                problems.append("HF.bin already exists")
            for flag, value in EXPECTED.items():
                if opts.get(flag) != value:
                    problems.append(f"{flag} {opts.get(flag)} != {value}")
            for key in ("dt", "version", "sdrop", "kappa", "rvfac", "rayset", "path_dur"):
                if str(root_hf[key]) != opts.get(f"--{key}"):
                    problems.append(f"--{key} {opts.get(f'--{key}')} differs from root_params {root_hf[key]}")
            if opts.get("--seed") != generator_seed(sim["srf_file"]):
                problems.append(f"--seed {opts.get('--seed')} != generator seed {generator_seed(sim['srf_file'])}")
            if opts.get("--slip") != sim["hf"]["slip"] or not Path(opts.get("--slip", "")).is_file():
                problems.append(f"--slip {opts.get('--slip')} is not sim_params' existing stoch file")
            if not Path(sim["srf_file"]).is_file():
                problems.append("SRF missing")
            unknown = set(opts) - set(EXPECTED) - {"--seed", "--slip"}
            if unknown:
                problems.append(f"unexpected options {sorted(unknown)}")
            seeds.setdefault(opts.get("--seed"), []).append(rel.name)
            outs.setdefault(out, []).append(rel.name)
        problems_total += bool(problems)
        print(f"{rel.name:<18} {'OK' if not problems else 'FAIL: ' + '; '.join(problems)}")

    for kind, seen in (("seed", seeds), ("output", outs)):
        for value, names in seen.items():
            if len(names) > 1:
                print(f"FAIL: {kind} {value} shared by {names}")
                problems_total += 1
    if size * len(targets) > free:
        print("FAIL: not enough free space")
        problems_total += 1
    print(f"\nall {len(targets)} OK" if not problems_total else f"\n{problems_total} problem(s)")
    sys.exit(1 if problems_total else 0)


if __name__ == "__main__":
    main()
