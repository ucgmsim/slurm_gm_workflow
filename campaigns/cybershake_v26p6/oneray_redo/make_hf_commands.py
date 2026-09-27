"""Build the OneRay HF redo commands for PalliserKai and WellTeast (Cybershake v26p6).

One command per realisation, in array order: the PalliserKai median, REL01-REL37,
then the WellTeast median, REL01-REL32 (71; WellTeast_REL04 has HF but no LF).

Each command reproduces the realisation's original run except for the 1D model,
in the format of Sung's generator (hpc3_submit/run_hf_command.py). The generator
cannot be rerun here: the v26p6 tree has the stoch files but not the SRFs.
  - seed and duration come from the original HF.bin header; the duration is the
    shortest decimal that gives back the header's float32;
  - dt, version, sdrop, kappa, rvfac, rayset and path_dur come from v26p6's
    root_params.yaml, which also names the OneRay model;
  - the station list is the fault's FD_STATLIST and the stoch file is the one
    sim_params.yaml names (v26p6's copy);
  - --hf_vel_mod_1d names Cant1D_v3-midQ_OneRay.1d and --sim_bin pins the binary,
    as for HikWgtnmax.
preflight_hf.py checks every command against the original run.

The original header is read from HF/Acc/HF.bin, or from HF.leer/Acc/HF.bin once
move_aside.py has run. Writes targets.txt and hf_commands.txt in the current
directory and reads everything else.
"""

from pathlib import Path

import numpy as np
import yaml

RUNS = Path("/nesi/nobackup/nesi00213/RunFolder/Cybershake/v26p6/Runs")
HF_SIM = "/nesi/project/nesi00213/Environments/mrd87_4/workflow/workflow/calculation/hf_sim.py"
MODEL = "/nesi/project/nesi00213/VelocityModel/Mod-1D/Cant1D_v3-midQ_OneRay.1d"
SIM_BIN = "/nesi/project/nesi00213/tools/hb_high_binmod_v5.4.5.3"
LEER = "Cant1D_v2-midQ_leer.1d"
PREFIX = f"srun --quit-on-interrupt --kill-on-bad-exit=1 python {HF_SIM}"
FAULTS = {"PalliserKai": 37, "WellTeast": 32}


def targets():
    for fault, n_rel in FAULTS.items():
        yield RUNS / fault / fault
        for i in range(1, n_rel + 1):
            yield RUNS / fault / f"{fault}_REL{i:02d}"


def original_hf(rel):
    """The original (leer) run's HF.bin, moved aside or still in place."""
    moved = rel / "HF.leer" / "Acc" / "HF.bin"
    return moved if moved.exists() else rel / "HF" / "Acc" / "HF.bin"


def read_header(path):
    """qcore HFSeis header: 16 int32, 24 float32, then the stoch and 1D model names."""
    with open(path, "rb") as f:
        i4 = np.fromfile(f, "i4", 16)
        f4 = np.fromfile(f, "f4", 24)
        stoch, model = (s.decode() for s in np.fromfile(f, "S64", 2))
    return i4, f4, stoch, model


def main():
    root_hf = yaml.safe_load((RUNS / "root_params.yaml").read_text())["hf"]
    assert root_hf["hf_vel_mod_1d"] == MODEL, root_hf["hf_vel_mod_1d"]
    lines, rels = [], []
    for rel in targets():
        statlist = yaml.safe_load((rel.parent / "fault_params.yaml").read_text())["FD_STATLIST"]
        stoch = yaml.safe_load((rel / "sim_params.yaml").read_text())["hf"]["slip"]
        i4, f4, stoch_name, model = read_header(original_hf(rel))
        assert model == LEER, f"{rel.name}: original names {model}"
        assert stoch_name == Path(stoch).name == f"{rel.name}.stoch", f"{rel.name}: stoch {stoch_name} vs {stoch}"
        duration = np.format_float_positional(f4[0], unique=True, trim="-")
        assert np.float32(duration) == f4[0]
        lines.append(
            f"{PREFIX} {statlist} {rel}/HF/Acc/HF.bin --duration {duration} --dt {root_hf['dt']} "
            f"--seed {int(i4[2])} --version {root_hf['version']} --slip {stoch} --sdrop {root_hf['sdrop']} "
            f"--kappa {root_hf['kappa']} --rvfac {root_hf['rvfac']} --rayset {root_hf['rayset']} "
            f"--path_dur {root_hf['path_dur']} --hf_vel_mod_1d {MODEL} --sim_bin {SIM_BIN}")
        rels.append(str(rel))
    Path("targets.txt").write_text("\n".join(rels) + "\n")
    Path("hf_commands.txt").write_text("\n".join(lines) + "\n")
    print(f"wrote {len(rels)} targets to targets.txt and {len(lines)} commands to hf_commands.txt")


if __name__ == "__main__":
    main()
