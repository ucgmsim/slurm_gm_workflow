"""Write the canonical WellTeast BB station list: HF station order minus 320077e.

Reads the HF.bin header of every WellTeast realisation (and the fault's FD
station list, for comparison) and writes nothing but OUT. Refuses unless all
HF headers carry the same names in the same order, so the list does not
depend on which realisation it was taken from.

Usage: python make_wellteast_station_list.py OUT
"""

import hashlib
import sys
from pathlib import Path

from qcore.timeseries import HFSeis

FAULT = Path("/nesi/nobackup/nesi00213/RunFolder/Cybershake/v26p6/Runs/WellTeast")
FD_LL = FAULT / "fd_rt01-h0.100.ll"
DROP = "320077e"


def main():
    out = Path(sys.argv[1])
    if out.exists():
        sys.exit(f"{out} already exists - refusing to overwrite")

    rels = [FAULT / "WellTeast"] + sorted(FAULT.glob("WellTeast_REL[0-9][0-9]"))
    orders = {
        rel.name: [str(n) for n in HFSeis(str(rel / "HF" / "Acc" / "HF.bin")).stations.name]
        for rel in rels
    }
    ref_name, ref = next(iter(orders.items()))
    differ = [name for name, names in orders.items() if names != ref]
    print(f"HF headers read: {len(orders)}; identical order to {ref_name}: {len(orders) - len(differ)}")
    if differ:
        sys.exit(f"HF station order differs in: {differ}")
    if len(set(ref)) != len(ref) or "" in ref:
        sys.exit("HF names are not unique and non-blank")
    if ref.count(DROP) != 1:
        sys.exit(f"{DROP} appears {ref.count(DROP)} times in HF, expected once")

    fd = [line.split()[2] for line in FD_LL.read_text().splitlines() if line.strip()]
    print(f"FD list {FD_LL.name}: {len(fd)} names; same names and order as HF: {fd == ref}")

    canon = [n for n in ref if n != DROP]
    header = [
        f"# Canonical BB station set for Cybershake v26p6 WellTeast: {len(canon)} stations.",
        f"# HF.bin station order (identical in all {len(orders)} realisations) with {DROP} removed.",
        f"# {DROP} is missing from LF in most realisations (lost at an EMOD3D MPI domain",
        "# boundary), so it is dropped from every realisation to keep the set uniform.",
        "# Decision and rationale: docs/superpowers/specs/2026-09-21-bb-station-reconciliation-design.md",
        "# on branch nesi-cybershake-v26p6 of ucgmsim/slurm_gm_workflow.",
    ]
    out.write_text("\n".join(header + canon) + "\n")
    print(f"wrote {out}: {len(canon)} names, md5 {hashlib.md5(out.read_bytes()).hexdigest()}")


if __name__ == "__main__":
    main()
