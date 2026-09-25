"""Read-only pre-flight for the WellTeast canonical BB runs.

For each target, resolves the station set exactly as bb_sim.py will (the same
function on the same inputs) and checks it yields the canonical list, so no
array task can abort at start-up or index the wrong station. Also checks
that HF finished every listed station (hf_sim's own checkpoint: e_dist > 0),
bb_sim's LF/HF duration test, and that the vs30 file covers the list.
Reports whether a BB.bin is present, which must not be the case at submit.

Usage, in the job's own environment:
  source /nesi/project/nesi00213/Environments/mrd87_4/py311/bin/activate
  PYTHONPATH=/nesi/project/nesi00213/Environments/arr65_v26p6/workflow \\
    python preflight_wellteast_bb.py LIST TARGETS
"""

import sys
from pathlib import Path

import numpy as np
from qcore.timeseries import HFSeis, LFSeis

import workflow
from workflow.calculation.bb_station_set import read_station_list, resolve_station_set

VS30 = "/nesi/project/nesi00213/StationInfo/non_uniform_whole_nz_with_real_stations-hh400_v20p3_land.vs30"


def check(rel: Path, canon: list[str]):
    lf = LFSeis(str(rel / "LF" / "OutBin"))
    hf = HFSeis(str(rel / "HF" / "Acc" / "HF.bin"))
    shape = f"LF {lf.stations.size} records, HF {hf.stations.size}"
    try:
        s = resolve_station_set(lf.stations.name, hf.stations.name, station_list=canon)
    except Exception as error:
        return shape, [f"resolve failed: {error}"]

    problems = []
    if [str(n) for n in s.names] != canon:
        problems.append("resolved names differ from the list")
    if [str(n) for n in lf.stations.name[s.lf_idx]] != canon:
        problems.append("lf_idx points at the wrong names")
    if [str(n) for n in hf.stations.name[s.hf_idx]] != canon:
        problems.append("hf_idx points at the wrong names")
    n_unfinished = int((hf.stations.e_dist[s.hf_idx] <= 0).sum())
    if n_unfinished:
        problems.append(f"HF unfinished for {n_unfinished} listed station(s)")
    if not np.isclose(lf.dt * lf.nt + lf.start_sec, hf.dt * hf.nt, atol=min(lf.dt, hf.dt)):
        problems.append("LF/HF duration mismatch")

    lf_has_drop = "320077e" in {str(n) for n in lf.stations.name}
    detail = f"{shape} -> {len(s.names)}; LF has 320077e: {lf_has_drop}; " + " ".join(s.report)
    return detail, problems


def main():
    canon = read_station_list(sys.argv[1])
    targets = [Path(p) for p in Path(sys.argv[2]).read_text().split()]
    print(f"workflow imported from: {workflow.__file__}")
    print(f"list: {sys.argv[1]} ({len(canon)} names); targets: {len(targets)}")

    vs30 = np.loadtxt(VS30, dtype=[("name", "|U8"), ("vs30", "f4")], comments=("#", "%"))
    missing = sorted(set(canon) - set(vs30["name"]))
    print(f"vs30 file covers the list: {not missing} {missing[:5]}", flush=True)

    n_fail = 0
    for rel in targets:
        detail, problems = check(rel, canon)
        n_fail += bool(problems)
        status = "OK" if not problems else "FAIL: " + "; ".join(problems)
        bb = "BB.bin PRESENT" if (rel / "BB" / "Acc" / "BB.bin").exists() else "BB.bin absent"
        print(f"{rel.name:<16} {status} | {bb} | {detail}", flush=True)

    print(f"\n{len(targets) - n_fail} of {len(targets)} targets pass")
    sys.exit(1 if n_fail or missing else 0)


if __name__ == "__main__":
    main()
