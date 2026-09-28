"""Write the HikWgtnmax BB targets and stage each realisation's e3d.par.

Each realisation's LF came from Cascade as a NetCDF plus a small `*_other.tar.gz`
holding its e3d.par, rank-0 rlog and SlipOut. bb_sim's LF NetCDF reader needs the
e3d.par. This extracts each one to STAGE/e3d_par/<realisation>_e3d.par, refusing
to replace a different file already there, and checks that:
  - the rlog's last line says EMOD3D finished;
  - e3d.par gives nt 71601, dt 0.005, flo 1.0, h 0.1 and the model grid of the
    VM's vm_params.yaml;
  - the NetCDF for the realisation exists at its expected size.
Writes TARGETS (median first, then REL01-REL50) and a TSV of what was staged,
and reads the tarballs only.

Usage: python make_bb_inputs.py TARGETS STAGED_TSV
"""

import hashlib
import sys
import tarfile
from pathlib import Path

import yaml

RUNS = Path("/nesi/nobackup/nesi00213/RunFolder/Cybershake/v26p5/Runs")
FAULT = RUNS / "HikWgtnmax"
STAGE = RUNS / "HikWgtnmax_LF_from_Cascade"
VM_PARAMS = Path("/nesi/nobackup/nesi00213/RunFolder/Cybershake/v26p5/Data/VMs/HikWgtnmax/vm_params.yaml")
NETCDF_SIZE = 14767960182


def parse_par(text):
    return {k.strip(): v.strip().strip('"') for k, _, v in (ln.partition("=") for ln in text.splitlines() if "=" in ln)}


def main():
    targets_out, staged_out = sys.argv[1:3]
    vm = yaml.safe_load(VM_PARAMS.read_text())
    names = ["HikWgtnmax"] + [f"HikWgtnmax_REL{i:02d}" for i in range(1, 51)]
    (STAGE / "e3d_par").mkdir(exist_ok=True)
    rows, problems = [], 0
    for name in names:
        issues = []
        with tarfile.open(STAGE / "other" / f"{name}_other.tar.gz") as tar:
            par_bytes = tar.extractfile("e3d.par").read()
            rlogs = [m for m in tar.getmembers() if m.name.startswith("Rlog/") and m.name.endswith(".rlog")]
            last = tar.extractfile(rlogs[0]).read().decode(errors="replace").strip().splitlines()[-1] if rlogs else ""
        par = parse_par(par_bytes.decode())
        if "FINISHED" not in last:
            issues.append(f"rlog does not end with FINISHED: {last[:60]!r}")
        expected = {"nt": "71601", "dt": "0.005", "flo": "1.0", "h": "0.1",
                    "nx": str(vm["nx"]), "ny": str(vm["ny"]), "nz": str(vm["nz"])}
        issues += [f"{k}={par.get(k)} (expected {v})" for k, v in expected.items() if par.get(k) != v]
        if abs(float(par.get("modelrot", "nan")) - vm["MODEL_ROT"]) > 1e-6:
            issues.append(f"modelrot {par.get('modelrot')} vs vm_params {vm['MODEL_ROT']}")
        nc = STAGE / f"{name}_seis.nc"
        if not nc.is_file() or nc.stat().st_size != NETCDF_SIZE:
            issues.append(f"{nc.name} missing or not {NETCDF_SIZE} bytes")
        dest = STAGE / "e3d_par" / f"{name}_e3d.par"
        if dest.exists() and dest.read_bytes() != par_bytes:
            issues.append(f"{dest} exists with different content - not replaced")
        elif not issues:
            dest.write_bytes(par_bytes)
        md5 = hashlib.md5(par_bytes).hexdigest()
        rows.append(f"{name}\t{FAULT / name}\t{nc}\t{dest}\t{md5}\t{par.get('version')}\t{last[-40:]}")
        problems += bool(issues)
        print(f"{name:18s} {'OK' if not issues else 'FAIL: ' + '; '.join(issues)}")
    Path(targets_out).write_text("".join(f"{FAULT / n}\n" for n in names))
    Path(staged_out).write_text("name\trealisation\tnetcdf\te3d_par\te3d_par_md5\temod3d_version\trlog_end\n"
                                + "\n".join(rows) + "\n")
    print(f"\n{len(names)} realisations, {problems} with problems; wrote {targets_out} and {staged_out}")
    sys.exit(1 if problems else 0)


if __name__ == "__main__":
    main()
