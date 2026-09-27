"""Move the leer outputs of the PalliserKai and WellTeast realisations aside before
the OneRay redo. A dry run unless --apply.

For each target, within its realisation directory, it renames whichever of these
exist:
    HF -> HF.leer, BB -> BB.leer, IM_calc -> IM_calc.leer,
    <rel>_im_calc.log -> <rel>_im_calc.log.leer
WellTeast_REL04 has only HF and an empty BB. Each rename stays in its directory,
so it is atomic: nothing is copied or deleted, and renaming back undoes it. BB.leer
takes Sung's BB/bb_sim_command.sh with it and, for the six WellTeast realisations
re-run on 2026-09-25, the BB.*.with_320077e originals.

Before renaming anything it checks every target and refuses the whole run if:
  - any HF/Acc/HF.bin is missing or its header doesn't name Cant1D_v2-midQ_leer.1d;
  - any .leer name already exists;
  - any realisation other than WellTeast_REL04 lacks a BB.bin or IM CSV.
With --apply, each rename is appended to MANIFEST as it happens (TSV: time, source,
destination, then size and mtime of every file under the source).

Usage: python move_aside.py TARGETS [--apply MANIFEST]
"""

import argparse
import datetime
import os
import sys
from pathlib import Path

LEER = b"Cant1D_v2-midQ_leer.1d"
NO_LF = {"WellTeast_REL04"}  # HF only: no LF, so no BB or IM


def model_name(hf_bin):
    with open(hf_bin, "rb") as f:
        f.seek(224)
        return f.read(64).split(b"\0")[0]


def plan(rel):
    """[(source, destination)] for one realisation, and any problems."""
    problems, moves = [], []
    hf_bin = rel / "HF" / "Acc" / "HF.bin"
    if not hf_bin.is_file():
        problems.append("no HF/Acc/HF.bin")
    elif model_name(hf_bin) != LEER:
        problems.append(f"HF.bin names {model_name(hf_bin).decode()}")
    if rel.name not in NO_LF:
        if not (rel / "BB" / "Acc" / "BB.bin").is_file():
            problems.append("no BB/Acc/BB.bin")
        if not (rel / "IM_calc" / f"{rel.name}.csv").is_file():
            problems.append("no IM CSV")
    for name in ("HF", "BB", "IM_calc", f"{rel.name}_im_calc.log"):
        src, dst = rel / name, rel / f"{name}.leer"
        if dst.exists():
            problems.append(f"{dst.name} already exists")
        if src.exists():
            moves.append((src, dst))
    return moves, problems


def inventory(path):
    """(relative path, size, mtime) of every file under path, or of path itself."""
    files = [path] if path.is_file() else sorted(p for p in path.rglob("*") if p.is_file())
    return [(str(p.relative_to(path.parent)), p.stat().st_size,
             datetime.datetime.fromtimestamp(p.stat().st_mtime).isoformat(timespec="seconds")) for p in files]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("targets")
    ap.add_argument("--apply", metavar="MANIFEST")
    args = ap.parse_args()
    targets = [Path(t) for t in Path(args.targets).read_text().split()]

    all_moves, n_problems = [], 0
    for rel in targets:
        moves, problems = plan(rel)
        n_problems += bool(problems)
        print(f"{rel.parent.name + '/' + rel.name:<30} "
              + ("FAIL: " + "; ".join(problems) if problems else ", ".join(f"{s.name} -> {d.name}" for s, d in moves)))
        all_moves += moves
    print(f"\n{len(targets)} targets, {len(all_moves)} renames planned, {n_problems} with problems")
    if n_problems:
        sys.exit("refusing: fix the problems above first; nothing was renamed")
    if not args.apply:
        print("dry run: nothing was renamed (use --apply MANIFEST)")
        return

    with open(args.apply, "a") as manifest:
        for src, dst in all_moves:
            files = inventory(src)
            # rename(2) would silently replace an empty directory or a file
            if dst.exists() or dst.is_symlink():
                sys.exit(f"refusing: {dst} appeared since the check; earlier renames are in the manifest")
            os.rename(src, dst)
            stamp = datetime.datetime.now().isoformat(timespec="seconds")
            manifest.write(f"{stamp}\t{src}\t{dst}\t"
                           + ";".join(f"{name}:{size}:{mtime}" for name, size, mtime in files) + "\n")
            manifest.flush()
    print(f"renamed {len(all_moves)}; manifest {args.apply}")


if __name__ == "__main__":
    main()
