"""Delete the leer-era outputs the OneRay redo replaced. A dry run unless --apply.

Deletes exactly the paths in DELETION_LIST, which inventory_leer.py wrote for
review. Before deleting anything it re-checks, for every path, that:
  - it is under the v26p6 Runs folder and its name ends in .leer or
    .corrupt_resume;
  - its realisation's OneRay replacement is in place, by re-running the checks
    of inventory_leer.py.
Any failure stops the run before a single deletion. With --apply, each path is
logged to LOG (time, path, bytes) just before it is removed.

Usage: python delete_leer.py TARGETS DELETION_LIST [--apply LOG]
"""

import argparse
import datetime
import shutil
import subprocess
import sys
from pathlib import Path

RUNS = Path("/nesi/nobackup/nesi00213/RunFolder/Cybershake/v26p6/Runs")
HERE = Path(__file__).resolve().parent


def size(path: Path) -> int:
    if path.is_file():
        return path.stat().st_size
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("targets")
    ap.add_argument("deletion_list")
    ap.add_argument("--apply", metavar="LOG")
    args = ap.parse_args()

    paths = [Path(p) for p in Path(args.deletion_list).read_text().split()]
    bad = [p for p in paths if RUNS not in p.parents or not (p.name.endswith(".leer") or p.name.endswith(".corrupt_resume"))]
    if bad:
        sys.exit(f"refusing: {len(bad)} listed paths are not leer-era outputs under {RUNS}, e.g. {bad[:2]}")
    # the replacements must still be in place (inventory_leer.py exits non-zero if not)
    check = subprocess.run([sys.executable, str(HERE / "inventory_leer.py"), args.targets],
                           capture_output=True, text=True)
    if check.returncode:
        sys.exit("refusing: a replacement is not in place:\n" + check.stdout[-2000:])
    missing = [p for p in paths if not p.exists()]
    total = sum(size(p) for p in paths if p.exists())
    print(f"{len(paths)} paths listed, {len(missing)} already gone, {total / 1e12:.2f} TB to delete; "
          "every replacement in place")
    if not args.apply:
        print("dry run: nothing deleted (use --apply LOG)")
        return
    with open(args.apply, "a") as log:
        for p in paths:
            if not p.exists():
                continue
            log.write(f"{datetime.datetime.now().isoformat(timespec='seconds')}\t{p}\t{size(p)}\n")
            log.flush()
            if p.is_dir():
                shutil.rmtree(p)
            else:
                p.unlink()
    print(f"deleted {len(paths) - len(missing)} paths; log {args.apply}")


if __name__ == "__main__":
    main()
