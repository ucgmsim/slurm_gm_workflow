"""Read-only inventory of the leer-era outputs the OneRay redo replaced, for review before deletion.

For each target it lists what a deletion would remove, with sizes:
  - HF.leer, BB.leer (including the BB.*.with_320077e originals) and
    IM_calc.leer;
  - <rel>_im_calc.log.leer;
  - BB/Acc/BB.{bin,log}.corrupt_resume, the corrupt resumes of 2026-09-28.
Before any of it is deleted, it confirms that the realisation's replacement is
in place:
  - HF names OneRay and says "Simulation completed";
  - BB.bin is the size of the BB.leer original;
  - the IM CSV exists (except for WellTeast_REL04, which has no LF).

Usage: python inventory_leer.py TARGETS [DELETION_LIST]
With DELETION_LIST it also writes the paths (one per line) for review.
"""

import sys
from pathlib import Path

NO_LF = {"WellTeast_REL04"}


def size(path: Path) -> int:
    if path.is_file():
        return path.stat().st_size
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


def main():
    targets = [Path(t) for t in Path(sys.argv[1]).read_text().split()]
    paths, total, not_ready = [], 0, []
    for rel in targets:
        items = [rel / "HF.leer", rel / "BB.leer", rel / "IM_calc.leer", rel / f"{rel.name}_im_calc.log.leer",
                 rel / "BB" / "Acc" / "BB.bin.corrupt_resume", rel / "BB" / "Acc" / "BB.log.corrupt_resume"]
        items = [p for p in items if p.exists()]
        hf = rel / "HF" / "Acc" / "HF.bin"
        ready = hf.is_file() and b"OneRay" in open(hf, "rb").read(288)[224:288] \
            and b"Simulation completed" in (rel / "HF" / "Acc" / "HF.log").read_bytes()[-65536:]
        if rel.name not in NO_LF:
            bb, orig = rel / "BB" / "Acc" / "BB.bin", rel / "BB.leer" / "Acc" / "BB.bin"
            ready = ready and bb.is_file() and orig.is_file() and bb.stat().st_size == orig.stat().st_size \
                and (rel / "IM_calc" / f"{rel.name}.csv").is_file()
        if not ready:
            not_ready.append(rel.name)
        sizes = {p: size(p) for p in items}
        total += sum(sizes.values())
        paths += items
        print(f"{rel.parent.name + '/' + rel.name:30s} {sum(sizes.values()) / 1e9:7.1f} GB  "
              f"{'replacement in place' if ready else 'REPLACEMENT NOT READY'}  "
              + ", ".join(f"{p.relative_to(rel)}" for p in items))
    print(f"\n{len(paths)} paths, {total / 1e12:.2f} TB; replacements not ready: {not_ready or 'none'}")
    if len(sys.argv) > 2:
        Path(sys.argv[2]).write_text("".join(f"{p}\n" for p in paths))
        print(f"wrote {sys.argv[2]}")
    sys.exit(1 if not_ready else 0)


if __name__ == "__main__":
    main()
