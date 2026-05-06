"""Check HF.bin files for FiordSZ09 REL00-REL41 on the remote HPC."""

from pathlib import Path

from qcore import timeseries

BASE_DIR = Path("/scratch/projects/rch-quakecore/Cybershake/v26p4/Runs/FiordSZ09")
FAULT = "FiordSZ09"
REL_INDICES = range(0, 42)


def main() -> None:
    missing_count = 0
    corrupt_count = 0

    for nn in REL_INDICES:
        rel = f"REL{nn:02d}"
        hf_path = BASE_DIR / f"{FAULT}_{rel}" / "HF" / "Acc" / "HF.bin"

        if not hf_path.exists():
            print(f"{rel}: MISSING ({hf_path})")
            missing_count += 1
            continue

        try:
            timeseries.HFSeis(str(hf_path))
        except Exception as exc:
            print(f"{rel}: CORRUPT - {exc} ({hf_path})")
            corrupt_count += 1

    total = len(REL_INDICES)
    ok_count = total - missing_count - corrupt_count
    print()
    print("Summary:")
    print(f"  Total checked: {total}")
    print(f"  Missing:       {missing_count}")
    print(f"  Corrupt:       {corrupt_count}")
    print(f"  OK:            {ok_count}")


if __name__ == "__main__":
    main()
