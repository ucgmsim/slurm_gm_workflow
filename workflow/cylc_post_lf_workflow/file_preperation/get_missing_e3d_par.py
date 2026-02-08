import os
import subprocess
from pathlib import Path

BASE_DIR = Path("/home/arr65/data/Cybershake/v25p11_configs_params")

REMOTE_HOST = "cascade"
REMOTE_BASE = "/uoc/project/uoc40001/scratch/baes/Cybershake/v25p11/Runs"

# Counters
missing_count = 0
available_count = 0
not_available_count = 0
missing_files = []

# List all faults (top-level directories)
faults = sorted(
    entry.name for entry in BASE_DIR.iterdir() if entry.is_dir()
)

for fault in faults:
    fault_dir = BASE_DIR / fault

    # List all realizations within this fault
    realizations = sorted(
        entry.name for entry in fault_dir.iterdir() if entry.is_dir()
    )

    for realization in realizations:
        e3d_par_file_path = fault_dir / realization / "LF" / "e3d.par"

        if not e3d_par_file_path.exists():
            missing_count += 1
            missing_files.append(str(e3d_par_file_path))
            remote_path = f"{REMOTE_HOST}:{REMOTE_BASE}/{fault}/{realization}/LF/e3d.par"
            local_dest = e3d_par_file_path.parent

            # Ensure the local LF directory exists
            local_dest.mkdir(parents=True, exist_ok=True)

            print(f"Missing: {e3d_par_file_path}")
            print(f"  Would fetch from {remote_path}")

            # Dry run: rsync commented out
            # result = subprocess.run(
            #     ["rsync", "-avz", remote_path, str(local_dest) + "/"],
            #     capture_output=True,
            #     text=True,
            # )
            #
            # if result.returncode == 0:
            #     available_count += 1
            #     print(f"  Successfully retrieved e3d.par for {fault}/{realization}")
            # else:
            #     not_available_count += 1
            #     print(f"  Failed to retrieve e3d.par for {fault}/{realization}")
            #     print(f"  stderr: {result.stderr.strip()}")

# Write missing files to output file
output_file = BASE_DIR / "missing_e3d_par.txt"
with open(output_file, "w") as f:
    for path in missing_files:
        f.write(path + "\n")

print(f"\n--- Summary ---")
print(f"Total missing: {missing_count}")
print(f"Missing files written to: {output_file}")
