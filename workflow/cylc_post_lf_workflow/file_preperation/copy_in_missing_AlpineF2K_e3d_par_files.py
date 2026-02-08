#!/usr/bin/env python3

import shutil
from pathlib import Path
from datetime import datetime
import argparse

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Copy e3d.par files with audit and logging.")
parser.add_argument("--dry-run", action="store_true", help="Simulate actions without copying files.")
args = parser.parse_args()
dry_run = args.dry_run

# Base directories
SRC_BASE = Path("/scratch/projects/rch-quakecore/Cybershake/setup_files_from_dropbox/v25p11/permanent_small_files/extracted/AlpineF2K_e3d")
DST_BASE = Path("/scratch/projects/rch-quakecore/Cybershake/setup_files_from_dropbox/v25p11/permanent_small_files/extracted/v25p11_configs_params/AlpineF2K")

# Log file
log_file = Path(f"copy_e3d_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

# Counters and lists
copied_count = 0
skipped_count = 0
missing_source_only = []
missing_target_only = []
both_missing = []
extra_target_folders = []

# Helper function to log messages to terminal and file
def log(msg):
    print(msg)
    with open(log_file, "a") as f:
        f.write(msg + "\n")

# Get list of source folders
source_folders = [f.name for f in SRC_BASE.iterdir() if f.is_dir()]

# Loop over source folders
for folder_name in source_folders:
    src_file = SRC_BASE / folder_name / "LF" / "e3d.par"
    dst_folder = DST_BASE / folder_name / "LF"
    dst_file = dst_folder / "e3d.par"

    src_missing = not src_file.is_file()
    dst_missing = not dst_folder.is_dir()

    if src_missing and dst_missing:
        both_missing.append(folder_name)
        log(f"Warning: Both source file and destination folder missing for {folder_name}, skipping.")
        continue
    elif src_missing:
        missing_source_only.append(folder_name)
        log(f"Warning: Source file missing for {folder_name}, skipping.")
        continue
    elif dst_missing:
        missing_target_only.append(folder_name)
        log(f"Warning: Destination folder missing for {folder_name}, skipping.")
        continue

    # Skip copy if target exists and is newer
    if dst_file.exists() and dst_file.stat().st_mtime >= src_file.stat().st_mtime:
        log(f"Skipping copy for {folder_name}, target file is newer or same age.")
        skipped_count += 1
        continue

    # Copy file (or simulate if dry-run)
    if dry_run:
        log(f"[Dry run] Would copy {src_file} -> {dst_file}")
    else:
        shutil.copy2(src_file, dst_file)
        log(f"Copied {src_file} -> {dst_file}")
    copied_count += 0 if dry_run else 1

# Check for extra target folders
for tgt_folder in DST_BASE.iterdir():
    if tgt_folder.is_dir() and tgt_folder.name not in source_folders:
        extra_target_folders.append(tgt_folder.name)

# Sort all lists alphabetically
missing_source_only.sort()
missing_target_only.sort()
both_missing.sort()
extra_target_folders.sort()

# Print and log summary
log("======================")
log("Summary:")
log(f"Files successfully copied: {copied_count}")
log(f"Files skipped (target newer or same): {skipped_count}")

if missing_source_only:
    log(f"Folders with missing source file (destination exists): {', '.join(missing_source_only)}")
else:
    log("No folders with missing source file.")

if missing_target_only:
    log(f"Folders with missing destination folder (source exists): {', '.join(missing_target_only)}")
else:
    log("No folders with missing destination folder.")

if both_missing:
    log(f"Folders missing both source file and destination folder: {', '.join(both_missing)}")
else:
    log("No folders missing both source and destination.")

if extra_target_folders:
    log(f"Extra folders in the target with no corresponding source: {', '.join(extra_target_folders)}")
else:
    log("No extra folders in the target.")

log("======================")
log(f"Summary also written to: {log_file}")
log(f"Dry-run mode: {'ON, no files copied' if dry_run else 'OFF'}")
