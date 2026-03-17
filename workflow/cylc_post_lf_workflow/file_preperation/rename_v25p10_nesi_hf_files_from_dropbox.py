#!/usr/bin/env python3
"""
Download NeSI HF tar files from Dropbox, restructure and rename the contents
to match the expected layout, re-tar, and upload to the target Dropbox location.

Faults and tar files are discovered dynamically by listing
dropbox:/QuakeCoRE/gmsim_scratch/v25p10/HF_nesi/ with rclone.

Each fault folder contains tar files named like:
    Hope1888_HF.tar          (median realization)
    Hope1888_REL04_HF.tar
    Hope1888_REL20_HF.tar

NeSI source layout inside each tar (v25p10 median realization):
    Runs/{realization}/{realization}/HF/Acc/HF.bin
    Runs/{realization}/{realization}/HF/Acc/HF.log

NeSI source layout inside each tar (v25p10 REL realization):
    Runs/{fault}/{realization}/HF/Acc/HF.bin
    Runs/{fault}/{realization}/HF/Acc/HF.log

NeSI source layout inside each tar (AlpineF2K anomaly):
    {realization}/HF/Acc/HF.bin
    {realization}/HF/Acc/HF.log

Target layout inside the output tar:
    {realization}_HF/{realization}_HF.bin
    {realization}_HF/{realization}_HF.log
"""

import os
import shutil
import subprocess
import tarfile

source_base = "dropbox:/QuakeCoRE/gmsim_scratch/v25p10/HF_nesi"
working_dir = "/home/arr65/data/Cybershake_mock_dir_structure/Cybershake/HF_files/working_dir"
upload_base = "dropbox:/QuakeCoRE/gmsim_scratch/v25p10/HF"

os.makedirs(working_dir, exist_ok=True)


def rclone_copy(src, dst):
    """Copy a single file or directory using rclone."""
    cmd = ["rclone", "copy", src, dst, "--progress"]
    print(f"    Running: {' '.join(cmd)}")
    result = subprocess.run(cmd)
    if result.returncode != 0:
        raise RuntimeError(f"rclone copy failed with exit code {result.returncode}")
    print(f"    rclone copy complete.")


def rclone_list_dirs(path):
    """List subdirectory names at a remote rclone path."""
    cmd = ["rclone", "lsf", "--dirs-only", path]
    print(f"    Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"rclone lsf --dirs-only failed: {result.stderr}")
    # lsf appends '/' to directory names
    return [line.rstrip("/") for line in result.stdout.splitlines() if line.strip()]


def rclone_file_exists(remote_dir, filename):
    """Return True if filename already exists at remote_dir on Dropbox."""
    cmd = ["rclone", "lsf", "--files-only", remote_dir]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        return False
    return filename in result.stdout.splitlines()


def rclone_list_files(path):
    """List filenames at a remote rclone path (non-recursive)."""
    cmd = ["rclone", "lsf", "--files-only", path]
    print(f"    Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"rclone lsf --files-only failed: {result.stderr}")
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def extract_tar(tar_path, dest_dir):
    """Extract a tar archive to dest_dir."""
    print(f"    Source tar : {tar_path}")
    print(f"    Extract to : {dest_dir}")
    with tarfile.open(tar_path, "r:*") as tar:
        tar.extractall(dest_dir)
    print(f"    Extraction complete.")


def create_restructured_tar(realization, fault, extract_dir, output_tar_path):
    """
    Find HF.bin and HF.log inside the NeSI nested structure and create a new
    tar with the target layout: {realization}_HF/{realization}_HF.{ext}
    """
    # Try all known tar layout variants
    candidate_dirs = [
        os.path.join(extract_dir, "Runs", fault, realization, "HF", "Acc"),        # v25p10 REL layout
        os.path.join(extract_dir, "Runs", realization, realization, "HF", "Acc"),  # v25p10 median layout
        os.path.join(extract_dir, realization, "HF", "Acc"),                        # AlpineF2K layout
    ]
    acc_dir = next((d for d in candidate_dirs if os.path.isdir(d)), None)
    if acc_dir is None:
        print(f"    ERROR: Acc directory not found. Tried:")
        for d in candidate_dirs:
            print(f"      {d}")
        print(f"    Listing contents of extract_dir for diagnosis:")
        for root, dirs, files in os.walk(extract_dir):
            level = root.replace(extract_dir, "").count(os.sep)
            indent = "    " + "  " * level
            print(f"{indent}{os.path.basename(root)}/")
            sub_indent = "    " + "  " * (level + 1)
            for f in files:
                print(f"{sub_indent}{f}")
        raise FileNotFoundError(
            f"Expected Acc directory not found. Tried:\n" +
            "\n".join(f"  {d}" for d in candidate_dirs)
        )
    print(f"    Looking for HF.bin and HF.log in: {acc_dir}")

    top_dir_name = f"{realization}_HF"
    print(f"    Output tar path : {output_tar_path}")
    print(f"    Top-level dir inside tar : {top_dir_name}/")
    with tarfile.open(output_tar_path, "w") as tar:
        for ext in ("bin", "log"):
            src_file = os.path.join(acc_dir, f"HF.{ext}")
            if not os.path.isfile(src_file):
                print(f"    WARNING: {src_file} not found, skipping")
                continue
            arcname = os.path.join(top_dir_name, f"{realization}_HF.{ext}")
            tar.add(src_file, arcname=arcname)
            print(f"    Packed: {src_file}")
            print(f"         -> {arcname}")
    print(f"    Tar creation complete.")


# Discover faults dynamically
print("Discovering faults from Dropbox...")
fault_list = rclone_list_dirs(source_base)
print(f"Found faults: {fault_list}\n")

FAULTS_TO_SKIP = {"Hope1888"}

for fault in fault_list:
    if fault in FAULTS_TO_SKIP:
        print(f"\nSkipping fault: {fault}")
        continue

    source_dir = f"{source_base}/{fault}"
    upload_dir = f"{upload_base}/{fault}/"

    print(f"\n{'='*60}")
    print(f"  Fault: {fault}")
    print(f"{'='*60}")

    # Discover tar files for this fault
    all_files = rclone_list_files(source_dir)
    tar_files = [f for f in all_files if f.endswith("_HF.tar")]
    if not tar_files:
        print(f"  No *_HF.tar files found for {fault}, skipping.")
        continue
    print(f"  Found tar files: {tar_files}")

    for tar_file_name in tar_files:
        # Derive realization name by stripping _HF.tar suffix
        realization = tar_file_name[: -len("_HF.tar")]

        source_file = f"{source_dir}/{tar_file_name}"
        downloaded_tar = os.path.join(working_dir, tar_file_name)
        extract_dir = os.path.join(working_dir, f"{realization}_extracted")
        restructured_tar = os.path.join(working_dir, tar_file_name)

        print(f"\n{'='*60}")
        print(f"  Processing: {realization}")
        print(f"{'='*60}")

        # Skip if already uploaded
        if rclone_file_exists(upload_dir, tar_file_name):
            print(f"  Already exists at {upload_dir}{tar_file_name}, skipping.")
            continue

        try:
            # Step 1: Download tar from Dropbox
            print(f"\n[Step 1/5] Downloading NeSI tar from Dropbox")
            print(f"    Source      : {source_file}")
            print(f"    Destination : {working_dir}")
            rclone_copy(source_file, working_dir)

            # Step 2: Extract into a temporary directory
            print(f"\n[Step 2/5] Extracting downloaded tar")
            os.makedirs(extract_dir, exist_ok=True)
            extract_tar(downloaded_tar, extract_dir)

            # Step 3: Remove the downloaded tar to free space before creating the restructured one
            print(f"\n[Step 3/5] Deleting downloaded tar to free disk space")
            print(f"    Deleting: {downloaded_tar}")
            os.remove(downloaded_tar)
            print(f"    Deleted.")

            # Step 4: Create new tar with restructured layout
            print(f"\n[Step 4/5] Creating restructured tar with renamed files")
            create_restructured_tar(realization, fault, extract_dir, restructured_tar)

            # Step 5: Upload the restructured tar to Dropbox
            print(f"\n[Step 5/5] Uploading restructured tar to Dropbox")
            print(f"    Source      : {restructured_tar}")
            print(f"    Destination : {upload_dir}")
            rclone_copy(restructured_tar, upload_dir)

            print(f"\n  SUCCESS: {realization} processed and uploaded.")

        finally:
            # Clean up local working files regardless of success or failure
            print(f"\n[Cleanup] Cleaning up local files")
            if os.path.exists(extract_dir):
                shutil.rmtree(extract_dir)
                print(f"    Removed extract dir : {extract_dir}")
            if os.path.exists(restructured_tar):
                os.remove(restructured_tar)
                print(f"    Removed local tar   : {restructured_tar}")

print(f"\n{'='*60}")
print("  All realizations processed successfully.")
print(f"{'='*60}")