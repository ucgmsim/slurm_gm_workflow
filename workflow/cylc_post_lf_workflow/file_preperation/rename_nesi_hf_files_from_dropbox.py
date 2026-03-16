#!/usr/bin/env python3
"""
Download NeSI HF tar files from Dropbox, restructure and rename the contents
to match the expected layout, re-tar, and upload to the target Dropbox location.

NeSI source layout inside tar:
    ./{realization}/HF/Acc/HF.bin
    ./{realization}/HF/Acc/HF.log

Target layout inside the output tar:
    {realization}_HF/{realization}_HF.bin
    {realization}_HF/{realization}_HF.log
"""

import os
import shutil
import subprocess
import tarfile

numerical_part_of_realization_list = ["01",
                                    "03",
                                    "04",
                                    "07",
                                    "08",
                                    "16",
                                    "18",
                                    "19",
                                    "23",
                                    "25",
                                    "28",
                                    "29",
                                    "30",
                                    "45",
                                    "46",
                                    "47"]


# numerical_part_of_realization_list = ["01"]

realization_list = [f"AlpineF2K_REL{num}" for num in numerical_part_of_realization_list]

source_dir = "dropbox:/QuakeCoRE/gmsim_scratch/v25p11/HF_nesi"
working_dir = "/home/arr65/data/Cybershake_mock_dir_structure/Cybershake/HF_files/working_dir"
upload_dir = "dropbox:/QuakeCoRE/gmsim_scratch/v25p11/HF/AlpineF2K/"

os.makedirs(working_dir, exist_ok=True)

def rclone_copy(src, dst):
    """Copy a single file or directory using rclone."""
    cmd = ["rclone", "copy", src, dst, "--progress"]
    print(f"    Running: {' '.join(cmd)}")
    result = subprocess.run(cmd)
    if result.returncode != 0:
        raise RuntimeError(f"rclone copy failed with exit code {result.returncode}")
    print(f"    rclone copy complete.")


def extract_tar(tar_path, dest_dir):
    """Extract a tar archive to dest_dir."""
    print(f"    Source tar : {tar_path}")
    print(f"    Extract to : {dest_dir}")
    with tarfile.open(tar_path, "r:*") as tar:
        tar.extractall(dest_dir)
    print(f"    Extraction complete.")


def create_restructured_tar(realization, extract_dir, output_tar_path):
    """
    Find HF.bin and HF.log inside the NeSI nested structure and create a new
    tar with the target layout: {realization}_HF/{realization}_HF.{ext}
    """
    acc_dir = os.path.join(extract_dir, realization, "HF", "Acc")
    print(f"    Looking for HF.bin and HF.log in: {acc_dir}")
    if not os.path.isdir(acc_dir):
        raise FileNotFoundError(
            f"Expected Acc directory not found: {acc_dir}"
        )

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


for realization in realization_list:
    tar_file_name = f"{realization}_HF.tar"
    source_file = f"{source_dir}/{tar_file_name}"
    downloaded_tar = os.path.join(working_dir, tar_file_name)
    extract_dir = os.path.join(working_dir, f"{realization}_extracted")
    restructured_tar = os.path.join(working_dir, tar_file_name)

    print(f"\n{'='*60}")
    print(f"  Processing: {realization}")
    print(f"{'='*60}")

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
        create_restructured_tar(realization, extract_dir, restructured_tar)

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