#!/usr/bin/env python

import argparse
import os
import shutil
import subprocess
import sys
import tarfile
from qcore import utils

# Base path for Cybershake data
CYBERSHAKE_BASE = "/scratch/projects/rch-quakecore/Cybershake"
STAGED_BASE = f"{CYBERSHAKE_BASE}/staged_for_upload"

# Placeholder for Dropbox upload location
DROPBOX_REMOTE = "dropbox:Cybershake/uploads"


def create_tarball(source_dir, tarball_path, arcname_prefix):
    """
    Create a tarball of source_dir with arcname_prefix as the root directory in the archive.
    
    Args:
        source_dir: The directory to tar
        tarball_path: Where to save the tarball
        arcname_prefix: The path prefix to use inside the tarball
    """
    print(f"Creating tarball: {tarball_path}")
    with tarfile.open(tarball_path, "w:gz") as tar:
        tar.add(source_dir, arcname=arcname_prefix)
    print(f"Tarball created successfully: {tarball_path}")


def upload_to_dropbox(tarball_path, remote_path):
    """
    Upload a tarball to Dropbox using rclone.
    
    Args:
        tarball_path: Path to the tarball to upload
        remote_path: The rclone remote destination
    """
    cmd = ["rclone", "copy", tarball_path, remote_path, "--progress"]
    print(f"Uploading {tarball_path} to {remote_path}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error uploading to Dropbox: {result.stderr}")
        raise RuntimeError(f"rclone upload failed: {result.stderr}")
    print(f"Upload completed: {tarball_path}")


def main():
    parser = argparse.ArgumentParser(description="Stage and upload post-HF results to Dropbox")
    parser.add_argument("rel_dir", help="Path to REL_DIR containing sim_params.yaml")
    args = parser.parse_args()

    version = "v25p11"

    # Construct path to sim_params.yaml
    sim_params_path = os.path.join(args.rel_dir, "sim_params.yaml")

    print(f"Loading parameters from: {sim_params_path}")
    # params = utils.load_sim_params(sim_yaml_path=sim_params_path)
    print("Parameters loaded successfully")

    rel_name = os.path.basename(args.rel_dir)  # e.g., "MS09_REL01"
    fault_dir = os.path.dirname(args.rel_dir)
    fault_name = os.path.basename(fault_dir)   # e.g., "MS09"

    # Source directories
    hf_src_dir = f"{CYBERSHAKE_BASE}/{version}/Runs/{fault_name}/{rel_name}/HF/Acc"
    bb_src_dir = f"{CYBERSHAKE_BASE}/{version}/Runs/{fault_name}/{rel_name}/BB/Acc"
    im_src_dir = f"{CYBERSHAKE_BASE}/{version}/Runs/{fault_name}/{rel_name}/IM_calc"

    # Staged destination directories
    hf_staged_dir = f"{STAGED_BASE}/{version}/Runs/{fault_name}/{rel_name}/HF/Acc"
    bb_staged_dir = f"{STAGED_BASE}/{version}/Runs/{fault_name}/{rel_name}/BB/Acc"
    im_staged_dir = f"{STAGED_BASE}/Runs/{fault_name}/{rel_name}/IM_calc"

    # Archive name prefixes (path structure inside tarballs)
    hf_arcname = f"{version}/Runs/{fault_name}/{rel_name}/HF/Acc"
    bb_arcname = f"{version}/Runs/{fault_name}/{rel_name}/BB/Acc"
    im_arcname = f"Runs/{fault_name}/{rel_name}/IM_calc"

    # Tarball paths (stored alongside the staged directories)
    hf_tarball = f"{STAGED_BASE}/{version}/Runs/{fault_name}/{rel_name}_HF_Acc.tar.gz"
    bb_tarball = f"{STAGED_BASE}/{version}/Runs/{fault_name}/{rel_name}_BB_Acc.tar.gz"
    im_tarball = f"{STAGED_BASE}/Runs/{fault_name}/{rel_name}_IM_calc.tar.gz"

    prefix = f"{rel_name}_"

    # Rename HF files in source directory
    print("\n=== Renaming HF files ===")
    for fname in ("HF.bin", "HF.log", "SEED"):
        src = os.path.join(hf_src_dir, fname)
        dst = os.path.join(hf_src_dir, f"{prefix}{fname}")
        if os.path.exists(src):
            os.rename(src, dst)
            print(f"Renamed: {fname} -> {prefix}{fname}")

    # Rename BB files in source directory
    print("\n=== Renaming BB files ===")
    for fname in ("BB.bin", "BB.log"):
        src = os.path.join(bb_src_dir, fname)
        dst = os.path.join(bb_src_dir, f"{prefix}{fname}")
        if os.path.exists(src):
            os.rename(src, dst)
            print(f"Renamed: {fname} -> {prefix}{fname}")

    # Step 1: Copy directories to staged_for_upload
    print("\n=== Copying directories to staged_for_upload ===")
    
    dirs_to_process = [
        (hf_src_dir, hf_staged_dir, hf_arcname, hf_tarball, "HF/Acc"),
        (bb_src_dir, bb_staged_dir, bb_arcname, bb_tarball, "BB/Acc"),
        (im_src_dir, im_staged_dir, im_arcname, im_tarball, "IM_calc"),
    ]

    for src_dir, staged_dir, arcname, tarball_path, desc in dirs_to_process:
        if not os.path.exists(src_dir):
            print(f"Warning: Source directory does not exist: {src_dir}")
            continue

        # Create parent directories if needed
        os.makedirs(os.path.dirname(staged_dir), exist_ok=True)

        # Copy directory
        print(f"\nCopying {desc}: {src_dir} -> {staged_dir}")
        shutil.copytree(src_dir, staged_dir)
        print(f"Copy completed: {desc}")

        # Step 2: Create tarball
        print(f"\nCreating tarball for {desc}")
        create_tarball(staged_dir, tarball_path, arcname)

        # Step 3: Delete the copied directory (not the original)
        print(f"\nRemoving staged directory: {staged_dir}")
        shutil.rmtree(staged_dir)
        print(f"Removed staged directory: {staged_dir}")

        # Step 4: Upload to Dropbox
        print(f"\nUploading {desc} tarball to Dropbox")
        upload_to_dropbox(tarball_path, DROPBOX_REMOTE)

    print("\n=== All uploads completed successfully ===")


if __name__ == "__main__":
    main()
