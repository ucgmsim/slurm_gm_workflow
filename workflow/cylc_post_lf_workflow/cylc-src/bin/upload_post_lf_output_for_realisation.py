#!/usr/bin/env python

import argparse
import os
import subprocess
import tarfile

# Base path for Cybershake data
CYBERSHAKE_BASE = "/scratch/projects/rch-quakecore/Cybershake"
STAGED_BASE = f"{CYBERSHAKE_BASE}/staged_for_upload"
DROPBOX_BASE = "dropbox:/QuakeCoRE/gmsim_scratch"

def create_flat_tarball(source_dir, tarball_path):
    """
    Create a tarball of files in source_dir with no directory structure.
    When extracted, files will be placed directly without any path prefix.
    
    Args:
        source_dir: The directory containing files to tar
        tarball_path: Where to save the tarball
    """
    print(f"Creating tarball: {tarball_path}")
    with tarfile.open(tarball_path, "w") as tar:
        for item in os.listdir(source_dir):
            item_path = os.path.join(source_dir, item)
            # Use just the filename as arcname (no directory structure)
            tar.add(item_path, arcname=item)
            print(f"  Added: {item}")
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


def rename_items_with_prefix(directory, prefix):
    """Rename all files and subdirectories in a directory by adding a prefix."""
    if not os.path.exists(directory):
        print(f"Warning: Directory does not exist: {directory}")
        return
    
    print(f"\n=== Renaming items in {directory} ===")
    for name in os.listdir(directory):
        src = os.path.join(directory, name)
        dst = os.path.join(directory, f"{prefix}{name}")
        os.rename(src, dst)
        item_type = "dir" if os.path.isdir(dst) else "file"
        print(f"Renamed {item_type}: {name} -> {prefix}{name}")


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

    prefix = f"{rel_name}_"

    # Types to process: (type_name, source_subdir, should_rename)
    types_to_process = [
        ("HF", "HF/Acc", True),
        ("BB", "BB/Acc", True),
        ("IM", "IM", False),
    ]

    # Process each type: rename, create tarball, and upload
    print("\n=== Processing directories ===")

    for type_name, src_subdir, should_rename in types_to_process:
        src_dir = f"{CYBERSHAKE_BASE}/{version}/Runs/{fault_name}/{rel_name}/{src_subdir}"
        tarball_path = f"{STAGED_BASE}/{version}/{type_name}/{fault_name}/{rel_name}_{type_name}.tar"
        remote_path = f"{DROPBOX_BASE}/{version}/{type_name}/{fault_name}"
        if not os.path.exists(src_dir):
            print(f"Warning: Source directory does not exist: {src_dir}")
            continue

        # Rename files and subdirectories if needed
        if should_rename:
            rename_items_with_prefix(src_dir, prefix)

        # Create parent directories for tarball if needed
        os.makedirs(os.path.dirname(tarball_path), exist_ok=True)

        # Create tarball directly from source directory (flat, no directory structure)
        print(f"\nCreating tarball from {src_dir}")
        create_flat_tarball(src_dir, tarball_path)

        # Upload to Dropbox
        print(f"\nUploading {tarball_path} to {remote_path}")
        upload_to_dropbox(tarball_path, remote_path)

        # Delete tarball after successful upload
        print(f"Deleting staged tarball: {tarball_path}")
        os.remove(tarball_path)

    print("\n=== All uploads completed successfully ===")


if __name__ == "__main__":
    main()
