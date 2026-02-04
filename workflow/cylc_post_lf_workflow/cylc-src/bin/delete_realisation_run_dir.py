#!/usr/bin/env python

import argparse
import os
import shutil


def main():
    parser = argparse.ArgumentParser(description="Cleanup realisation-dependent data after upload")
    parser.add_argument("rel_dir", help="Path to REL_DIR containing sim_params.yaml")
    args = parser.parse_args()

    rel_name = os.path.basename(args.rel_dir)  # e.g., "MS09_REL01"
    fault_dir = os.path.dirname(args.rel_dir)
    fault_name = os.path.basename(fault_dir)   # e.g., "MS09"

    # The realisation directory to delete
    # This is the same as args.rel_dir, but we construct it explicitly for clarity
    realisation_dir = f"/scratch/projects/rch-quakecore/Cybershake/v25p11/Runs/{fault_name}/{rel_name}"

    print(f"Cleaning up realisation-dependent data for {fault_name}/{rel_name}")
    print(f"Directory to delete: {realisation_dir}")

    if os.path.exists(realisation_dir):
        print(f"Deleting directory: {realisation_dir}")
        # TEMPORARY: Deletion disabled for testing - uncomment line below to restore
        shutil.rmtree(realisation_dir)
        # print(f"(SKIPPED - deletion temporarily disabled)")
        print(f"Successfully deleted: {realisation_dir}")
    else:
        print(f"Warning: Directory does not exist: {realisation_dir}")

    print("Cleanup completed")


if __name__ == "__main__":
    main()
